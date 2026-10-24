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

const std = @import("std");
const runtime_callback_abi = @import("../../runtime_callback_abi.zig");
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const Crc32 = @import("antfly_hash").Crc32;
const platform_sync = @import("antfly_platform").sync;
const builtin = @import("builtin");
const platform = @import("antfly_platform");
const byte_copy = @import("../../common/byte_copy.zig");
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const read_admission = @import("../../common/read_admission.zig");
const threaded_io_limits = @import("antfly_runtime_fs").threaded_io_limits;

const RuntimeMemory = @import("../runtime_memory.zig");
const MemoryParent = @import("antfly_runtime_abi").memory_abi.Parent;

const Allocator = std.mem.Allocator;
const CounterU64 = platform.atomic.Value(u64);
const supports_native_storage = builtin.os.tag != .freestanding;
const supports_posix_fd_cache = supports_native_storage and
    builtin.os.tag != .windows and
    builtin.os.tag != .wasi and
    (builtin.os.tag == .linux or builtin.link_libc) and
    @hasDecl(std.posix.system, "pread");
// Keep the production storage runtime Threaded while Evented is qualified
// with workload and recovery tests. Native positional reads also use the fd
// cache directly, so switching std.Io alone would not convert every read path.
const supports_evented_runtime = false;
// Standalone NativeStorage values used by focused tools/tests have no
// BackendRuntime handle, so keep their local cache conservative. Production
// runtimes inject handles to the single RLIMIT-derived process pool.
const fallback_cached_native_fds: usize = 64;
const unlimited_fd_admission_capacity: usize = 1024;
const fd_cache_shard_count: usize = if (builtin.os.tag == .freestanding) 1 else 16;
const max_posix_io_chunk: usize = 64 * 1024 * 1024;
const cold_atomic_writeback_chunk: usize = 64 * 1024 * 1024;

pub const RuntimeKind = enum {
    threaded,
    evented,
};

pub const NativeStorageStats = struct {
    fd_cache_entries: usize = 0,
    fd_admitted_descriptors: usize = 0,
    fd_persistent_descriptors: usize = 0,
    fd_admission_capacity: usize = 0,
    fd_persistent_reserve: usize = 0,
    fd_admission_waiters: usize = 0,
    fd_admission_waits: u64 = 0,
    fd_persistent_admission_failures: u64 = 0,
};

pub fn nativeFdAdmissionCapacityForSoftLimit(soft_limit: u64) usize {
    if (soft_limit == std.math.maxInt(u64)) return unlimited_fd_admission_capacity;
    // Public inbound sockets independently receive at most one quarter of the
    // process table. Give native storage one half, leaving the final quarter
    // for listeners, Raft, outbound providers, logs, and operator recovery
    // traffic. A third was too conservative: at RLIMIT_NOFILE=256 the 85-slot
    // pool could not retain the lifetime locks required by a normal multi-shard
    // graph/full-text workload even though the process still had ample OS
    // descriptor headroom.
    const bounded = @max(@as(u64, 1), soft_limit / 2);
    return @intCast(@min(bounded, unlimited_fd_admission_capacity));
}

fn configuredNativeFdAdmissionCapacity() usize {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding or builtin.os.tag == .wasi)
        return fallback_cached_native_fds;
    const limit = std.posix.getrlimit(.NOFILE) catch return fallback_cached_native_fds;
    const raw: u64 = if (limit.cur == std.math.maxInt(@TypeOf(limit.cur)))
        std.math.maxInt(u64)
    else
        @intCast(limit.cur);
    return nativeFdAdmissionCapacityForSoftLimit(raw);
}

pub const AtomicWriteSink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        len: *const fn (*anyopaque) usize,
        append_slice: *const fn (*anyopaque, []const u8) anyerror!void,
        write_at: *const fn (*anyopaque, usize, []const u8) anyerror!void,
        crc32_prefix: *const fn (*anyopaque, usize) anyerror!u32,
        crc32_range: *const fn (*anyopaque, usize, usize) anyerror!u32,
        set_cache_intent: ?*const fn (*anyopaque, AtomicWriteCacheIntent) void = null,
        finish: *const fn (*anyopaque) anyerror!void,
        /// Consumes the writer on success AND failure, like finish. The
        /// returned read-only mapping owns its inode independently of storage.
        finish_source: ?*const fn (*anyopaque) anyerror!@import("../../segment_source.zig").Source = null,
        finish_mapped: ?*const fn (*anyopaque) anyerror!@import("../../segment_source.zig").MappedArtifact = null,
        abort: *const fn (*anyopaque) void,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn len(self: *const AtomicWriteSink) usize {
        return self.vtable.len(self.ptr);
    }

    pub fn appendSlice(self: *AtomicWriteSink, bytes: []const u8) !void {
        try BoundaryAbi.call("append_slice", self.boundary_dispatch, self.vtable.append_slice, .{ self.ptr, bytes });
    }

    pub fn appendByte(self: *AtomicWriteSink, byte: u8) !void {
        const one = [1]u8{byte};
        try self.appendSlice(&one);
    }

    pub fn writeAt(self: *AtomicWriteSink, offset: usize, bytes: []const u8) !void {
        try BoundaryAbi.call("write_at", self.boundary_dispatch, self.vtable.write_at, .{ self.ptr, offset, bytes });
    }

    pub fn crc32Prefix(self: *AtomicWriteSink, len_prefix: usize) !u32 {
        return try BoundaryAbi.call("crc32_prefix", self.boundary_dispatch, self.vtable.crc32_prefix, .{ self.ptr, len_prefix });
    }

    pub fn crc32Range(self: *AtomicWriteSink, offset: usize, range_len: usize) !u32 {
        return try BoundaryAbi.call("crc32_range", self.boundary_dispatch, self.vtable.crc32_range, .{ self.ptr, offset, range_len });
    }

    /// Marks a maintenance output as a one-pass sequential stream. Native
    /// storage may keep these pages out of the process page-cache footprint;
    /// compatibility backends safely ignore the advisory hint.
    pub fn setCacheIntent(self: *AtomicWriteSink, intent: AtomicWriteCacheIntent) void {
        if (self.vtable.set_cache_intent) |set_cache_intent| set_cache_intent(self.ptr, intent);
    }

    /// Atomically publish the written bytes at the requested destination.
    /// Consumes the sink whether publishing succeeds or fails.
    /// Unsupported capability does not consume the writer. A supported
    /// implementation consumes it even when publication or mapping fails.
    pub fn finishSource(self: *AtomicWriteSink) !@import("../../segment_source.zig").Source {
        const finish_source = self.vtable.finish_source orelse return error.ImmutableReadSourceUnsupported;
        return BoundaryAbi.call("finish_source", self.boundary_dispatch, finish_source, .{self.ptr});
    }

    pub fn finishMapped(self: *AtomicWriteSink) !@import("../../segment_source.zig").MappedArtifact {
        const finish_mapped = self.vtable.finish_mapped orelse return error.MappedArtifactUnsupported;
        return BoundaryAbi.call("finish_mapped", self.boundary_dispatch, finish_mapped, .{self.ptr});
    }

    pub fn finish(self: *AtomicWriteSink) !void {
        try BoundaryAbi.call("finish", self.boundary_dispatch, self.vtable.finish, .{self.ptr});
    }

    /// Discard all bytes and remove any temporary object.
    /// Consumes the sink.
    pub fn abort(self: *AtomicWriteSink) void {
        self.vtable.abort(self.ptr);
    }
};

pub const AtomicWriteCacheIntent = enum {
    normal,
    cold_sequential,
};

/// Keep the public storage hint independent of Zig's OS-specific constant
/// containers. POSIX_FADV is a namespace of integer constants rather than an
/// enum on Linux, and fadvise accepts the platform ABI's integer value.
fn linuxWriteCacheAdvice(intent: AtomicWriteCacheIntent) usize {
    return switch (intent) {
        .normal => std.os.linux.POSIX_FADV.NORMAL,
        .cold_sequential => std.os.linux.POSIX_FADV.SEQUENTIAL,
    };
}

test "Linux write cache advice preserves the fadvise ABI" {
    try std.testing.expectEqual(
        @as(usize, std.os.linux.POSIX_FADV.NORMAL),
        linuxWriteCacheAdvice(.normal),
    );
    try std.testing.expectEqual(
        @as(usize, std.os.linux.POSIX_FADV.SEQUENTIAL),
        linuxWriteCacheAdvice(.cold_sequential),
    );
}

/// A file lease for one-pass maintenance reads. Native implementations use a
/// descriptor distinct from the shared foreground FD cache, so cache policy
/// cannot leak across concurrent query reads.
pub const ColdReadRange = struct {
    offset: u64,
    destination: []u8,
};

pub const ColdReadStats = struct {
    logical_bytes: u64 = 0,
    physical_bytes: u64 = 0,
    physical_reads: u64 = 0,

    pub fn add(self: *ColdReadStats, other: ColdReadStats) void {
        self.logical_bytes +|= other.logical_bytes;
        self.physical_bytes +|= other.physical_bytes;
        self.physical_reads +|= other.physical_reads;
    }
};

pub const ColdSequentialReader = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        read_range_alloc: *const fn (*anyopaque, Allocator, u64, usize) anyerror![]u8,
        read_range_into: *const fn (*anyopaque, u64, []u8) anyerror!void,
        read_ranges_into: *const fn (*anyopaque, []const ColdReadRange) anyerror!ColdReadStats,
        release_scratch: *const fn (*anyopaque) void,
        deinit: *const fn (*anyopaque) void,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn readRangeAlloc(self: *ColdSequentialReader, allocator: Allocator, offset: u64, len: usize) ![]u8 {
        return try BoundaryAbi.call("read_range_alloc", self.boundary_dispatch, self.vtable.read_range_alloc, .{ self.ptr, allocator, offset, len });
    }

    pub fn readRangeInto(self: *ColdSequentialReader, offset: u64, out: []u8) !void {
        return try BoundaryAbi.call("read_range_into", self.boundary_dispatch, self.vtable.read_range_into, .{ self.ptr, offset, out });
    }

    /// Reads ranges ordered by ascending file offset. Implementations may
    /// coalesce neighboring requests, but every destination remains independently
    /// owned by the caller for the duration of this synchronous operation.
    pub fn readRangesInto(self: *ColdSequentialReader, ranges: []const ColdReadRange) !ColdReadStats {
        return try BoundaryAbi.call("read_ranges_into", self.boundary_dispatch, self.vtable.read_ranges_into, .{ self.ptr, ranges });
    }

    /// Releases implementation-private bounce buffers after a large batch.
    /// The descriptor and its cold-cache policy remain valid for later reads.
    pub fn releaseScratch(self: *ColdSequentialReader) void {
        self.vtable.release_scratch(self.ptr);
    }

    pub fn deinit(self: *ColdSequentialReader) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }
};

pub const RangeReadFuture = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        wait: *const fn (*anyopaque) anyerror![]u8,
        cancel: *const fn (*anyopaque) void,
        wait_with_admission: ?*const fn (*anyopaque, read_admission.Port) anyerror![]u8 = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    /// Complete this one-shot read and transfer the returned bytes to the
    /// caller. After wait or cancel, the future must not be used again.
    pub fn wait(self: *RangeReadFuture) ![]u8 {
        return try BoundaryAbi.call("wait", self.boundary_dispatch, self.vtable.wait, .{self.ptr});
    }

    /// Consume the one-shot future under the original synchronous authority.
    /// The allocator must be the one used to begin the read. Compatibility
    /// providers keep CPU charged; native providers loan it only for the join.
    pub fn waitWithAdmission(self: *RangeReadFuture, allocator: Allocator, control: read_admission.Port) ![]u8 {
        if (self.vtable.wait_with_admission) |wait_controlled| return wait_controlled(self.ptr, control);
        control.checkpoint() catch |err| {
            self.cancel();
            return err;
        };
        const bytes = try self.wait();
        errdefer allocator.free(bytes);
        try control.checkpoint();
        return bytes;
    }

    /// Best-effort cancellation. Synchronous/completed futures just release
    /// their owned bytes if the caller has not waited yet.
    pub fn cancel(self: *RangeReadFuture) void {
        self.vtable.cancel(self.ptr);
    }
};

/// Owns one accepted durability operation. A failed controlled wait preserves
/// the operation for retry; it never cancels or releases accepted physical I/O.
/// Consumers must serialize wait/deinit and retain this owner after rejection.
/// This retains executor/descriptor ownership, not a filesystem publication
/// generation. The caller must keep its WAL/file publication lease through
/// physical completion, including rejected waits.
pub const SyncOperation = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        wait_with_admission: *const fn (*anyopaque, read_admission.Port) anyerror!void,
        deinit: *const fn (*anyopaque) void,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn waitWithAdmission(self: *SyncOperation, original: read_admission.Port) !void {
        try BoundaryAbi.call("wait_with_admission", self.boundary_dispatch, self.vtable.wait_with_admission, .{ self.ptr, original });
    }

    /// Mandatory physical join under quiesced cleanup ownership. This may
    /// block and must not be used as automatic cleanup of a rejected wait.
    pub fn deinit(self: *SyncOperation) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }
};

/// Bounded control-file bytes remain borrowed from this owner until deinit.
/// File-size validation and decoding belong on admitted caller CPU.
pub const ControlFileResult = struct { file_size: u64, bytes: []const u8 };
pub const max_control_file_bytes: usize = 4096;
pub const max_atomic_write_bytes: usize = 16 * 1024;
pub const ControlFileReadOperation = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    pub const VTable = struct {
        wait_with_admission: *const fn (*anyopaque, read_admission.Port) anyerror!ControlFileResult,
        deinit: *const fn (*anyopaque) void,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);
    pub fn waitWithAdmission(self: *ControlFileReadOperation, original: read_admission.Port) !ControlFileResult {
        return BoundaryAbi.call("wait_with_admission", self.boundary_dispatch, self.vtable.wait_with_admission, .{ self.ptr, original });
    }
    /// Quiesced mandatory join; rejected waits retain this owner for retry.
    pub fn deinit(self: *ControlFileReadOperation) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }
};

/// Minimum physical completion resources prepared before publication ownership.
/// One descriptor, bounded request/result storage and one admitted worker are
/// reusable across a serialized sequence of reads and durability barriers.
/// The caller must retire each operation before starting the next, keep this
/// grant alive until every operation is retired, and serialize all methods.
/// Acquiring publication must be a try: drop an idle tentative grant before
/// waiting for publication, since a legacy publication holder may need its FD.
pub const DurabilityResources = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    pub const VTable = struct {
        begin_read: *const fn (*anyopaque, []const u8, usize, read_admission.Port) anyerror!ControlFileReadOperation,
        begin_sync: *const fn (*anyopaque, []const u8, bool, read_admission.Port) anyerror!SyncOperation,
        deinit: *const fn (*anyopaque) void,
        retire_with_admission: ?*const fn (*anyopaque, read_admission.Port) anyerror!void = null,
        begin_append: ?*const fn (*anyopaque, []const u8, bool, read_admission.Port) anyerror!SyncOperation = null,
        begin_control_replace: ?*const fn (*anyopaque, []const u8, []const u8, read_admission.Port) anyerror!SyncOperation = null,
        begin_create_directory: ?*const fn (*anyopaque, []const u8, read_admission.Port) anyerror!SyncOperation = null,
        begin_private_create: ?*const fn (*anyopaque, []const u8, []const u8, read_admission.Port) anyerror!SyncOperation = null,
        begin_private_write: ?*const fn (*anyopaque, u64, []const u8, read_admission.Port) anyerror!SyncOperation = null,
        begin_private_publish: ?*const fn (*anyopaque, read_admission.Port) anyerror!SyncOperation = null,
        begin_private_discard: ?*const fn (*anyopaque, read_admission.Port) anyerror!SyncOperation = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);
    /// Appends the frozen bytes prepared with this grant. The caller holds
    /// publication until physical completion and must not reissue on wait
    /// rejection. No sync or directory creation is implied by this phase.
    /// A physical failure can leave a partial tail; recovery belongs to the
    /// publication owner, rather than replaying an ambiguous append.
    pub fn beginPreparedAppend(self: *DurabilityResources, path: []const u8, create_if_missing: bool, original: read_admission.Port) !SyncOperation {
        try original.checkpoint();
        const begin = self.vtable.begin_append orelse return error.ExecutionWaitUnsupported;
        return BoundaryAbi.call("begin_append", self.boundary_dispatch, begin, .{ self.ptr, path, create_if_missing, original });
    }
    /// Small metadata is copied into the grant's fixed scratch on caller CPU.
    /// The caller serializes this reserved sibling temporary path with its
    /// publication guard. Completion includes file sync, rename and parent sync.
    pub fn beginControlReplace(self: *DurabilityResources, path: []const u8, bytes: []const u8, original: read_admission.Port) !SyncOperation {
        try original.checkpoint();
        const begin = self.vtable.begin_control_replace orelse return error.ExecutionWaitUnsupported;
        return BoundaryAbi.call("begin_control_replace", self.boundary_dispatch, begin, .{ self.ptr, path, bytes, original });
    }
    /// Create a single child of an existing parent, with no recursive fallback.
    /// Namespace durability is a separately ordered beginSyncParent phase.
    pub fn beginCreateDirectory(self: *DurabilityResources, path: []const u8, original: read_admission.Port) !SyncOperation {
        try original.checkpoint();
        const begin = self.vtable.begin_create_directory orelse return error.ExecutionWaitUnsupported;
        return BoundaryAbi.call("begin_create_directory", self.boundary_dispatch, begin, .{ self.ptr, path, original });
    }
    pub fn beginControlRead(self: *DurabilityResources, path: []const u8, limit: usize, original: read_admission.Port) !ControlFileReadOperation {
        return BoundaryAbi.call("begin_read", self.boundary_dispatch, self.vtable.begin_read, .{ self.ptr, path, limit, original });
    }
    pub fn beginSyncContents(self: *DurabilityResources, path: []const u8, original: read_admission.Port) !SyncOperation {
        return BoundaryAbi.call("begin_sync", self.boundary_dispatch, self.vtable.begin_sync, .{ self.ptr, path, false, original });
    }
    pub fn beginSyncParent(self: *DurabilityResources, path: []const u8, original: read_admission.Port) !SyncOperation {
        return BoundaryAbi.call("begin_sync", self.boundary_dispatch, self.vtable.begin_sync, .{ self.ptr, path, true, original });
    }
    /// A private-file session exclusively owns this grant until publication or
    /// discard completes. Parent directories already exist. The caller owns
    /// the unique temporary name and any possibly published output obligation.
    pub fn beginPrivateCreate(self: *DurabilityResources, path: []const u8, temporary: []const u8, original: read_admission.Port) !SyncOperation {
        const begin = self.vtable.begin_private_create orelse return error.ExecutionWaitUnsupported;
        return BoundaryAbi.call("begin_private_create", self.boundary_dispatch, begin, .{ self.ptr, path, temporary, original });
    }
    /// Copy one bounded command before dispatch. It cannot reference caller
    /// bytes after returning, and a rejected wait must retain the operation.
    pub fn beginPrivateWrite(self: *DurabilityResources, offset: u64, bytes: []const u8, original: read_admission.Port) !SyncOperation {
        const begin = self.vtable.begin_private_write orelse return error.ExecutionWaitUnsupported;
        return BoundaryAbi.call("begin_private_write", self.boundary_dispatch, begin, .{ self.ptr, offset, bytes, original });
    }
    pub fn beginPrivatePublish(self: *DurabilityResources, original: read_admission.Port) !SyncOperation {
        const begin = self.vtable.begin_private_publish orelse return error.ExecutionWaitUnsupported;
        return BoundaryAbi.call("begin_private_publish", self.boundary_dispatch, begin, .{ self.ptr, original });
    }
    pub fn beginPrivateDiscard(self: *DurabilityResources, original: read_admission.Port) !SyncOperation {
        const begin = self.vtable.begin_private_discard orelse return error.ExecutionWaitUnsupported;
        return BoundaryAbi.call("begin_private_discard", self.boundary_dispatch, begin, .{ self.ptr, original });
    }

    pub fn retireWithAdmission(self: *DurabilityResources, original: read_admission.Port) !void {
        const retire = self.vtable.retire_with_admission orelse return error.ExecutionWaitUnsupported;
        try BoundaryAbi.call("retire_with_admission", self.boundary_dispatch, retire, .{ self.ptr, original });
        self.* = undefined;
    }

    /// Mandatory quiesced join, including any accepted phase. Never cancel a
    /// durability operation or release its publication lease before this join.
    pub fn deinit(self: *DurabilityResources) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }
};

pub const ReadRuntime = if (builtin.os.tag == .freestanding)
    struct {
        pub fn getIo(_: *const ReadRuntime) ?std.Io {
            return null;
        }

        pub fn init(_: anytype) ReadRuntime {
            return .{};
        }
    }
else
    struct {
        io: std.Io,

        pub fn getIo(self: *const ReadRuntime) ?std.Io {
            return self.io;
        }

        pub fn init(io: std.Io) ReadRuntime {
            return .{ .io = io };
        }
    };

const CompletedRangeReadFuture = struct {
    allocator: Allocator,
    bytes: ?[]u8 = null,
    err: ?anyerror = null,

    const vtable: RangeReadFuture.VTable = .{
        .wait = wait,
        .cancel = cancel,
    };

    fn create(storage: Storage, allocator: Allocator, path: []const u8, offset: u64, len: usize) !RangeReadFuture {
        return createControlled(storage, allocator, path, offset, len, null);
    }

    fn createControlled(storage: Storage, allocator: Allocator, path: []const u8, offset: u64, len: usize, control: ?read_admission.Port) !RangeReadFuture {
        const self = try allocator.create(CompletedRangeReadFuture);
        self.* = .{ .allocator = allocator };
        self.bytes = (if (control) |budget| storage.readFileRangeAllocWithAdmission(allocator, path, offset, len, budget) else storage.readFileRangeAlloc(allocator, path, offset, len)) catch |err| blk: {
            self.err = err;
            break :blk null;
        };
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn wait(ptr: *anyopaque) ![]u8 {
        const self: *CompletedRangeReadFuture = @ptrCast(@alignCast(ptr));
        const allocator = self.allocator;
        defer allocator.destroy(self);
        if (self.err) |err| return err;
        const bytes = self.bytes orelse return error.CanceledRangeRead;
        self.bytes = null;
        return bytes;
    }

    fn cancel(ptr: *anyopaque) void {
        const self: *CompletedRangeReadFuture = @ptrCast(@alignCast(ptr));
        const allocator = self.allocator;
        if (self.bytes) |bytes| allocator.free(bytes);
        allocator.destroy(self);
    }
};

pub const NativePathLockMode = enum {
    shared,
    exclusive,
};

pub const NativePathLockOptions = struct {
    nonblocking: bool = false,
};

pub const NativePathLock = struct {
    lock_file: NativePathLockFile,

    pub fn release(self: *NativePathLock) void {
        self.lock_file.close();
        self.* = undefined;
    }
};

pub const NativePathLockFileOptions = struct {
    create_if_missing: bool = true,
};

pub const NativePathLockFile = struct {
    memory: ?*RuntimeMemory.Owner = null,
    pool: ?*NativeStoragePool = null,
    io_impl: if (supports_native_storage) std.Io.Threaded else void,
    file: std.Io.File,
    fd_cache: *FdCache,
    locked: bool = false,

    pub fn lock(self: *NativePathLockFile, mode: NativePathLockMode) !void {
        if (comptime !supports_native_storage) {
            return error.UnsupportedPlatform;
        } else {
            std.debug.assert(!self.locked);
            try self.file.lock(self.io_impl.io(), switch (mode) {
                .shared => .shared,
                .exclusive => .exclusive,
            });
            self.locked = true;
        }
    }

    pub fn tryLock(self: *NativePathLockFile, mode: NativePathLockMode) !bool {
        if (comptime !supports_native_storage) {
            return error.UnsupportedPlatform;
        } else {
            std.debug.assert(!self.locked);
            const locked = try self.file.tryLock(self.io_impl.io(), switch (mode) {
                .shared => .shared,
                .exclusive => .exclusive,
            });
            self.locked = locked;
            return locked;
        }
    }

    pub fn unlock(self: *NativePathLockFile) void {
        if (comptime supports_native_storage) {
            if (!self.locked) return;
            self.file.unlock(self.io_impl.io());
            self.locked = false;
        }
    }

    pub fn close(self: *NativePathLockFile) void {
        if (comptime supports_native_storage) {
            self.unlock();
            self.file.close(self.io_impl.io());
            self.fd_cache.releasePersistentDescriptors(self.io_impl.io(), 1);
            self.io_impl.deinit();
            if (self.memory) |memory| memory.destroy();
            if (self.pool) |pool| pool.releaseState();
            self.* = undefined;
        }
    }
};

pub fn nativeRealPathAlloc(allocator: Allocator, path: []const u8) ![:0]u8 {
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();

    if (std.fs.path.isAbsolute(path)) {
        return try std.Io.Dir.realPathFileAbsoluteAlloc(io_impl.io(), path, allocator);
    }
    return try std.Io.Dir.cwd().realPathFileAlloc(io_impl.io(), path, allocator);
}

fn nativeRootIdentityAlloc(_: *anyopaque, allocator: Allocator, root_dir: []const u8) ![]u8 {
    const canonical = nativeRealPathAlloc(allocator, root_dir) catch |err| switch (err) {
        error.FileNotFound => return try nativeMissingRootIdentityAlloc(allocator, root_dir),
        else => return err,
    };
    defer allocator.free(canonical);
    return try allocator.dupe(u8, canonical);
}

fn nativeMissingRootIdentityAlloc(allocator: Allocator, root_dir: []const u8) ![]u8 {
    if (std.fs.path.isAbsolute(root_dir)) {
        return try std.fs.path.resolve(allocator, &.{root_dir});
    }

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const cwd = try std.Io.Dir.cwd().realPathFileAlloc(io_impl.io(), ".", allocator);
    defer allocator.free(cwd);
    return try std.fs.path.resolve(allocator, &.{ cwd, root_dir });
}

fn openNativePathFile(io: std.Io, path: []const u8) !std.Io.File {
    if (std.fs.path.isAbsolute(path)) {
        return try std.Io.Dir.openFileAbsolute(io, path, .{});
    }
    return try std.Io.Dir.cwd().openFile(io, path, .{});
}

pub fn openNativePathLockFile(
    allocator: Allocator,
    path: []const u8,
    options: NativePathLockFileOptions,
) !NativePathLockFile {
    return try openNativePathLockFileWithPool(allocator, path, options, null);
}

pub fn openNativePathLockFileWithPool(
    allocator: Allocator,
    path: []const u8,
    options: NativePathLockFileOptions,
    pool: ?*NativeStoragePool,
) !NativePathLockFile {
    if (pool) |owner| try owner.retainState();
    errdefer if (pool) |owner| owner.releaseState();
    const parent = if (pool) |owner| owner.memory_parent else null;
    const memory = if (parent) |value| try RuntimeMemory.Owner.create(allocator, value) else null;
    errdefer if (memory) |owner| owner.destroy();
    var result = try openNativePathLockFileWithCache(
        if (memory) |owner| owner.allocator() else allocator,
        path,
        options,
        if (pool) |configured| configured.fd_cache else processNativeFdCache(),
        if (memory) |owner| owner.stackAllocator() else null,
    );
    result.memory = memory;
    result.pool = pool;
    return result;
}

fn openNativePathLockFileWithCache(
    allocator: Allocator,
    path: []const u8,
    options: NativePathLockFileOptions,
    fd_cache: *FdCache,
    stack_allocator: ?Allocator,
) !NativePathLockFile {
    if (comptime !supports_native_storage) {
        return error.UnsupportedPlatform;
    } else {
        var io_impl = threaded_io_limits.initOwnedService(allocator, stack_allocator);
        errdefer io_impl.deinit();

        const open_descriptor_count = createPathDescriptorCount(path);
        // Path locks live for the backend lifetime. They share the process budget,
        // but must never queue behind other lifetime descriptors: once that class
        // fills the budget, no waiter can make progress until a backend closes.
        // Reserved headroom protects opens from transient load; true persistent
        // exhaustion is reported instead of hanging startup or restore.
        try fd_cache.reservePersistentDescriptors(io_impl.io(), open_descriptor_count);
        var reserved_descriptor_count = open_descriptor_count;
        errdefer fd_cache.releasePersistentDescriptors(io_impl.io(), reserved_descriptor_count);

        const file = if (options.create_if_missing)
            try fs_paths.createFilePortable(io_impl.io(), path, .{ .read = true, .truncate = false })
        else
            try openNativePathFile(io_impl.io(), path);
        errdefer file.close(io_impl.io());
        if (reserved_descriptor_count > 1) {
            fd_cache.releasePersistentDescriptors(io_impl.io(), reserved_descriptor_count - 1);
            reserved_descriptor_count = 1;
        }

        return .{
            .io_impl = io_impl,
            .file = file,
            .fd_cache = fd_cache,
        };
    }
}

fn createPathDescriptorCount(path: []const u8) usize {
    // fs_paths opens a parent directory and then the new file for absolute
    // paths and relative paths containing a directory component.
    return if (std.fs.path.isAbsolute(path) or std.fs.path.dirname(path) != null) 2 else 1;
}

pub fn acquireNativePathLock(
    allocator: Allocator,
    path: []const u8,
    mode: NativePathLockMode,
    options: NativePathLockOptions,
) !NativePathLock {
    return try acquireNativePathLockWithPool(allocator, path, mode, options, null);
}

pub fn acquireNativePathLockWithPool(
    allocator: Allocator,
    path: []const u8,
    mode: NativePathLockMode,
    options: NativePathLockOptions,
    pool: ?*NativeStoragePool,
) !NativePathLock {
    var lock_file = try openNativePathLockFileWithPool(allocator, path, .{ .create_if_missing = true }, pool);
    errdefer lock_file.close();

    if (options.nonblocking) {
        if (!try lock_file.tryLock(mode)) return error.WouldBlock;
    } else {
        try lock_file.lock(mode);
    }

    return .{
        .lock_file = lock_file,
    };
}

pub const FileTrailer = struct {
    /// Bytes and size must describe the same pinned storage generation.
    bytes: []u8,
    file_size: u64,

    pub fn deinit(self: *FileTrailer, allocator: Allocator) void {
        allocator.free(self.bytes);
        self.* = undefined;
    }
};

/// Borrowed, type-erased storage view. The provider which produced this value
/// must outlive every call through it. Callers that launch work beyond a call
/// boundary must use a provider operation whose returned handle owns its own
/// lifetime (for example RangeReadFuture or AtomicWriteSink). Native callers
/// that retain a Storage view itself must retain a NativeStorage.Lease and
/// obtain the view from that lease.
pub const Storage = struct {
    pub const Trailer = FileTrailer;

    pub const Lease = struct {
        view: Storage,
        release: *const fn (*anyopaque) void,

        pub fn deinit(self: *Lease) void {
            self.release(self.view.ptr);
            self.* = undefined;
        }
    };

    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        /// Physical directory for private build files; null disables filesystem staging.
        private_scratch_directory: ?*const fn (*anyopaque) ?[]const u8 = null,
        /// Exact immutable native checkpoint, owned by the returned buffer.
        /// Providers pin the complete primary/projection generation and expose
        /// authenticated remote extents; mutable or partial views return null.
        native_checkpoint_reference_alloc: ?*const fn (*anyopaque, Allocator) anyerror!?[]u8 = null,
        /// Optional contiguous, file-backed view. The caller owns the mapping
        /// and releases its lease with deinit; no provider lifetime is retained.
        map_immutable_artifact: ?*const fn (*anyopaque, Allocator, []const u8) anyerror!@import("../../segment_source.zig").MappedArtifact = null,
        open_immutable_source: ?*const fn (*anyopaque, Allocator, []const u8) anyerror!@import("../../segment_source.zig").Source = null,
        /// Artifact-specific reclamation lease with provider-independent
        /// descriptor/runtime ownership. Persistent readers use this capability.
        open_leased_immutable_source: ?*const fn (*anyopaque, Allocator, []const u8) anyerror!@import("../../segment_source.zig").Source = null,
        acquire_lease: ?*const fn (*anyopaque) anyerror!Lease = null,
        create_dir_path: *const fn (*anyopaque, []const u8) anyerror!void,
        read_file_alloc: *const fn (*anyopaque, Allocator, []const u8, usize) anyerror![]u8,
        read_file_alloc_with_admission: ?*const fn (*anyopaque, Allocator, []const u8, usize, read_admission.Port) anyerror![]u8 = null,
        read_file_range_alloc: *const fn (*anyopaque, Allocator, []const u8, u64, usize) anyerror![]u8,
        begin_read_file_range_alloc: ?*const fn (*anyopaque, Allocator, []const u8, u64, usize) anyerror!RangeReadFuture = null,
        begin_read_file_range_alloc_with_runtime: ?*const fn (*anyopaque, ?ReadRuntime, Allocator, []const u8, u64, usize) anyerror!RangeReadFuture = null,
        begin_read_file_range_alloc_with_admission: ?*const fn (*anyopaque, ?ReadRuntime, Allocator, []const u8, u64, usize, read_admission.Port) anyerror!RangeReadFuture = null,
        read_file_range_into: ?*const fn (*anyopaque, []const u8, u64, []u8) anyerror!void = null,
        /// Native providers loan CPU only for the physical positional read.
        read_file_range_into_with_admission: ?*const fn (*anyopaque, []const u8, u64, []u8, read_admission.Port) anyerror!void = null,
        read_file_range_at_most_into: ?*const fn (*anyopaque, []const u8, u64, []u8) anyerror!usize = null,
        file_size: *const fn (*anyopaque, []const u8) anyerror!u64,
        file_size_with_admission: ?*const fn (*anyopaque, []const u8, read_admission.Port) anyerror!u64 = null,
        read_file_trailer_alloc_with_admission: ?*const fn (*anyopaque, Allocator, []const u8, usize, read_admission.Port) anyerror!FileTrailer = null,
        /// Reads an exact suffix and reports the size already observed while
        /// locating it, avoiding a separate metadata operation.
        read_file_trailer_alloc: ?*const fn (*anyopaque, Allocator, []const u8, usize) anyerror!FileTrailer = null,
        write_file_absolute: *const fn (*anyopaque, []const u8, []const u8) anyerror!void,
        append_file_absolute: ?*const fn (*anyopaque, []const u8, []const u8, bool) anyerror!void = null,
        begin_atomic_write: ?*const fn (*anyopaque, Allocator, []const u8) anyerror!AtomicWriteSink = null,
        begin_cold_sequential_read: ?*const fn (*anyopaque, Allocator, []const u8) anyerror!ColdSequentialReader = null,
        begin_cold_random_read: ?*const fn (*anyopaque, Allocator, []const u8) anyerror!ColdSequentialReader = null,
        try_begin_cold_random_reads: ?*const fn (*anyopaque, Allocator, []const []const u8) anyerror!?[]ColdSequentialReader = null,
        cold_sequential_reader_capacity: ?*const fn (*anyopaque) usize = null,
        /// Persists file contents without implying namespace durability.
        sync_contents_absolute: ?*const fn (*anyopaque, []const u8) anyerror!void = null,
        /// Persists the parent namespace entry for the supplied path.
        sync_parent_absolute: ?*const fn (*anyopaque, []const u8) anyerror!void = null,
        rename_absolute: *const fn (*anyopaque, []const u8, []const u8) anyerror!void,
        delete_file_absolute: *const fn (*anyopaque, []const u8) anyerror!void,
        delete_tree: *const fn (*anyopaque, []const u8) anyerror!void,
        list_file_names_alloc: ?*const fn (*anyopaque, Allocator, []const u8) anyerror![][]u8 = null,
        /// Immediate child directories, including virtual directories inferred
        /// from descendant files. Missing directories return an empty list.
        list_directory_names_alloc: ?*const fn (*anyopaque, Allocator, []const u8) anyerror![][]u8 = null,
        now_ns: *const fn (*anyopaque) u64,
        root_identity_alloc: ?*const fn (*anyopaque, Allocator, []const u8) anyerror![]u8 = null,
        /// Writes require a caller-owned publication lease, including staging
        /// immutable files whose namespace shares the primary database.
        requires_foreground_publication: ?*const fn (*anyopaque) bool = null,
        /// The host guarantees that rename_absolute is an atomic replacement
        /// when source and destination are siblings on the same storage.
        rename_is_atomic: bool = false,
        /// Logical storage paths share the process host namespace and may be
        /// captured and atomically published as a complete directory
        /// generation. This is intentionally distinct from path locking:
        /// locking alone does not prove that a host-directory rename publishes
        /// the bytes addressed through this storage implementation.
        supports_host_path_generation_publication: bool = false,
        supports_native_path_locks: bool = false,
        begin_sync_contents_with_admission: ?*const fn (*anyopaque, []const u8, read_admission.Port) anyerror!SyncOperation = null,
        begin_sync_parent_with_admission: ?*const fn (*anyopaque, []const u8, read_admission.Port) anyerror!SyncOperation = null,
        begin_control_file_read_with_admission: ?*const fn (*anyopaque, []const u8, usize, read_admission.Port) anyerror!ControlFileReadOperation = null,
        try_prepare_durability_with_admission: ?*const fn (*anyopaque, read_admission.Port) anyerror!?DurabilityResources = null,
        try_prepare_append_with_admission: ?*const fn (*anyopaque, []const u8, read_admission.Port) anyerror!?DurabilityResources = null,
        append_resource_bytes: ?*const fn (*anyopaque, usize) anyerror!u64 = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn privateScratchDirectory(self: Storage, fallback: []const u8) ?[]const u8 {
        if (self.vtable.private_scratch_directory) |directory| return directory(self.ptr);
        return fallback;
    }

    pub fn nativeCheckpointReferenceAlloc(self: Storage, a: Allocator) !?[]u8 {
        const reference = self.vtable.native_checkpoint_reference_alloc orelse return null;
        return BoundaryAbi.call("native_checkpoint_reference_alloc", self.boundary_dispatch, reference, .{ self.ptr, a });
    }

    pub fn createDirPath(self: Storage, path: []const u8) !void {
        return BoundaryAbi.call("create_dir_path", self.boundary_dispatch, self.vtable.create_dir_path, .{ self.ptr, path });
    }

    /// Providers must explicitly support ownership. A borrowed fallback would
    /// turn delayed maintenance into a use-after-free during index shutdown.
    pub fn acquireLease(self: Storage) !Lease {
        const acquire = self.vtable.acquire_lease orelse return error.UnsupportedStorageLease;
        return BoundaryAbi.call("acquire_lease", self.boundary_dispatch, acquire, .{self.ptr});
    }

    pub fn readFileAlloc(self: Storage, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
        return BoundaryAbi.call("read_file_alloc", self.boundary_dispatch, self.vtable.read_file_alloc, .{ self.ptr, allocator, path, max_bytes });
    }

    pub fn readFileRangeAlloc(self: Storage, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
        return BoundaryAbi.call("read_file_range_alloc", self.boundary_dispatch, self.vtable.read_file_range_alloc, .{ self.ptr, allocator, path, offset, len });
    }

    pub fn readFileRangeAllocWithAdmission(self: Storage, allocator: Allocator, path: []const u8, offset: u64, len: usize, control: read_admission.Port) ![]u8 {
        try control.checkpoint();
        const out = try allocator.alloc(u8, len);
        errdefer allocator.free(out);
        if (self.vtable.read_file_range_into_with_admission) |read| {
            try read(self.ptr, path, offset, out, control);
        } else try self.readFileRangeInto(allocator, path, offset, out);
        try control.checkpoint();
        return out;
    }

    /// Controlled into-buffer reads fail closed when the provider cannot carry
    /// the original authority. The output belongs to the caller on every exit.
    pub fn readFileRangeIntoWithAdmission(self: Storage, path: []const u8, offset: u64, out: []u8, control: read_admission.Port) !void {
        try control.checkpoint();
        const read = self.vtable.read_file_range_into_with_admission orelse return error.ExecutionWaitUnsupported;
        var relay: read_admission.ErrorRelay = .{};
        const borrowed = try relay.bind(control);
        BoundaryAbi.call("read_file_range_into_with_admission", self.boundary_dispatch, read, .{ self.ptr, path, offset, out, borrowed }) catch |err| return relay.rejection orelse err;
        if (relay.rejection) |err| return err;
        try control.checkpoint();
    }

    pub fn readFileAllocWithAdmission(self: Storage, allocator: Allocator, path: []const u8, max_bytes: usize, control: read_admission.Port) ![]u8 {
        try control.checkpoint();
        // Providers bind their own immutable generation. Native opt-in retains
        // one descriptor across metadata and data; compatibility keeps the
        // existing complete-file operation instead of re-resolving its pathname.
        const out = if (self.vtable.read_file_alloc_with_admission) |read|
            try read(self.ptr, allocator, path, max_bytes, control)
        else
            try self.readFileAlloc(allocator, path, max_bytes);
        errdefer allocator.free(out);
        try control.checkpoint();
        if (out.len > max_bytes) return error.StreamTooLong;
        return out;
    }

    pub fn beginReadFileRangeAlloc(self: Storage, allocator: Allocator, path: []const u8, offset: u64, len: usize) !RangeReadFuture {
        if (self.vtable.begin_read_file_range_alloc) |begin_read_file_range_alloc| {
            return try BoundaryAbi.call("begin_read_file_range_alloc", self.boundary_dispatch, begin_read_file_range_alloc, .{ self.ptr, allocator, path, offset, len });
        }
        return try CompletedRangeReadFuture.create(self, allocator, path, offset, len);
    }

    pub fn beginReadFileRangeAllocWithRuntime(self: Storage, read_runtime: ?ReadRuntime, allocator: Allocator, path: []const u8, offset: u64, len: usize) !RangeReadFuture {
        if (self.vtable.begin_read_file_range_alloc_with_runtime) |begin_read_file_range_alloc_with_runtime| {
            return try BoundaryAbi.call("begin_read_file_range_alloc_with_runtime", self.boundary_dispatch, begin_read_file_range_alloc_with_runtime, .{ self.ptr, read_runtime, allocator, path, offset, len });
        }
        return try self.beginReadFileRangeAlloc(allocator, path, offset, len);
    }

    pub fn readFileRangeInto(self: Storage, allocator: Allocator, path: []const u8, offset: u64, out: []u8) !void {
        if (self.vtable.read_file_range_into) |read_file_range_into| {
            return BoundaryAbi.call("read_file_range_into", self.boundary_dispatch, read_file_range_into, .{ self.ptr, path, offset, out });
        }
        const loaded = try self.readFileRangeAlloc(allocator, path, offset, out.len);
        defer allocator.free(loaded);
        if (loaded.len != out.len) return error.EndOfStream;
        @memcpy(out, loaded);
    }

    pub fn beginReadFileRangeAllocWithAdmission(self: Storage, read_runtime: ?ReadRuntime, allocator: Allocator, path: []const u8, offset: u64, len: usize, control: read_admission.Port) !RangeReadFuture {
        try control.checkpoint();
        var future = if (self.vtable.begin_read_file_range_alloc_with_admission) |begin|
            try begin(self.ptr, read_runtime, allocator, path, offset, len, control)
        else
            try CompletedRangeReadFuture.createControlled(self, allocator, path, offset, len, control);
        errdefer future.cancel();
        try control.checkpoint();
        return future;
    }

    pub fn readFileRangeAtMostInto(self: Storage, allocator: Allocator, path: []const u8, offset: u64, out: []u8) !usize {
        if (self.vtable.read_file_range_at_most_into) |read_file_range_at_most_into| {
            return try BoundaryAbi.call("read_file_range_at_most_into", self.boundary_dispatch, read_file_range_at_most_into, .{ self.ptr, path, offset, out });
        }
        const size = self.fileSize(path) catch |err| switch (err) {
            error.FileNotFound => return 0,
            else => return err,
        };
        if (offset >= size) return 0;
        const len: usize = @intCast(@min(size - offset, out.len));
        try self.readFileRangeInto(allocator, path, offset, out[0..len]);
        return len;
    }

    /// Optional immutable, lifetime-pinned random access. Unsupported backends
    /// must not emulate this by resolving the current pathname on each read.
    /// The storage owner must outlive the returned source. Each source pins
    /// one immutable artifact version until close; pathname re-resolution is
    /// forbidden after opening, including during vacuum and replacement.
    pub fn mapImmutableArtifact(self: Storage, allocator: Allocator, path: []const u8) !@import("../../segment_source.zig").MappedArtifact {
        const map = self.vtable.map_immutable_artifact orelse return error.MappedArtifactUnsupported;
        return BoundaryAbi.call("map_immutable_artifact", self.boundary_dispatch, map, .{ self.ptr, allocator, path });
    }

    pub fn openLeasedImmutableSource(self: Storage, allocator: Allocator, path: []const u8) !@import("../../segment_source.zig").Source {
        const open = self.vtable.open_leased_immutable_source orelse return error.ImmutableReadSourceUnsupported;
        return BoundaryAbi.call("open_leased_immutable_source", self.boundary_dispatch, open, .{ self.ptr, allocator, path });
    }

    pub fn openImmutableSource(self: Storage, allocator: Allocator, path: []const u8) !@import("../../segment_source.zig").Source {
        const open = self.vtable.open_immutable_source orelse return error.ImmutableReadSourceUnsupported;
        return BoundaryAbi.call("open_immutable_source", self.boundary_dispatch, open, .{ self.ptr, allocator, path });
    }

    pub fn fileSize(self: Storage, path: []const u8) !u64 {
        return BoundaryAbi.call("file_size", self.boundary_dispatch, self.vtable.file_size, .{ self.ptr, path });
    }

    pub fn fileSizeWithAdmission(self: Storage, path: []const u8, control: read_admission.Port) !u64 {
        try control.checkpoint();
        const size = if (self.vtable.file_size_with_admission) |stat| try stat(self.ptr, path, control) else try self.fileSize(path);
        try control.checkpoint();
        return size;
    }

    pub fn readFileTrailerAllocWithAdmission(self: Storage, allocator: Allocator, path: []const u8, len: usize, control: read_admission.Port) !FileTrailer {
        try control.checkpoint();
        // Compatibility providers retain their existing generation binding;
        // independently re-resolving size and range would weaken that contract.
        var trailer = if (self.vtable.read_file_trailer_alloc_with_admission) |read|
            try read(self.ptr, allocator, path, len, control)
        else
            try self.readFileTrailerAlloc(allocator, path, len);
        errdefer trailer.deinit(allocator);
        try control.checkpoint();
        if (trailer.bytes.len != len or trailer.file_size < len) return error.EndOfStream;
        return trailer;
    }

    pub fn readFileTrailerAlloc(self: Storage, allocator: Allocator, path: []const u8, len: usize) !FileTrailer {
        if (self.vtable.read_file_trailer_alloc) |read_file_trailer_alloc| {
            var trailer = try BoundaryAbi.call("read_file_trailer_alloc", self.boundary_dispatch, read_file_trailer_alloc, .{ self.ptr, allocator, path, len });
            errdefer trailer.deinit(allocator);
            if (trailer.bytes.len != len or trailer.file_size < len)
                return error.EndOfStream;
            return trailer;
        }

        const size = try self.fileSize(path);
        if (size < len) return error.EndOfStream;
        return .{
            .bytes = try self.readFileRangeAlloc(allocator, path, size - len, len),
            .file_size = size,
        };
    }

    pub fn writeFileAbsolute(self: Storage, path: []const u8, contents: []const u8) !void {
        return BoundaryAbi.call("write_file_absolute", self.boundary_dispatch, self.vtable.write_file_absolute, .{ self.ptr, path, contents });
    }

    pub fn appendFileAbsolute(self: Storage, allocator: Allocator, path: []const u8, contents: []const u8, sync: bool) !void {
        if (self.vtable.append_file_absolute) |append_file_absolute| {
            return BoundaryAbi.call("append_file_absolute", self.boundary_dispatch, append_file_absolute, .{ self.ptr, path, contents, sync });
        }

        const existing = self.readFileAlloc(allocator, path, std.math.maxInt(usize)) catch |err| switch (err) {
            error.FileNotFound => {
                try self.writeFileAbsolute(path, contents);
                if (sync) try self.syncFileContentsAbsolute(path);
                return;
            },
            else => return err,
        };
        defer allocator.free(existing);

        const joined = try allocator.alloc(u8, existing.len + contents.len);
        defer allocator.free(joined);
        @memcpy(joined[0..existing.len], existing);
        @memcpy(joined[existing.len..], contents);
        try self.writeFileAbsolute(path, joined);
        if (sync) try self.syncFileContentsAbsolute(path);
    }

    pub fn beginAtomicWrite(self: Storage, allocator: Allocator, path: []const u8) !AtomicWriteSink {
        if (self.vtable.begin_atomic_write) |begin_atomic_write| {
            return try BoundaryAbi.call("begin_atomic_write", self.boundary_dispatch, begin_atomic_write, .{ self.ptr, allocator, path });
        }
        if (self.vtable.sync_contents_absolute == null or
            self.vtable.sync_parent_absolute == null or
            !self.vtable.rename_is_atomic)
        {
            return error.DurableAtomicWriteUnsupported;
        }
        return try BufferedAtomicWriteSink.create(allocator, self, path);
    }

    pub fn beginColdSequentialRead(self: Storage, allocator: Allocator, path: []const u8) !ColdSequentialReader {
        if (self.vtable.begin_cold_sequential_read) |begin_cold_sequential_read| {
            return try BoundaryAbi.call("begin_cold_sequential_read", self.boundary_dispatch, begin_cold_sequential_read, .{ self.ptr, allocator, path });
        }
        return try GenericColdSequentialReader.create(allocator, self, path);
    }

    /// Opens a maintenance-private descriptor for sparse immutable reads.
    /// Native implementations bypass completed pages without leaking their
    /// cache policy into the foreground descriptor cache.
    pub fn beginColdRandomRead(self: Storage, allocator: Allocator, path: []const u8) !ColdSequentialReader {
        if (self.vtable.begin_cold_random_read) |begin_cold_random_read| {
            return try BoundaryAbi.call("begin_cold_random_read", self.boundary_dispatch, begin_cold_random_read, .{ self.ptr, allocator, path });
        }
        return try GenericColdSequentialReader.create(allocator, self, path);
    }

    /// All-or-nothing private-reader admission. Never waits for descriptors:
    /// callers may already own a checkpoint output and must not hold that
    /// resource while waiting for another builder's partial input set.
    /// Null means use the existing generation's positional read path.
    pub fn tryBeginColdRandomReads(self: Storage, allocator: Allocator, paths: []const []const u8) !?[]ColdSequentialReader {
        const begin = self.vtable.try_begin_cold_random_reads orelse return null;
        return try BoundaryAbi.call("try_begin_cold_random_reads", self.boundary_dispatch, begin, .{ self.ptr, allocator, paths });
    }

    /// Acquires a descriptor/handle bound to the currently opened file, not a
    /// path which may later be atomically replaced. Generation capture uses
    /// this stronger contract so materialization can safely outlive mutation
    /// admission and CURRENT rotation.
    pub fn beginStableSequentialRead(self: Storage, allocator: Allocator, path: []const u8) !ColdSequentialReader {
        const begin = self.vtable.begin_cold_sequential_read orelse
            return error.StableFileLeaseUnsupported;
        return try BoundaryAbi.call("begin_cold_sequential_read", self.boundary_dispatch, begin, .{ self.ptr, allocator, path });
    }

    pub fn beginSequentialRead(self: Storage, allocator: Allocator, path: []const u8) !ColdSequentialReader {
        return try GenericColdSequentialReader.create(allocator, self, path);
    }

    /// A cursor over an immutable path pinned by the caller's run snapshot.
    /// Private cold descriptors live only for one read window, never across
    /// admission for another input or the output writer. Unlike a stable file
    /// lease, this must not be used for replaceable WAL or CURRENT paths.
    pub fn beginWindowedColdRead(self: Storage, allocator: Allocator, path: []const u8) !ColdSequentialReader {
        return try GenericColdSequentialReader.createWithPolicy(allocator, self, path, self.coldSequentialReaderCapacity() != 0);
    }

    /// Maximum private maintenance descriptors that can coexist while
    /// retaining one slot for the output writer and the persistent reserve.
    /// Compatibility readers use normal range I/O and consume no such leases.
    pub fn coldSequentialReaderCapacity(self: Storage) usize {
        if (self.vtable.cold_sequential_reader_capacity) |capacity| return capacity(self.ptr);
        return std.math.maxInt(usize);
    }

    /// Nonblocking descriptor admission honors existing FIFO waiters. A null
    /// result owns nothing; callers wait/retry only after dropping any other
    /// tentative resources. Missing providers never fall back to raw I/O.
    pub fn tryPrepareDurabilityWithAdmission(self: Storage, original: read_admission.Port) !?DurabilityResources {
        try original.checkpoint();
        const prepare = self.vtable.try_prepare_durability_with_admission orelse return error.ExecutionWaitUnsupported;
        return BoundaryAbi.call("try_prepare_durability_with_admission", self.boundary_dispatch, prepare, .{ self.ptr, original });
    }

    /// Freeze encoded bytes on admitted caller CPU before acquiring publication.
    /// This grant owns their allocation, descriptor and worker until retirement.
    /// Null or error holds no tentative resources and performs no write.
    /// Additional frozen bytes and grant header, excluding the service's
    /// separately governed executor heap/stacks and descriptor pool.
    pub fn appendResourceBytes(self: Storage, len: usize) !u64 {
        const bytes = self.vtable.append_resource_bytes orelse return error.ExecutionWaitUnsupported;
        return BoundaryAbi.call("append_resource_bytes", self.boundary_dispatch, bytes, .{ self.ptr, len });
    }

    pub fn tryPrepareAppendWithAdmission(self: Storage, bytes: []const u8, original: read_admission.Port) !?DurabilityResources {
        try original.checkpoint();
        const prepare = self.vtable.try_prepare_append_with_admission orelse return error.ExecutionWaitUnsupported;
        return BoundaryAbi.call("try_prepare_append_with_admission", self.boundary_dispatch, prepare, .{ self.ptr, bytes, original });
    }

    pub fn beginControlFileReadWithAdmission(self: Storage, path: []const u8, limit: usize, original: read_admission.Port) !ControlFileReadOperation {
        try original.checkpoint();
        if (limit == 0 or limit > max_control_file_bytes) return error.ResourceBudgetExceeded;
        const begin = self.vtable.begin_control_file_read_with_admission orelse return error.ExecutionWaitUnsupported;
        return BoundaryAbi.call("begin_control_file_read_with_admission", self.boundary_dispatch, begin, .{ self.ptr, path, limit, original });
    }

    /// Fail closed without an owning provider; a legacy synchronous sync is
    /// not an interruptible substitute. No original control escapes this call.
    pub fn beginSyncFileContentsWithAdmission(self: Storage, path: []const u8, original: read_admission.Port) !SyncOperation {
        try original.checkpoint();
        const begin = self.vtable.begin_sync_contents_with_admission orelse return error.ExecutionWaitUnsupported;
        return BoundaryAbi.call("begin_sync_contents_with_admission", self.boundary_dispatch, begin, .{ self.ptr, path, original });
    }

    pub fn beginSyncParentWithAdmission(self: Storage, path: []const u8, original: read_admission.Port) !SyncOperation {
        try original.checkpoint();
        const begin = self.vtable.begin_sync_parent_with_admission orelse return error.ExecutionWaitUnsupported;
        return BoundaryAbi.call("begin_sync_parent_with_admission", self.boundary_dispatch, begin, .{ self.ptr, path, original });
    }

    pub fn syncFileContentsAbsolute(self: Storage, path: []const u8) !void {
        if (self.vtable.sync_contents_absolute) |sync_contents_absolute| {
            return try BoundaryAbi.call("sync_contents_absolute", self.boundary_dispatch, sync_contents_absolute, .{ self.ptr, path });
        }
        return error.DurableFileSyncUnsupported;
    }

    /// Persists creation or removal of `path` without resyncing its contents.
    pub fn syncParentAbsolute(self: Storage, path: []const u8) !void {
        if (self.vtable.sync_parent_absolute) |sync_parent_absolute| {
            return try BoundaryAbi.call("sync_parent_absolute", self.boundary_dispatch, sync_parent_absolute, .{ self.ptr, path });
        }
        return error.DurableDirectorySyncUnsupported;
    }

    pub fn renameAbsolute(self: Storage, old_path: []const u8, new_path: []const u8) !void {
        return BoundaryAbi.call("rename_absolute", self.boundary_dispatch, self.vtable.rename_absolute, .{ self.ptr, old_path, new_path });
    }

    pub fn deleteFileAbsolute(self: Storage, path: []const u8) !void {
        return BoundaryAbi.call("delete_file_absolute", self.boundary_dispatch, self.vtable.delete_file_absolute, .{ self.ptr, path });
    }

    pub fn deleteTree(self: Storage, path: []const u8) !void {
        return BoundaryAbi.call("delete_tree", self.boundary_dispatch, self.vtable.delete_tree, .{ self.ptr, path });
    }

    /// Returns owned basenames for regular files immediately below `path`.
    /// Native generation stores use this to reconcile immutable files against
    /// their authoritative CURRENT record after crashes and failed unlinks.
    pub fn listFileNamesAlloc(self: Storage, allocator: Allocator, path: []const u8) ![][]u8 {
        const list = self.vtable.list_file_names_alloc orelse return error.DirectoryListingUnsupported;
        return try BoundaryAbi.call("list_file_names_alloc", self.boundary_dispatch, list, .{ self.ptr, allocator, path });
    }

    pub fn listDirectoryNamesAlloc(self: Storage, allocator: Allocator, path: []const u8) ![][]u8 {
        const list = self.vtable.list_directory_names_alloc orelse return error.DirectoryListingUnsupported;
        return BoundaryAbi.call("list_directory_names_alloc", self.boundary_dispatch, list, .{ self.ptr, allocator, path }) catch |err| switch (err) {
            error.FileNotFound => allocator.alloc([]u8, 0),
            else => return err,
        };
    }

    /// Logical file bytes in a generation. Released descriptors/snapshots from
    /// each listing keep recursive traversal from retaining backend resources.
    pub fn treeBytes(self: Storage, allocator: Allocator, path: []const u8) anyerror!u64 {
        const files = self.listFileNamesAlloc(allocator, path) catch |err| switch (err) {
            error.FileNotFound => return 0,
            else => return err,
        };
        defer freeFileNames(allocator, files);
        var total: u64 = 0;
        for (files) |name| {
            const child = try std.fs.path.join(allocator, &.{ path, name });
            defer allocator.free(child);
            total +|= self.fileSize(child) catch |err| switch (err) {
                error.FileNotFound => 0, // Concurrent reclamation.
                else => return err,
            };
        }
        const directories = try self.listDirectoryNamesAlloc(allocator, path);
        defer freeFileNames(allocator, directories);
        for (directories) |name| {
            const child = try std.fs.path.join(allocator, &.{ path, name });
            defer allocator.free(child);
            total +|= try self.treeBytes(allocator, child);
        }
        return total;
    }

    pub fn freeFileNames(allocator: Allocator, names: [][]u8) void {
        for (names) |name| allocator.free(name);
        allocator.free(names);
    }

    pub fn nowNs(self: Storage) u64 {
        return self.vtable.now_ns(self.ptr);
    }

    pub fn requiresForegroundPublication(self: Storage) bool {
        const requires = self.vtable.requires_foreground_publication orelse return false;
        return requires(self.ptr);
    }

    pub fn rootIdentityAlloc(self: Storage, allocator: Allocator, root_dir: []const u8) ![]u8 {
        if (self.vtable.root_identity_alloc) |root_identity_alloc| {
            return try BoundaryAbi.call("root_identity_alloc", self.boundary_dispatch, root_identity_alloc, .{ self.ptr, allocator, root_dir });
        }
        return try std.fmt.allocPrint(allocator, "storage:{x}:{x}:{s}", .{
            @intFromPtr(self.ptr),
            @intFromPtr(self.vtable),
            root_dir,
        });
    }

    pub fn supportsNativePathLocks(self: Storage) bool {
        return self.vtable.supports_native_path_locks;
    }

    pub fn supportsHostPathGenerationPublication(self: Storage) bool {
        return self.vtable.supports_host_path_generation_publication;
    }
};

const GenericColdSequentialReader = struct {
    allocator: Allocator,
    storage: Storage,
    path: []u8,
    cold_windows: bool = false,

    const vtable: ColdSequentialReader.VTable = .{
        .read_range_alloc = readRangeAlloc,
        .read_range_into = readRangeInto,
        .read_ranges_into = readRangesInto,
        .release_scratch = releaseScratch,
        .deinit = deinitErased,
    };

    fn create(allocator: Allocator, storage: Storage, path: []const u8) !ColdSequentialReader {
        return createWithPolicy(allocator, storage, path, false);
    }

    fn createWithPolicy(allocator: Allocator, storage: Storage, path: []const u8, cold_windows: bool) !ColdSequentialReader {
        const self = try allocator.create(GenericColdSequentialReader);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .storage = storage,
            .path = try allocator.dupe(u8, path),
            .cold_windows = cold_windows,
        };
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn readRangeAlloc(ptr: *anyopaque, allocator: Allocator, offset: u64, len: usize) ![]u8 {
        const self: *GenericColdSequentialReader = @ptrCast(@alignCast(ptr));
        if (self.cold_windows) {
            var window = try self.storage.beginColdSequentialRead(self.allocator, self.path);
            defer window.deinit();
            return try window.readRangeAlloc(allocator, offset, len);
        }
        return try self.storage.readFileRangeAlloc(allocator, self.path, offset, len);
    }

    fn readRangeInto(ptr: *anyopaque, offset: u64, out: []u8) !void {
        const self: *GenericColdSequentialReader = @ptrCast(@alignCast(ptr));
        if (self.cold_windows) {
            var window = try self.storage.beginColdSequentialRead(self.allocator, self.path);
            defer window.deinit();
            return try window.readRangeInto(offset, out);
        }
        return try self.storage.readFileRangeInto(self.allocator, self.path, offset, out);
    }

    fn readRangesInto(ptr: *anyopaque, ranges: []const ColdReadRange) !ColdReadStats {
        const self: *GenericColdSequentialReader = @ptrCast(@alignCast(ptr));
        if (self.cold_windows and ranges.len != 0) {
            var window = try self.storage.beginColdSequentialRead(self.allocator, self.path);
            defer window.deinit();
            return try window.readRangesInto(ranges);
        }
        var stats: ColdReadStats = .{};
        for (ranges) |range| {
            try readRangeInto(ptr, range.offset, range.destination);
            stats.logical_bytes +|= range.destination.len;
            stats.physical_bytes +|= range.destination.len;
            stats.physical_reads +|= @intFromBool(range.destination.len != 0);
        }
        return stats;
    }

    fn releaseScratch(_: *anyopaque) void {}

    fn deinitErased(ptr: *anyopaque) void {
        const self: *GenericColdSequentialReader = @ptrCast(@alignCast(ptr));
        const allocator = self.allocator;
        allocator.free(self.path);
        allocator.destroy(self);
    }
};

pub fn createDirPathPortable(io: anytype, path: []const u8) !void {
    return fs_paths.createDirPathPortable(io, path);
}

/// Borrowed `std.Io` storage adapter. This keeps durable LSM operations on the
/// caller's I/O implementation, including deterministic VOPR filesystems,
/// instead of silently creating a native threaded storage lane.
pub const IoStorage = struct {
    receiver: @import("antfly_runtime_abi").io_abi.Receiver,

    pub fn init(io: std.Io) IoStorage {
        return .{ .receiver = @import("antfly_runtime_abi").io_abi.Borrow.init(&io).receive() catch unreachable };
    }

    pub fn storage(self: *IoStorage) Storage {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable: Storage.VTable = .{
        .create_dir_path = createDirPath,
        .read_file_alloc = readFileAlloc,
        .read_file_range_alloc = readFileRangeAlloc,
        .read_file_range_into = readFileRangeInto,
        .read_file_range_at_most_into = readFileRangeAtMostInto,
        .file_size = fileSize,
        .read_file_trailer_alloc = readFileTrailerAlloc,
        .write_file_absolute = writeFileAbsolute,
        .append_file_absolute = appendFileAbsolute,
        .sync_contents_absolute = syncContentsAbsolute,
        .sync_parent_absolute = syncParentAbsolute,
        .rename_absolute = renameAbsolute,
        .delete_file_absolute = deleteFileAbsolute,
        .delete_tree = deleteTree,
        .list_file_names_alloc = listFileNamesAlloc,
        .list_directory_names_alloc = listDirectoryNamesAlloc,
        .now_ns = nowNs,
        .root_identity_alloc = rootIdentityAlloc,
        .rename_is_atomic = true,
        .supports_native_path_locks = false,
    };

    fn context(ptr: *anyopaque) *IoStorage {
        return @ptrCast(@alignCast(ptr));
    }

    fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
        try fs_paths.createDirPathPortable(context(ptr).receiver.io(), path);
    }

    fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
        return try std.Io.Dir.cwd().readFileAlloc(context(ptr).receiver.io(), path, allocator, .limited(max_bytes));
    }

    fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
        return try readFileRangeWithIo(context(ptr).receiver.io(), allocator, path, offset, len);
    }

    fn readFileRangeInto(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8) !void {
        try readFileRangeWithIoInto(context(ptr).receiver.io(), path, offset, out);
    }

    fn readFileRangeAtMostInto(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8) !usize {
        return try readFileRangeWithIoAtMostInto(context(ptr).receiver.io(), path, offset, out);
    }

    fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
        return try fileSizeWithIo(context(ptr).receiver.io(), path);
    }

    fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !FileTrailer {
        return try readFileTrailerWithIo(context(ptr).receiver.io(), allocator, path, len);
    }

    fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
        try std.Io.Dir.cwd().writeFile(context(ptr).receiver.io(), .{ .sub_path = path, .data = contents });
    }

    fn appendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
        const io = context(ptr).receiver.io();
        var file = try fs_paths.createFilePortable(io, path, .{ .read = true, .truncate = false });
        defer file.close(io);
        const offset = try file.length(io);
        try file.writePositionalAll(io, contents, offset);
        if (sync) try file.sync(io);
    }

    fn syncContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
        try syncFileContentsPathWithIo(context(ptr).receiver.io(), path);
    }

    fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
        try syncParentPathWithIo(context(ptr).receiver.io(), path);
    }

    fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
        try std.Io.Dir.rename(std.Io.Dir.cwd(), old_path, std.Io.Dir.cwd(), new_path, context(ptr).receiver.io());
    }

    fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
        try deleteFilePathWithIo(context(ptr).receiver.io(), path);
    }

    fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
        try std.Io.Dir.cwd().deleteTree(context(ptr).receiver.io(), path);
    }

    fn listFileNamesAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8) ![][]u8 {
        return listFileNamesWithIo(allocator, context(ptr).receiver.io(), path);
    }

    fn listDirectoryNamesAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8) ![][]u8 {
        return listNamesWithIo(allocator, context(ptr).receiver.io(), path, .directory);
    }

    fn nowNs(ptr: *anyopaque) u64 {
        const value = std.Io.Clock.awake.now(context(ptr).receiver.io()).toNanoseconds();
        return if (value <= 0) 0 else @intCast(@min(value, std.math.maxInt(u64)));
    }

    fn rootIdentityAlloc(ptr: *anyopaque, allocator: Allocator, root_dir: []const u8) ![]u8 {
        return try std.fmt.allocPrint(allocator, "std-io:{x}:{s}", .{ @intFromPtr(context(ptr)), root_dir });
    }
};

/// Thin wrapper for host-provided storage callbacks.
/// Intended for embedders that want durable LSM semantics without native fs access,
/// such as wasm or foreign host runtimes. Durable writers require either a
/// host-provided atomic writer or an explicitly atomic rename plus separate
/// file-content and parent-namespace sync callbacks. A host with one combined
/// durability barrier may install it in both callback slots.
pub const HostStorage = struct {
    ptr: *anyopaque,
    vtable: *const Storage.VTable,

    pub fn init(ptr: *anyopaque, vtable: *const Storage.VTable) HostStorage {
        return .{
            .ptr = ptr,
            .vtable = vtable,
        };
    }

    pub fn storage(self: HostStorage) Storage {
        return .{
            .ptr = self.ptr,
            .vtable = self.vtable,
        };
    }
};

const BufferedAtomicWriteSink = struct {
    allocator: Allocator,
    storage: Storage,
    final_path: []u8,
    tmp_path: []u8,
    out: std.ArrayListUnmanaged(u8) = .empty,

    fn create(allocator: Allocator, storage: Storage, path: []const u8) !AtomicWriteSink {
        const self = try allocator.create(BufferedAtomicWriteSink);
        errdefer allocator.destroy(self);

        const final_path = try allocator.dupe(u8, path);
        errdefer allocator.free(final_path);

        const tmp_path = try tempSiblingPath(allocator, path);
        errdefer allocator.free(tmp_path);

        self.* = .{
            .allocator = allocator,
            .storage = storage,
            .final_path = final_path,
            .tmp_path = tmp_path,
        };
        return .{
            .ptr = self,
            .vtable = &buffered_atomic_write_sink_vtable,
        };
    }

    fn deinit(self: *BufferedAtomicWriteSink) void {
        self.out.deinit(self.allocator);
        self.allocator.free(self.final_path);
        self.allocator.free(self.tmp_path);
        self.allocator.destroy(self);
    }

    fn len(ptr: *anyopaque) usize {
        const self: *BufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        return self.out.items.len;
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *BufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        try byte_copy.appendSlicePossiblyAliased(&self.out, self.allocator, bytes);
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *BufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (offset > self.out.items.len or bytes.len > self.out.items.len - offset) return error.InvalidAtomicWriteOffset;
        byte_copy.copyPossiblyAliased(self.out.items[offset..][0..bytes.len], bytes);
    }

    fn crc32Prefix(ptr: *anyopaque, len_prefix: usize) !u32 {
        const self: *BufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (len_prefix > self.out.items.len) return error.InvalidAtomicWriteOffset;
        return Crc32.hash(self.out.items[0..len_prefix]);
    }

    fn crc32Range(ptr: *anyopaque, offset: usize, range_len: usize) !u32 {
        const self: *BufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (offset > self.out.items.len or range_len > self.out.items.len - offset) return error.InvalidAtomicWriteOffset;
        return Crc32.hash(self.out.items[offset..][0..range_len]);
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *BufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        defer self.deinit();

        self.storage.writeFileAbsolute(self.tmp_path, self.out.items) catch |err| {
            self.storage.deleteFileAbsolute(self.tmp_path) catch {};
            return err;
        };
        self.storage.syncFileContentsAbsolute(self.tmp_path) catch |err| {
            self.storage.deleteFileAbsolute(self.tmp_path) catch {};
            return err;
        };
        self.storage.renameAbsolute(self.tmp_path, self.final_path) catch |err| {
            self.storage.deleteFileAbsolute(self.tmp_path) catch {};
            return err;
        };
        try self.storage.syncParentAbsolute(self.final_path);
    }

    fn abort(ptr: *anyopaque) void {
        const self: *BufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        self.storage.deleteFileAbsolute(self.tmp_path) catch {};
        self.deinit();
    }
};

const buffered_atomic_write_sink_vtable: AtomicWriteSink.VTable = .{
    .len = BufferedAtomicWriteSink.len,
    .append_slice = BufferedAtomicWriteSink.appendSlice,
    .write_at = BufferedAtomicWriteSink.writeAt,
    .crc32_prefix = BufferedAtomicWriteSink.crc32Prefix,
    .crc32_range = BufferedAtomicWriteSink.crc32Range,
    .finish = BufferedAtomicWriteSink.finish,
    .abort = BufferedAtomicWriteSink.abort,
};

const FdCache = if (!supports_posix_fd_cache)
    struct {
        admission_epoch: std.atomic.Value(u32) = .init(0),

        pub fn init(_: Allocator, _: usize) FdCache {
            return .{};
        }

        pub fn deinit(_: *FdCache) void {}

        pub fn snapshotStats(_: *const FdCache) NativeStorageStats {
            return .{};
        }

        pub fn readRangeAlloc(_: *FdCache, _: u64, _: Allocator, _: []const u8, _: u64, _: usize) ![]u8 {
            return error.UnsupportedNativeStorageRuntime;
        }

        pub fn fileSize(_: *FdCache, _: u64, _: []const u8) !u64 {
            return error.UnsupportedNativeStorageRuntime;
        }

        pub fn readTrailerAlloc(_: *FdCache, _: u64, _: Allocator, _: []const u8, _: usize) !FileTrailer {
            return error.UnsupportedNativeStorageRuntime;
        }

        pub fn invalidatePath(_: *FdCache, _: u64, _: []const u8) void {}
        pub fn fencePathRetainingEntries(_: *FdCache, _: u64, _: std.Io, _: []const u8, _: ?read_admission.Port) !void {}
        pub fn invalidateTree(_: *FdCache, _: u64, _: []const u8) void {}
        pub fn invalidateRename(_: *FdCache, _: u64, _: []const u8, _: []const u8) void {}
        pub fn invalidateNamespace(_: *FdCache, _: u64) void {}
        pub fn reserveDescriptors(_: *FdCache, _: std.Io, _: usize) !void {}
        pub fn reserveDescriptorsWithAdmission(_: *FdCache, _: std.Io, _: usize, _: read_admission.Port) !void {}
        pub fn tryReserveDescriptors(_: *FdCache, _: std.Io, _: usize) bool {
            return false;
        }
        pub fn releaseDescriptors(_: *FdCache, _: std.Io, _: usize) void {}
        pub fn reservePersistentDescriptors(_: *FdCache, _: std.Io, _: usize) !void {}
        pub fn releasePersistentDescriptors(_: *FdCache, _: std.Io, _: usize) void {}
        pub fn signalAdmissionChanged(_: *FdCache, _: std.Io) void {}
        pub fn admissionEpoch(_: *const FdCache) u32 {
            return 0;
        }
    }
else
    struct {
        const BucketMap = std.AutoHashMapUnmanaged(u64, std.ArrayListUnmanaged(*Entry));

        const Entry = struct {
            namespace: u64,
            path_hash: u64,
            path: [:0]u8,
            fd: std.posix.fd_t,
            ref_count: usize = 0,
            last_access: u64 = 0,
            invalidated: bool = false,
            lru_prev: ?*Entry = null,
            lru_next: ?*Entry = null,

            fn deinit(self: *Entry, allocator: Allocator) void {
                closeFd(self.fd);
                allocator.free(self.path);
                self.* = undefined;
            }
        };

        const Shard = struct {
            mutex: std.atomic.Mutex = .unlocked,
            entries: BucketMap = .empty,
            lru_head: ?*Entry = null,
            lru_tail: ?*Entry = null,
            // Incremented before every mutation fence affecting this shard.
            // Cache misses open outside the shard lock, then compare the
            // observed epoch before insertion so an invalidate/rename cannot
            // be lost in that window.
            invalidation_epoch: u64 = 0,
        };

        const AdmissionWaiter = struct {
            previous: ?*AdmissionWaiter = null,
            next: ?*AdmissionWaiter = null,
            count: usize,
        };

        allocator: Allocator,
        shards: []Shard,
        capacity: usize,
        persistent_reserve: usize,
        persistent_open_headroom: usize,
        admitted_descriptors: std.atomic.Value(usize) = .init(0),
        persistent_descriptors: std.atomic.Value(usize) = .init(0),
        cache_entry_count: std.atomic.Value(usize) = .init(0),
        admission_waiters: std.atomic.Value(usize) = .init(0),
        admission_waits: CounterU64 = .init(0),
        persistent_admission_failures: CounterU64 = .init(0),
        admission_epoch: std.atomic.Value(u32) = .init(0),
        admission_mutex: std.Io.Mutex = .init,
        admission_changed: std.Io.Condition = .init,
        admission_head: ?*AdmissionWaiter = null,
        admission_tail: ?*AdmissionWaiter = null,
        access_clock: CounterU64 = .init(0),
        evict_cursor: std.atomic.Value(usize) = .init(0),
        evict_mutex: std.atomic.Mutex = .unlocked,

        fn init(allocator: Allocator, capacity: usize) FdCache {
            const shards = allocator.alloc(Shard, fd_cache_shard_count) catch @panic("OOM");
            @memset(shards, .{});
            return .{
                .allocator = allocator,
                .shards = shards,
                .capacity = @max(@as(usize, 1), capacity),
                // Keep a small slice unavailable to transient leases so a
                // backend can still acquire its lifetime lock files under
                // read/write pressure. Small test and emergency limits retain
                // their full capacity.
                .persistent_reserve = if (capacity < 4) 0 else @min(@as(usize, 64), @max(@as(usize, 2), capacity / 8)),
                // Creating one lifetime lock can transiently require both the
                // newly-created descriptor and the final reopened descriptor.
                .persistent_open_headroom = if (capacity < 4) 0 else 2,
            };
        }

        fn deinit(self: *FdCache) void {
            for (self.shards) |*shard| {
                var current = shard.lru_head;
                while (current) |entry| {
                    const next = entry.lru_next;
                    entry.deinit(self.allocator);
                    self.allocator.destroy(entry);
                    current = next;
                }

                var it = shard.entries.iterator();
                while (it.next()) |bucket| {
                    bucket.value_ptr.deinit(self.allocator);
                }
                shard.entries.deinit(self.allocator);
            }
            self.allocator.free(self.shards);
            self.* = undefined;
        }

        fn snapshotStats(self: *const FdCache) NativeStorageStats {
            return .{
                .fd_cache_entries = self.cache_entry_count.load(.monotonic),
                .fd_admitted_descriptors = self.admitted_descriptors.load(.monotonic),
                .fd_persistent_descriptors = self.persistent_descriptors.load(.monotonic),
                .fd_admission_capacity = self.capacity,
                .fd_persistent_reserve = self.persistent_reserve,
                .fd_admission_waiters = self.admission_waiters.load(.monotonic),
                .fd_admission_waits = self.admission_waits.load(.monotonic),
                .fd_persistent_admission_failures = self.persistent_admission_failures.load(.monotonic),
            };
        }

        fn readRangeAlloc(self: *FdCache, namespace: u64, io: std.Io, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const entry = try self.retain(namespace, io, path);
            defer self.release(io, entry);

            const out = try allocator.alloc(u8, len);
            errdefer allocator.free(out);
            try readAllAtOffset(entry.fd, out, offset);
            return out;
        }

        fn readRangeInto(self: *FdCache, namespace: u64, io: std.Io, path: []const u8, offset: u64, out: []u8) !void {
            const entry = try self.retain(namespace, io, path);
            defer self.release(io, entry);
            try readAllAtOffset(entry.fd, out, offset);
        }

        fn readRangeIntoWithAdmission(self: *FdCache, namespace: u64, io: std.Io, path: []const u8, offset: u64, out: []u8, control: read_admission.Port) !void {
            try control.checkpoint();
            const entry = try self.retainControlled(namespace, io, path, control);
            defer self.release(io, entry);
            if (out.len == 0) {
                try control.checkpoint();
                return;
            }
            // Cache preparation and destination allocation run with CPU
            // charged; only descriptor contention/open and this pread loan it.
            // Keep the FD use through rejected resumption.
            if (try control.executionWait()) |boundary| {
                try boundary.run(readAllAtOffset, .{ entry.fd, out, offset });
            } else try readAllAtOffset(entry.fd, out, offset);
            try control.checkpoint();
        }

        fn readRangeAtMostInto(self: *FdCache, namespace: u64, io: std.Io, path: []const u8, offset: u64, out: []u8) !usize {
            const entry = try self.retain(namespace, io, path);
            defer self.release(io, entry);
            return try readAtMostAtOffset(entry.fd, out, offset);
        }

        fn fileSize(self: *FdCache, namespace: u64, io: std.Io, path: []const u8) !u64 {
            const entry = try self.retain(namespace, io, path);
            defer self.release(io, entry);
            return try fileSizeFromFd(entry.fd);
        }

        fn fileSizeWithAdmission(self: *FdCache, namespace: u64, io: std.Io, path: []const u8, control: read_admission.Port) !u64 {
            try control.checkpoint();
            const entry = try self.retainControlled(namespace, io, path, control);
            defer self.release(io, entry);
            const size = if (try control.executionWait()) |boundary| try boundary.run(fileSizeFromFd, .{entry.fd}) else try fileSizeFromFd(entry.fd);
            try control.checkpoint();
            return size;
        }

        fn readFileAllocWithAdmission(self: *FdCache, namespace: u64, io: std.Io, allocator: Allocator, path: []const u8, max_bytes: usize, control: read_admission.Port) ![]u8 {
            try control.checkpoint();
            const entry = try self.retainControlled(namespace, io, path, control);
            defer self.release(io, entry);
            const size = if (try control.executionWait()) |boundary| try boundary.run(fileSizeFromFd, .{entry.fd}) else try fileSizeFromFd(entry.fd);
            try control.checkpoint();
            if (size > max_bytes) return error.StreamTooLong;
            const len = std.math.cast(usize, size) orelse return error.StreamTooLong;
            const out = try allocator.alloc(u8, len);
            errdefer allocator.free(out);
            if (len != 0) {
                if (try control.executionWait()) |boundary| {
                    try boundary.run(readAllAtOffset, .{ entry.fd, out, 0 });
                } else try readAllAtOffset(entry.fd, out, 0);
            }
            const final_size = if (try control.executionWait()) |boundary| try boundary.run(fileSizeFromFd, .{entry.fd}) else try fileSizeFromFd(entry.fd);
            if (final_size > size) return error.StreamTooLong;
            if (final_size < size) return error.EndOfStream;
            try control.checkpoint();
            return out;
        }

        fn readTrailerAllocWithAdmission(self: *FdCache, namespace: u64, io: std.Io, allocator: Allocator, path: []const u8, len: usize, control: read_admission.Port) !FileTrailer {
            try control.checkpoint();
            const entry = try self.retainControlled(namespace, io, path, control);
            defer self.release(io, entry);
            // Both metadata and suffix bytes describe this one inode even if
            // pathname replacement or vacuum retires the cache owner between IOs.
            const size = if (try control.executionWait()) |boundary| try boundary.run(fileSizeFromFd, .{entry.fd}) else try fileSizeFromFd(entry.fd);
            try control.checkpoint();
            if (size < len) return error.EndOfStream;
            const out = try allocator.alloc(u8, len);
            errdefer allocator.free(out);
            if (len != 0) {
                if (try control.executionWait()) |boundary| {
                    try boundary.run(readAllAtOffset, .{ entry.fd, out, size - len });
                } else try readAllAtOffset(entry.fd, out, size - len);
            }
            try control.checkpoint();
            return .{ .bytes = out, .file_size = size };
        }

        fn readTrailerAlloc(self: *FdCache, namespace: u64, io: std.Io, allocator: Allocator, path: []const u8, len: usize) !FileTrailer {
            const entry = try self.retain(namespace, io, path);
            defer self.release(io, entry);

            const size = try fileSizeFromFd(entry.fd);
            if (size < len) return error.EndOfStream;

            const out = try allocator.alloc(u8, len);
            errdefer allocator.free(out);
            try readAllAtOffset(entry.fd, out, size - len);
            return .{ .bytes = out, .file_size = size };
        }

        /// Caller CPU fences cached inode identities without retiring a file
        /// descriptor or freeing metadata under this guard. The bucket walk is
        /// bounded by the shared descriptor ceiling; hash collisions may retire
        /// cache eligibility conservatively. Existing read pins remain valid.
        fn fencePathRetainingEntries(self: *FdCache, namespace: u64, io: std.Io, path: []const u8, original: ?read_admission.Port) !void {
            const hash = namespacedPathHash(namespace, path);
            const shard = self.shardForHash(hash);
            if (original) |control| {
                try @import("../../common/cleanup_wait.zig").lockAtomic(&shard.mutex, io, control);
            } else _ = lockAtomic(&shard.mutex);
            defer shard.mutex.unlock();
            shard.invalidation_epoch +%= 1;
            if (shard.entries.getPtr(hash)) |bucket|
                for (bucket.items) |entry| {
                    entry.invalidated = true;
                };
        }

        fn invalidatePath(self: *FdCache, namespace: u64, path: []const u8) void {
            const path_hash = namespacedPathHash(namespace, path);
            const shard = self.shardForHash(path_hash);
            const locked = lockAtomic(&shard.mutex);
            defer if (locked) shard.mutex.unlock();

            shard.invalidation_epoch +%= 1;

            var matched_any = false;
            if (shard.entries.getPtr(path_hash)) |bucket| {
                for (bucket.items) |entry| {
                    if (entry.namespace != namespace or !std.mem.eql(u8, entry.path, path)) continue;
                    entry.invalidated = true;
                    matched_any = true;
                }
            }

            if (!matched_any) return;

            while (self.findInvalidatedEntryLocked(shard, namespace, path_hash, path)) |entry| {
                if (entry.ref_count != 0) break;
                self.removeEntryLocked(shard, entry);
            }
        }

        fn invalidateTree(self: *FdCache, namespace: u64, path: []const u8) void {
            for (self.shards) |*shard| {
                const locked = lockAtomic(&shard.mutex);
                shard.invalidation_epoch +%= 1;

                var current = shard.lru_head;
                while (current) |entry| {
                    const next = entry.lru_next;
                    if (entry.namespace == namespace and pathContains(path, entry.path)) {
                        entry.invalidated = true;
                        if (entry.ref_count == 0) self.removeEntryLocked(shard, entry);
                    }
                    current = next;
                }
                if (locked) shard.mutex.unlock();
            }
        }

        fn invalidateRename(self: *FdCache, namespace: u64, old_path: []const u8, new_path: []const u8) void {
            self.invalidatePath(namespace, old_path);
            self.invalidatePath(namespace, new_path);
        }

        fn invalidateNamespace(self: *FdCache, namespace: u64) void {
            for (self.shards) |*shard| {
                const locked = lockAtomic(&shard.mutex);
                shard.invalidation_epoch +%= 1;
                var current = shard.lru_head;
                while (current) |entry| {
                    const next = entry.lru_next;
                    if (entry.namespace == namespace) {
                        entry.invalidated = true;
                        if (entry.ref_count == 0) self.removeEntryLocked(shard, entry);
                    }
                    current = next;
                }
                if (locked) shard.mutex.unlock();
            }
        }

        fn retain(self: *FdCache, namespace: u64, io: std.Io, path: []const u8) !*Entry {
            return self.retainControlled(namespace, io, path, null);
        }

        fn retainControlled(self: *FdCache, namespace: u64, io: std.Io, path: []const u8, control: ?read_admission.Port) !*Entry {
            if (control) |budget| try budget.checkpoint();
            const path_hash = namespacedPathHash(namespace, path);
            const shard = self.shardForHash(path_hash);
            // Cache hits already own a stable filename and descriptor. Avoid
            // allocating a transient pathname (mmap/munmap in the process-wide
            // page-allocated pool) on every point read. Misses still allocate
            // outside the mutex and recheck both the entry and mutation epoch.
            {
                const locked = try lockAtomicWithAdmission(&shard.mutex, control);
                defer if (locked) shard.mutex.unlock();
                if (self.findEntryLocked(shard, namespace, path_hash, path)) |existing| {
                    existing.ref_count += 1;
                    existing.last_access = self.nextAccessLocked();
                    self.touchEntryLocked(shard, existing);
                    return existing;
                }
            }
            const owned_path = try self.allocator.dupeSentinel(u8, path, 0);
            var owned_path_active = true;
            errdefer if (owned_path_active) self.allocator.free(owned_path);

            while (true) {
                var observed_invalidation_epoch: u64 = 0;
                const locked = try lockAtomicWithAdmission(&shard.mutex, control);
                if (self.findEntryLocked(shard, namespace, path_hash, path)) |existing| {
                    existing.ref_count += 1;
                    existing.last_access = self.nextAccessLocked();
                    self.touchEntryLocked(shard, existing);
                    if (locked) shard.mutex.unlock();
                    self.allocator.free(owned_path);
                    owned_path_active = false;
                    return existing;
                }
                observed_invalidation_epoch = shard.invalidation_epoch;
                if (locked) shard.mutex.unlock();

                if (control) |budget| try self.reserveDescriptorsWithAdmission(io, 1, budget) else try self.reserveDescriptors(io, 1);
                var descriptor_reserved = true;
                errdefer if (descriptor_reserved) self.releaseDescriptors(io, 1);

                // Open can return a new owned descriptor after authority has
                // stopped. Discard it under the original owner before exposing
                // a failed resumption; the reservation errdefer then retires.
                const fd = if (control) |budget| opened: {
                    if (try budget.executionWait()) |boundary| {
                        break :opened try boundary.runOwned(openReadFd, .{owned_path}, closeFd, .{});
                    }
                    break :opened try openReadFd(owned_path);
                } else try openReadFd(owned_path);
                var fd_open = true;
                errdefer if (fd_open) closeFd(fd);

                if (builtin.is_test and test_fd_cache_pause_after_open.load(.acquire)) {
                    test_fd_cache_open_paused.store(true, .release);
                    while (!test_fd_cache_release_after_open.load(.acquire)) {
                        if (control) |budget| try budget.waitForProgress() else io.sleep(.fromMilliseconds(1), .awake) catch {};
                    }
                }

                if (control) |budget| try budget.checkpoint();
                const entry = try self.allocator.create(Entry);
                var entry_active = true;
                errdefer if (entry_active) self.allocator.destroy(entry);
                entry.* = .{
                    .namespace = namespace,
                    .path_hash = path_hash,
                    .path = owned_path,
                    .fd = fd,
                    .ref_count = 1,
                    .last_access = self.nextAccessLocked(),
                };

                const insertion_locked = try lockAtomicWithAdmission(&shard.mutex, control);
                errdefer if (insertion_locked) shard.mutex.unlock();

                if (self.findEntryLocked(shard, namespace, path_hash, owned_path)) |existing| {
                    existing.ref_count += 1;
                    existing.last_access = self.nextAccessLocked();
                    self.touchEntryLocked(shard, existing);
                    if (insertion_locked) shard.mutex.unlock();
                    closeFd(fd);
                    fd_open = false;
                    self.allocator.destroy(entry);
                    entry_active = false;
                    self.allocator.free(owned_path);
                    owned_path_active = false;
                    descriptor_reserved = false;
                    self.releaseDescriptors(io, 1);
                    return existing;
                }

                if (shard.invalidation_epoch != observed_invalidation_epoch) {
                    if (insertion_locked) shard.mutex.unlock();
                    closeFd(fd);
                    fd_open = false;
                    self.allocator.destroy(entry);
                    entry_active = false;
                    descriptor_reserved = false;
                    self.releaseDescriptors(io, 1);
                    continue;
                }

                const bucket = try shard.entries.getOrPut(self.allocator, path_hash);
                if (!bucket.found_existing) bucket.value_ptr.* = .empty;
                try bucket.value_ptr.append(self.allocator, entry);
                self.linkEntryLocked(shard, entry);
                _ = self.cache_entry_count.fetchAdd(1, .monotonic);
                if (insertion_locked) shard.mutex.unlock();

                entry_active = false;
                fd_open = false;
                owned_path_active = false;
                descriptor_reserved = false;
                return entry;
            }
        }

        fn release(self: *FdCache, io: std.Io, entry: *Entry) void {
            const shard = self.shardForHash(entry.path_hash);
            const locked = lockAtomic(&shard.mutex);
            var removed = false;

            std.debug.assert(entry.ref_count > 0);
            entry.ref_count -= 1;
            entry.last_access = self.nextAccessLocked();
            self.touchEntryLocked(shard, entry);
            if (entry.ref_count == 0 and entry.invalidated) {
                self.removeEntryLocked(shard, entry);
                removed = true;
                if (locked) shard.mutex.unlock();
                self.signalAdmissionChanged(io);
                return;
            }
            if (entry.ref_count == 0 and self.admission_waiters.load(.acquire) > 0) {
                self.removeEntryLocked(shard, entry);
                removed = true;
            }
            if (locked) shard.mutex.unlock();
            if (removed) self.signalAdmissionChanged(io);
        }

        fn tryReserveDescriptors(self: *FdCache, io: std.Io, count: usize) bool {
            if (count == 0) return true;
            if (count > self.capacity - self.persistent_reserve) return false;
            if (!self.admission_mutex.tryLock()) return false;
            defer self.admission_mutex.unlock(io);
            // Do not bypass a queued operation or retain a partial reservation.
            if (self.admission_head != null or !self.makeCapacityAvailableWithoutWaiting(count)) return false;
            _ = self.admitted_descriptors.fetchAdd(count, .acq_rel);
            return true;
        }

        fn reserveDescriptors(self: *FdCache, io: std.Io, count: usize) !void {
            if (count == 0) return;
            if (count > self.capacity - self.persistent_reserve) return error.DescriptorAdmissionCapacityTooSmall;
            self.admission_mutex.lockUncancelable(io);
            defer self.admission_mutex.unlock(io);

            if (self.admission_head == null and self.makeCapacityAvailable(count)) {
                _ = self.admitted_descriptors.fetchAdd(count, .acq_rel);
                return;
            }
            if (self.admission_head == null and self.persistentDescriptorsPreventTransientProgress(count)) {
                _ = self.persistent_admission_failures.fetchAdd(1, .monotonic);
                return error.PersistentDescriptorAdmissionExhausted;
            }

            var waiter = AdmissionWaiter{ .count = count };
            self.enqueueAdmissionWaiter(&waiter);
            _ = self.admission_waiters.fetchAdd(1, .acq_rel);
            _ = self.admission_waits.fetchAdd(1, .monotonic);
            errdefer {
                self.removeAdmissionWaiter(&waiter);
                _ = self.admission_waiters.fetchSub(1, .acq_rel);
                self.admission_changed.broadcast(io);
            }
            while (true) {
                if (self.admission_head == &waiter and self.makeCapacityAvailable(count)) {
                    self.removeAdmissionWaiter(&waiter);
                    _ = self.admission_waiters.fetchSub(1, .acq_rel);
                    _ = self.admitted_descriptors.fetchAdd(count, .acq_rel);
                    self.admission_changed.broadcast(io);
                    return;
                }
                // A transient holder can eventually release and wake us. A
                // pool occupied only by lifetime descriptors cannot make
                // progress until a backend is closed, and the caller may own
                // the cache-open lock required to choose that victim. Surface
                // the persistent-pressure error instead of sleeping forever.
                if (self.admission_head == &waiter and self.persistentDescriptorsPreventTransientProgress(count)) {
                    _ = self.persistent_admission_failures.fetchAdd(1, .monotonic);
                    return error.PersistentDescriptorAdmissionExhausted;
                }
                try self.admission_changed.wait(io, &self.admission_mutex);
            }
        }

        fn reserveDescriptorsWithAdmission(self: *FdCache, io: std.Io, count: usize, control: read_admission.Port) !void {
            try control.checkpoint();
            if (count == 0) return;
            if (count > self.capacity - self.persistent_reserve) return error.DescriptorAdmissionCapacityTooSmall;
            try @import("../../common/bounded_mutex.zig").lock(&self.admission_mutex, io, control);
            var locked = true;
            defer if (locked) self.admission_mutex.unlock(io);
            // Try-only eviction cannot suspend while holding admission. A
            // controlled waiter joins the same weighted FIFO as legacy calls.
            if (self.admission_head == null and self.makeCapacityAvailableWithoutWaiting(count)) {
                _ = self.admitted_descriptors.fetchAdd(count, .acq_rel);
                return;
            }
            var waiter = AdmissionWaiter{ .count = count };
            self.enqueueAdmissionWaiter(&waiter);
            _ = self.admission_waiters.fetchAdd(1, .acq_rel);
            _ = self.admission_waits.fetchAdd(1, .monotonic);
            var queued = true;
            defer if (queued) {
                // Mandatory retirement must unlink the stack-owned node even
                // after authority rejection. It never requests a fresh grant.
                if (!locked) {
                    self.admission_mutex.lockUncancelable(io);
                    locked = true;
                }
                self.removeAdmissionWaiter(&waiter);
                _ = self.admission_waiters.fetchSub(1, .acq_rel);
                self.admission_changed.broadcast(io);
            };
            while (true) {
                // Metadata ownership forbids a yielding CPU checkpoint.
                // Progress/reentry occurs only after releasing this guard.
                try control.checkLifetime();
                if (self.admission_head == &waiter and self.makeCapacityAvailableWithoutWaiting(count)) {
                    self.removeAdmissionWaiter(&waiter);
                    queued = false;
                    _ = self.admission_waiters.fetchSub(1, .acq_rel);
                    _ = self.admitted_descriptors.fetchAdd(count, .acq_rel);
                    self.admission_changed.broadcast(io);
                    return;
                }
                if (self.admission_head == &waiter and self.persistentDescriptorsPreventTransientProgress(count)) {
                    _ = self.persistent_admission_failures.fetchAdd(1, .monotonic);
                    return error.PersistentDescriptorAdmissionExhausted;
                }
                self.admission_mutex.unlock(io);
                locked = false;
                try control.waitForProgress();
                try @import("../../common/bounded_mutex.zig").lock(&self.admission_mutex, io, control);
                locked = true;
            }
        }

        fn persistentDescriptorsPreventTransientProgress(self: *FdCache, count: usize) bool {
            const persistent = self.persistent_descriptors.load(.acquire);
            const admitted = self.admitted_descriptors.load(.acquire);
            if (admitted != persistent) return false;

            const unused_reserve = self.persistent_reserve -| persistent;
            const remaining_capacity = self.capacity -| persistent;
            const open_headroom = if (remaining_capacity >= self.persistent_open_headroom)
                self.persistent_open_headroom
            else
                0;
            const transient_capacity = self.capacity - @max(unused_reserve, open_headroom);
            return count > transient_capacity -| admitted;
        }

        fn makeCapacityAvailable(self: *FdCache, count: usize) bool {
            return self.makeCapacityAvailableMode(count, false);
        }

        fn makeCapacityAvailableWithoutWaiting(self: *FdCache, count: usize) bool {
            return self.makeCapacityAvailableMode(count, true);
        }

        fn makeCapacityAvailableMode(self: *FdCache, count: usize, try_only: bool) bool {
            // The reserve is startup headroom, not a permanent tax. Once
            // lifetime locks consume it, transient leases may use the freed
            // portion while retaining the two-descriptor peak needed by the
            // next create/openat. Without this dynamic accounting, enough
            // open backends can make transient admission wait forever even
            // though the aggregate pool still has unused descriptors.
            const persistent = self.persistent_descriptors.load(.acquire);
            const unused_reserve = self.persistent_reserve -| persistent;
            const remaining_capacity = self.capacity -| persistent;
            const open_headroom = if (remaining_capacity >= self.persistent_open_headroom)
                self.persistent_open_headroom
            else
                0;
            const held_headroom = @max(unused_reserve, open_headroom);
            const transient_capacity = self.capacity - held_headroom;
            if (count > transient_capacity) return false;
            const available_at = transient_capacity - count;
            while (self.admitted_descriptors.load(.acquire) > available_at) {
                if (!(if (try_only) self.tryEvictOne() else self.evictOne())) break;
            }
            return self.admitted_descriptors.load(.acquire) <= available_at;
        }

        /// Admit a descriptor that remains open for a backend lifetime. This
        /// path is deliberately non-blocking: if retained descriptors truly
        /// fill the process storage budget, waiting cannot guarantee progress.
        /// Callers receive an actionable resource error instead of deadlocking
        /// table startup, restore, or shutdown coordination.
        fn reservePersistentDescriptors(self: *FdCache, io: std.Io, count: usize) !void {
            if (count == 0) return;
            if (count > self.capacity) return error.DescriptorAdmissionCapacityTooSmall;

            self.admission_mutex.lockUncancelable(io);
            defer self.admission_mutex.unlock(io);
            const available_at = self.capacity - count;
            while (self.admitted_descriptors.load(.acquire) > available_at and self.evictOne()) {}
            if (self.admitted_descriptors.load(.acquire) > available_at) {
                _ = self.persistent_admission_failures.fetchAdd(1, .monotonic);
                return error.PersistentDescriptorAdmissionExhausted;
            }
            _ = self.admitted_descriptors.fetchAdd(count, .acq_rel);
            _ = self.persistent_descriptors.fetchAdd(count, .acq_rel);
        }

        fn enqueueAdmissionWaiter(self: *FdCache, waiter: *AdmissionWaiter) void {
            waiter.previous = self.admission_tail;
            waiter.next = null;
            if (self.admission_tail) |tail| tail.next = waiter else self.admission_head = waiter;
            self.admission_tail = waiter;
        }

        fn removeAdmissionWaiter(self: *FdCache, waiter: *AdmissionWaiter) void {
            if (waiter.previous) |previous| previous.next = waiter.next else self.admission_head = waiter.next;
            if (waiter.next) |next| next.previous = waiter.previous else self.admission_tail = waiter.previous;
            waiter.previous = null;
            waiter.next = null;
        }

        fn releaseDescriptors(self: *FdCache, io: std.Io, count: usize) void {
            if (count == 0) return;
            const previous = self.admitted_descriptors.fetchSub(count, .acq_rel);
            std.debug.assert(previous >= count);
            _ = self.admission_epoch.fetchAdd(1, .release);
            self.signalAdmissionChanged(io);
        }

        fn releasePersistentDescriptors(self: *FdCache, io: std.Io, count: usize) void {
            if (count == 0) return;
            const previous_persistent = self.persistent_descriptors.fetchSub(count, .acq_rel);
            std.debug.assert(previous_persistent >= count);
            self.releaseDescriptors(io, count);
        }

        fn signalAdmissionChanged(self: *FdCache, io: std.Io) void {
            std.Io.futexWake(io, u32, &self.admission_epoch.raw, std.math.maxInt(u32));
            // The registering waiter always performs a final capacity/eviction
            // check, so skipping the uncontended mutex on the hot release path
            // cannot lose progress.
            if (self.admission_waiters.load(.acquire) == 0) return;
            self.admission_mutex.lockUncancelable(io);
            // Requests have different weights. The FIFO head is the only
            // request allowed to claim newly available capacity.
            self.admission_changed.broadcast(io);
            self.admission_mutex.unlock(io);
        }

        fn admissionEpoch(self: *const FdCache) u32 {
            return self.admission_epoch.load(.acquire);
        }

        fn evictOne(self: *FdCache) bool {
            const start = self.evict_cursor.fetchAdd(1, .monotonic);
            for (0..self.shards.len) |offset| {
                const shard = &self.shards[(start + offset) % self.shards.len];
                const locked = lockAtomic(&shard.mutex);

                var current = shard.lru_head;
                while (current) |entry| {
                    if (entry.ref_count == 0) {
                        self.removeEntryLocked(shard, entry);
                        if (locked) shard.mutex.unlock();
                        return true;
                    }
                    current = entry.lru_next;
                }
                if (locked) shard.mutex.unlock();
            }
            return false;
        }
        // Optional read windows must not wait behind another storage owner.
        // Scan the finite shard registry, skipping locks already held elsewhere.
        fn tryEvictOne(self: *FdCache) bool {
            const start = self.evict_cursor.fetchAdd(1, .monotonic);
            for (0..self.shards.len) |offset| {
                const shard = &self.shards[(start + offset) % self.shards.len];
                if (!shard.mutex.tryLock()) continue;
                defer shard.mutex.unlock();
                var current = shard.lru_head;
                while (current) |entry| {
                    if (entry.ref_count == 0) {
                        self.removeEntryLocked(shard, entry);
                        return true;
                    }
                    current = entry.lru_next;
                }
            }
            return false;
        }

        fn findEntryLocked(self: *FdCache, shard: *Shard, namespace: u64, path_hash: u64, path: []const u8) ?*Entry {
            _ = self;
            const bucket = shard.entries.getPtr(path_hash) orelse return null;
            var i = bucket.items.len;
            while (i > 0) {
                i -= 1;
                const entry = bucket.items[i];
                if (!entry.invalidated and entry.namespace == namespace and std.mem.eql(u8, entry.path, path)) return entry;
            }
            return null;
        }

        fn findInvalidatedEntryLocked(self: *FdCache, shard: *Shard, namespace: u64, path_hash: u64, path: []const u8) ?*Entry {
            _ = self;
            const bucket = shard.entries.getPtr(path_hash) orelse return null;
            var i = bucket.items.len;
            while (i > 0) {
                i -= 1;
                const entry = bucket.items[i];
                if (entry.invalidated and entry.namespace == namespace and std.mem.eql(u8, entry.path, path)) return entry;
            }
            return null;
        }

        fn removeEntryLocked(self: *FdCache, shard: *Shard, entry: *Entry) void {
            const bucket = shard.entries.getPtr(entry.path_hash) orelse unreachable;
            for (bucket.items, 0..) |bucket_entry, i| {
                if (bucket_entry != entry) continue;
                _ = bucket.orderedRemove(i);
                if (bucket.items.len == 0) {
                    var removed = shard.entries.fetchRemove(entry.path_hash) orelse unreachable;
                    removed.value.deinit(self.allocator);
                }
                self.unlinkEntryLocked(shard, entry);
                std.debug.assert(entry.ref_count == 0);
                _ = self.cache_entry_count.fetchSub(1, .monotonic);
                _ = self.admitted_descriptors.fetchSub(1, .monotonic);
                _ = self.admission_epoch.fetchAdd(1, .release);
                entry.deinit(self.allocator);
                self.allocator.destroy(entry);
                return;
            }
            unreachable;
        }

        fn shardForHash(self: *FdCache, path_hash: u64) *Shard {
            return &self.shards[@intCast(path_hash % self.shards.len)];
        }

        fn linkEntryLocked(self: *FdCache, shard: *Shard, entry: *Entry) void {
            _ = self;
            entry.lru_prev = shard.lru_tail;
            entry.lru_next = null;
            if (entry.lru_prev) |prev| {
                prev.lru_next = entry;
            } else {
                shard.lru_head = entry;
            }
            shard.lru_tail = entry;
        }

        fn unlinkEntryLocked(self: *FdCache, shard: *Shard, entry: *Entry) void {
            _ = self;
            if (entry.lru_prev) |prev| {
                prev.lru_next = entry.lru_next;
            } else {
                shard.lru_head = entry.lru_next;
            }
            if (entry.lru_next) |next| {
                next.lru_prev = entry.lru_prev;
            } else {
                shard.lru_tail = entry.lru_prev;
            }
            entry.lru_prev = null;
            entry.lru_next = null;
        }

        fn touchEntryLocked(self: *FdCache, shard: *Shard, entry: *Entry) void {
            if (shard.lru_tail == entry) return;
            self.unlinkEntryLocked(shard, entry);
            self.linkEntryLocked(shard, entry);
        }

        fn nextAccessLocked(self: *FdCache) u64 {
            return self.access_clock.fetchAdd(1, .monotonic) + 1;
        }
    };

var process_native_storage_pool_mutex: std.atomic.Mutex = .unlocked;
var process_native_fd_cache: ?*FdCache = null;
var native_storage_cache_namespace: @import("antfly_platform").atomic.Value(u64) = .init(1);

fn processNativeFdCache() *FdCache {
    const locked = lockAtomic(&process_native_storage_pool_mutex);
    defer if (locked) process_native_storage_pool_mutex.unlock();
    if (process_native_fd_cache) |cache| return cache;

    // The process admission domain intentionally lives for the process
    // lifetime. This prevents one BackendRuntime from invalidating permits or
    // cached descriptors still owned by another runtime during shutdown.
    const cache = std.heap.page_allocator.create(FdCache) catch @panic("OOM");
    cache.* = FdCache.init(std.heap.page_allocator, configuredNativeFdAdmissionCapacity());
    process_native_fd_cache = cache;
    return cache;
}

/// Handle to the process-wide descriptor admission domain shared by every
/// BackendRuntime and all of their native stores. Cached LSM entries and
/// transient opens consume the same aggregate, RLIMIT-derived budget.
pub const NativeStoragePool = struct {
    fd_cache: *FdCache,
    owned_allocator: ?Allocator = null,
    /// Borrowed through every retained native state/path lock and physical join.
    memory_parent: ?MemoryParent = null,
    state_mutex: std.Io.Mutex = .init,
    states_drained: std.Io.Condition = .init,
    state_count: usize = 0,
    states_closing: bool = false,

    fn retainState(self: *NativeStoragePool) !void {
        const io = std.Io.Threaded.global_single_threaded.io();
        self.state_mutex.lockUncancelable(io);
        defer self.state_mutex.unlock(io);
        if (self.states_closing) return error.StorageClosed;
        if (self.state_count == std.math.maxInt(usize)) return error.StorageClosed;
        self.state_count += 1;
    }
    fn releaseState(self: *NativeStoragePool) void {
        const io = std.Io.Threaded.global_single_threaded.io();
        self.state_mutex.lockUncancelable(io);
        defer self.state_mutex.unlock(io);
        std.debug.assert(self.state_count != 0);
        self.state_count -= 1;
        if (self.state_count == 0) self.states_drained.broadcast(io);
    }
    pub fn closeAdmission(self: *NativeStoragePool) void {
        const io = std.Io.Threaded.global_single_threaded.io();
        self.state_mutex.lockUncancelable(io);
        defer self.state_mutex.unlock(io);
        self.states_closing = true;
    }
    pub fn waitDrained(self: *NativeStoragePool) void {
        const io = std.Io.Threaded.global_single_threaded.io();
        self.state_mutex.lockUncancelable(io);
        defer self.state_mutex.unlock(io);
        while (self.state_count != 0) self.states_drained.waitUncancelable(io, &self.state_mutex);
    }
    pub fn retainedStates(self: *NativeStoragePool) usize {
        const io = std.Io.Threaded.global_single_threaded.io();
        self.state_mutex.lockUncancelable(io);
        defer self.state_mutex.unlock(io);
        return self.state_count;
    }

    pub fn init(_: Allocator) NativeStoragePool {
        return .{ .fd_cache = processNativeFdCache() };
    }

    pub fn initWithCapacityForTest(allocator: Allocator, capacity: usize) NativeStoragePool {
        const cache = allocator.create(FdCache) catch @panic("OOM");
        cache.* = FdCache.init(allocator, capacity);
        return .{ .fd_cache = cache, .owned_allocator = allocator };
    }

    pub fn deinit(self: *NativeStoragePool) void {
        self.closeAdmission();
        self.waitDrained();
        if (self.owned_allocator) |allocator| {
            self.fd_cache.deinit();
            allocator.destroy(self.fd_cache);
        }
        self.* = undefined;
    }

    pub fn snapshotStats(self: *const NativeStoragePool) NativeStorageStats {
        return self.fd_cache.snapshotStats();
    }

    pub fn admissionEpoch(self: *const NativeStoragePool) u32 {
        return self.fd_cache.admissionEpoch();
    }

    pub fn waitForAdmissionChange(self: *const NativeStoragePool, io: std.Io, observed_epoch: u32) !void {
        if (self.admissionEpoch() != observed_epoch) return;
        try std.Io.futexWait(io, u32, &self.fd_cache.admission_epoch.raw, observed_epoch);
    }

    pub fn signalAdmissionChanged(self: *NativeStoragePool, io: std.Io) void {
        self.fd_cache.signalAdmissionChanged(io);
    }

    pub fn tryReserveDescriptorsForTest(self: *NativeStoragePool, io: std.Io, count: usize) bool {
        if (!builtin.is_test) unreachable;
        return self.fd_cache.tryReserveDescriptors(io, count);
    }

    pub fn reserveDescriptorsForTest(self: *NativeStoragePool, io: std.Io, count: usize) !void {
        if (!builtin.is_test) unreachable;
        try self.fd_cache.reserveDescriptors(io, count);
    }

    pub fn reserveDescriptorsWithAdmissionForTest(self: *NativeStoragePool, io: std.Io, count: usize, control: read_admission.Port) !void {
        if (!builtin.is_test) unreachable;
        try self.fd_cache.reserveDescriptorsWithAdmission(io, count, control);
    }

    pub fn releaseDescriptorsForTest(self: *NativeStoragePool, io: std.Io, count: usize) void {
        if (!builtin.is_test) unreachable;
        self.fd_cache.releaseDescriptors(io, count);
    }
};

const NativeStorageState = struct {
    // Storage values returned by NativeStorage.storage() borrow their owner and
    // must not outlive it. Operations which do outlive a call (range futures,
    // descriptor permits, and atomic writers) explicitly retain this state.
    // Reclaim it once the owner and all such operations release their refs.
    allocator: Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    closing: std.atomic.Value(bool) = .init(false),
    threaded: std.Io.Threaded,
    fd_cache: *FdCache,
    cache_namespace: u64,
    memory: ?*RuntimeMemory.Owner,
    pool: ?*NativeStoragePool,

    fn create(backing: Allocator, kind: RuntimeKind, pool: ?*NativeStoragePool) !*NativeStorageState {
        if (kind != .threaded) return error.UnsupportedEventedIoRuntime;
        if (pool) |owner| try owner.retainState();
        errdefer if (pool) |owner| owner.releaseState();
        const parent = if (pool) |owner| owner.memory_parent else null;
        const memory = if (parent) |value| try RuntimeMemory.Owner.create(backing, value) else null;
        errdefer if (memory) |owner| owner.destroy();
        const allocator = if (memory) |owner| owner.allocator() else backing;
        const state = try allocator.create(NativeStorageState);
        errdefer allocator.destroy(state);
        state.* = undefined;
        state.allocator = allocator;
        state.refs = .init(1);
        state.closing = .init(false);
        // Native storage state may outlive its owning DB while range futures
        // drain. Prevent that retained runtime from growing without a bound.
        state.memory = memory;
        state.pool = pool;
        state.threaded = threaded_io_limits.initOwnedService(allocator, if (memory) |owner| owner.stackAllocator() else null);
        errdefer state.threaded.deinit();
        // NativeStorage.init is used by repository, recovery, and status
        // helpers as well as BackendRuntime-owned stores. All production
        // handles must join the same process admission domain; isolated caches
        // are available only through an explicit test pool.
        state.fd_cache = if (pool) |shared| shared.fd_cache else processNativeFdCache();
        state.cache_namespace = native_storage_cache_namespace.fetchAdd(1, .monotonic);
        return state;
    }

    fn fdCache(self: *NativeStorageState) *FdCache {
        return self.fd_cache;
    }

    fn fdCacheConst(self: *const NativeStorageState) *const FdCache {
        return self.fd_cache;
    }

    fn acquireFdPermit(self: *NativeStorageState) !NativeFdPermit {
        const retained = try self.retain();
        errdefer retained.release();
        const io = retained.threaded.io();
        const cache = retained.fdCache();
        try cache.reserveDescriptors(io, 1);
        return .{
            .state = retained,
            .cache = cache,
            .io = io,
            .count = 1,
        };
    }

    fn acquireFdPermits(self: *NativeStorageState, count: usize) !NativeFdPermit {
        const retained = try self.retain();
        errdefer retained.release();
        const io = retained.threaded.io();
        const cache = retained.fdCache();
        try cache.reserveDescriptors(io, count);
        return .{
            .state = retained,
            .cache = cache,
            .io = io,
            .count = count,
        };
    }

    fn acquireFdPermitWithAdmission(self: *NativeStorageState, control: read_admission.Port) !NativeFdPermit {
        const retained = try self.acquireLease();
        errdefer retained.release();
        const io = retained.threaded.io();
        const cache = retained.fdCache();
        const Borrow = struct {
            original: read_admission.Port,
            io: std.Io,
            fn checkpoint(raw: *anyopaque) !void {
                const borrowed: *@This() = @ptrCast(@alignCast(raw));
                try borrowed.original.checkpoint();
            }
            fn lifetime(raw: *anyopaque) !void {
                const borrowed: *@This() = @ptrCast(@alignCast(raw));
                try borrowed.original.checkLifetime();
            }
            fn deadline(raw: *anyopaque) !?u64 {
                const borrowed: *@This() = @ptrCast(@alignCast(raw));
                return borrowed.original.deadlineNs();
            }
            fn progress(raw: *anyopaque) !void {
                const borrowed: *@This() = @ptrCast(@alignCast(raw));
                // Reenter before retrying descriptor metadata. The caller
                // need not supply a backend-specific progress callback.
                try @import("../../common/cleanup_wait.zig").waitForRetry(borrowed.io, borrowed.original);
            }
        };
        var borrow: Borrow = .{ .original = control, .io = io };
        const payload: read_admission.Payload = .{ .ptr = &borrow, .vtable = &.{ .checkpoint = Borrow.checkpoint, .check_lifetime = Borrow.lifetime, .deadline_ns = Borrow.deadline, .wait_for_progress = Borrow.progress }, .execution_wait = try control.executionWaitAfterLifetime() };
        try cache.reserveDescriptorsWithAdmission(io, 1, .init(&payload));
        return .{ .state = retained, .cache = cache, .io = io, .count = 1 };
    }

    fn tryAcquireFdPermits(self: *NativeStorageState, count: usize) !?NativeFdPermit {
        const retained = try self.retain();
        const io = retained.threaded.io();
        const cache = retained.fdCache();
        if (!cache.tryReserveDescriptors(io, count)) {
            retained.release();
            return null;
        }
        return .{ .state = retained, .cache = cache, .io = io, .count = count };
    }

    fn retain(self: *NativeStorageState) !*NativeStorageState {
        while (true) {
            const current = self.refs.load(.acquire);
            if (current == 0) return error.StorageClosed;
            if (self.refs.cmpxchgWeak(current, current + 1, .acq_rel, .acquire)) |_| {
                continue;
            }
            return self;
        }
    }

    fn acquireLease(self: *NativeStorageState) !*NativeStorageState {
        if (self.closing.load(.acquire)) return error.StorageClosed;
        const retained = try self.retain();
        if (self.closing.load(.acquire)) {
            retained.release();
            return error.StorageClosed;
        }
        return retained;
    }

    fn closeStorageRef(self: *NativeStorageState) void {
        self.closing.store(true, .release);
        self.release();
    }

    fn release(self: *NativeStorageState) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        // Cached descriptors are shared for aggregate eviction, but file
        // identity is scoped to this storage generation. Invalidate the
        // namespace before destroying the I/O runtime so a restore or reopen
        // at the same pathname can never observe the old inode.
        self.fd_cache.invalidateNamespace(self.cache_namespace);
        self.fd_cache.signalAdmissionChanged(self.threaded.io());
        self.threaded.deinit();
        const allocator = self.allocator;
        const memory = self.memory;
        const pool = self.pool;
        allocator.destroy(self);
        if (memory) |owner| owner.destroy();
        if (pool) |owner| owner.releaseState();
    }

    /// Mutation paths call invalidation before and after the filesystem
    /// operation. The first pass retires cached entries; the second changes
    /// the epoch for a miss that opened the old inode inside the mutation
    /// window but had not inserted it yet.
    fn invalidatePath(self: *NativeStorageState, path: []const u8) void {
        self.fdCache().invalidatePath(self.cache_namespace, path);
        self.fdCache().signalAdmissionChanged(self.threaded.io());
    }

    fn invalidateRename(self: *NativeStorageState, old_path: []const u8, new_path: []const u8) void {
        self.fdCache().invalidateRename(self.cache_namespace, old_path, new_path);
        self.fdCache().signalAdmissionChanged(self.threaded.io());
    }

    fn invalidateTree(self: *NativeStorageState, path: []const u8) void {
        self.fdCache().invalidateTree(self.cache_namespace, path);
        self.fdCache().signalAdmissionChanged(self.threaded.io());
    }
};

/// A permit from the runtime-wide storage descriptor budget. Cached LSM files
/// retain one for the cache entry lifetime; short-lived consumers such as
/// full-text segment mmap release theirs as soon as the source fd is closed.
pub const NativeFdPermit = struct {
    state: *NativeStorageState,
    cache: *FdCache,
    io: std.Io,
    count: usize,
    active: bool = true,

    pub fn release(self: *NativeFdPermit) void {
        if (!self.active) return;
        self.active = false;
        self.cache.releaseDescriptors(self.io, self.count);
        self.state.release();
    }

    fn take(self: *NativeFdPermit) NativeFdPermit {
        std.debug.assert(self.active);
        std.debug.assert(self.count == 1);
        const owned = self.*;
        self.active = false;
        return owned;
    }

    pub fn splitOne(self: *NativeFdPermit) !NativeFdPermit {
        if (!self.active or self.count <= 1) return error.InvalidNativeFdPermit;
        const retained = try self.state.retain();
        self.count -= 1;
        return .{
            .state = retained,
            .cache = self.cache,
            .io = self.io,
            .count = 1,
        };
    }
};

/// Deterministic physical-worker retirement probes, active only in tests.
pub const NativeRangeReadTestControl = struct {
    pub var pause: std.atomic.Value(bool) = .init(false);
    pub var started: std.atomic.Value(bool) = .init(false);
    pub var release: std.atomic.Value(bool) = .init(false);
    pub var completed: std.atomic.Value(bool) = .init(false);
};

const NativeRangeReadFuture = if (!supports_native_storage or builtin.os.tag == .freestanding)
    struct {}
else
    struct {
        allocator: Allocator,
        state: *NativeStorageState,
        path: []u8,
        offset: u64,
        len: usize,
        io: std.Io,
        future: std.Io.Future(void) = undefined,
        bytes: ?[]u8 = null,
        err: ?anyerror = null,

        const vtable: RangeReadFuture.VTable = .{
            .wait = wait,
            .cancel = cancel,
        };

        fn create(read_runtime: ReadRuntime, state: *NativeStorageState, allocator: Allocator, path: []const u8, offset: u64, len: usize) !RangeReadFuture {
            const retained = try state.retain();
            errdefer retained.release();

            const self = try allocator.create(NativeRangeReadFuture);
            errdefer allocator.destroy(self);

            const path_copy = try allocator.dupe(u8, path);
            errdefer allocator.free(path_copy);

            self.* = .{
                .allocator = allocator,
                .state = retained,
                .path = path_copy,
                .offset = offset,
                .len = len,
                .io = read_runtime.io,
            };
            // Parallelism is optional for a range read. At the runtime's
            // worker limit, async runs on the caller instead of turning
            // temporary saturation into a failed query or indexing worker.
            self.future = read_runtime.io.async(run, .{self});
            return .{
                .ptr = self,
                .vtable = &vtable,
            };
        }

        fn run(self: *NativeRangeReadFuture) void {
            self.bytes = read(self) catch |err| {
                self.err = err;
                return;
            };
        }

        fn read(self: *NativeRangeReadFuture) ![]u8 {
            if (comptime supports_posix_fd_cache) {
                return try self.state.fdCache().readRangeAlloc(self.state.cache_namespace, self.io, self.allocator, self.path, self.offset, self.len);
            }
            return try readFileRangeWithIo(self.io, self.allocator, self.path, self.offset, self.len);
        }

        fn wait(ptr: *anyopaque) ![]u8 {
            const self: *NativeRangeReadFuture = @ptrCast(@alignCast(ptr));
            self.future.await(self.io);
            const allocator = self.allocator;
            defer {
                allocator.free(self.path);
                self.state.release();
                allocator.destroy(self);
            }
            if (self.err) |err| return err;
            const bytes = self.bytes orelse return error.CanceledRangeRead;
            self.bytes = null;
            return bytes;
        }

        fn cancel(ptr: *anyopaque) void {
            const self: *NativeRangeReadFuture = @ptrCast(@alignCast(ptr));
            self.future.cancel(self.io);
            const allocator = self.allocator;
            if (self.bytes) |bytes| allocator.free(bytes);
            allocator.free(self.path);
            self.state.release();
            allocator.destroy(self);
        }
    };

const ControlledNativeRangeReadFuture = if (!supports_native_storage or builtin.os.tag == .freestanding)
    struct {}
else
    struct {
        allocator: Allocator,
        state: *NativeStorageState,
        path: []u8,
        offset: u64,
        len: usize,
        io: std.Io,
        future: ?std.Io.Future(void) = null,
        entry: if (supports_posix_fd_cache) ?*FdCache.Entry else void = if (supports_posix_fd_cache) null else {},
        done: std.atomic.Value(bool) = .init(false),
        bytes: ?[]u8 = null,
        err: ?anyerror = null,

        const vtable: RangeReadFuture.VTable = .{
            .wait = wait,
            .cancel = cancel,
            .wait_with_admission = waitWithAdmission,
        };

        fn create(read_runtime: ReadRuntime, state: *NativeStorageState, allocator: Allocator, path: []const u8, offset: u64, len: usize) !RangeReadFuture {
            return createControlled(read_runtime, state, allocator, path, offset, len, null);
        }

        fn createControlled(read_runtime: ReadRuntime, state: *NativeStorageState, allocator: Allocator, path: []const u8, offset: u64, len: usize, control: ?read_admission.Port) !RangeReadFuture {
            const retained = try state.retain();
            errdefer retained.release();
            const self = try allocator.create(ControlledNativeRangeReadFuture);
            errdefer allocator.destroy(self);
            const path_copy = try allocator.dupe(u8, path);
            errdefer allocator.free(path_copy);
            self.* = .{ .allocator = allocator, .state = retained, .path = path_copy, .offset = offset, .len = len, .io = read_runtime.io };
            // All native POSIX allocations and descriptor/cache preparation
            // run on the charged caller. The worker receives only a retained
            // FD and a fixed destination; it never borrows caller authority.
            self.prepare(control) catch |err| {
                self.err = err;
                self.done.store(true, .release);
                return .{ .ptr = self, .vtable = &vtable };
            };
            if (comptime supports_posix_fd_cache) if (len == 0) {
                self.releaseEntry();
                self.done.store(true, .release);
                return .{ .ptr = self, .vtable = &vtable };
            };
            self.future = read_runtime.io.concurrent(run, .{self}) catch {
                // No worker capacity: use the same prepared operation inline.
                // Only the physical read releases the original runnable slot.
                if (control) |budget| {
                    if ((budget.executionWait() catch |err| failed: {
                        self.err = err;
                        break :failed null;
                    })) |boundary| {
                        boundary.run(perform, .{self}) catch |err| {
                            self.err = err;
                        };
                    } else if (self.err == null) perform(self) catch |err| {
                        self.err = err;
                    };
                } else perform(self) catch |err| {
                    self.err = err;
                };
                self.releaseEntry();
                self.done.store(true, .release);
                return .{ .ptr = self, .vtable = &vtable };
            };
            return .{ .ptr = self, .vtable = &vtable };
        }

        fn prepare(self: *ControlledNativeRangeReadFuture, control: ?read_admission.Port) !void {
            if (control) |budget| try budget.checkpoint();
            if (comptime supports_posix_fd_cache) {
                self.bytes = try self.allocator.alloc(u8, self.len);
                self.entry = try self.state.fdCache().retainControlled(self.state.cache_namespace, self.state.threaded.io(), self.path, control);
            }
            if (control) |budget| try budget.checkpoint();
        }

        fn run(self: *ControlledNativeRangeReadFuture) void {
            defer self.done.store(true, .release);
            // Physical retirement releases descriptor use before a later
            // prefetch needs capacity. Result buffers remain owned until join.
            defer self.releaseEntry();
            if (builtin.is_test and NativeRangeReadTestControl.pause.load(.acquire)) {
                defer NativeRangeReadTestControl.completed.store(true, .release);
                NativeRangeReadTestControl.started.store(true, .release);
                while (!NativeRangeReadTestControl.release.load(.acquire)) {
                    self.io.sleep(.fromMilliseconds(1), .awake) catch |err| {
                        self.err = err;
                        return;
                    };
                }
                perform(self) catch |err| {
                    self.err = err;
                };
                return;
            }
            perform(self) catch |err| {
                self.err = err;
            };
        }

        fn perform(self: *ControlledNativeRangeReadFuture) !void {
            if (comptime supports_posix_fd_cache) {
                if (self.len != 0) try readAllAtOffset(self.entry.?.fd, self.bytes.?, self.offset);
            } else {
                self.bytes = try readFileRangeWithIo(self.io, self.allocator, self.path, self.offset, self.len);
            }
        }

        fn join(self: *ControlledNativeRangeReadFuture) !void {
            if (self.future) |*future| future.await(self.io);
            self.future = null;
        }

        fn wait(ptr: *anyopaque) ![]u8 {
            return waitControlled(ptr, null);
        }

        fn waitWithAdmission(ptr: *anyopaque, control: read_admission.Port) ![]u8 {
            return waitControlled(ptr, control);
        }

        fn waitControlled(ptr: *anyopaque, control: ?read_admission.Port) ![]u8 {
            const self: *ControlledNativeRangeReadFuture = @ptrCast(@alignCast(ptr));
            defer self.retire();
            if (control) |budget| try budget.checkpoint();
            if (self.future != null) {
                if (control) |budget| {
                    if (!self.done.load(.acquire)) {
                        if (try budget.executionWait()) |boundary| try boundary.run(join, .{self}) else try self.join();
                    } else try self.join();
                } else try self.join();
            }
            if (control) |budget| try budget.checkpoint();
            if (self.err) |err| return err;
            const bytes = self.bytes orelse return error.CanceledRangeRead;
            self.bytes = null;
            return bytes;
        }

        fn releaseEntry(self: *ControlledNativeRangeReadFuture) void {
            if (comptime supports_posix_fd_cache) if (self.entry) |entry| {
                self.state.fdCache().release(self.state.threaded.io(), entry);
                self.entry = null;
            };
        }

        fn retire(self: *ControlledNativeRangeReadFuture) void {
            // Even a failed begin/checkpoint must physically join before
            // releasing the buffer, descriptor, state or allocator owner.
            if (self.future) |*future| future.cancel(self.io);
            if (self.bytes) |bytes| self.allocator.free(bytes);
            self.releaseEntry();
            const allocator = self.allocator;
            allocator.free(self.path);
            self.state.release();
            allocator.destroy(self);
        }

        fn cancel(ptr: *anyopaque) void {
            const self: *ControlledNativeRangeReadFuture = @ptrCast(@alignCast(ptr));
            self.retire();
        }
    };

/// Test-only physical retirement latch. It never participates in production
/// dispatch and does not retain an admission borrow in a filesystem worker.
pub const NativeSyncTestControl = struct {
    pub var pause: std.atomic.Value(bool) = .init(false);
    pub var started: std.atomic.Value(bool) = .init(false);
    pub var release: std.atomic.Value(bool) = .init(false);
    pub var calls: std.atomic.Value(usize) = .init(0);
    pub var reads: std.atomic.Value(usize) = .init(0);
    pub var appends: std.atomic.Value(usize) = .init(0);
    pub var metadata: std.atomic.Value(usize) = .init(0);
    pub var control_failure_after_rename: std.atomic.Value(bool) = .init(false);
    pub var append_failure_after: std.atomic.Value(usize) = .init(std.math.maxInt(usize));
    pub var sync_path_hashes: [2]std.atomic.Value(u64) = .{ .init(0), .init(0) };
    fn observe(read: bool, path: []const u8) void {
        if (read) {
            _ = reads.fetchAdd(1, .release);
        } else {
            const index = calls.fetchAdd(1, .release);
            if (index < sync_path_hashes.len) sync_path_hashes[index].store(std.hash.Wyhash.hash(0, path), .release);
        }
    }
};

const NativePhysicalOperation = if (!supports_native_storage or builtin.os.tag == .freestanding) struct {} else struct {
    const Kind = enum { contents, directory, control_read };
    allocator: Allocator,
    permit: NativeFdPermit,
    path: []u8,
    kind: Kind,
    buffer: ?[]u8 = null,
    file_size: u64 = 0,
    read_len: usize = 0,
    future: ?std.Io.Future(void) = null,
    done: std.atomic.Value(bool) = .init(false),
    failure: ?anyerror = null,

    const vtable: SyncOperation.VTable = .{ .wait_with_admission = waitWithAdmission, .deinit = deinit };
    const read_vtable: ControlFileReadOperation.VTable = .{ .wait_with_admission = waitReadWithAdmission, .deinit = deinit };

    fn createControlRead(state: *NativeStorageState, path: []const u8, limit: usize, original: read_admission.Port) !ControlFileReadOperation {
        if (limit == 0 or limit > max_control_file_bytes) return error.ResourceBudgetExceeded;
        const operation = try prepare(state, path, .control_read, limit, original);
        return .{ .ptr = operation, .vtable = &read_vtable };
    }

    fn create(state: *NativeStorageState, path: []const u8, kind: Kind, original: read_admission.Port) !SyncOperation {
        const operation = try prepare(state, path, kind, 0, original);
        return .{ .ptr = operation, .vtable = &vtable };
    }

    fn prepare(state: *NativeStorageState, path: []const u8, kind: Kind, limit: usize, original: read_admission.Port) !*NativePhysicalOperation {
        if (comptime !supports_posix_fd_cache) return error.ExecutionWaitUnsupported;
        if (path.len >= std.fs.max_path_bytes) return error.NameTooLong;
        _ = (try original.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
        _ = (try original.deadlineNs()) orelse return error.ExecutionWaitUnsupported;
        try original.checkpoint();
        var permit = try state.acquireFdPermitWithAdmission(original);
        errdefer permit.release();
        // The retained state owns this allocator and executor. Holding one
        // shared descriptor permit until deinit bounds accepted operation
        // metadata as well as physical descriptors, including rejected waits.
        const allocator = state.allocator;
        const self = try allocator.create(NativePhysicalOperation);
        errdefer allocator.destroy(self);
        const path_copy = try allocator.dupe(u8, path);
        errdefer allocator.free(path_copy);
        const buffer = if (kind == .control_read) try allocator.alloc(u8, limit) else null;
        errdefer if (buffer) |bytes| allocator.free(bytes);
        self.* = .{ .allocator = allocator, .permit = permit, .path = path_copy, .kind = kind, .buffer = buffer };
        try original.checkpoint();
        // Refuse dispatch failure instead of doing an unbounded synchronous
        // fallback. Preparation and descriptor admission ran on caller CPU.
        self.future = try permit.io.concurrent(run, .{self});
        return self;
    }

    fn run(self: *NativePhysicalOperation) void {
        defer self.done.store(true, .release);
        if (builtin.is_test and NativeSyncTestControl.pause.load(.acquire)) {
            NativeSyncTestControl.started.store(true, .release);
            while (!NativeSyncTestControl.release.load(.acquire))
                self.permit.io.sleep(.fromMilliseconds(1), .awake) catch {};
        }
        if (builtin.is_test) NativeSyncTestControl.observe(self.kind == .control_read, self.path);
        // This worker only opens, reads/syncs and closes the prepared path.
        // It performs no WAL decoding, flush, manifest work or admission call.
        const result = switch (self.kind) {
            .contents => fs_paths.syncFilePortable(self.permit.io, self.path),
            .directory => fs_paths.syncDirPortable(self.permit.io, self.path),
            .control_read => self.readControl(),
        };
        result catch |cause| {
            self.failure = cause;
        };
    }

    fn readControl(self: *NativePhysicalOperation) !void {
        const io = self.permit.io;
        const file = if (std.fs.path.isAbsolute(self.path)) try std.Io.Dir.openFileAbsolute(io, self.path, .{}) else try std.Io.Dir.cwd().openFile(io, self.path, .{});
        defer file.close(io);
        self.file_size = (try file.stat(io)).size;
        const buffer = self.buffer.?;
        // Oversized metadata is reported without hydrating or decoding it.
        if (self.file_size > buffer.len) return;
        var reader = file.reader(io, &.{});
        const bytes = buffer[0..@intCast(self.file_size)];
        try reader.interface.readSliceAll(bytes);
        self.read_len = bytes.len;
    }

    fn waitReadWithAdmission(raw: *anyopaque, original: read_admission.Port) !ControlFileResult {
        try waitWithAdmission(raw, original);
        const self: *NativePhysicalOperation = @ptrCast(@alignCast(raw));
        return .{ .file_size = self.file_size, .bytes = self.buffer.?[0..self.read_len] };
    }

    fn waitWithAdmission(raw: *anyopaque, original: read_admission.Port) !void {
        const self: *NativePhysicalOperation = @ptrCast(@alignCast(raw));
        const Ready = struct {
            fn get(ptr: *anyopaque) anyerror!bool {
                const operation: *NativePhysicalOperation = @ptrCast(@alignCast(ptr));
                return operation.done.load(.acquire);
            }
        };
        try @import("../../common/cleanup_wait.zig").until(self.permit.io, original, self, Ready.get, null);
        // Only successful original reentry authorizes the physical join and
        // result observation. A rejected wait leaves every resource owned.
        if (self.future) |*future| future.await(self.permit.io);
        self.future = null;
        if (self.failure) |cause| return cause;
    }

    fn deinit(raw: *anyopaque) void {
        const self: *NativePhysicalOperation = @ptrCast(@alignCast(raw));
        if (self.future) |*future| future.await(self.permit.io);
        const allocator = self.allocator;
        var permit = self.permit;
        if (self.buffer) |buffer| allocator.free(buffer);
        allocator.free(self.path);
        allocator.destroy(self);
        // Release last: the storage state may destroy its executor/allocator.
        // Never release it from its own running physical callback.
        permit.release();
    }
};

/// Owns a bounded mailbox and an already admitted worker. The physical worker
/// invokes no allocator, descriptor admission, task dispatcher or original
/// control callback. Callers prepare inputs and observe results on admitted CPU.
fn nativeAppendResourceBytes(_: *anyopaque, len: usize) !u64 {
    if (comptime !supports_native_storage or builtin.os.tag == .freestanding) return error.ExecutionWaitUnsupported;
    return std.math.add(u64, @sizeOf(NativeDurabilityResources), len) catch error.ResourceBudgetExceeded;
}

const NativeDurabilityResources = if (!supports_native_storage or builtin.os.tag == .freestanding) struct {} else struct {
    const Phase = enum(u8) { idle, queued, completed, stopping };
    const Kind = enum { control_read, contents, directory, append, control_replace, create_directory, private_create, private_write, private_publish, private_discard };
    allocator: Allocator,
    permit: NativeFdPermit,
    future: std.Io.Future(void) = undefined,
    wake: std.Io.Event = .unset,
    phase: std.atomic.Value(Phase) = .init(.idle),
    active: bool = false, // Serialized caller ownership; never read by worker.
    joined: bool = false,
    kind: Kind = .contents,
    path: [std.fs.max_path_bytes]u8 = undefined,
    path_len: usize = 0,
    temporary: [std.fs.max_path_bytes]u8 = undefined,
    temporary_len: usize = 0,
    buffer: [max_atomic_write_bytes]u8 = undefined,
    limit: usize = 0,
    result: ControlFileResult = .{ .file_size = 0, .bytes = &.{} },
    failure: ?anyerror = null,
    append_bytes: ?[]u8 = null,
    append_create: bool = false,
    invalidation_pending: bool = false,
    private_owned: bool = false, // Caller only; blocks unrelated commands.
    private_file: ?std.Io.File = null, // Worker only while an operation runs.
    private_temporary_owned: bool = false,
    private_final: [std.fs.max_path_bytes]u8 = undefined,
    private_final_len: usize = 0,
    private_offset: u64 = 0,

    const vtable: DurabilityResources.VTable = .{ .begin_read = beginRead, .begin_sync = beginSync, .deinit = deinit, .retire_with_admission = retireWithAdmission, .begin_append = beginAppend, .begin_control_replace = beginControlReplace, .begin_create_directory = beginCreateDirectory, .begin_private_create = beginPrivateCreate, .begin_private_write = beginPrivateWrite, .begin_private_publish = beginPrivatePublish, .begin_private_discard = beginPrivateDiscard };
    const sync_vtable: SyncOperation.VTable = .{ .wait_with_admission = wait, .deinit = retireOperation };
    const read_vtable: ControlFileReadOperation.VTable = .{ .wait_with_admission = waitRead, .deinit = retireOperation };

    fn requireControl(original: read_admission.Port) !void {
        _ = (try original.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
        _ = (try original.deadlineNs()) orelse return error.ExecutionWaitUnsupported;
        try original.checkpoint();
    }

    fn create(state: *NativeStorageState, original: read_admission.Port, append_bytes: ?[]const u8) !?DurabilityResources {
        if (comptime !supports_posix_fd_cache) return error.ExecutionWaitUnsupported;
        try requireControl(original);
        const retained = try state.acquireLease();
        const io = retained.threaded.io();
        const cache = retained.fdCache();
        // This try honors existing FD waiters and never waits for cache guards
        // or eviction. A failed compound acquisition holds no partial grant.
        if (!cache.tryReserveDescriptors(io, 1)) {
            retained.release();
            return null;
        }
        var permit: NativeFdPermit = .{ .state = retained, .cache = cache, .io = io, .count = 1 };
        errdefer permit.release();
        const allocator = state.allocator;
        const self = try allocator.create(NativeDurabilityResources);
        errdefer allocator.destroy(self);
        self.* = .{ .allocator = allocator, .permit = permit };
        if (append_bytes) |bytes| {
            self.append_bytes = try allocator.alloc(u8, bytes.len);
        }
        errdefer if (self.append_bytes) |bytes| allocator.free(bytes);
        if (append_bytes) |bytes| {
            var offset: usize = 0;
            while (offset < bytes.len) {
                try original.checkpoint();
                const end = offset + @min(bytes.len - offset, 16 * 1024);
                @memcpy(self.append_bytes.?[offset..end], bytes[offset..end]);
                offset = end;
            }
        }
        try original.checkpoint();
        // Dispatch refusal has no inline fallback. Once accepted, the single
        // worker remains available even if subsequent dispatch is saturated.
        self.future = try io.concurrent(run, .{self});
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn prepareCommand(self: *NativeDurabilityResources, path: []const u8, kind: Kind, limit: usize, create_if_missing: bool, original: read_admission.Port) !void {
        try requireControl(original);
        if (self.active or self.joined or self.phase.load(.acquire) == .stopping) return error.ResourceBudgetExceeded;
        const private = kind == .private_create or kind == .private_write or kind == .private_publish or kind == .private_discard;
        if (self.private_owned and !private) return error.ResourceBudgetExceeded;
        if (path.len >= self.path.len) return error.NameTooLong;
        if (kind == .control_read and (limit == 0 or limit > max_control_file_bytes)) return error.ResourceBudgetExceeded;
        std.debug.assert(self.phase.load(.acquire) == .idle);
        @memcpy(self.path[0..path.len], path);
        self.path_len = path.len;
        self.kind = kind;
        self.limit = limit;
        self.append_create = create_if_missing;
        self.invalidation_pending = false;
        self.failure = null;
        self.result = .{ .file_size = 0, .bytes = &.{} };
        self.active = true;
    }

    fn submit(self: *NativeDurabilityResources) void {
        self.phase.store(.queued, .release);
        self.wake.set(self.permit.io);
    }

    fn begin(self: *NativeDurabilityResources, path: []const u8, kind: Kind, limit: usize, create_if_missing: bool, original: read_admission.Port) !void {
        try self.prepareCommand(path, kind, limit, create_if_missing, original);
        self.submit();
    }

    fn beginControlReplace(raw: *anyopaque, path: []const u8, bytes: []const u8, original: read_admission.Port) !SyncOperation {
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        if (bytes.len > max_control_file_bytes) return error.ResourceBudgetExceeded;
        // Reserved to the publication owner; a stale crash temporary is safe
        // to replace under that same exclusive filesystem publication lock.
        var temporary: [std.fs.max_path_bytes]u8 = undefined;
        const tmp = std.fmt.bufPrint(&temporary, "{s}.admitted-tmp", .{path}) catch return error.NameTooLong;
        try self.permit.cache.fencePathRetainingEntries(self.permit.state.cache_namespace, self.permit.io, path, original);
        try self.prepareCommand(path, .control_replace, bytes.len, false, original);
        @memcpy(self.buffer[0..bytes.len], bytes);
        @memcpy(self.temporary[0..tmp.len], tmp);
        self.temporary_len = tmp.len;
        self.invalidation_pending = true;
        self.submit();
        return .{ .ptr = self, .vtable = &sync_vtable };
    }

    fn beginCreateDirectory(raw: *anyopaque, path: []const u8, original: read_admission.Port) !SyncOperation {
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        try self.begin(path, .create_directory, 0, false, original);
        return .{ .ptr = self, .vtable = &sync_vtable };
    }

    fn replaceControl(self: *NativeDurabilityResources, path: []const u8) !void {
        const io = self.permit.io;
        const temporary = self.temporary[0..self.temporary_len];
        // A reserved stale crash temporary may be removed under publication.
        // Exclusive creation avoids following a replacement symlink.
        deleteFilePathWithIo(io, temporary) catch |cause| {
            if (cause != error.FileNotFound) return cause;
        };
        errdefer deleteFilePathWithIo(io, temporary) catch {};
        {
            const file = try openFilePathForWriteWithIo(io, temporary, .{ .exclusive = true });
            defer file.close(io);
            var writer = file.writer(io, &.{});
            writer.interface.writeAll(self.buffer[0..self.limit]) catch |cause| return writer.err orelse cause;
            writer.end() catch |cause| return writer.err orelse cause;
            try file.sync(io);
        }
        try std.Io.Dir.cwd().rename(temporary, std.Io.Dir.cwd(), path, io);
        if (builtin.is_test and NativeSyncTestControl.control_failure_after_rename.load(.acquire)) return error.InputOutput;
        const parent = std.fs.path.dirname(path) orelse ".";
        try fs_paths.syncDirPortable(io, parent);
    }

    fn createDirectory(self: *NativeDurabilityResources, path: []const u8) !void {
        const io = self.permit.io;
        std.Io.Dir.cwd().createDir(io, path, .default_dir) catch |cause| {
            if (cause != error.PathAlreadyExists) return cause;
            var directory = try std.Io.Dir.cwd().openDir(io, path, .{});
            directory.close(io);
        };
    }

    fn beginRead(raw: *anyopaque, path: []const u8, limit: usize, original: read_admission.Port) !ControlFileReadOperation {
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        try self.begin(path, .control_read, limit, false, original);
        return .{ .ptr = self, .vtable = &read_vtable };
    }

    fn beginSync(raw: *anyopaque, path: []const u8, parent: bool, original: read_admission.Port) !SyncOperation {
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        if (path.len >= self.path.len) return error.NameTooLong;
        const selected = if (parent) std.fs.path.dirname(path) orelse if (std.fs.path.isAbsolute(path)) "/" else "." else path;
        try self.begin(selected, if (parent) .directory else .contents, 0, false, original);
        return .{ .ptr = self, .vtable = &sync_vtable };
    }

    fn beginAppend(raw: *anyopaque, path: []const u8, create_if_missing: bool, original: read_admission.Port) !SyncOperation {
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        if (self.append_bytes == null) return error.ExecutionWaitUnsupported;
        try self.begin(path, .append, 0, create_if_missing, original);
        return .{ .ptr = self, .vtable = &sync_vtable };
    }

    fn appendPrepared(self: *NativeDurabilityResources, path: []const u8) !void {
        const io = self.permit.io;
        const file = if (self.append_create)
            try openFilePathForWriteWithIo(io, path, .{ .truncate = false })
        else if (std.fs.path.isAbsolute(path))
            try std.Io.Dir.openFileAbsolute(io, path, .{ .mode = .write_only })
        else
            try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .write_only });
        defer file.close(io);
        const size = (try file.stat(io)).size;
        var buffer: [4096]u8 = undefined;
        var writer = file.writer(io, &buffer);
        writer.seekTo(size) catch |cause| return writer.err orelse cause;
        const bytes = self.append_bytes.?;
        const write_len = if (builtin.is_test) @min(bytes.len, NativeSyncTestControl.append_failure_after.load(.acquire)) else bytes.len;
        writer.interface.writeAll(bytes[0..write_len]) catch |cause| return writer.err orelse cause;
        writer.end() catch |cause| return writer.err orelse cause;
        if (builtin.is_test and write_len != bytes.len) return error.NoSpaceLeft;
    }

    fn beginPrivateCreate(raw: *anyopaque, path: []const u8, temporary: []const u8, original: read_admission.Port) !SyncOperation {
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        if (self.private_owned) return error.ResourceBudgetExceeded;
        if (temporary.len >= self.temporary.len or std.mem.eql(u8, path, temporary)) return error.InvalidArgument;
        // Rename must remain within the same directory/filesystem. Exclusive
        // creation never removes or truncates a colliding temporary object.
        if (!std.mem.eql(u8, std.fs.path.dirname(path) orelse ".", std.fs.path.dirname(temporary) orelse ".")) return error.InvalidArgument;
        try self.prepareCommand(path, .private_create, 0, false, original);
        @memcpy(self.private_final[0..path.len], path);
        self.private_final_len = path.len;
        @memcpy(self.temporary[0..temporary.len], temporary);
        self.temporary_len = temporary.len;
        self.private_owned = true;
        self.submit();
        return .{ .ptr = self, .vtable = &sync_vtable };
    }

    fn preparePrivate(self: *NativeDurabilityResources, kind: Kind, original: read_admission.Port) !void {
        if (!self.private_owned) return error.ResourceBudgetExceeded;
        try self.prepareCommand(self.private_final[0..self.private_final_len], kind, 0, false, original);
    }

    fn beginPrivateWrite(raw: *anyopaque, offset: u64, bytes: []const u8, original: read_admission.Port) !SyncOperation {
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        if (bytes.len > max_atomic_write_bytes) return error.ResourceBudgetExceeded;
        try self.preparePrivate(.private_write, original);
        @memcpy(self.buffer[0..bytes.len], bytes);
        self.limit = bytes.len;
        self.private_offset = offset;
        self.submit();
        return .{ .ptr = self, .vtable = &sync_vtable };
    }

    fn beginPrivatePublish(raw: *anyopaque, original: read_admission.Port) !SyncOperation {
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        if (!self.private_owned) return error.ResourceBudgetExceeded;
        try self.permit.cache.fencePathRetainingEntries(self.permit.state.cache_namespace, self.permit.io, self.private_final[0..self.private_final_len], original);
        try self.preparePrivate(.private_publish, original);
        self.invalidation_pending = true;
        self.submit();
        return .{ .ptr = self, .vtable = &sync_vtable };
    }

    fn beginPrivateDiscard(raw: *anyopaque, original: read_admission.Port) !SyncOperation {
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        try self.preparePrivate(.private_discard, original);
        self.submit();
        return .{ .ptr = self, .vtable = &sync_vtable };
    }

    fn createPrivate(self: *NativeDurabilityResources) !void {
        std.debug.assert(self.private_file == null and !self.private_temporary_owned);
        self.private_file = try openFilePathForWriteWithIo(self.permit.io, self.temporary[0..self.temporary_len], .{ .exclusive = true });
        self.private_temporary_owned = true;
    }

    fn writePrivate(self: *NativeDurabilityResources) !void {
        const file = self.private_file orelse return error.InvalidArgument;
        var writer = file.writer(self.permit.io, &.{});
        writer.seekTo(self.private_offset) catch |cause| return writer.err orelse cause;
        const n = if (builtin.is_test) @min(self.limit, NativeSyncTestControl.append_failure_after.load(.acquire)) else self.limit;
        writer.interface.writeAll(self.buffer[0..n]) catch |cause| return writer.err orelse cause;
        // end() sets file length to this write's position, which would
        // truncate data after an in-place header/backpatch command.
        writer.interface.flush() catch |cause| return writer.err orelse cause;
        if (n != self.limit) return error.NoSpaceLeft;
    }

    fn publishPrivate(self: *NativeDurabilityResources) !void {
        const io = self.permit.io;
        const file = self.private_file orelse return error.InvalidArgument;
        try file.sync(io);
        file.close(io);
        self.private_file = null;
        // Transfer the still-reserved descriptor slot to directory sync.
        // No new application admission or worker dispatch occurs here.
        const final = self.private_final[0..self.private_final_len];
        try std.Io.Dir.cwd().rename(self.temporary[0..self.temporary_len], std.Io.Dir.cwd(), final, io);
        self.private_temporary_owned = false; // Commit rename before failure.
        if (builtin.is_test and NativeSyncTestControl.control_failure_after_rename.load(.acquire)) return error.InputOutput;
        try fs_paths.syncDirPortable(io, std.fs.path.dirname(final) orelse ".");
    }

    fn discardPrivate(self: *NativeDurabilityResources) !void {
        if (self.private_file) |file| {
            file.close(self.permit.io);
            self.private_file = null;
        }
        if (self.private_temporary_owned) {
            deleteFilePathWithIo(self.permit.io, self.temporary[0..self.temporary_len]) catch |cause| {
                if (cause != error.FileNotFound) return cause;
            };
            self.private_temporary_owned = false;
        }
        // A possibly renamed destination is never removed by abandonment.
    }

    fn run(self: *NativeDurabilityResources) void {
        const io = self.permit.io;
        while (true) {
            self.wake.waitUncancelable(io);
            // Reset before loading the command. A next phase may be posted
            // immediately after completed is published; its wake must survive
            // until this loop consumes it. No producer rewrites a queued slot.
            self.wake.reset();
            switch (self.phase.load(.acquire)) {
                .stopping => return,
                .queued => {},
                else => unreachable,
            }
            if (builtin.is_test and NativeSyncTestControl.pause.load(.acquire)) {
                NativeSyncTestControl.started.store(true, .release);
                while (!NativeSyncTestControl.release.load(.acquire))
                    io.sleep(.fromMilliseconds(1), .awake) catch {};
            }
            const path = self.path[0..self.path_len];
            if (builtin.is_test) {
                if (self.kind == .append) {
                    _ = NativeSyncTestControl.appends.fetchAdd(1, .release);
                } else if (self.kind == .control_replace or self.kind == .create_directory or self.kind == .private_create or self.kind == .private_write or self.kind == .private_publish or self.kind == .private_discard) {
                    _ = NativeSyncTestControl.metadata.fetchAdd(1, .release);
                } else NativeSyncTestControl.observe(self.kind == .control_read, path);
            }
            const result = switch (self.kind) {
                .contents => fs_paths.syncFilePortable(io, path),
                .directory => fs_paths.syncDirPortable(io, path),
                .control_read => self.readControl(path),
                .append => self.appendPrepared(path),
                .control_replace => self.replaceControl(path),
                .create_directory => self.createDirectory(path),
                .private_create => self.createPrivate(),
                .private_write => self.writePrivate(),
                .private_publish => self.publishPrivate(),
                .private_discard => self.discardPrivate(),
            };
            result catch |cause| {
                self.failure = cause;
            };
            // Last access to request/result fields for this phase. Retiring
            // it can immediately publish another request on the same worker.
            self.phase.store(.completed, .release);
        }
    }

    fn readControl(self: *NativeDurabilityResources, path: []const u8) !void {
        const io = self.permit.io;
        const file = if (std.fs.path.isAbsolute(path)) try std.Io.Dir.openFileAbsolute(io, path, .{}) else try std.Io.Dir.cwd().openFile(io, path, .{});
        defer file.close(io);
        self.result.file_size = (try file.stat(io)).size;
        if (self.result.file_size > self.limit) return;
        const bytes = self.buffer[0..@intCast(self.result.file_size)];
        var reader = file.reader(io, &.{});
        try reader.interface.readSliceAll(bytes);
        self.result.bytes = bytes;
    }

    fn ready(raw: *anyopaque) !bool {
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        return self.phase.load(.acquire) == .completed;
    }

    fn wait(raw: *anyopaque, original: read_admission.Port) !void {
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        std.debug.assert(self.active);
        // Only the atomic completion bit is inspected while CPU is parked.
        try @import("../../common/cleanup_wait.zig").until(self.permit.io, original, self, ready, null);
        if (self.invalidation_pending) {
            try self.permit.cache.fencePathRetainingEntries(self.permit.state.cache_namespace, self.permit.io, self.path[0..self.path_len], original);
            self.invalidation_pending = false;
        }
        if (self.failure) |cause| return cause;
    }

    fn waitRead(raw: *anyopaque, original: read_admission.Port) !ControlFileResult {
        try wait(raw, original);
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        return self.result;
    }

    fn retireOperation(raw: *anyopaque) void {
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        std.debug.assert(self.active);
        // Quiesced mandatory join of accepted physical work. Do not stop the
        // mailbox worker first: an accepted request must run to completion.
        while (self.phase.load(.acquire) != .completed)
            self.permit.io.sleep(.fromMilliseconds(1), .awake) catch {};
        if (self.invalidation_pending) {
            self.permit.cache.fencePathRetainingEntries(self.permit.state.cache_namespace, self.permit.io, self.path[0..self.path_len], null) catch unreachable;
            self.invalidation_pending = false;
        }
        if ((self.kind == .private_publish or self.kind == .private_discard) and self.failure == null) self.private_owned = false;
        self.active = false;
        self.phase.store(.idle, .release);
    }

    fn retireWithAdmission(raw: *anyopaque, original: read_admission.Port) !void {
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        try requireControl(original);
        if (self.active or self.private_owned) return error.ResourceBudgetExceeded;
        self.phase.store(.stopping, .release);
        self.wake.set(self.permit.io);
        if (!self.joined) {
            const boundary = (try original.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
            try boundary.begin(boundary.ptr);
            self.future.await(self.permit.io);
            self.joined = true; // Commit the join before fallible reentry.
            try boundary.end(boundary.ptr);
        }
        try original.checkpoint();
        // The caller retains the storage owner until this grant retires, so
        // permit release cannot tear down the executor under admitted CPU.
        if (self.permit.state.refs.load(.acquire) <= 1) return error.ExecutionWaitUnsupported;
        self.destroy();
    }

    fn destroy(self: *NativeDurabilityResources) void {
        const allocator = self.allocator;
        var permit = self.permit;
        if (self.append_bytes) |bytes| allocator.free(bytes);
        allocator.destroy(self);
        permit.release();
    }

    fn deinit(raw: *anyopaque) void {
        const self: *NativeDurabilityResources = @ptrCast(@alignCast(raw));
        if (self.active) retireOperation(self);
        if (self.private_owned) {
            // Explicit mandatory fallback uses the admitted worker too. It
            // never publishes unfinished output or deletes a renamed target.
            self.kind = .private_discard;
            self.failure = null;
            self.submit();
            while (self.phase.load(.acquire) != .completed)
                self.permit.io.sleep(.fromMilliseconds(1), .awake) catch {};
            self.private_owned = false;
        }
        self.phase.store(.stopping, .release);
        self.wake.set(self.permit.io);
        if (!self.joined) self.future.await(self.permit.io);
        self.destroy();
    }
};

pub const NativeStorage = if (!supports_native_storage) UnsupportedNativeStorage else if (supports_evented_runtime) EventedNativeStorage else ThreadedNativeStorage;
const UnsupportedNativeStorage = struct {
    pub const Lease = struct {
        pub fn deinit(_: *Lease) void {}
        pub fn storage(_: *Lease) Storage {
            @panic("native storage is unavailable on this target");
        }
    };

    pub fn init(_: Allocator, _: RuntimeKind) !NativeStorage {
        return error.UnsupportedNativeStorageRuntime;
    }

    pub fn initWithPool(_: Allocator, _: RuntimeKind, _: ?*NativeStoragePool) !NativeStorage {
        return error.UnsupportedNativeStorageRuntime;
    }

    pub fn deinit(_: *NativeStorage) void {}

    pub fn snapshotStats(_: *const NativeStorage) NativeStorageStats {
        return .{};
    }

    pub fn acquireFdPermit(_: *NativeStorage) !NativeFdPermit {
        return error.UnsupportedNativeStorageRuntime;
    }

    pub fn acquireFdPermits(_: *NativeStorage, _: usize) !NativeFdPermit {
        return error.UnsupportedNativeStorageRuntime;
    }

    pub fn acquireLease(_: *NativeStorage) !Lease {
        return error.UnsupportedNativeStorageRuntime;
    }

    pub fn beginAtomicWriteWithPermit(_: *NativeStorage, _: Allocator, _: []const u8, _: *NativeFdPermit) !AtomicWriteSink {
        return error.UnsupportedNativeStorageRuntime;
    }

    pub fn validateFdPermit(_: *NativeStorage, _: *const NativeFdPermit) !void {
        return error.UnsupportedNativeStorageRuntime;
    }

    pub fn storage(_: *NativeStorage) Storage {
        @panic("native storage is unavailable on this target");
    }
};
const EventedNativeStorage = struct {
    runtime: union(RuntimeKind) {
        threaded: std.Io.Threaded,
        evented: std.Io.Evented,
    },
    state: *NativeStorageState,

    const native_vtable: Storage.VTable = .{
        .create_dir_path = createDirPath,
        .read_file_alloc = readFileAlloc,
        .read_file_alloc_with_admission = readFileAllocWithAdmission,
        .read_file_range_alloc = readFileRangeAlloc,
        .begin_read_file_range_alloc_with_runtime = beginReadFileRangeAllocWithRuntime,
        .begin_read_file_range_alloc_with_admission = beginReadFileRangeAllocWithAdmission,
        .read_file_range_into = readFileRangeInto,
        .read_file_range_into_with_admission = readFileRangeIntoWithAdmission,
        .read_file_range_at_most_into = readFileRangeAtMostInto,
        .file_size = fileSize,
        .file_size_with_admission = fileSizeWithAdmission,
        .read_file_trailer_alloc_with_admission = readFileTrailerAllocWithAdmission,
        .read_file_trailer_alloc = readFileTrailerAlloc,
        .write_file_absolute = writeFileAbsolute,
        .append_file_absolute = appendFileAbsolute,
        .begin_atomic_write = beginAtomicWrite,
        .begin_cold_sequential_read = beginColdSequentialRead,
        .begin_cold_random_read = beginColdRandomRead,
        .try_begin_cold_random_reads = tryBeginNativeColdRandomReads,
        .cold_sequential_reader_capacity = coldSequentialReaderCapacity,
        .sync_contents_absolute = syncFileContentsAbsolute,
        .sync_parent_absolute = syncParentAbsolute,
        .rename_absolute = renameAbsolute,
        .delete_file_absolute = deleteFileAbsolute,
        .delete_tree = deleteTree,
        .list_file_names_alloc = listFileNamesAlloc,
        .list_directory_names_alloc = listDirectoryNamesAlloc,
        .now_ns = nowNs,
        .root_identity_alloc = nativeRootIdentityAlloc,
        .rename_is_atomic = true,
        .supports_host_path_generation_publication = true,
        .supports_native_path_locks = true,
        .begin_sync_contents_with_admission = beginSyncContents,
        .begin_sync_parent_with_admission = beginSyncParent,
        .begin_control_file_read_with_admission = beginControlRead,
        .try_prepare_durability_with_admission = tryPrepareDurability,
        .try_prepare_append_with_admission = tryPrepareAppend,
        .append_resource_bytes = nativeAppendResourceBytes,
    };

    pub fn init(allocator: Allocator, kind: RuntimeKind) !NativeStorage {
        return try initWithPool(allocator, kind, null);
    }

    pub fn initWithPool(allocator: Allocator, kind: RuntimeKind, pool: ?*NativeStoragePool) !NativeStorage {
        var runtime = switch (kind) {
            .threaded => .{ .threaded = threaded_io_limits.initService(allocator) },
            .evented => blk2: {
                var evented: std.Io.Evented = undefined;
                try std.Io.Evented.init(&evented, allocator, .{});
                break :blk2 .{ .evented = evented };
            },
        };
        errdefer switch (runtime) {
            .threaded => |*threaded| threaded.deinit(),
            .evented => |*evented| std.Io.Evented.deinit(evented),
        };
        return .{
            .runtime = runtime,
            .state = try NativeStorageState.create(allocator, kind, pool),
        };
    }

    pub fn deinit(self: *NativeStorage) void {
        self.state.closeStorageRef();
        switch (self.runtime) {
            .threaded => |*threaded| threaded.deinit(),
            .evented => |*evented| std.Io.Evented.deinit(evented),
        }
        self.* = undefined;
    }

    pub fn snapshotStats(self: *const NativeStorage) NativeStorageStats {
        return self.state.fdCacheConst().snapshotStats();
    }

    pub fn acquireFdPermit(self: *NativeStorage) !NativeFdPermit {
        return try self.state.acquireFdPermit();
    }

    pub fn storage(self: *NativeStorage) Storage {
        return .{
            .ptr = self,
            .vtable = &native_vtable,
        };
    }

    fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        switch (self.runtime) {
            .threaded => |*threaded| try createDirPathPortable(threaded.io(), path),
            .evented => |*evented| try createDirPathPortable(evented.io(), path),
        }
    }

    fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        return switch (self.runtime) {
            .threaded => |*threaded| try std.Io.Dir.cwd().readFileAlloc(threaded.io(), path, allocator, .limited(max_bytes)),
            .evented => |*evented| try std.Io.Dir.cwd().readFileAlloc(evented.io(), path, allocator, .limited(max_bytes)),
        };
    }

    fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        if (comptime supports_posix_fd_cache) {
            const state = try self.state.retain();
            defer state.release();
            return try state.fdCache().readRangeAlloc(state.cache_namespace, state.threaded.io(), allocator, path, offset, len);
        }
        return switch (self.runtime) {
            .threaded => |*threaded| try readFileRangeWithIo(threaded.io(), allocator, path, offset, len),
            .evented => |*evented| try readFileRangeWithIo(evented.io(), allocator, path, offset, len),
        };
    }

    fn beginReadFileRangeAllocWithAdmission(ptr: *anyopaque, read_runtime: ?ReadRuntime, allocator: Allocator, path: []const u8, offset: u64, len: usize, control: read_admission.Port) !RangeReadFuture {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        const runtime = read_runtime orelse return CompletedRangeReadFuture.createControlled(self.storage(), allocator, path, offset, len, control);
        return ControlledNativeRangeReadFuture.createControlled(runtime, self.state, allocator, path, offset, len, control);
    }

    fn beginReadFileRangeAllocWithRuntime(ptr: *anyopaque, read_runtime: ?ReadRuntime, allocator: Allocator, path: []const u8, offset: u64, len: usize) !RangeReadFuture {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        const runtime = read_runtime orelse {
            const sync_storage: Storage = .{ .ptr = ptr, .vtable = &native_vtable };
            return try CompletedRangeReadFuture.create(sync_storage, allocator, path, offset, len);
        };
        return try NativeRangeReadFuture.create(runtime, self.state, allocator, path, offset, len);
    }

    fn readFileRangeInto(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8) !void {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        if (comptime supports_posix_fd_cache) {
            const state = try self.state.retain();
            defer state.release();
            return try state.fdCache().readRangeInto(state.cache_namespace, state.threaded.io(), path, offset, out);
        }
        return switch (self.runtime) {
            .threaded => |*threaded| try readFileRangeWithIoInto(threaded.io(), path, offset, out),
            .evented => |*evented| try readFileRangeWithIoInto(evented.io(), path, offset, out),
        };
    }

    fn readFileRangeIntoWithAdmission(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8, control: read_admission.Port) !void {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        if (comptime supports_posix_fd_cache) {
            const state = try self.state.retain();
            defer state.release();
            return state.fdCache().readRangeIntoWithAdmission(state.cache_namespace, state.threaded.io(), path, offset, out, control);
        }
        try readFileRangeInto(ptr, path, offset, out);
    }

    fn readFileRangeAtMostInto(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8) !usize {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        if (comptime supports_posix_fd_cache) {
            const state = try self.state.retain();
            defer state.release();
            return try state.fdCache().readRangeAtMostInto(state.cache_namespace, state.threaded.io(), path, offset, out);
        }
        return switch (self.runtime) {
            .threaded => |*threaded| try readFileRangeWithIoAtMostInto(threaded.io(), path, offset, out),
            .evented => |*evented| try readFileRangeWithIoAtMostInto(evented.io(), path, offset, out),
        };
    }

    fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        if (comptime supports_posix_fd_cache) {
            const state = try self.state.retain();
            defer state.release();
            return try state.fdCache().fileSize(state.cache_namespace, state.threaded.io(), path);
        }
        return switch (self.runtime) {
            .threaded => |*threaded| try fileSizeWithIo(threaded.io(), path),
            .evented => |*evented| try fileSizeWithIo(evented.io(), path),
        };
    }

    fn readFileAllocWithAdmission(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize, control: read_admission.Port) ![]u8 {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        if (comptime supports_posix_fd_cache) {
            const state = try self.state.retain();
            defer state.release();
            return state.fdCache().readFileAllocWithAdmission(state.cache_namespace, state.threaded.io(), allocator, path, max_bytes, control);
        }
        return readFileAlloc(ptr, allocator, path, max_bytes);
    }

    fn fileSizeWithAdmission(ptr: *anyopaque, path: []const u8, control: read_admission.Port) !u64 {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        if (comptime supports_posix_fd_cache) {
            const state = try self.state.retain();
            defer state.release();
            return state.fdCache().fileSizeWithAdmission(state.cache_namespace, state.threaded.io(), path, control);
        }
        return fileSize(ptr, path);
    }

    fn readFileTrailerAllocWithAdmission(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize, control: read_admission.Port) !FileTrailer {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        if (comptime supports_posix_fd_cache) {
            const state = try self.state.retain();
            defer state.release();
            return state.fdCache().readTrailerAllocWithAdmission(state.cache_namespace, state.threaded.io(), allocator, path, len, control);
        }
        return readFileTrailerAlloc(ptr, allocator, path, len);
    }

    fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !FileTrailer {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        if (comptime supports_posix_fd_cache) {
            const state = try self.state.retain();
            defer state.release();
            return try state.fdCache().readTrailerAlloc(state.cache_namespace, state.threaded.io(), allocator, path, len);
        }
        return switch (self.runtime) {
            .threaded => |*threaded| try readFileTrailerWithIo(threaded.io(), allocator, path, len),
            .evented => |*evented| try readFileTrailerWithIo(evented.io(), allocator, path, len),
        };
    }

    fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        self.state.invalidatePath(path);
        defer self.state.invalidatePath(path);
        switch (self.runtime) {
            .threaded => |*threaded| try writeFileAbsoluteWithIo(threaded.io(), path, contents),
            .evented => |*evented| try writeFileAbsoluteWithIo(evented.io(), path, contents),
        }
    }

    fn appendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        self.state.invalidatePath(path);
        defer self.state.invalidatePath(path);
        switch (self.runtime) {
            .threaded => |*threaded| try appendFileAbsoluteWithIo(threaded.io(), path, contents, sync),
            .evented => |*evented| try appendFileAbsoluteWithIo(evented.io(), path, contents, sync),
        }
    }

    fn beginAtomicWrite(ptr: *anyopaque, allocator: Allocator, path: []const u8) !AtomicWriteSink {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        return try NativeAtomicWriteSink.create(allocator, path, self.state);
    }

    fn beginColdSequentialRead(ptr: *anyopaque, allocator: Allocator, path: []const u8) !ColdSequentialReader {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        return try NativeColdSequentialReader.create(allocator, path, self.state, .sequential);
    }

    fn beginColdRandomRead(ptr: *anyopaque, allocator: Allocator, path: []const u8) !ColdSequentialReader {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        return try NativeColdSequentialReader.create(allocator, path, self.state, .random);
    }

    fn coldSequentialReaderCapacity(ptr: *anyopaque) usize {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        return nativeColdSequentialReaderCapacity(self.state);
    }

    fn tryPrepareAppend(ptr: *anyopaque, bytes: []const u8, original: read_admission.Port) !?DurabilityResources {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        return NativeDurabilityResources.create(self.state, original, bytes);
    }

    fn tryPrepareDurability(ptr: *anyopaque, original: read_admission.Port) !?DurabilityResources {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        return NativeDurabilityResources.create(self.state, original, null);
    }

    fn beginControlRead(ptr: *anyopaque, path: []const u8, limit: usize, original: read_admission.Port) !ControlFileReadOperation {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        const state = self.state;
        return NativePhysicalOperation.createControlRead(state, path, limit, original);
    }

    fn beginSyncContents(ptr: *anyopaque, path: []const u8, original: read_admission.Port) !SyncOperation {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        const state = self.state;
        return NativePhysicalOperation.create(state, path, .contents, original);
    }

    fn beginSyncParent(ptr: *anyopaque, path: []const u8, original: read_admission.Port) !SyncOperation {
        if (path.len >= std.fs.max_path_bytes) return error.NameTooLong;
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        const state = self.state;
        const parent = std.fs.path.dirname(path) orelse if (std.fs.path.isAbsolute(path)) "/" else ".";
        return NativePhysicalOperation.create(state, parent, .directory, original);
    }

    fn syncFileContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        switch (self.runtime) {
            .threaded => |*threaded| try syncFileContentsPathWithIo(threaded.io(), path),
            .evented => |*evented| try syncFileContentsPathWithIo(evented.io(), path),
        }
    }

    fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        switch (self.runtime) {
            .threaded => |*threaded| try syncParentPathWithIo(threaded.io(), path),
            .evented => |*evented| try syncParentPathWithIo(evented.io(), path),
        }
    }

    fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        self.state.invalidateRename(old_path, new_path);
        defer self.state.invalidateRename(old_path, new_path);
        switch (self.runtime) {
            .threaded => |*threaded| try renamePathWithIo(threaded.io(), old_path, new_path),
            .evented => |*evented| try renamePathWithIo(evented.io(), old_path, new_path),
        }
    }

    fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        self.state.invalidatePath(path);
        defer self.state.invalidatePath(path);
        switch (self.runtime) {
            .threaded => |*threaded| try deleteFilePathWithIo(threaded.io(), path),
            .evented => |*evented| try deleteFilePathWithIo(evented.io(), path),
        }
    }

    fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        self.state.invalidateTree(path);
        defer self.state.invalidateTree(path);
        switch (self.runtime) {
            .threaded => |*threaded| try std.Io.Dir.cwd().deleteTree(threaded.io(), path),
            .evented => |*evented| try std.Io.Dir.cwd().deleteTree(evented.io(), path),
        }
    }

    fn listFileNamesAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8) ![][]u8 {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        return switch (self.runtime) {
            .threaded => |*threaded| try listFileNamesWithIo(allocator, threaded.io(), path),
            .evented => |*evented| try listFileNamesWithIo(allocator, evented.io(), path),
        };
    }

    fn listDirectoryNamesAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8) ![][]u8 {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        return switch (self.runtime) {
            .threaded => |*threaded| try listNamesWithIo(allocator, threaded.io(), path, .directory),
            .evented => |*evented| try listNamesWithIo(allocator, evented.io(), path, .directory),
        };
    }

    fn nowNs(ptr: *anyopaque) u64 {
        const self: *NativeStorage = @ptrCast(@alignCast(ptr));
        return switch (self.runtime) {
            .threaded => |*threaded| blk2: {
                const now = std.Io.Timestamp.now(threaded.io(), .awake);
                break :blk2 @intCast(now.toNanoseconds());
            },
            .evented => |*evented| blk2: {
                const now = std.Io.Timestamp.now(evented.io(), .awake);
                break :blk2 @intCast(now.toNanoseconds());
            },
        };
    }
};
const ThreadedNativeStorage = struct {
    state: *NativeStorageState,

    pub const Lease = struct {
        state: *NativeStorageState,
        active: bool = true,

        /// Returns a borrowed view whose lifetime is bounded by this lease.
        pub fn storage(self: *Lease) Storage {
            std.debug.assert(self.active);
            return .{
                .ptr = self.state,
                .vtable = &threaded_only_vtable,
            };
        }

        pub fn deinit(self: *Lease) void {
            if (!self.active) return;
            self.active = false;
            self.state.release();
            self.* = undefined;
        }
    };

    const threaded_only_vtable: Storage.VTable = .{
        .acquire_lease = acquireErasedLease,
        .create_dir_path = createDirPath,
        .read_file_alloc = readFileAlloc,
        .read_file_alloc_with_admission = readFileAllocWithAdmission,
        .read_file_range_alloc = readFileRangeAlloc,
        .begin_read_file_range_alloc_with_runtime = beginReadFileRangeAllocWithRuntime,
        .begin_read_file_range_alloc_with_admission = beginReadFileRangeAllocWithAdmission,
        .read_file_range_into = readFileRangeInto,
        .read_file_range_into_with_admission = readFileRangeIntoWithAdmission,
        .read_file_range_at_most_into = readFileRangeAtMostInto,
        .file_size = fileSize,
        .file_size_with_admission = fileSizeWithAdmission,
        .read_file_trailer_alloc_with_admission = readFileTrailerAllocWithAdmission,
        .read_file_trailer_alloc = readFileTrailerAlloc,
        .write_file_absolute = writeFileAbsolute,
        .append_file_absolute = appendFileAbsolute,
        .begin_atomic_write = beginAtomicWrite,
        .begin_cold_sequential_read = beginColdSequentialRead,
        .begin_cold_random_read = beginColdRandomRead,
        .try_begin_cold_random_reads = tryBeginNativeColdRandomReads,
        .cold_sequential_reader_capacity = coldSequentialReaderCapacity,
        .sync_contents_absolute = syncFileContentsAbsolute,
        .sync_parent_absolute = syncParentAbsolute,
        .rename_absolute = renameAbsolute,
        .delete_file_absolute = deleteFileAbsolute,
        .delete_tree = deleteTree,
        .list_file_names_alloc = listFileNamesAlloc,
        .list_directory_names_alloc = listDirectoryNamesAlloc,
        .now_ns = nowNs,
        .root_identity_alloc = rootIdentityAlloc,
        .rename_is_atomic = true,
        .supports_host_path_generation_publication = true,
        .supports_native_path_locks = true,
        .begin_sync_contents_with_admission = beginSyncContents,
        .begin_sync_parent_with_admission = beginSyncParent,
        .begin_control_file_read_with_admission = beginControlRead,
        .try_prepare_durability_with_admission = tryPrepareDurability,
        .try_prepare_append_with_admission = tryPrepareAppend,
        .append_resource_bytes = nativeAppendResourceBytes,
    };

    pub fn init(allocator: Allocator, kind: RuntimeKind) !NativeStorage {
        return try initWithPool(allocator, kind, null);
    }

    pub fn initWithPool(allocator: Allocator, kind: RuntimeKind, pool: ?*NativeStoragePool) !NativeStorage {
        return .{ .state = try NativeStorageState.create(allocator, kind, pool) };
    }

    pub fn deinit(self: *NativeStorage) void {
        self.state.closeStorageRef();
        self.* = undefined;
    }

    pub fn snapshotStats(self: *const NativeStorage) NativeStorageStats {
        return self.state.fdCacheConst().snapshotStats();
    }

    pub fn acquireFdPermit(self: *NativeStorage) !NativeFdPermit {
        return try self.state.acquireFdPermit();
    }

    pub fn acquireFdPermits(self: *NativeStorage, count: usize) !NativeFdPermit {
        if (count == 0) return error.InvalidNativeFdPermit;
        return try self.state.acquireFdPermits(count);
    }

    /// Keeps the native state and its threaded I/O runtime alive after the
    /// owning NativeStorage begins shutdown. No new lease may be acquired
    /// once shutdown has started.
    pub fn acquireLease(self: *NativeStorage) !Lease {
        return .{ .state = try self.state.acquireLease() };
    }

    fn acquireErasedLease(ptr: *anyopaque) !Storage.Lease {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const retained = try state.acquireLease();
        return .{
            .view = .{ .ptr = retained, .vtable = &threaded_only_vtable },
            .release = releaseErasedLease,
        };
    }

    fn releaseErasedLease(ptr: *anyopaque) void {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        state.release();
    }

    pub fn beginAtomicWriteWithPermit(
        self: *NativeStorage,
        allocator: Allocator,
        path: []const u8,
        permit: *NativeFdPermit,
    ) !AtomicWriteSink {
        if (!permit.active or permit.count != 1 or permit.state != self.state) return error.InvalidNativeFdPermit;
        return try NativeAtomicWriteSink.createWithPermit(allocator, path, self.state, permit);
    }

    pub fn validateFdPermit(self: *NativeStorage, permit: *const NativeFdPermit) !void {
        if (!permit.active or permit.count != 1 or permit.state != self.state) return error.InvalidNativeFdPermit;
    }

    pub fn storage(self: *NativeStorage) Storage {
        // The returned view borrows self.state; retained child operations
        // keep the state alive independently until their own deinit.
        return .{
            .ptr = self.state,
            .vtable = &threaded_only_vtable,
        };
    }

    fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        // Recursive directory creation opens the next directory before
        // closing its parent.
        var permit = try state.acquireFdPermits(2);
        defer permit.release();
        try createDirPathPortable(permit.io, path);
    }

    fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        var permit = try state.acquireFdPermit();
        defer permit.release();
        return try std.Io.Dir.cwd().readFileAlloc(permit.io, path, allocator, .limited(max_bytes));
    }

    fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const retained = try state.retain();
        defer retained.release();
        if (comptime supports_posix_fd_cache) {
            return try retained.fdCache().readRangeAlloc(retained.cache_namespace, retained.threaded.io(), allocator, path, offset, len);
        }
        return try readFileRangeWithIo(retained.threaded.io(), allocator, path, offset, len);
    }

    fn beginReadFileRangeAllocWithAdmission(ptr: *anyopaque, read_runtime: ?ReadRuntime, allocator: Allocator, path: []const u8, offset: u64, len: usize, control: read_admission.Port) !RangeReadFuture {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const sync_storage: Storage = .{ .ptr = state, .vtable = &threaded_only_vtable };
        const runtime = read_runtime orelse return CompletedRangeReadFuture.createControlled(sync_storage, allocator, path, offset, len, control);
        return ControlledNativeRangeReadFuture.createControlled(runtime, state, allocator, path, offset, len, control);
    }

    fn beginReadFileRangeAllocWithRuntime(ptr: *anyopaque, read_runtime: ?ReadRuntime, allocator: Allocator, path: []const u8, offset: u64, len: usize) !RangeReadFuture {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const runtime = read_runtime orelse {
            const sync_storage: Storage = .{ .ptr = ptr, .vtable = &threaded_only_vtable };
            return try CompletedRangeReadFuture.create(sync_storage, allocator, path, offset, len);
        };
        return try NativeRangeReadFuture.create(runtime, state, allocator, path, offset, len);
    }

    fn readFileRangeInto(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8) !void {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const retained = try state.retain();
        defer retained.release();
        if (comptime supports_posix_fd_cache) {
            return try retained.fdCache().readRangeInto(retained.cache_namespace, retained.threaded.io(), path, offset, out);
        }
        return try readFileRangeWithIoInto(retained.threaded.io(), path, offset, out);
    }

    fn readFileRangeIntoWithAdmission(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8, control: read_admission.Port) !void {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const retained = try state.retain();
        defer retained.release();
        if (comptime supports_posix_fd_cache) {
            return retained.fdCache().readRangeIntoWithAdmission(retained.cache_namespace, retained.threaded.io(), path, offset, out, control);
        }
        try readFileRangeInto(ptr, path, offset, out);
    }

    fn readFileRangeAtMostInto(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8) !usize {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const retained = try state.retain();
        defer retained.release();
        if (comptime supports_posix_fd_cache) {
            return try retained.fdCache().readRangeAtMostInto(retained.cache_namespace, retained.threaded.io(), path, offset, out);
        }
        return try readFileRangeWithIoAtMostInto(retained.threaded.io(), path, offset, out);
    }

    fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const retained = try state.retain();
        defer retained.release();
        if (comptime supports_posix_fd_cache) {
            return try retained.fdCache().fileSize(retained.cache_namespace, retained.threaded.io(), path);
        }
        return try fileSizeWithIo(retained.threaded.io(), path);
    }

    fn readFileAllocWithAdmission(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize, control: read_admission.Port) ![]u8 {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const retained = try state.retain();
        defer retained.release();
        if (comptime supports_posix_fd_cache) {
            return retained.fdCache().readFileAllocWithAdmission(retained.cache_namespace, retained.threaded.io(), allocator, path, max_bytes, control);
        }
        return readFileAlloc(ptr, allocator, path, max_bytes);
    }

    fn fileSizeWithAdmission(ptr: *anyopaque, path: []const u8, control: read_admission.Port) !u64 {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const retained = try state.retain();
        defer retained.release();
        if (comptime supports_posix_fd_cache) {
            return retained.fdCache().fileSizeWithAdmission(retained.cache_namespace, retained.threaded.io(), path, control);
        }
        return fileSize(ptr, path);
    }

    fn readFileTrailerAllocWithAdmission(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize, control: read_admission.Port) !FileTrailer {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const retained = try state.retain();
        defer retained.release();
        if (comptime supports_posix_fd_cache) {
            return retained.fdCache().readTrailerAllocWithAdmission(retained.cache_namespace, retained.threaded.io(), allocator, path, len, control);
        }
        return readFileTrailerAlloc(ptr, allocator, path, len);
    }

    fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !FileTrailer {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const retained = try state.retain();
        defer retained.release();
        if (comptime supports_posix_fd_cache) {
            return try retained.fdCache().readTrailerAlloc(retained.cache_namespace, retained.threaded.io(), allocator, path, len);
        }
        return try readFileTrailerWithIo(retained.threaded.io(), allocator, path, len);
    }

    fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        var permit = try state.acquireFdPermits(createPathDescriptorCount(path));
        defer permit.release();
        permit.state.invalidatePath(path);
        defer permit.state.invalidatePath(path);
        try writeFileAbsoluteWithIo(permit.io, path, contents);
    }

    fn appendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        var permit = try state.acquireFdPermits(createPathDescriptorCount(path));
        defer permit.release();
        permit.state.invalidatePath(path);
        defer permit.state.invalidatePath(path);
        try appendFileAbsoluteWithIo(permit.io, path, contents, sync);
    }

    fn beginAtomicWrite(ptr: *anyopaque, allocator: Allocator, path: []const u8) !AtomicWriteSink {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        return try NativeAtomicWriteSink.create(allocator, path, state);
    }

    fn beginColdSequentialRead(ptr: *anyopaque, allocator: Allocator, path: []const u8) !ColdSequentialReader {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        return try NativeColdSequentialReader.create(allocator, path, state, .sequential);
    }

    fn beginColdRandomRead(ptr: *anyopaque, allocator: Allocator, path: []const u8) !ColdSequentialReader {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        return try NativeColdSequentialReader.create(allocator, path, state, .random);
    }

    fn coldSequentialReaderCapacity(ptr: *anyopaque) usize {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        return nativeColdSequentialReaderCapacity(state);
    }

    fn tryPrepareAppend(ptr: *anyopaque, bytes: []const u8, original: read_admission.Port) !?DurabilityResources {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        return NativeDurabilityResources.create(state, original, bytes);
    }

    fn tryPrepareDurability(ptr: *anyopaque, original: read_admission.Port) !?DurabilityResources {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        return NativeDurabilityResources.create(state, original, null);
    }

    fn beginControlRead(ptr: *anyopaque, path: []const u8, limit: usize, original: read_admission.Port) !ControlFileReadOperation {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        return NativePhysicalOperation.createControlRead(state, path, limit, original);
    }

    fn beginSyncContents(ptr: *anyopaque, path: []const u8, original: read_admission.Port) !SyncOperation {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        return NativePhysicalOperation.create(state, path, .contents, original);
    }

    fn beginSyncParent(ptr: *anyopaque, path: []const u8, original: read_admission.Port) !SyncOperation {
        if (path.len >= std.fs.max_path_bytes) return error.NameTooLong;
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const parent = std.fs.path.dirname(path) orelse if (std.fs.path.isAbsolute(path)) "/" else ".";
        return NativePhysicalOperation.create(state, parent, .directory, original);
    }

    fn syncFileContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        var permit = try state.acquireFdPermit();
        defer permit.release();
        try syncFileContentsPathWithIo(permit.io, path);
    }

    fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        var permit = try state.acquireFdPermit();
        defer permit.release();
        try syncParentPathWithIo(permit.io, path);
    }

    fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const retained = try state.retain();
        defer retained.release();
        retained.invalidateRename(old_path, new_path);
        defer retained.invalidateRename(old_path, new_path);
        try renamePathWithIo(retained.threaded.io(), old_path, new_path);
    }

    fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const retained = try state.retain();
        defer retained.release();
        retained.invalidatePath(path);
        defer retained.invalidatePath(path);
        if (comptime supports_posix_fd_cache) {
            // unlink(2) does not open a descriptor. Keeping rollback
            // deletion outside admission guarantees an unpublished file
            // can be removed even while the FD budget is saturated.
            try deleteFilePathPosix(path);
        } else {
            try deleteFilePathWithIo(retained.threaded.io(), path);
        }
    }

    fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        // The O(1)-memory traversal holds at most the active directory,
        // its parent, and the next directory while descending.
        var permit = try state.acquireFdPermits(3);
        defer permit.release();
        permit.state.invalidateTree(path);
        defer permit.state.invalidateTree(path);
        try std.Io.Dir.cwd().deleteTreeMinStackSize(permit.io, path);
    }

    fn listFileNamesAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8) ![][]u8 {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        var permit = try state.acquireFdPermit();
        defer permit.release();
        return try listFileNamesWithIo(allocator, permit.io, path);
    }

    fn listDirectoryNamesAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8) ![][]u8 {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        var permit = try state.acquireFdPermit();
        defer permit.release();
        return try listNamesWithIo(allocator, permit.io, path, .directory);
    }

    fn rootIdentityAlloc(ptr: *anyopaque, allocator: Allocator, root_dir: []const u8) ![]u8 {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        var permit = try state.acquireFdPermit();
        defer permit.release();
        return try nativeRootIdentityAlloc(ptr, allocator, root_dir);
    }

    fn nowNs(ptr: *anyopaque) u64 {
        const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
        const retained = state.retain() catch return 0;
        defer retained.release();
        const now = std.Io.Timestamp.now(retained.threaded.io(), .awake);
        return @intCast(now.toNanoseconds());
    }
};

fn writeFileAbsoluteWithIo(io: anytype, path: []const u8, contents: []const u8) !void {
    var file = openFilePathForWriteWithIo(io, path, .{ .truncate = true }) catch |err| {
        std.log.err("lsm writeFileAbsolute create failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(err) });
        return err;
    };
    defer file.close(io);

    var file_buf: [4096]u8 = undefined;
    var writer = file.writer(io, &file_buf);
    writer.interface.writeAll(contents) catch |err| {
        const actual = if (err == error.WriteFailed) writer.err orelse err else err;
        std.log.err("lsm writeFileAbsolute write failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(actual) });
        return actual;
    };
    writer.end() catch |err| {
        const actual = if (err == error.WriteFailed) writer.err orelse err else err;
        std.log.err("lsm writeFileAbsolute finish failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(actual) });
        return actual;
    };
}

fn listFileNamesWithIo(allocator: Allocator, io: anytype, path: []const u8) ![][]u8 {
    return listNamesWithIo(allocator, io, path, .file);
}

fn listNamesWithIo(allocator: Allocator, io: anytype, path: []const u8, kind: std.Io.File.Kind) ![][]u8 {
    var dir = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openDirAbsolute(io, path, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true });
    defer dir.close(io);
    var names = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (names.items) |name| allocator.free(name);
        names.deinit(allocator);
    }
    var iterator = dir.iterate();
    while (try iterator.next(io)) |entry| {
        if (entry.kind != kind) continue;
        const name = try allocator.dupe(u8, entry.name);
        errdefer allocator.free(name);
        try names.append(allocator, name);
    }
    return try names.toOwnedSlice(allocator);
}

fn appendFileAbsoluteWithIo(io: anytype, path: []const u8, contents: []const u8, sync: bool) !void {
    var file = openFilePathForWriteWithIo(io, path, .{ .truncate = false }) catch |err| {
        std.log.err("lsm appendFileAbsolute create failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(err) });
        return err;
    };
    defer file.close(io);

    const size = (try file.stat(io)).size;
    var file_buf: [4096]u8 = undefined;
    var writer = file.writer(io, &file_buf);
    writer.seekTo(size) catch |err| {
        const actual = if (err == error.WriteFailed) writer.err orelse err else err;
        std.log.err("lsm appendFileAbsolute seek failed path={s} offset={} err={s}", .{ path, size, @errorName(actual) });
        return actual;
    };
    writer.interface.writeAll(contents) catch |err| {
        const actual = if (err == error.WriteFailed) writer.err orelse err else err;
        std.log.err("lsm appendFileAbsolute write failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(actual) });
        return actual;
    };
    writer.end() catch |err| {
        const actual = if (err == error.WriteFailed) writer.err orelse err else err;
        std.log.err("lsm appendFileAbsolute finish failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(actual) });
        return actual;
    };
    if (sync) file.sync(io) catch |err| {
        std.log.err("lsm appendFileAbsolute sync failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(err) });
        return err;
    };
}

fn syncFileContentsPathWithIo(io: anytype, path: []const u8) !void {
    try fs_paths.syncFilePortable(io, path);
}

fn syncParentPathWithIo(io: anytype, path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse if (std.fs.path.isAbsolute(path)) "/" else ".";
    try fs_paths.syncDirPortable(io, parent);
}

fn openFilePathForWriteWithIo(io: anytype, path: []const u8, flags: std.Io.Dir.CreateFileOptions) !std.Io.File {
    return try fs_paths.createFilePortable(io, path, flags);
}

fn readFileRangeWithIo(io: anytype, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    var reader = file.reader(io, &.{});
    try reader.seekTo(offset);
    const out = try allocator.alloc(u8, len);
    errdefer allocator.free(out);
    try reader.interface.readSliceAll(out);
    return out;
}

fn readFileRangeWithIoInto(io: anytype, path: []const u8, offset: u64, out: []u8) !void {
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    var reader = file.reader(io, &.{});
    try reader.seekTo(offset);
    try reader.interface.readSliceAll(out);
}

fn readFileRangeWithIoAtMostInto(io: anytype, path: []const u8, offset: u64, out: []u8) !usize {
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    var reader = file.reader(io, &.{});
    try reader.seekTo(offset);
    return try reader.interface.readSliceShort(out);
}

fn fileSizeWithIo(io: anytype, path: []const u8) !u64 {
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    return (try file.stat(io)).size;
}

fn readFileTrailerWithIo(io: anytype, allocator: Allocator, path: []const u8, len: usize) !FileTrailer {
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    const size = (try file.stat(io)).size;
    if (size < len) return error.EndOfStream;

    var reader = file.reader(io, &.{});
    try reader.seekTo(size - len);
    const out = try allocator.alloc(u8, len);
    errdefer allocator.free(out);
    try reader.interface.readSliceAll(out);
    return .{ .bytes = out, .file_size = size };
}

fn renamePathWithIo(io: anytype, old_path: []const u8, new_path: []const u8) !void {
    if (std.fs.path.isAbsolute(old_path) and std.fs.path.isAbsolute(new_path)) {
        try renameAbsoluteWithIo(io, old_path, new_path);
        return;
    }
    try std.Io.Dir.rename(std.Io.Dir.cwd(), old_path, std.Io.Dir.cwd(), new_path, io);
}

fn renameAbsoluteWithIo(io: anytype, old_path: []const u8, new_path: []const u8) !void {
    if (builtin.os.tag != .windows and builtin.os.tag != .wasi and builtin.os.tag != .freestanding) {
        // The direct syscall is atomic and does not need two temporary parent
        // directory descriptors, so it cannot oversubscribe FD admission.
        return try renameAbsoluteDirectPosix(old_path, new_path);
    }

    const old_parent_path = std.fs.path.dirname(old_path) orelse return error.FileNotFound;
    const new_parent_path = std.fs.path.dirname(new_path) orelse return error.FileNotFound;
    const old_base_name = std.fs.path.basename(old_path);
    const new_base_name = std.fs.path.basename(new_path);

    var old_parent = try std.Io.Dir.openDirAbsolute(io, old_parent_path, .{});
    defer old_parent.close(io);
    var new_parent = try std.Io.Dir.openDirAbsolute(io, new_parent_path, .{});
    defer new_parent.close(io);

    try std.Io.Dir.rename(old_parent, old_base_name, new_parent, new_base_name, io);
}

fn renameAbsolutePosix(old_path: []const u8, new_path: []const u8) !void {
    const old_parent_path = std.fs.path.dirname(old_path) orelse return error.FileNotFound;
    const new_parent_path = std.fs.path.dirname(new_path) orelse return error.FileNotFound;
    const old_base_name = std.fs.path.basename(old_path);
    const new_base_name = std.fs.path.basename(new_path);

    const old_parent_fd = try std.posix.openat(std.posix.AT.FDCWD, old_parent_path, .{
        .ACCMODE = .RDONLY,
        .DIRECTORY = true,
        .CLOEXEC = true,
    }, 0);
    defer closeFd(old_parent_fd);
    const new_parent_fd = try std.posix.openat(std.posix.AT.FDCWD, new_parent_path, .{
        .ACCMODE = .RDONLY,
        .DIRECTORY = true,
        .CLOEXEC = true,
    }, 0);
    defer closeFd(new_parent_fd);

    const allocator = std.heap.page_allocator;
    const old_base_name_z = try allocator.dupeSentinel(u8, old_base_name, 0);
    defer allocator.free(old_base_name_z);
    const new_base_name_z = try allocator.dupeSentinel(u8, new_base_name, 0);
    defer allocator.free(new_base_name_z);

    while (true) {
        const rc = std.posix.system.renameat(old_parent_fd, old_base_name_z, new_parent_fd, new_base_name_z);
        switch (std.posix.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            .ACCES, .PERM, .ROFS => return error.AccessDenied,
            .BUSY => return error.FileBusy,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .LOOP => return error.SymLinkLoop,
            .MLINK => return error.LinkQuotaExceeded,
            .NAMETOOLONG => return error.NameTooLong,
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            .NOMEM => return error.SystemResources,
            .NOSPC => return error.NoSpaceLeft,
            .NOTEMPTY, .EXIST => return error.PathAlreadyExists,
            .XDEV => return error.RenameAcrossMountPoints,
            .INVAL => return renameAbsoluteDirectPosix(old_path, new_path),
            else => |err| return std.posix.unexpectedErrno(err),
        }
    }
}

fn renameAbsoluteDirectPosix(old_path: []const u8, new_path: []const u8) !void {
    const allocator = std.heap.page_allocator;
    const old_path_z = try allocator.dupeSentinel(u8, old_path, 0);
    defer allocator.free(old_path_z);
    const new_path_z = try allocator.dupeSentinel(u8, new_path, 0);
    defer allocator.free(new_path_z);

    while (true) {
        const rc = std.posix.system.rename(old_path_z, new_path_z);
        switch (std.posix.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            .ACCES, .PERM, .ROFS => return error.AccessDenied,
            .BUSY => return error.FileBusy,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .LOOP => return error.SymLinkLoop,
            .MLINK => return error.LinkQuotaExceeded,
            .NAMETOOLONG => return error.NameTooLong,
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            .NOMEM => return error.SystemResources,
            .NOSPC => return error.NoSpaceLeft,
            .NOTEMPTY, .EXIST => return error.PathAlreadyExists,
            .XDEV => return error.RenameAcrossMountPoints,
            .INVAL => {
                std.log.err("lsm rename invalid old={s} new={s}", .{ old_path, new_path });
                return error.InvalidArgument;
            },
            else => |err| return std.posix.unexpectedErrno(err),
        }
    }
}

fn deleteFilePathWithIo(io: anytype, path: []const u8) !void {
    if (!std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.cwd().deleteFile(io, path);
        return;
    }

    const parent_path = std.fs.path.dirname(path) orelse return error.FileNotFound;
    const base_name = std.fs.path.basename(path);
    var parent = try std.Io.Dir.openDirAbsolute(io, parent_path, .{});
    defer parent.close(io);
    try parent.deleteFile(io, base_name);
}

fn createAtomicWriteFdPosix(path: []const u8) !std.posix.fd_t {
    return try std.posix.openat(std.posix.AT.FDCWD, path, .{
        .ACCMODE = .RDWR,
        .CREAT = true,
        .CLOEXEC = true,
        .TRUNC = true,
    }, std.Io.File.Permissions.default_file.toMode());
}

fn deleteFilePathPosix(path: []const u8) !void {
    const allocator = std.heap.page_allocator;
    const path_z = try allocator.dupeSentinel(u8, path, 0);
    defer allocator.free(path_z);

    while (true) {
        const rc = std.posix.system.unlink(path_z);
        switch (std.posix.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            .ACCES, .PERM, .ROFS => return error.AccessDenied,
            .BUSY => return error.FileBusy,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .LOOP => return error.SymLinkLoop,
            .NAMETOOLONG => return error.NameTooLong,
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            else => |err| return std.posix.unexpectedErrno(err),
        }
    }
}

fn readAllAtOffset(fd: std.posix.fd_t, bytes: []u8, offset: u64) !void {
    var read_len: usize = 0;
    while (read_len < bytes.len) {
        const chunk_len = @min(max_posix_io_chunk, bytes.len - read_len);
        const rc = std.posix.system.pread(fd, bytes.ptr + read_len, chunk_len, @intCast(offset + read_len));
        switch (std.posix.errno(rc)) {
            .SUCCESS => {
                const n: usize = @intCast(rc);
                if (n == 0) return error.EndOfStream;
                read_len += n;
            },
            .INTR => continue,
            else => |err| return posixReadError(err),
        }
    }
}

fn readAtMostAtOffset(fd: std.posix.fd_t, bytes: []u8, offset: u64) !usize {
    var read_len: usize = 0;
    while (read_len < bytes.len) {
        const chunk_len = @min(max_posix_io_chunk, bytes.len - read_len);
        const rc = std.posix.system.pread(fd, bytes.ptr + read_len, chunk_len, @intCast(offset + read_len));
        switch (std.posix.errno(rc)) {
            .SUCCESS => {
                const n: usize = @intCast(rc);
                if (n == 0) return read_len;
                read_len += n;
            },
            .INTR => continue,
            else => |err| return posixReadError(err),
        }
    }
    return read_len;
}

fn readOnceAtOffset(fd: std.posix.fd_t, bytes: []u8, offset: u64) !usize {
    while (true) {
        const rc = std.posix.system.pread(fd, bytes.ptr, bytes.len, @intCast(offset));
        switch (std.posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => |err| return posixReadError(err),
        }
    }
}

fn writeAllAtOffset(fd: std.posix.fd_t, bytes: []const u8, offset: u64) !void {
    var written: usize = 0;
    while (written < bytes.len) {
        const chunk_len = @min(max_posix_io_chunk, bytes.len - written);
        const rc = std.posix.system.pwrite(fd, bytes.ptr + written, chunk_len, @intCast(offset + written));
        switch (std.posix.errno(rc)) {
            .SUCCESS => {
                const n: usize = @intCast(rc);
                if (n == 0) return error.WriteZero;
                written += n;
            },
            .INTR => continue,
            else => |err| return posixWriteError(err),
        }
    }
}

fn closeFd(fd: std.posix.fd_t) void {
    _ = std.posix.system.close(fd);
}

fn fileSizeFromFd(fd: std.posix.fd_t) !u64 {
    if (builtin.os.tag == .linux) {
        const linux = std.os.linux;
        const empty_path: [*:0]const u8 = "";
        while (true) {
            var statx = std.mem.zeroes(linux.Statx);
            switch (linux.errno(linux.statx(fd, empty_path, linux.AT.EMPTY_PATH, .{ .SIZE = true }, &statx))) {
                .SUCCESS => {
                    if (!statx.mask.SIZE) return error.Unexpected;
                    return statx.size;
                },
                .INTR => continue,
                else => |err| return posixStatError(err),
            }
        }
    } else {
        var stat: std.posix.Stat = undefined;
        while (true) {
            const rc = std.posix.system.fstat(fd, &stat);
            switch (std.posix.errno(rc)) {
                .SUCCESS => return @intCast(stat.size),
                .INTR => continue,
                else => |err| return posixStatError(err),
            }
        }
    }
}

fn seekFd(fd: std.posix.fd_t, offset: i64, whence: usize) !u64 {
    while (true) {
        const rc = std.posix.system.lseek(fd, offset, @intCast(whence));
        switch (std.posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            else => |err| return posixStatError(err),
        }
    }
}

fn posixReadError(err: std.posix.E) anyerror {
    return switch (err) {
        .AGAIN => error.WouldBlock,
        .BADF => error.InvalidFileDescriptor,
        .FAULT => error.InvalidAddress,
        .INVAL => error.InvalidArgument,
        .IO => error.InputOutput,
        .NXIO => error.NoDevice,
        .OVERFLOW => error.FileTooBig,
        else => std.posix.unexpectedErrno(err),
    };
}

fn posixWriteError(err: std.posix.E) anyerror {
    return switch (err) {
        .ACCES, .PERM, .ROFS => error.AccessDenied,
        .AGAIN => error.WouldBlock,
        .BADF => error.InvalidFileDescriptor,
        .DQUOT => error.DiskQuota,
        .FBIG, .OVERFLOW => error.FileTooBig,
        .FAULT => error.InvalidAddress,
        .INVAL => error.InvalidArgument,
        .IO => error.InputOutput,
        .NOSPC => error.NoSpaceLeft,
        .NXIO => error.NoDevice,
        .PIPE => error.BrokenPipe,
        else => std.posix.unexpectedErrno(err),
    };
}

fn posixStatError(err: std.posix.E) anyerror {
    return switch (err) {
        .ACCES, .PERM => error.AccessDenied,
        .BADF => error.InvalidFileDescriptor,
        .FAULT => error.InvalidAddress,
        .IO => error.InputOutput,
        .LOOP => error.SymLinkLoop,
        .NAMETOOLONG => error.NameTooLong,
        .NOENT => error.FileNotFound,
        .NOMEM => error.SystemResources,
        .NOTDIR => error.NotDir,
        .OVERFLOW => error.FileTooBig,
        else => std.posix.unexpectedErrno(err),
    };
}

const NativeBufferedAtomicWriteSink = struct {
    allocator: Allocator,
    state: *NativeStorageState,
    final_path: []u8,
    tmp_path: []u8,
    out: std.ArrayListUnmanaged(u8) = .empty,

    fn create(allocator: Allocator, path: []const u8, state: *NativeStorageState) !AtomicWriteSink {
        const retained_state = try state.retain();
        errdefer retained_state.release();

        const self = try allocator.create(NativeBufferedAtomicWriteSink);
        errdefer allocator.destroy(self);

        const final_path = try allocator.dupe(u8, path);
        errdefer allocator.free(final_path);

        const tmp_path = try tempSiblingPath(allocator, path);
        errdefer allocator.free(tmp_path);

        self.* = .{
            .allocator = allocator,
            .state = retained_state,
            .final_path = final_path,
            .tmp_path = tmp_path,
        };
        return .{
            .ptr = self,
            .vtable = &native_buffered_atomic_write_sink_vtable,
        };
    }

    fn deinit(self: *NativeBufferedAtomicWriteSink) void {
        self.state.release();
        self.out.deinit(self.allocator);
        self.allocator.free(self.final_path);
        self.allocator.free(self.tmp_path);
        self.allocator.destroy(self);
    }

    fn len(ptr: *anyopaque) usize {
        const self: *NativeBufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        return self.out.items.len;
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *NativeBufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        try byte_copy.appendSlicePossiblyAliased(&self.out, self.allocator, bytes);
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *NativeBufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (offset > self.out.items.len or bytes.len > self.out.items.len - offset) return error.InvalidAtomicWriteOffset;
        byte_copy.copyPossiblyAliased(self.out.items[offset..][0..bytes.len], bytes);
    }

    fn crc32Prefix(ptr: *anyopaque, len_prefix: usize) !u32 {
        const self: *NativeBufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (len_prefix > self.out.items.len) return error.InvalidAtomicWriteOffset;
        return Crc32.hash(self.out.items[0..len_prefix]);
    }

    fn crc32Range(ptr: *anyopaque, offset: usize, range_len: usize) !u32 {
        const self: *NativeBufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (offset > self.out.items.len or range_len > self.out.items.len - offset) return error.InvalidAtomicWriteOffset;
        return Crc32.hash(self.out.items[offset..][0..range_len]);
    }

    fn deleteStagingFile(self: *NativeBufferedAtomicWriteSink) void {
        const io = self.state.threaded.io();
        const previous = io.swapCancelProtection(.blocked);
        defer _ = io.swapCancelProtection(previous);
        deleteFilePathWithIo(io, self.tmp_path) catch {};
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *NativeBufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        defer self.deinit();

        const io = self.state.threaded.io();
        self.state.invalidatePath(self.tmp_path);
        writeFileAbsoluteWithIo(io, self.tmp_path, self.out.items) catch |err| {
            self.deleteStagingFile();
            self.state.invalidatePath(self.tmp_path);
            return err;
        };
        syncFileContentsPathWithIo(io, self.tmp_path) catch |err| {
            self.deleteStagingFile();
            self.state.invalidatePath(self.tmp_path);
            return err;
        };
        self.state.invalidateRename(self.tmp_path, self.final_path);
        defer self.state.invalidateRename(self.tmp_path, self.final_path);
        renamePathWithIo(io, self.tmp_path, self.final_path) catch |err| {
            self.deleteStagingFile();
            return err;
        };
        try syncParentPathWithIo(io, self.final_path);
    }

    fn abort(ptr: *anyopaque) void {
        const self: *NativeBufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        self.state.invalidatePath(self.tmp_path);
        self.deleteStagingFile();
        self.state.invalidatePath(self.tmp_path);
        self.deinit();
    }
};

const native_buffered_atomic_write_sink_vtable: AtomicWriteSink.VTable = .{
    .len = NativeBufferedAtomicWriteSink.len,
    .append_slice = NativeBufferedAtomicWriteSink.appendSlice,
    .write_at = NativeBufferedAtomicWriteSink.writeAt,
    .crc32_prefix = NativeBufferedAtomicWriteSink.crc32Prefix,
    .crc32_range = NativeBufferedAtomicWriteSink.crc32Range,
    .finish = NativeBufferedAtomicWriteSink.finish,
    .abort = NativeBufferedAtomicWriteSink.abort,
};

fn tryBeginNativeColdRandomReads(ptr: *anyopaque, allocator: Allocator, paths: []const []const u8) !?[]ColdSequentialReader {
    if (paths.len == 0) return try allocator.alloc(ColdSequentialReader, 0);
    const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
    var reservation = (try state.tryAcquireFdPermits(paths.len)) orelse return null;
    defer reservation.release();
    const readers = try allocator.alloc(ColdSequentialReader, paths.len);
    errdefer allocator.free(readers);
    var initialized: usize = 0;
    errdefer for (readers[0..initialized]) |*reader| reader.deinit();
    for (paths, readers) |path, *reader| {
        var permit = if (reservation.count == 1) reservation.take() else try reservation.splitOne();
        defer permit.release();
        reader.* = try NativeColdSequentialReader.createWithPermit(allocator, path, &permit, .random);
        initialized += 1;
    }
    return readers;
}

fn nativeColdSequentialReaderCapacity(state: *const NativeStorageState) usize {
    if (comptime !supports_posix_fd_cache) return 0;
    const cache = state.fdCacheConst();
    const unavailable = cache.persistent_reserve +| 1; // atomic output writer
    return cache.capacity -| unavailable;
}

const NativeColdSequentialReader = struct {
    const direct_alignment: usize = 4096;
    const max_coalesced_gap: usize = 4096;
    const max_coalesced_bytes: usize = 1024 * 1024;

    allocator: Allocator,
    fd_permit: NativeFdPermit,
    fd: std.posix.fd_t,
    direct_io: std.atomic.Value(bool) = .init(false),
    direct_mutex: std.atomic.Mutex = .unlocked,
    direct_scratch: ?[]align(direct_alignment) u8 = null,

    const vtable: ColdSequentialReader.VTable = .{
        .read_range_alloc = readRangeAlloc,
        .read_range_into = readRangeInto,
        .read_ranges_into = readRangesInto,
        .release_scratch = releaseScratch,
        .deinit = deinitErased,
    };

    const Intent = enum { sequential, random };

    const OpenedFd = struct {
        fd: std.posix.fd_t,
        direct_io: bool,
    };

    fn openCold(path: []const u8) !OpenedFd {
        const regular_flags: std.posix.O = .{
            .ACCMODE = .RDONLY,
            .CLOEXEC = true,
        };
        if (comptime builtin.os.tag == .linux) {
            var direct_flags = regular_flags;
            direct_flags.DIRECT = true;
            if (std.posix.openat(std.posix.AT.FDCWD, path, direct_flags, 0)) |fd| {
                return .{ .fd = fd, .direct_io = true };
            } else |_| {
                // Some filesystems (notably tmpfs and network filesystems) do
                // not support O_DIRECT. Preserve availability with a buffered
                // descriptor whose pages are marked as one-shot below.
            }
        }
        return .{
            .fd = try std.posix.openat(std.posix.AT.FDCWD, path, regular_flags, 0),
            .direct_io = false,
        };
    }

    fn create(allocator: Allocator, path: []const u8, state: *NativeStorageState, intent: Intent) !ColdSequentialReader {
        if (comptime !supports_posix_fd_cache) return error.UnsupportedNativeStorageRuntime;

        var permit = try state.acquireFdPermit();
        defer permit.release();
        return try createWithPermit(allocator, path, &permit, intent);
    }

    fn createWithPermit(allocator: Allocator, path: []const u8, permit: *NativeFdPermit, intent: Intent) !ColdSequentialReader {
        if (comptime !supports_posix_fd_cache) return error.UnsupportedNativeStorageRuntime;
        const opened = try openCold(path);
        errdefer closeFd(opened.fd);

        switch (builtin.os.tag) {
            .macos, .ios, .tvos, .watchos, .visionos =>
            // This descriptor is maintenance-private, so F_NOCACHE cannot
            // alter foreground query reads through the shared descriptor cache.
            _ = std.posix.system.fcntl(opened.fd, std.posix.F.NOCACHE, @as(usize, 1)),
            .linux => if (!opened.direct_io) {
                _ = std.os.linux.fadvise(opened.fd, 0, 0, switch (intent) {
                    .sequential => std.os.linux.POSIX_FADV.SEQUENTIAL,
                    .random => std.os.linux.POSIX_FADV.RANDOM,
                });
                // Since Linux 6.3 NOREUSE tells the replacement algorithm that
                // these pages are maintenance-only. Older kernels safely treat
                // it as a no-op; unlike DONTNEED it cannot evict a foreground
                // mmap of the same immutable inode.
                _ = std.os.linux.fadvise(opened.fd, 0, 0, std.os.linux.POSIX_FADV.NOREUSE);
            },
            else => {},
        }

        const self = try allocator.create(NativeColdSequentialReader);
        self.* = .{
            .allocator = allocator,
            .fd_permit = permit.take(),
            .fd = opened.fd,
            .direct_io = .init(opened.direct_io),
        };
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn readRangeAlloc(ptr: *anyopaque, allocator: Allocator, offset: u64, len: usize) ![]u8 {
        const self: *NativeColdSequentialReader = @ptrCast(@alignCast(ptr));
        const out = try allocator.alloc(u8, len);
        errdefer allocator.free(out);
        try self.readInto(offset, out);
        return out;
    }

    fn readRangeInto(ptr: *anyopaque, offset: u64, out: []u8) !void {
        const self: *NativeColdSequentialReader = @ptrCast(@alignCast(ptr));
        return try self.readInto(offset, out);
    }

    fn readInto(self: *NativeColdSequentialReader, offset: u64, out: []u8) !void {
        if (out.len == 0) return;
        // Buffered scalar reads can fill the caller's destination directly.
        // Batch coalescing still uses the private scatter extent below.
        if (!self.direct_io.load(.acquire)) return try readAllAtOffset(self.fd, out, offset);
        const ranges = [1]ColdReadRange{.{ .offset = offset, .destination = out }};
        _ = try self.readRanges(&ranges);
    }

    fn readRangesInto(ptr: *anyopaque, ranges: []const ColdReadRange) !ColdReadStats {
        const self: *NativeColdSequentialReader = @ptrCast(@alignCast(ptr));
        return try self.readRanges(ranges);
    }

    fn releaseScratch(ptr: *anyopaque) void {
        const self: *NativeColdSequentialReader = @ptrCast(@alignCast(ptr));
        const locked = lockAtomic(&self.direct_mutex);
        defer if (locked) self.direct_mutex.unlock();
        if (self.direct_scratch) |scratch| self.allocator.free(scratch);
        self.direct_scratch = null;
    }

    fn ensureScratch(self: *NativeColdSequentialReader, required: usize) ![]align(direct_alignment) u8 {
        if (self.direct_scratch == null or self.direct_scratch.?.len < required) {
            const replacement = try self.allocator.alignedAlloc(
                u8,
                .fromByteUnits(direct_alignment),
                required,
            );
            if (self.direct_scratch) |scratch| self.allocator.free(scratch);
            self.direct_scratch = replacement;
        }
        return self.direct_scratch.?[0..required];
    }

    fn readRanges(self: *NativeColdSequentialReader, ranges: []const ColdReadRange) !ColdReadStats {
        var stats: ColdReadStats = .{};
        for (ranges) |range| stats.logical_bytes +|= range.destination.len;
        if (ranges.len == 0) return stats;

        // Buffered platforms can fill a singleton destination directly. Most
        // projection-build leaf batches touch a shard once, so forcing those
        // reads through the coalescing bounce buffer adds a copy without
        // creating a larger physical extent. O_DIRECT still needs the aligned
        // scratch path below.
        if (ranges.len == 1 and !self.direct_io.load(.acquire)) {
            const range = ranges[0];
            if (range.destination.len != 0) {
                try readAllAtOffset(self.fd, range.destination, range.offset);
                stats.physical_bytes = range.destination.len;
                stats.physical_reads = 1;
            }
            return stats;
        }

        // O_DIRECT requires aligned offsets, lengths, and destination buffers.
        // One reusable extent per descriptor coalesces the already sorted
        // projection stream. This avoids issuing and serializing one aligned
        // read for every small vector while retaining bounded scratch memory.
        const locked = lockAtomic(&self.direct_mutex);
        defer if (locked) self.direct_mutex.unlock();

        var start: usize = 0;
        while (start < ranges.len) {
            while (start < ranges.len and ranges[start].destination.len == 0) : (start += 1) {}
            if (start == ranges.len) break;
            const first_end = std.math.add(u64, ranges[start].offset, ranges[start].destination.len) catch
                return error.OutOfMemory;
            const logical_start = ranges[start].offset;
            var logical_end = first_end;
            var end = start + 1;
            while (end < ranges.len) : (end += 1) {
                const next = ranges[end];
                if (next.destination.len == 0) continue;
                if (next.offset < logical_start) return error.UnsortedColdReadRanges;
                const next_end = std.math.add(u64, next.offset, next.destination.len) catch return error.OutOfMemory;
                const permitted_end = std.math.add(u64, logical_end, max_coalesced_gap) catch std.math.maxInt(u64);
                const extent_bytes = next_end -| logical_start;
                if (next.offset > permitted_end or extent_bytes > max_coalesced_bytes) break;
                logical_end = @max(logical_end, next_end);
            }

            var read_start = logical_start;
            var read_end = logical_end;
            const direct = self.direct_io.load(.acquire);
            if (direct) {
                read_start -= read_start % direct_alignment;
                const padded_end = std.math.add(u64, read_end, direct_alignment - 1) catch return error.OutOfMemory;
                read_end = padded_end & ~@as(u64, direct_alignment - 1);
            }
            const read_len = std.math.cast(usize, read_end - read_start) orelse return error.OutOfMemory;
            const scratch = try self.ensureScratch(read_len);
            if (direct) {
                const actual = readOnceAtOffset(self.fd, scratch, read_start) catch |err| switch (err) {
                    error.InvalidArgument => blk: {
                        // A filesystem may accept O_DIRECT at open but impose a
                        // larger per-inode alignment. Keep the same coalesced
                        // logical extent when switching to the buffered
                        // fallback. Do not require alignment padding past EOF
                        // once the descriptor is buffered.
                        try self.disableDirectIo();
                        self.direct_io.store(false, .release);
                        const buffered_len = std.math.cast(usize, logical_end - read_start) orelse
                            return error.OutOfMemory;
                        try readAllAtOffset(self.fd, scratch[0..buffered_len], read_start);
                        break :blk buffered_len;
                    },
                    else => return err,
                };
                if (actual < logical_end - read_start) return error.EndOfStream;
                stats.physical_bytes +|= actual;
            } else {
                try readAllAtOffset(self.fd, scratch, read_start);
                stats.physical_bytes +|= read_len;
            }
            stats.physical_reads +|= 1;

            for (ranges[start..end]) |range| {
                if (range.destination.len == 0) continue;
                const relative = std.math.cast(usize, range.offset - read_start) orelse return error.OutOfMemory;
                const relative_end = std.math.add(usize, relative, range.destination.len) catch return error.OutOfMemory;
                if (relative_end > scratch.len) return error.EndOfStream;
                @memcpy(range.destination, scratch[relative..relative_end]);
            }
            start = end;
        }
        return stats;
    }

    fn disableDirectIo(self: *NativeColdSequentialReader) !void {
        if (comptime builtin.os.tag != .linux) return;
        var flags: usize = while (true) {
            const rc = std.posix.system.fcntl(self.fd, std.posix.F.GETFL, @as(usize, 0));
            switch (std.posix.errno(rc)) {
                .SUCCESS => break @intCast(rc),
                .INTR => continue,
                else => |err| return std.posix.unexpectedErrno(err),
            }
        };
        flags &= ~(@as(usize, 1) << @bitOffsetOf(std.posix.O, "DIRECT"));
        while (true) {
            const rc = std.posix.system.fcntl(self.fd, std.posix.F.SETFL, flags);
            switch (std.posix.errno(rc)) {
                .SUCCESS => {
                    _ = std.os.linux.fadvise(self.fd, 0, 0, std.os.linux.POSIX_FADV.NOREUSE);
                    return;
                },
                .INTR => continue,
                else => |err| return std.posix.unexpectedErrno(err),
            }
        }
    }

    fn deinitErased(ptr: *anyopaque) void {
        const self: *NativeColdSequentialReader = @ptrCast(@alignCast(ptr));
        if (self.direct_scratch) |scratch| self.allocator.free(scratch);
        closeFd(self.fd);
        self.fd_permit.release();
        const allocator = self.allocator;
        allocator.destroy(self);
    }
};

const NativeAtomicWriteSink = struct {
    allocator: Allocator,
    state: *NativeStorageState,
    fd_permit: NativeFdPermit,
    final_path: []u8,
    tmp_path: []u8,
    fd: std.posix.fd_t,
    bytes_written: usize = 0,
    cache_intent: AtomicWriteCacheIntent = .normal,
    last_cold_drain_bytes: usize = 0,

    fn create(allocator: Allocator, path: []const u8, state: *NativeStorageState) !AtomicWriteSink {
        if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) {
            // std.Io rename does not expose a write-through durability contract
            // on these targets. Fail before creating the temporary file rather
            // than acknowledge an atomic write whose namespace may be lost.
            return error.DurableAtomicRenameUnsupported;
        }

        var fd_permit = try state.acquireFdPermit();
        errdefer fd_permit.release();
        return try createWithPermit(allocator, path, state, &fd_permit);
    }

    fn createWithPermit(
        allocator: Allocator,
        path: []const u8,
        state: *NativeStorageState,
        permit: *NativeFdPermit,
    ) !AtomicWriteSink {
        if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) {
            return error.DurableAtomicRenameUnsupported;
        }
        if (!permit.active or permit.count != 1 or permit.state != state) return error.InvalidNativeFdPermit;

        const final_path = try allocator.dupe(u8, path);
        errdefer allocator.free(final_path);

        const tmp_path = try tempSiblingPath(allocator, path);
        errdefer allocator.free(tmp_path);

        const retained = try state.retain();
        errdefer retained.release();
        const fd = try createAtomicWriteFdPosix(tmp_path);
        errdefer closeFd(fd);

        const self = try allocator.create(NativeAtomicWriteSink);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .state = retained,
            .fd_permit = permit.take(),
            .final_path = final_path,
            .tmp_path = tmp_path,
            .fd = fd,
        };
        return .{
            .ptr = self,
            .vtable = &native_atomic_write_sink_vtable,
        };
    }

    fn deinit(self: *NativeAtomicWriteSink) void {
        if (self.fd >= 0) closeFd(self.fd);
        self.fd_permit.release();
        self.state.release();
        self.allocator.free(self.final_path);
        self.allocator.free(self.tmp_path);
        self.allocator.destroy(self);
    }

    fn len(ptr: *anyopaque) usize {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        return self.bytes_written;
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (self.cache_intent != .cold_sequential) {
            try writeAllAtOffset(self.fd, bytes, @intCast(self.bytes_written));
            self.bytes_written += bytes.len;
            return;
        }

        // Callers are allowed to hand the sink an entire multi-gigabyte
        // immutable block.  Chunk here, below that API boundary, so the cache
        // residency bound cannot accidentally depend on their buffering.
        var consumed: usize = 0;
        while (consumed < bytes.len) {
            const since_drain = self.bytes_written - self.last_cold_drain_bytes;
            const until_drain = cold_atomic_writeback_chunk - since_drain;
            const chunk_len = @min(bytes.len - consumed, until_drain);
            try writeAllAtOffset(self.fd, bytes[consumed..][0..chunk_len], @intCast(self.bytes_written));
            self.bytes_written += chunk_len;
            consumed += chunk_len;
            try self.drainColdWritebackIfNeeded();
        }
    }

    fn drainColdWritebackIfNeeded(self: *NativeAtomicWriteSink) !void {
        if (self.cache_intent != .cold_sequential or
            self.bytes_written - self.last_cold_drain_bytes < cold_atomic_writeback_chunk) return;

        // F_NODIRECT lets macOS aggregate writes efficiently, but dirty pages
        // cannot be discarded until writeback. Bound that dirty window and
        // make every completed prefix immediately reclaimable instead of
        // retaining a multi-gigabyte checkpoint or compaction until close.
        try fs_paths.syncFileFdPortable(self.fd);
        self.last_cold_drain_bytes = self.bytes_written;
        if (builtin.os.tag == .linux) {
            _ = std.os.linux.fadvise(self.fd, 0, @intCast(self.last_cold_drain_bytes), std.os.linux.POSIX_FADV.DONTNEED);
        }
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (offset > self.bytes_written or bytes.len > self.bytes_written - offset) return error.InvalidAtomicWriteOffset;
        try writeAllAtOffset(self.fd, bytes, @intCast(offset));
    }

    fn crc32Prefix(ptr: *anyopaque, len_prefix: usize) !u32 {
        return crc32Range(ptr, 0, len_prefix);
    }

    fn crc32Range(ptr: *anyopaque, range_offset: usize, range_len: usize) !u32 {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (range_offset > self.bytes_written or range_len > self.bytes_written - range_offset) return error.InvalidAtomicWriteOffset;

        var crc = Crc32.init();
        var offset: usize = 0;
        var buf: [64 * 1024]u8 = undefined;
        while (offset < range_len) {
            const n = @min(buf.len, range_len - offset);
            try readAllAtOffset(self.fd, buf[0..n], @intCast(range_offset + offset));
            crc.update(buf[0..n]);
            offset += n;
        }
        return crc.final();
    }

    fn setCacheIntent(ptr: *anyopaque, intent: AtomicWriteCacheIntent) void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        self.cache_intent = intent;
        self.last_cold_drain_bytes = self.bytes_written;
        switch (builtin.os.tag) {
            .macos, .ios, .tvos, .watchos, .visionos => {
                const enabled: usize = if (intent == .cold_sequential) 1 else 0;
                // F_NOCACHE is advisory. Failure must not change durability or
                // make a portable storage implementation unusable.
                _ = std.posix.system.fcntl(self.fd, std.posix.F.NOCACHE, enabled);
                // Allow buffered writeback internally while still preventing
                // the completed one-pass output from polluting the file cache.
                // Without F_NODIRECT, macOS may turn F_NOCACHE into slow
                // synchronous direct writes.
                _ = std.posix.system.fcntl(self.fd, std.posix.F.NODIRECT, enabled);
            },
            .linux => {
                _ = std.os.linux.fadvise(self.fd, 0, 0, linuxWriteCacheAdvice(intent));
            },
            else => {},
        }
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        defer self.deinit();

        fs_paths.syncFileFdPortable(self.fd) catch |err| {
            closeFd(self.fd);
            self.fd = -1;
            self.state.invalidatePath(self.tmp_path);
            deleteFilePathPosix(self.tmp_path) catch {};
            self.state.invalidatePath(self.tmp_path);
            return err;
        };
        if (self.cache_intent == .cold_sequential and builtin.os.tag == .linux) {
            // Dirty pages become discardable only after the durability sync.
            // Bound each completed compaction/checkpoint output rather than
            // retaining every prior output until global memory pressure.
            _ = std.os.linux.fadvise(self.fd, 0, 0, std.os.linux.POSIX_FADV.DONTNEED);
        }
        closeFd(self.fd);
        self.fd = -1;
        // The data-file descriptor is closed, so its existing admission slot
        // can be transferred directly to the parent-directory descriptor.
        // Do not relinquish and reacquire it here: another opener could claim
        // the slot and make finish fail after the rename has already published
        // the destination.

        self.state.invalidateRename(self.tmp_path, self.final_path);
        defer self.state.invalidateRename(self.tmp_path, self.final_path);
        renameAbsoluteDirectPosix(self.tmp_path, self.final_path) catch |err| {
            deleteFilePathPosix(self.tmp_path) catch {};
            return err;
        };
        try syncParentDirectoryPosix(self.final_path);
    }

    fn abort(ptr: *anyopaque) void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (self.fd >= 0) {
            closeFd(self.fd);
            self.fd = -1;
        }
        self.state.invalidatePath(self.tmp_path);
        deleteFilePathPosix(self.tmp_path) catch {};
        self.state.invalidatePath(self.tmp_path);
        self.deinit();
    }
};

const native_atomic_write_sink_vtable: AtomicWriteSink.VTable = .{
    .len = NativeAtomicWriteSink.len,
    .append_slice = NativeAtomicWriteSink.appendSlice,
    .write_at = NativeAtomicWriteSink.writeAt,
    .crc32_prefix = NativeAtomicWriteSink.crc32Prefix,
    .crc32_range = NativeAtomicWriteSink.crc32Range,
    .set_cache_intent = NativeAtomicWriteSink.setCacheIntent,
    .finish = NativeAtomicWriteSink.finish,
    .abort = NativeAtomicWriteSink.abort,
};

fn syncParentDirectoryPosix(path: []const u8) !void {
    const parent_path = std.fs.path.dirname(path) orelse if (std.fs.path.isAbsolute(path)) "/" else ".";
    const parent_fd = try std.posix.openat(std.posix.AT.FDCWD, parent_path, .{
        .ACCMODE = .RDONLY,
        .DIRECTORY = true,
        .CLOEXEC = true,
    }, 0);
    defer closeFd(parent_fd);
    try fs_paths.syncDirectoryFdPortable(parent_fd);
}

pub const MemoryStorage = struct {
    allocator: Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    files: std.StringHashMapUnmanaged([]u8) = .empty,
    tick: u64 = 1,
    sync_contents_calls: u64 = 0,

    pub fn init(allocator: Allocator) MemoryStorage {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *MemoryStorage) void {
        var it = self.files.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.files.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn storage(self: *MemoryStorage) Storage {
        return .{
            .ptr = self,
            .vtable = &memory_vtable,
        };
    }
};

fn openReadFd(path: [*:0]const u8) !std.posix.fd_t {
    return std.posix.openatZ(std.posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
}

fn lockAtomicWithAdmission(mutex: *std.atomic.Mutex, control: ?read_admission.Port) !bool {
    if (control) |budget| {
        try @import("../../common/bounded_mutex.zig").lockAtomic(mutex, budget);
        return true;
    }
    return lockAtomic(mutex);
}

fn lockAtomic(mutex: *std.atomic.Mutex) bool {
    if (builtin.os.tag == .freestanding) return false;
    platform_sync.lockYielding(mutex);
    return true;
}

fn memoryScratchDirectory(_: *anyopaque) ?[]const u8 {
    return null;
}

const memory_vtable: Storage.VTable = .{
    .private_scratch_directory = memoryScratchDirectory,
    .create_dir_path = memoryCreateDirPath,
    .read_file_alloc = memoryReadFileAlloc,
    .read_file_range_alloc = memoryReadFileRangeAlloc,
    .file_size = memoryFileSize,
    .read_file_trailer_alloc = memoryReadFileTrailerAlloc,
    .write_file_absolute = memoryWriteFileAbsolute,
    .append_file_absolute = memoryAppendFileAbsolute,
    .sync_contents_absolute = memorySyncFileContentsAbsolute,
    .sync_parent_absolute = memorySyncParentAbsolute,
    .rename_absolute = memoryRenameAbsolute,
    .delete_file_absolute = memoryDeleteFileAbsolute,
    .delete_tree = memoryDeleteTree,
    .list_file_names_alloc = memoryListFileNamesAlloc,
    .list_directory_names_alloc = memoryListDirectoryNamesAlloc,
    .now_ns = memoryNowNs,
    .rename_is_atomic = true,
};

test "native path locking does not imply host generation publication" {
    var lock_only_vtable = memory_vtable;
    lock_only_vtable.supports_native_path_locks = true;
    var memory = MemoryStorage.init(std.testing.allocator);
    defer memory.deinit();
    const lock_only: Storage = .{ .ptr = &memory, .vtable = &lock_only_vtable };

    try std.testing.expect(lock_only.supportsNativePathLocks());
    try std.testing.expect(!lock_only.supportsHostPathGenerationPublication());
}

fn memoryCreateDirPath(_: *anyopaque, _: []const u8) !void {}

fn memoryListDirectoryNamesAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8) ![][]u8 {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();
    return listMapNamesAlloc(allocator, path, &self.files, true);
}

/// Shared directory inference for map-backed file services. This only matches
/// descendants below a separator boundary, never neighboring prefix names.
pub fn listMapNamesAlloc(allocator: Allocator, path: []const u8, files: anytype, directories: bool) ![][]u8 {
    const directory = std.mem.trimEnd(u8, path, "/");
    const prefix = try std.fmt.allocPrint(allocator, "{s}/", .{directory});
    defer allocator.free(prefix);
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(allocator);
    var names = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (names.items) |name| allocator.free(name);
        names.deinit(allocator);
    }
    var iterator = files.keyIterator();
    while (iterator.next()) |stored_path| {
        if (!std.mem.startsWith(u8, stored_path.*, prefix)) continue;
        const relative = stored_path.*[prefix.len..];
        const separator = std.mem.indexOfScalar(u8, relative, '/');
        if (directories != (separator != null)) continue;
        const name = relative[0..(separator orelse relative.len)];
        if (name.len == 0 or seen.contains(name)) continue;
        const owned = try allocator.dupe(u8, name);
        errdefer allocator.free(owned);
        try seen.put(allocator, owned, {});
        try names.append(allocator, owned);
    }
    return try names.toOwnedSlice(allocator);
}

fn memoryListFileNamesAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8) ![][]u8 {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();
    return listMapNamesAlloc(allocator, path, &self.files, false);
}

fn memoryReadFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const stored = self.files.get(path) orelse return error.FileNotFound;
    if (stored.len > max_bytes) return error.FileTooBig;
    return try allocator.dupe(u8, stored);
}

fn memoryReadFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const stored = self.files.get(path) orelse return error.FileNotFound;
    const start: usize = @intCast(offset);
    if (start > stored.len or stored.len - start < len) return error.EndOfStream;
    return try allocator.dupe(u8, stored[start .. start + len]);
}

fn memoryFileSize(ptr: *anyopaque, path: []const u8) !u64 {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const stored = self.files.get(path) orelse return error.FileNotFound;
    return stored.len;
}

fn memoryReadFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !FileTrailer {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const stored = self.files.get(path) orelse return error.FileNotFound;
    if (stored.len < len) return error.EndOfStream;
    return .{
        .bytes = try allocator.dupe(u8, stored[stored.len - len ..]),
        .file_size = stored.len,
    };
}

fn memoryWriteFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const owned_path = try self.allocator.dupe(u8, path);
    errdefer self.allocator.free(owned_path);
    const owned_contents = try self.allocator.dupe(u8, contents);
    errdefer self.allocator.free(owned_contents);

    const gop = try self.files.getOrPut(self.allocator, owned_path);
    if (gop.found_existing) {
        self.allocator.free(owned_path);
        self.allocator.free(gop.value_ptr.*);
        gop.value_ptr.* = owned_contents;
    } else {
        gop.value_ptr.* = owned_contents;
    }
}

fn memoryAppendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
    _ = sync;
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    if (self.files.getPtr(path)) |value_ptr| {
        const old = value_ptr.*;
        const joined = try self.allocator.alloc(u8, old.len + contents.len);
        errdefer self.allocator.free(joined);
        @memcpy(joined[0..old.len], old);
        @memcpy(joined[old.len..], contents);
        self.allocator.free(old);
        value_ptr.* = joined;
        return;
    }

    const owned_path = try self.allocator.dupe(u8, path);
    errdefer self.allocator.free(owned_path);
    const owned_contents = try self.allocator.dupe(u8, contents);
    errdefer self.allocator.free(owned_contents);
    try self.files.putNoClobber(self.allocator, owned_path, owned_contents);
}

fn memorySyncFileContentsAbsolute(ptr: *anyopaque, _: []const u8) !void {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();
    self.sync_contents_calls += 1;
}

fn memorySyncParentAbsolute(_: *anyopaque, _: []const u8) !void {}

fn memoryRenameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    if (!self.files.contains(old_path)) return error.FileNotFound;
    if (std.mem.eql(u8, old_path, new_path)) return;
    // Finish fallible preparation while both paths still own their bytes.
    // Removing the source guarantees capacity for one nonfallible insertion.
    const new_key = try self.allocator.dupe(u8, new_path);
    const removed = self.files.fetchRemove(old_path).?;
    const old_key = removed.key;
    const value = removed.value;
    const gop = self.files.getOrPutAssumeCapacity(new_key);
    if (gop.found_existing) {
        self.allocator.free(new_key);
        self.allocator.free(gop.value_ptr.*);
        gop.value_ptr.* = value;
    } else {
        gop.value_ptr.* = value;
    }
    self.allocator.free(old_key);
}

fn memoryDeleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const removed = self.files.fetchRemove(path) orelse return error.FileNotFound;
    self.allocator.free(removed.key);
    self.allocator.free(removed.value);
}

fn memoryDeleteTree(ptr: *anyopaque, path: []const u8) !void {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    var doomed = std.ArrayListUnmanaged([]const u8).empty;
    defer doomed.deinit(self.allocator);

    var it = self.files.iterator();
    while (it.next()) |entry| {
        if (!pathContains(path, entry.key_ptr.*)) continue;
        try doomed.append(self.allocator, entry.key_ptr.*);
    }

    for (doomed.items) |doomed_key| {
        const removed = self.files.fetchRemove(doomed_key) orelse continue;
        self.allocator.free(removed.key);
        self.allocator.free(removed.value);
    }
}

fn memoryNowNs(ptr: *anyopaque) u64 {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();
    const current = self.tick;
    self.tick += 1;
    return current;
}

fn pathContains(prefix: []const u8, path: []const u8) bool {
    // Preserve the storage namespace's empty spelling for the absolute root.
    if (prefix.len == 0) return path.len == 0 or path[0] == '/';

    // Storage paths use '/' on every backend. Treat redundant trailing
    // separators as spelling aliases without allocating a normalized copy.
    // Keep one separator for the root so "/" contains every absolute path.
    var prefix_len = prefix.len;
    while (prefix_len > 1 and prefix[prefix_len - 1] == '/') prefix_len -= 1;
    const normalized = prefix[0..prefix_len];
    if (normalized.len == 1 and normalized[0] == '/') {
        return path.len > 0 and path[0] == '/';
    }
    if (!std.mem.startsWith(u8, path, normalized)) return false;
    if (path.len == normalized.len) return true;
    return path[normalized.len] == '/';
}

fn tempSiblingPath(allocator: Allocator, path: []const u8) ![]u8 {
    const nonce = atomic_write_nonce.fetchAdd(1, .monotonic);
    return try std.fmt.allocPrint(allocator, "{s}.tmp-{d}", .{ path, nonce });
}

var atomic_write_nonce: CounterU64 = .init(0);
var test_fd_cache_pause_after_open: std.atomic.Value(bool) = .init(false);
var test_fd_cache_open_paused: std.atomic.Value(bool) = .init(false);
var test_fd_cache_release_after_open: std.atomic.Value(bool) = .init(false);

fn hashPath(path: []const u8) u64 {
    return std.hash.Wyhash.hash(0x6d3b7a1db6f9c24f, path);
}

fn namespacedPathHash(namespace: u64, path: []const u8) u64 {
    return std.hash.Wyhash.hash(namespace ^ 0x6d3b7a1db6f9c24f, path);
}

test "host storage delegates through callbacks" {
    var backing = MemoryStorage.init(std.testing.allocator);
    defer backing.deinit();

    const HostContext = struct {
        backing: *MemoryStorage,
        trailer_reads: usize = 0,
        content_syncs: usize = 0,
        parent_syncs: usize = 0,

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !FileTrailer {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.trailer_reads += 1;
            return self.backing.storage().readFileTrailerAlloc(allocator, path, len);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn syncFileContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.content_syncs += 1;
            return self.backing.storage().syncFileContentsAbsolute(path);
        }

        fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.parent_syncs += 1;
            return self.backing.storage().syncParentAbsolute(path);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    const host_vtable: Storage.VTable = .{
        .create_dir_path = HostContext.createDirPath,
        .read_file_alloc = HostContext.readFileAlloc,
        .read_file_range_alloc = HostContext.readFileRangeAlloc,
        .file_size = HostContext.fileSize,
        .read_file_trailer_alloc = HostContext.readFileTrailerAlloc,
        .write_file_absolute = HostContext.writeFileAbsolute,
        .rename_absolute = HostContext.renameAbsolute,
        .delete_file_absolute = HostContext.deleteFileAbsolute,
        .delete_tree = HostContext.deleteTree,
        .now_ns = HostContext.nowNs,
    };

    var host_ctx = HostContext{ .backing = &backing };
    const host = HostStorage.init(&host_ctx, &host_vtable).storage();

    try host.createDirPath("/host");
    try host.writeFileAbsolute("/host/a.txt", "hello");
    const hello = try host.readFileAlloc(std.testing.allocator, "/host/a.txt", 32);
    defer std.testing.allocator.free(hello);
    try std.testing.expectEqualStrings("hello", hello);
    const ell = try host.readFileRangeAlloc(std.testing.allocator, "/host/a.txt", 1, 3);
    defer std.testing.allocator.free(ell);
    try std.testing.expectEqualStrings("ell", ell);
    var llo = try host.readFileTrailerAlloc(std.testing.allocator, "/host/a.txt", 3);
    defer llo.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("llo", llo.bytes);
    try std.testing.expectEqual(@as(u64, 5), llo.file_size);
    try std.testing.expectEqual(@as(usize, 1), host_ctx.trailer_reads);

    try host.renameAbsolute("/host/a.txt", "/host/b.txt");
    try std.testing.expectError(error.FileNotFound, host.readFileAlloc(std.testing.allocator, "/host/a.txt", 32));
    const renamed = try host.readFileAlloc(std.testing.allocator, "/host/b.txt", 32);
    defer std.testing.allocator.free(renamed);
    try std.testing.expectEqualStrings("hello", renamed);

    try host.writeFileAbsolute("/host/sub/c.txt", "world");
    try host.deleteTree("/host/sub");
    try std.testing.expectError(error.FileNotFound, host.readFileAlloc(std.testing.allocator, "/host/sub/c.txt", 32));

    try host.deleteFileAbsolute("/host/b.txt");
    try std.testing.expectError(error.FileNotFound, host.readFileAlloc(std.testing.allocator, "/host/b.txt", 32));

    const t0 = host.nowNs();
    const t1 = host.nowNs();
    try std.testing.expect(t1 > t0);
    try std.testing.expectError(
        error.DurableAtomicWriteUnsupported,
        host.beginAtomicWrite(std.testing.allocator, "/host/not-durable"),
    );
    try std.testing.expectError(
        error.DurableFileSyncUnsupported,
        host.syncFileContentsAbsolute("/host/not-durable"),
    );
    try std.testing.expectError(
        error.DurableDirectorySyncUnsupported,
        host.syncParentAbsolute("/host/not-durable"),
    );

    var durable_host_vtable = host_vtable;
    durable_host_vtable.sync_contents_absolute = HostContext.syncFileContentsAbsolute;
    const durable_host = HostStorage.init(&host_ctx, &durable_host_vtable).storage();
    try std.testing.expectError(
        error.DurableAtomicWriteUnsupported,
        durable_host.beginAtomicWrite(std.testing.allocator, "/host/not-atomic"),
    );
    try std.testing.expectError(
        error.DurableDirectorySyncUnsupported,
        durable_host.syncParentAbsolute("/host/not-parent-durable"),
    );
    try durable_host.appendFileAbsolute(std.testing.allocator, "/host/durable.log", "a", true);
    try durable_host.appendFileAbsolute(std.testing.allocator, "/host/durable.log", "b", true);
    try std.testing.expectEqual(@as(usize, 2), host_ctx.content_syncs);
    const durable_contents = try durable_host.readFileAlloc(std.testing.allocator, "/host/durable.log", 32);
    defer std.testing.allocator.free(durable_contents);
    try std.testing.expectEqualStrings("ab", durable_contents);

    var atomic_host_vtable = durable_host_vtable;
    atomic_host_vtable.sync_parent_absolute = HostContext.syncParentAbsolute;
    atomic_host_vtable.rename_is_atomic = true;
    const atomic_host = HostStorage.init(&host_ctx, &atomic_host_vtable).storage();
    var atomic_writer = try atomic_host.beginAtomicWrite(std.testing.allocator, "/host/atomic.bin");
    try atomic_writer.appendSlice("durable");
    try atomic_writer.finish();
    try std.testing.expectEqual(@as(usize, 3), host_ctx.content_syncs);
    try std.testing.expectEqual(@as(usize, 1), host_ctx.parent_syncs);
    const atomic_contents = try atomic_host.readFileAlloc(std.testing.allocator, "/host/atomic.bin", 32);
    defer std.testing.allocator.free(atomic_contents);
    try std.testing.expectEqualStrings("durable", atomic_contents);
}

test "storage range read future fallback waits and cancels" {
    var backing = MemoryStorage.init(std.testing.allocator);
    defer backing.deinit();

    const storage = backing.storage();
    try storage.writeFileAbsolute("/future/a.txt", "hello");

    var future = try storage.beginReadFileRangeAlloc(std.testing.allocator, "/future/a.txt", 1, 3);
    const bytes = try future.wait();
    defer std.testing.allocator.free(bytes);
    try std.testing.expectEqualStrings("ell", bytes);

    var canceled = try storage.beginReadFileRangeAlloc(std.testing.allocator, "/future/a.txt", 0, 5);
    canceled.cancel();
}

test "native range reads progress without concurrency capacity" {
    if (!supports_native_storage) return error.SkipZigTest;
    var native = try NativeStorage.init(std.testing.allocator, .threaded);
    defer native.deinit();
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .nothing,
        .concurrent_limit = .nothing,
    });
    defer io_impl.deinit();
    const runtime = ReadRuntime.init(io_impl.io());
    var test_tmp = try TestDirectory.init("range-read-saturation");
    defer test_tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}-payload", .{test_tmp.path()});
    defer native.storage().deleteFileAbsolute(path) catch {};
    try native.storage().writeFileAbsolute(path, "hello");

    var future = try NativeRangeReadFuture.create(runtime, native.state, std.testing.allocator, path, 1, 3);
    const bytes = try future.wait();
    defer std.testing.allocator.free(bytes);
    try std.testing.expectEqualStrings("ell", bytes);
    var canceled = try NativeRangeReadFuture.create(runtime, native.state, std.testing.allocator, path, 0, 5);
    canceled.cancel();
    var missing = try NativeRangeReadFuture.create(runtime, native.state, std.testing.allocator, "/nonexistent/antfly-range-read-saturation", 0, 1);
    try std.testing.expectError(error.FileNotFound, missing.wait());
}

test "cold sequential reader is isolated from foreground descriptor cache" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;

    var native = try NativeStorage.init(std.testing.allocator, .threaded);
    defer native.deinit();

    var path_buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/antfly-storage-cold-reader-{d}", .{atomic_write_nonce.fetchAdd(1, .monotonic)});
    defer native.storage().deleteFileAbsolute(path) catch {};
    try native.storage().writeFileAbsolute(path, "abcdefgh");

    const warm = try native.storage().readFileRangeAlloc(std.testing.allocator, path, 0, 1);
    std.testing.allocator.free(warm);
    const before = native.snapshotStats().fd_admitted_descriptors;
    var reader = try native.storage().beginColdSequentialRead(std.testing.allocator, path);
    try std.testing.expectEqual(before + 1, native.snapshotStats().fd_admitted_descriptors);

    const cold = try reader.readRangeAlloc(std.testing.allocator, 2, 4);
    defer std.testing.allocator.free(cold);
    try std.testing.expectEqualStrings("cdef", cold);

    // A normal read remains usable while the maintenance-private descriptor
    // carries its cold policy.
    const foreground = try native.storage().readFileRangeAlloc(std.testing.allocator, path, 0, 2);
    defer std.testing.allocator.free(foreground);
    try std.testing.expectEqualStrings("ab", foreground);

    reader.deinit();
    try std.testing.expectEqual(before, native.snapshotStats().fd_admitted_descriptors);

    var random_reader = try native.storage().beginColdRandomRead(std.testing.allocator, path);
    try std.testing.expectEqual(before + 1, native.snapshotStats().fd_admitted_descriptors);
    var random_buf: [3]u8 = undefined;
    try random_reader.readRangeInto(4, &random_buf);
    try std.testing.expectEqualStrings("efg", &random_buf);
    var first: [2]u8 = undefined;
    var second: [3]u8 = undefined;
    const batch_stats = try random_reader.readRangesInto(&.{
        .{ .offset = 0, .destination = &first },
        .{ .offset = 4, .destination = &second },
    });
    try std.testing.expectEqualStrings("ab", &first);
    try std.testing.expectEqualStrings("efg", &second);
    try std.testing.expectEqual(@as(u64, 5), batch_stats.logical_bytes);
    try std.testing.expectEqual(@as(u64, 1), batch_stats.physical_reads);
    try std.testing.expect(batch_stats.physical_bytes >= batch_stats.logical_bytes);
    random_reader.releaseScratch();
    try random_reader.readRangeInto(0, &first);
    try std.testing.expectEqualStrings("ab", &first);
    var singleton: [2]u8 = undefined;
    const singleton_stats = try random_reader.readRangesInto(&.{.{
        .offset = 2,
        .destination = &singleton,
    }});
    try std.testing.expectEqualStrings("cd", &singleton);
    try std.testing.expectEqual(@as(u64, 2), singleton_stats.logical_bytes);
    try std.testing.expectEqual(@as(u64, 1), singleton_stats.physical_reads);
    try std.testing.expect(singleton_stats.physical_bytes >= singleton_stats.logical_bytes);
    random_reader.deinit();
    try std.testing.expectEqual(before, native.snapshotStats().fd_admitted_descriptors);
}

test "windowed cold cursors release input admission before output work" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    var pool = NativeStoragePool.initWithCapacityForTest(alloc, 4);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(alloc, .threaded, &pool);
    defer native.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const input = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/window-input", .{tmp.sub_path});
    defer alloc.free(input);
    const output = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/window-output", .{tmp.sub_path});
    defer alloc.free(output);
    try native.storage().writeFileAbsolute(input, "abcdefgh");
    const baseline = native.snapshotStats().fd_admitted_descriptors;
    var cursors: [32]ColdSequentialReader = undefined;
    var initialized: usize = 0;
    defer for (cursors[0..initialized]) |*cursor| cursor.deinit();
    for (&cursors) |*cursor| {
        cursor.* = try native.storage().beginWindowedColdRead(alloc, input);
        initialized += 1;
    }
    // A merge wider than the descriptor pool owns no input permits between
    // windows. Opening output cannot wait behind its own retained inputs.
    try std.testing.expectEqual(baseline, native.snapshotStats().fd_admitted_descriptors);
    // Directory creation itself needs two transient permits even when the
    // path exists. The old one-output-slot estimate deadlocked at this edge.
    try native.storage().createDirPath(std.fs.path.dirname(output).?);
    var sink = try native.storage().beginAtomicWrite(alloc, output);
    var sink_active = true;
    defer if (sink_active) sink.abort();
    const with_output = native.snapshotStats().fd_admitted_descriptors;
    for (&cursors) |*cursor| {
        var bytes: [4]u8 = undefined;
        try cursor.readRangeInto(2, &bytes);
        try std.testing.expectEqualStrings("cdef", &bytes);
        try std.testing.expectEqual(with_output, native.snapshotStats().fd_admitted_descriptors);
        try sink.appendSlice(&bytes);
    }
    var first: [2]u8 = undefined;
    var last: [2]u8 = undefined;
    const stats = try cursors[0].readRangesInto(&.{
        .{ .offset = 0, .destination = &first },
        .{ .offset = 6, .destination = &last },
    });
    try std.testing.expectEqualStrings("ab", &first);
    try std.testing.expectEqualStrings("gh", &last);
    try std.testing.expectEqual(@as(u64, 4), stats.logical_bytes);
    const owned = try cursors[0].readRangeAlloc(alloc, 1, 3);
    defer alloc.free(owned);
    try std.testing.expectEqualStrings("bcd", owned);
    try std.testing.expectError(error.EndOfStream, cursors[0].readRangeInto(8, &first));
    try std.testing.expectEqual(with_output, native.snapshotStats().fd_admitted_descriptors);
    sink_active = false;
    try sink.finish();
    try std.testing.expectEqual(baseline, native.snapshotStats().fd_admitted_descriptors);
}

test "cold reader group admission is atomic and releases failed opens" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    var pool = NativeStoragePool.initWithCapacityForTest(alloc, 8);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(alloc, .threaded, &pool);
    defer native.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/group-read", .{tmp.sub_path});
    defer alloc.free(path);
    try native.storage().writeFileAbsolute(path, "payload");
    const baseline = native.snapshotStats().fd_admitted_descriptors;
    const first = (try native.storage().tryBeginColdRandomReads(alloc, &.{ path, path, path })).?;
    defer alloc.free(first);
    // A second complete set cannot fit. It must return immediately without
    // reserving any partial set, even while the first set remains retained.
    const held = native.snapshotStats().fd_admitted_descriptors;
    try std.testing.expect((try native.storage().tryBeginColdRandomReads(alloc, &.{ path, path, path, path })) == null);
    try std.testing.expectEqual(held, native.snapshotStats().fd_admitted_descriptors);
    for (first) |*reader| reader.deinit();
    try std.testing.expectEqual(baseline, native.snapshotStats().fd_admitted_descriptors);
    try std.testing.expectError(error.FileNotFound, native.storage().tryBeginColdRandomReads(alloc, &.{ path, "/missing-antfly-cold-group-file" }));
    try std.testing.expectEqual(baseline, native.snapshotStats().fd_admitted_descriptors);
}

test "cold sequential reader capacity preserves output and persistent admission" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;

    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 4);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(std.testing.allocator, .threaded, &pool);
    defer native.deinit();

    // Capacity four reserves two descriptors for lifetime locks and one for
    // the compaction output, leaving exactly one private input descriptor.
    try std.testing.expectEqual(@as(usize, 1), native.storage().coldSequentialReaderCapacity());
}

test "native atomic write sink supports patching and crc before finish" {
    if (!supports_native_storage) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();

    var native = try NativeStorage.init(std.testing.allocator, .threaded);
    defer native.deinit();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}-antfly-storage-atomic-{d}", .{ test_tmp.path(), atomic_write_nonce.fetchAdd(1, .monotonic) });
    defer native.storage().deleteFileAbsolute(path) catch {};

    var writer = try native.storage().beginAtomicWrite(std.testing.allocator, path);
    var active = true;
    defer if (active) writer.abort();
    writer.setCacheIntent(.cold_sequential);
    const native_writer: *NativeAtomicWriteSink = @ptrCast(@alignCast(writer.ptr));
    try std.testing.expectEqual(AtomicWriteCacheIntent.cold_sequential, native_writer.cache_intent);
    if (supports_posix_fd_cache) {
        try std.testing.expectEqual(@as(usize, 1), native.snapshotStats().fd_admitted_descriptors);
    }

    try writer.appendSlice("hello _____");
    try writer.writeAt(6, "world");
    try std.testing.expectEqual(Crc32.hash("hello world"), try writer.crc32Prefix(writer.len()));

    active = false;
    try writer.finish();
    if (supports_posix_fd_cache) {
        try std.testing.expectEqual(@as(usize, 0), native.snapshotStats().fd_admitted_descriptors);
    }

    const written = try native.storage().readFileAlloc(std.testing.allocator, path, 64);
    defer std.testing.allocator.free(written);
    try std.testing.expectEqualStrings("hello world", written);
}

test "native storage retained runtime has a finite worker ceiling" {
    if (!supports_native_storage) return error.SkipZigTest;

    var native = try NativeStorage.init(std.testing.allocator, .threaded);
    defer native.deinit();
    try std.testing.expectEqual(
        std.Io.Limit.limited(threaded_io_limits.service),
        native.state.threaded.concurrent_limit,
    );
}

test "native fd cache hits reuse their path without allocation" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    var test_tmp = try TestDirectory.init("fd-hit");
    defer test_tmp.cleanup();
    var native = try NativeStorage.init(std.testing.allocator, .threaded);
    defer native.deinit();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}-cached-file", .{test_tmp.path()});
    defer native.storage().deleteFileAbsolute(path) catch {};
    try native.storage().writeFileAbsolute(path, "value");
    // The process-wide FD pool uses the page allocator. Exercise that shape,
    // and reject any allocation after the descriptor has been populated.
    var failing = std.testing.FailingAllocator.init(std.heap.page_allocator, .{});
    var cache = FdCache.init(failing.allocator(), 8);
    defer cache.deinit();
    const first = try cache.retain(1, std.testing.io, path);
    cache.release(std.testing.io, first);
    const before = failing.alloc_index;
    const started = @import("antfly_platform").time.monotonicNs();
    for (0..20000) |_| {
        const entry = try cache.retain(1, std.testing.io, path);
        cache.release(std.testing.io, entry);
    }
    std.debug.print("fd cache 20000 hits: {d} ns, {d} allocations\n", .{ @import("antfly_platform").time.monotonicNs() - started, failing.alloc_index - before });
    failing.fail_index = failing.alloc_index;
    const hit = try cache.retain(1, std.testing.io, path);
    cache.release(std.testing.io, hit);
    try std.testing.expectEqual(before, failing.alloc_index);
}

test "native fd cache retries an open that straddles a mutation fence" {
    if (!supports_posix_fd_cache or builtin.single_threaded) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();

    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 4);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(std.testing.allocator, .threaded, &pool);
    defer native.deinit();

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}-antfly-storage-invalidation-race-{d}", .{ test_tmp.path(), atomic_write_nonce.fetchAdd(1, .monotonic) });
    defer native.storage().deleteFileAbsolute(path) catch {};
    try native.storage().writeFileAbsolute(path, "old");

    var replacement_buf: [288]u8 = undefined;
    const replacement_path = try std.fmt.bufPrint(&replacement_buf, "{s}.replacement", .{path});
    defer native.storage().deleteFileAbsolute(replacement_path) catch {};
    try native.storage().writeFileAbsolute(replacement_path, "new");

    // Model the first half of the mutation fence before the reader opens the
    // old inode. Only the matching post-rename fence can make this miss retry.
    native.state.invalidateRename(replacement_path, path);

    test_fd_cache_pause_after_open.store(true, .release);
    test_fd_cache_open_paused.store(false, .release);
    test_fd_cache_release_after_open.store(false, .release);
    defer {
        test_fd_cache_pause_after_open.store(false, .release);
        test_fd_cache_open_paused.store(false, .release);
        test_fd_cache_release_after_open.store(true, .release);
    }

    const Reader = struct {
        storage: Storage,
        path: []const u8,
        bytes: [3]u8 = undefined,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.storage.readFileRangeInto(std.heap.page_allocator, self.path, 0, &self.bytes) catch |err| {
                self.err = err;
            };
        }
    };
    var reader = Reader{ .storage = native.storage(), .path = path };
    var reader_future = std.Io.async(io, Reader.run, .{&reader});
    var reader_awaited = false;
    defer if (!reader_awaited) {
        test_fd_cache_release_after_open.store(true, .release);
        reader_future.await(io);
    };

    for (0..10_000) |_| {
        if (test_fd_cache_open_paused.load(.acquire)) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(test_fd_cache_open_paused.load(.acquire));

    try renameAbsoluteDirectPosix(replacement_path, path);
    native.state.invalidateRename(replacement_path, path);

    test_fd_cache_release_after_open.store(true, .release);
    reader_future.await(io);
    reader_awaited = true;
    try std.testing.expect(reader.err == null);
    try std.testing.expectEqualStrings("new", reader.bytes[0..]);
}

test "native fd cache invalidates a deleted tree spelled with trailing separators" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();

    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 8);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(std.testing.allocator, .threaded, &pool);
    defer native.deinit();

    const nonce = atomic_write_nonce.fetchAdd(1, .monotonic);
    var root_buf: [std.fs.max_path_bytes]u8 = undefined;
    const root = try std.fmt.bufPrint(&root_buf, "{s}-antfly-storage-tree-invalidation-{d}", .{ test_tmp.path(), nonce });
    defer native.storage().deleteTree(root) catch {};
    try native.storage().createDirPath(root);

    var path_buf: [288]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/cached.bin", .{root});
    try native.storage().writeFileAbsolute(path, "old");
    const cached = try native.storage().readFileRangeAlloc(std.testing.allocator, path, 0, 3);
    defer std.testing.allocator.free(cached);
    try std.testing.expectEqualStrings("old", cached);
    try std.testing.expectEqual(@as(usize, 1), pool.snapshotStats().fd_cache_entries);

    var alias_buf: [288]u8 = undefined;
    const trailing_alias = try std.fmt.bufPrint(&alias_buf, "{s}///", .{root});
    try native.storage().deleteTree(trailing_alias);
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_cache_entries);
    try std.testing.expectError(
        error.FileNotFound,
        native.storage().readFileRangeAlloc(std.testing.allocator, path, 0, 3),
    );
}

test "path containment handles root and trailing separators" {
    try std.testing.expect(pathContains("/", "/tree/file"));
    try std.testing.expect(pathContains("/tree///", "/tree/file"));
    try std.testing.expect(pathContains("/tree///", "/tree"));
    try std.testing.expect(!pathContains("/tree///", "/treehouse/file"));
    try std.testing.expect(pathContains("", "/tree/file"));
    try std.testing.expect(!pathContains("", "relative/file"));
}

test "native atomic write finish retains admission through parent sync" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();

    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 4);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(std.testing.allocator, .threaded, &pool);
    defer native.deinit();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}-antfly-storage-atomic-admission-handoff-{d}", .{ test_tmp.path(), atomic_write_nonce.fetchAdd(1, .monotonic) });
    defer native.storage().deleteFileAbsolute(path) catch {};

    var writer = try native.storage().beginAtomicWrite(std.testing.allocator, path);
    var writer_active = true;
    defer if (writer_active) writer.abort();
    try writer.appendSlice("durable");

    // Fill the persistent class to the point where relinquishing the writer's
    // transient slot would make reacquisition fail. The closed data file and
    // parent-directory fsync are sequential users of that same admitted slot.
    const io = native.state.threaded.io();
    try pool.fd_cache.reservePersistentDescriptors(io, 2);
    var persistent_held = true;
    defer if (persistent_held) pool.fd_cache.releasePersistentDescriptors(io, 2);

    writer_active = false;
    try writer.finish();
    try std.testing.expectEqual(@as(usize, 2), pool.snapshotStats().fd_admitted_descriptors);

    pool.fd_cache.releasePersistentDescriptors(io, 2);
    persistent_held = false;
    const written = try native.storage().readFileAlloc(std.testing.allocator, path, 64);
    defer std.testing.allocator.free(written);
    try std.testing.expectEqualStrings("durable", written);
}

test "native atomic write sink cleans temporary file when content sync fails" {
    if (!supports_native_storage) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return error.SkipZigTest;

    var native = try NativeStorage.init(std.testing.allocator, .threaded);
    defer native.deinit();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}-antfly-storage-atomic-sync-failure-{d}", .{ test_tmp.path(), atomic_write_nonce.fetchAdd(1, .monotonic) });
    defer native.storage().deleteFileAbsolute(path) catch {};

    var writer = try native.storage().beginAtomicWrite(std.testing.allocator, path);
    const impl: *NativeAtomicWriteSink = @ptrCast(@alignCast(writer.ptr));
    const tmp_path = try std.testing.allocator.dupe(u8, impl.tmp_path);
    defer std.testing.allocator.free(tmp_path);
    try writer.appendSlice("uncommitted");

    closeFd(impl.fd);
    impl.fd = -1;
    try std.testing.expectError(error.InvalidFileDescriptor, writer.finish());

    try std.testing.expectError(
        error.FileNotFound,
        native.storage().fileSize(tmp_path),
    );
    try std.testing.expectError(
        error.FileNotFound,
        native.storage().fileSize(path),
    );
}

test "buffered atomic write sink supports overlapping writes and appends" {
    var backing = MemoryStorage.init(std.testing.allocator);
    defer backing.deinit();

    var writer = try backing.storage().beginAtomicWrite(std.testing.allocator, "/alias-safe.bin");
    var active = true;
    defer if (active) writer.abort();

    try writer.appendSlice("abcdef");
    const impl: *BufferedAtomicWriteSink = @ptrCast(@alignCast(writer.ptr));
    try writer.writeAt(2, impl.out.items[0..4]);
    try std.testing.expectEqualStrings("ababcd", impl.out.items);

    try writer.appendSlice(impl.out.items[1..5]);
    try std.testing.expectEqualStrings("ababcdbabc", impl.out.items);

    active = false;
    try writer.finish();

    const written = try backing.storage().readFileAlloc(std.testing.allocator, "/alias-safe.bin", 64);
    defer std.testing.allocator.free(written);
    try std.testing.expectEqualStrings("ababcdbabc", written);
}

test "native fd cache evicts to per-store budget" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();

    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, fallback_cached_native_fds);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(std.testing.allocator, .threaded, &pool);
    defer native.deinit();

    const base_nonce = atomic_write_nonce.fetchAdd(1, .monotonic);
    for (0..fallback_cached_native_fds + 8) |i| {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "{s}-antfly-storage-fd-cache-{d}-{d}", .{ test_tmp.path(), base_nonce, i });
        try native.storage().writeFileAbsolute(path, "x");
        defer native.storage().deleteFileAbsolute(path) catch {};

        const bytes = try native.storage().readFileAlloc(std.testing.allocator, path, 8);
        defer std.testing.allocator.free(bytes);
        try std.testing.expectEqualStrings("x", bytes);
    }

    const stats = native.snapshotStats();
    try std.testing.expect(stats.fd_cache_entries <= stats.fd_admission_capacity);
    try std.testing.expectEqual(fallback_cached_native_fds, stats.fd_admission_capacity);
}

test "native fd admission reserves non-storage process capacity" {
    try std.testing.expectEqual(@as(usize, 4), nativeFdAdmissionCapacityForSoftLimit(8));
    try std.testing.expectEqual(@as(usize, 32), nativeFdAdmissionCapacityForSoftLimit(64));
    try std.testing.expectEqual(@as(usize, 128), nativeFdAdmissionCapacityForSoftLimit(256));
    try std.testing.expectEqual(@as(usize, 512), nativeFdAdmissionCapacityForSoftLimit(1024));
    try std.testing.expectEqual(unlimited_fd_admission_capacity, nativeFdAdmissionCapacityForSoftLimit(std.math.maxInt(u64)));
    try std.testing.expectEqual(unlimited_fd_admission_capacity, nativeFdAdmissionCapacityForSoftLimit(1_048_576));
}

test "backend runtime native fd pools share one process admission domain" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;

    var first = NativeStoragePool.init(std.testing.allocator);
    defer first.deinit();
    var second = NativeStoragePool.init(std.testing.allocator);
    defer second.deinit();

    try std.testing.expectEqual(first.fd_cache, second.fd_cache);
    try std.testing.expectEqual(configuredNativeFdAdmissionCapacity(), first.snapshotStats().fd_admission_capacity);

    var first_storage = try NativeStorage.init(std.testing.allocator, .threaded);
    defer first_storage.deinit();
    var second_storage = try NativeStorage.init(std.testing.allocator, .threaded);
    defer second_storage.deinit();
    try std.testing.expectEqual(first_storage.state.fdCache(), second_storage.state.fdCache());
    try std.testing.expectEqual(first.fd_cache, first_storage.state.fdCache());
}

test "transient native writes wait before opening at descriptor capacity" {
    if (!supports_posix_fd_cache or builtin.single_threaded) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();

    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 2);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(std.testing.allocator, .threaded, &pool);
    defer native.deinit();
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const nonce = atomic_write_nonce.fetchAdd(1, .monotonic);
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}-antfly-storage-transient-admission-{d}", .{ test_tmp.path(), nonce });
    defer native.storage().deleteFileAbsolute(path) catch {};

    var occupying_permit = try native.acquireFdPermit();
    var permit_active = true;
    defer if (permit_active) occupying_permit.release();

    const Worker = struct {
        storage: Storage,
        path: []const u8,
        failed: *std.atomic.Value(bool),

        fn run(self: *@This()) void {
            self.storage.writeFileAbsolute(self.path, "bounded") catch {
                self.failed.store(true, .release);
            };
        }
    };
    var failed = std.atomic.Value(bool).init(false);
    var worker = Worker{ .storage = native.storage(), .path = path, .failed = &failed };
    var future = std.Io.async(io, Worker.run, .{&worker});
    var future_awaited = false;
    defer if (!future_awaited) {
        if (permit_active) {
            occupying_permit.release();
            permit_active = false;
        }
        future.await(io);
    };

    for (0..10_000) |_| {
        if (pool.snapshotStats().fd_admission_waiters > 0) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(usize, 1), pool.snapshotStats().fd_admitted_descriptors);
    try std.testing.expectEqual(@as(usize, 1), pool.snapshotStats().fd_admission_waiters);

    occupying_permit.release();
    permit_active = false;
    future.await(io);
    future_awaited = true;
    try std.testing.expect(!failed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
}

test "persistent path locks use reserved headroom under transient saturation" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();

    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 8);
    defer pool.deinit();
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    try pool.fd_cache.reserveDescriptors(io, 6);
    const held_descriptors: usize = 6;
    defer pool.fd_cache.releaseDescriptors(io, held_descriptors);

    const nonce = atomic_write_nonce.fetchAdd(1, .monotonic);
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}-antfly-storage-path-lock-admission-{d}", .{ test_tmp.path(), nonce });
    defer deleteFilePathPosix(path) catch {};

    var lock_file = try openNativePathLockFileWithCache(
        std.testing.allocator,
        path,
        .{ .create_if_missing = true },
        pool.fd_cache,
        null,
    );
    defer lock_file.close();
    const stats = pool.snapshotStats();
    try std.testing.expectEqual(@as(usize, 7), stats.fd_admitted_descriptors);
    try std.testing.expectEqual(@as(usize, 1), stats.fd_persistent_descriptors);
    try std.testing.expectEqual(@as(usize, 2), stats.fd_persistent_reserve);
}

test "persistent path lock exhaustion fails without waiting" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();

    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 2);
    defer pool.deinit();
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    try pool.fd_cache.reservePersistentDescriptors(io, 2);
    defer pool.fd_cache.releasePersistentDescriptors(io, 2);

    const nonce = atomic_write_nonce.fetchAdd(1, .monotonic);
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}-antfly-storage-path-lock-exhaustion-{d}", .{ test_tmp.path(), nonce });
    defer deleteFilePathPosix(path) catch {};

    try std.testing.expectError(
        error.PersistentDescriptorAdmissionExhausted,
        openNativePathLockFileWithCache(std.testing.allocator, path, .{ .create_if_missing = true }, pool.fd_cache, null),
    );
    const stats = pool.snapshotStats();
    try std.testing.expectEqual(@as(usize, 0), stats.fd_admission_waiters);
    try std.testing.expectEqual(@as(u64, 1), stats.fd_persistent_admission_failures);
}

test "transient admission fails fast when only persistent descriptors prevent progress" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;

    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 4);
    defer pool.deinit();
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    // Two lifetime descriptors consume the transient ceiling after open
    // headroom is retained. No short-lived holder can wake an admission wait,
    // so a cache owner must be allowed to close an idle backend instead.
    try pool.fd_cache.reservePersistentDescriptors(io, 2);
    var persistent_held = true;
    defer if (persistent_held) pool.fd_cache.releasePersistentDescriptors(io, 2);
    try std.testing.expectError(
        error.PersistentDescriptorAdmissionExhausted,
        pool.fd_cache.reserveDescriptors(io, 1),
    );
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admission_waiters);

    pool.fd_cache.releasePersistentDescriptors(io, 2);
    persistent_held = false;
    try pool.fd_cache.reserveDescriptors(io, 1);
    defer pool.fd_cache.releaseDescriptors(io, 1);
}

test "persistent path locks honor an explicitly configured pool" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();

    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 2);
    defer pool.deinit();
    const nonce = atomic_write_nonce.fetchAdd(1, .monotonic);
    var first_buf: [std.fs.max_path_bytes]u8 = undefined;
    const first_path = try std.fmt.bufPrint(&first_buf, "{s}-antfly-storage-configured-lock-pool-{d}-first", .{ test_tmp.path(), nonce });
    defer deleteFilePathPosix(first_path) catch {};
    var second_buf: [std.fs.max_path_bytes]u8 = undefined;
    const second_path = try std.fmt.bufPrint(&second_buf, "{s}-antfly-storage-configured-lock-pool-{d}-second", .{ test_tmp.path(), nonce });
    defer deleteFilePathPosix(second_path) catch {};

    var first = try openNativePathLockFileWithPool(
        std.testing.allocator,
        first_path,
        .{ .create_if_missing = true },
        &pool,
    );
    defer first.close();
    try std.testing.expectEqual(@as(usize, 1), pool.snapshotStats().fd_persistent_descriptors);
    try std.testing.expectError(
        error.PersistentDescriptorAdmissionExhausted,
        openNativePathLockFileWithPool(
            std.testing.allocator,
            second_path,
            .{ .create_if_missing = true },
            &pool,
        ),
    );
}

test "persistent locks consume reserved headroom without stranding transient capacity" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;

    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 42);
    defer pool.deinit();
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    // Model nineteen open backends (root + WAL locks each). The original
    // fixed-reserve calculation capped transient admission at 37 and would
    // wait forever here despite four unused aggregate slots.
    try pool.fd_cache.reservePersistentDescriptors(io, 38);
    defer pool.fd_cache.releasePersistentDescriptors(io, 38);
    try std.testing.expect(pool.fd_cache.makeCapacityAvailable(2));
    try pool.fd_cache.reserveDescriptors(io, 2);
    defer pool.fd_cache.releaseDescriptors(io, 2);

    // The dynamic transient ceiling still preserves the exact two-descriptor
    // peak required to open the next lifetime path lock.
    try pool.fd_cache.reservePersistentDescriptors(io, 2);
    defer pool.fd_cache.releasePersistentDescriptors(io, 2);
    const stats = pool.snapshotStats();
    try std.testing.expectEqual(@as(usize, 42), stats.fd_admitted_descriptors);
    try std.testing.expectEqual(@as(usize, 40), stats.fd_persistent_descriptors);
    try std.testing.expectEqual(@as(usize, 5), stats.fd_persistent_reserve);
}

test "shared native fd cache blocks before opening more than 64 files across stores" {
    if (!supports_posix_fd_cache or builtin.single_threaded) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();

    const capacity = 64;
    const worker_count = capacity + 8;
    const store_count = 3;
    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, capacity);
    defer pool.deinit();
    const transient_capacity = capacity - pool.snapshotStats().fd_persistent_reserve;
    var stores: [store_count]NativeStorage = undefined;
    var stores_initialized: usize = 0;
    defer while (stores_initialized > 0) {
        stores_initialized -= 1;
        stores[stores_initialized].deinit();
    };
    for (&stores) |*native| {
        native.* = try NativeStorage.initWithPool(std.testing.allocator, .threaded, &pool);
        stores_initialized += 1;
    }

    const nonce = atomic_write_nonce.fetchAdd(1, .monotonic);
    var paths: [worker_count][std.fs.max_path_bytes]u8 = undefined;
    var path_lens: [worker_count]usize = undefined;
    for (0..worker_count) |i| {
        const path = try std.fmt.bufPrint(&paths[i], "{s}-antfly-storage-shared-fd-cache-{d}-{d}", .{ test_tmp.path(), nonce, i });
        path_lens[i] = path.len;
        try stores[i % store_count].storage().writeFileAbsolute(path, "x");
    }
    defer for (0..worker_count) |i| stores[i % store_count].storage().deleteFileAbsolute(paths[i][0..path_lens[i]]) catch {};

    const Worker = struct {
        cache: *FdCache,
        namespace: u64,
        io: std.Io,
        path: []const u8,
        release_all: *std.Io.Event,
        acquired: *std.atomic.Value(usize),
        failures: *std.atomic.Value(usize),

        fn run(self: *@This()) void {
            const entry = self.cache.retain(self.namespace, self.io, self.path) catch {
                _ = self.failures.fetchAdd(1, .monotonic);
                return;
            };
            _ = self.acquired.fetchAdd(1, .release);
            self.release_all.waitUncancelable(self.io);
            self.cache.release(self.io, entry);
        }
    };

    // Every task intentionally holds a lease or waits in descriptor admission.
    // Give the threaded test executor one slot per task so the harness itself
    // cannot become the bottleneck before the >64 contention point is reached.
    // Keep the platform's default stack size: the linked test graph's static
    // TLS can exceed a hand-picked 512 KiB stack on glibc (pthread EINVAL).
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .limited(worker_count),
    });
    defer io_impl.deinit();
    const io = io_impl.io();
    var release_all: std.Io.Event = .unset;
    var acquired = std.atomic.Value(usize).init(0);
    var failures = std.atomic.Value(usize).init(0);
    var workers: [worker_count]Worker = undefined;
    var group = std.Io.Group.init;
    var group_awaited = false;
    defer if (!group_awaited) {
        release_all.set(io);
        group.await(io) catch {};
    };
    for (0..worker_count) |i| {
        workers[i] = .{
            .cache = stores[i % store_count].state.fdCache(),
            .namespace = stores[i % store_count].state.cache_namespace,
            .io = io,
            .path = paths[i][0..path_lens[i]],
            .release_all = &release_all,
            .acquired = &acquired,
            .failures = &failures,
        };
        group.async(io, Worker.run, .{&workers[i]});
    }

    for (0..10_000) |_| {
        if (acquired.load(.acquire) == transient_capacity and pool.snapshotStats().fd_admission_waiters > 0) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    const blocked_stats = pool.snapshotStats();
    try std.testing.expectEqual(transient_capacity, acquired.load(.acquire));
    try std.testing.expectEqual(transient_capacity, blocked_stats.fd_cache_entries);
    try std.testing.expectEqual(transient_capacity, blocked_stats.fd_admitted_descriptors);
    try std.testing.expect(blocked_stats.fd_admission_waiters > 0);
    try std.testing.expect(blocked_stats.fd_admission_waits > 0);

    release_all.set(io);
    try group.await(io);
    group_awaited = true;
    try std.testing.expectEqual(@as(usize, 0), failures.load(.acquire));
    try std.testing.expect(pool.snapshotStats().fd_cache_entries <= capacity);
}

test "weighted native fd admission preserves FIFO progress" {
    if (!supports_posix_fd_cache or builtin.single_threaded) return error.SkipZigTest;

    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 3);
    defer pool.deinit();
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    try pool.fd_cache.reserveDescriptors(io, 3);
    var held_descriptors: usize = 3;
    defer pool.fd_cache.releaseDescriptors(io, held_descriptors);

    const Worker = struct {
        cache: *FdCache,
        io: std.Io,
        count: usize,
        acquired: *std.atomic.Value(bool),
        release_gate: ?*std.Io.Event,

        fn run(self: *@This()) void {
            self.cache.reserveDescriptors(self.io, self.count) catch return;
            self.acquired.store(true, .release);
            if (self.release_gate) |gate| gate.waitUncancelable(self.io);
            self.cache.releaseDescriptors(self.io, self.count);
        }
    };

    var large_acquired = std.atomic.Value(bool).init(false);
    var small_acquired = std.atomic.Value(bool).init(false);
    var release_large: std.Io.Event = .unset;
    var large = Worker{
        .cache = pool.fd_cache,
        .io = io,
        .count = 2,
        .acquired = &large_acquired,
        .release_gate = &release_large,
    };
    var large_future = std.Io.async(io, Worker.run, .{&large});
    var large_awaited = false;
    defer if (!large_awaited) {
        release_large.set(io);
        large_future.await(io);
    };
    for (0..10_000) |_| {
        if (pool.snapshotStats().fd_admission_waiters >= 1) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(pool.snapshotStats().fd_admission_waiters >= 1);

    var small = Worker{
        .cache = pool.fd_cache,
        .io = io,
        .count = 1,
        .acquired = &small_acquired,
        .release_gate = null,
    };
    var small_future = std.Io.async(io, Worker.run, .{&small});
    var small_awaited = false;
    defer if (!small_awaited) {
        // The small request is queued behind the large request. Unblock the
        // older request first on every error path so cleanup cannot deadlock
        // while awaiting the younger future.
        release_large.set(io);
        small_future.await(io);
    };
    for (0..10_000) |_| {
        if (pool.snapshotStats().fd_admission_waiters >= 2) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(pool.snapshotStats().fd_admission_waiters >= 2);

    // One released descriptor would admit the younger one-fd request, but it
    // must remain queued so capacity can accumulate for the older request.
    pool.fd_cache.releaseDescriptors(io, 1);
    held_descriptors -= 1;
    try io.sleep(.fromMilliseconds(10), .awake);
    try std.testing.expect(!large_acquired.load(.acquire));
    try std.testing.expect(!small_acquired.load(.acquire));
    // Optional private-reader batches cannot steal the free slot from the
    // older weighted waiter, even though one descriptor is available.
    const optional_reserved = pool.fd_cache.tryReserveDescriptors(io, 1);
    if (optional_reserved) pool.fd_cache.releaseDescriptors(io, 1);
    try std.testing.expect(!optional_reserved);
    try std.testing.expectEqual(@as(usize, 2), pool.snapshotStats().fd_admitted_descriptors);

    pool.fd_cache.releaseDescriptors(io, 1);
    held_descriptors -= 1;
    for (0..10_000) |_| {
        if (large_acquired.load(.acquire)) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(large_acquired.load(.acquire));
    try std.testing.expect(!small_acquired.load(.acquire));
    release_large.set(io);
    for (0..10_000) |_| {
        if (small_acquired.load(.acquire)) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(small_acquired.load(.acquire));
    large_future.await(io);
    large_awaited = true;
    small_future.await(io);
    small_awaited = true;
}

test "shared native fd admission wait is cancellation aware" {
    if (!supports_posix_fd_cache or builtin.single_threaded) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();

    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 2);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(std.testing.allocator, .threaded, &pool);
    defer native.deinit();
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const nonce = atomic_write_nonce.fetchAdd(1, .monotonic);
    var second_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const second_path = try std.fmt.bufPrint(&second_path_buf, "{s}-antfly-storage-fd-cancel-{d}-waiting", .{ test_tmp.path(), nonce });
    try native.storage().writeFileAbsolute(second_path, "b");
    defer native.storage().deleteFileAbsolute(second_path) catch {};

    // Model concurrent short-lived permits held by full-text mmap and an
    // atomic writer. The cached LSM read must share this same budget.
    var transient_a = try native.acquireFdPermit();
    defer transient_a.release();
    var transient_b = try native.acquireFdPermit();
    defer transient_b.release();

    var future = try native.storage().beginReadFileRangeAllocWithRuntime(
        ReadRuntime.init(io),
        std.testing.allocator,
        second_path,
        0,
        1,
    );
    var future_active = true;
    defer if (future_active) future.cancel();

    for (0..10_000) |_| {
        if (pool.snapshotStats().fd_admission_waiters > 0) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(usize, 1), pool.snapshotStats().fd_admission_waiters);
    future.cancel();
    future_active = false;

    const stats = pool.snapshotStats();
    try std.testing.expectEqual(@as(usize, 0), stats.fd_admission_waiters);
    try std.testing.expectEqual(@as(usize, 0), stats.fd_cache_entries);
    try std.testing.expectEqual(@as(usize, 2), stats.fd_admitted_descriptors);
}

test "native storage state is reclaimed after owner deinit" {
    if (!supports_native_storage) return error.SkipZigTest;

    // std.testing.allocator reports any state or runtime allocation left by
    // these repeated complete lifecycles.
    for (0..64) |_| {
        var native = try NativeStorage.init(std.testing.allocator, .threaded);
        native.deinit();
    }
}

test "native storage lease owns state past owner deinit" {
    if (!supports_native_storage) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}-antfly-storage-owned-lease-{d}", .{ test_tmp.path(), atomic_write_nonce.fetchAdd(1, .monotonic) });

    var native = try NativeStorage.init(std.testing.allocator, .threaded);
    var lease = try native.acquireLease();
    defer lease.deinit();
    native.deinit();

    const storage_view = lease.storage();
    try storage_view.writeFileAbsolute(path, "owned lease");
    defer storage_view.deleteFileAbsolute(path) catch {};
    const loaded = try storage_view.readFileAlloc(std.testing.allocator, path, 64);
    defer std.testing.allocator.free(loaded);
    try std.testing.expectEqualStrings("owned lease", loaded);
}

test "native atomic write sink retains invalidation state past storage deinit" {
    if (!supports_native_storage) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return error.SkipZigTest;

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}-antfly-storage-atomic-lease-{d}", .{ test_tmp.path(), atomic_write_nonce.fetchAdd(1, .monotonic) });

    var native = try NativeStorage.init(std.testing.allocator, .threaded);
    var writer = try native.storage().beginAtomicWrite(std.testing.allocator, path);
    var active = true;
    defer if (active) writer.abort();

    try writer.appendSlice("leased");
    native.deinit();

    active = false;
    try writer.finish();

    var verifier = try NativeStorage.init(std.testing.allocator, .threaded);
    defer verifier.deinit();
    defer verifier.storage().deleteFileAbsolute(path) catch {};

    const written = try verifier.storage().readFileAlloc(std.testing.allocator, path, 64);
    defer std.testing.allocator.free(written);
    try std.testing.expectEqualStrings("leased", written);
}

test "native buffered atomic write sink retains invalidation state past storage deinit" {
    if (!supports_native_storage) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("storage");
    defer test_tmp.cleanup();
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return error.SkipZigTest;

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}-antfly-storage-buffered-atomic-lease-{d}", .{ test_tmp.path(), atomic_write_nonce.fetchAdd(1, .monotonic) });

    var native = try NativeStorage.init(std.testing.allocator, .threaded);
    var writer = try NativeBufferedAtomicWriteSink.create(std.testing.allocator, path, native.state);
    var active = true;
    defer if (active) writer.abort();

    try writer.appendSlice("buffered _____");
    try writer.writeAt(9, "lease");
    try std.testing.expectEqual(Crc32.hash("buffered lease"), try writer.crc32Prefix(writer.len()));
    native.deinit();

    active = false;
    try writer.finish();

    var verifier = try NativeStorage.init(std.testing.allocator, .threaded);
    defer verifier.deinit();
    defer verifier.storage().deleteFileAbsolute(path) catch {};

    const written = try verifier.storage().readFileAlloc(std.testing.allocator, path, 64);
    defer std.testing.allocator.free(written);
    try std.testing.expectEqualStrings("buffered lease", written);
}

test "native fd admission try skips held admission and eviction locks" {
    if (!supports_posix_fd_cache or builtin.single_threaded) return error.SkipZigTest;
    inline for (.{ false, true }) |hold_shards| {
        var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 2);
        defer pool.deinit();
        const cache = pool.fd_cache;
        try cache.reserveDescriptors(std.testing.io, 2);
        defer cache.releaseDescriptors(std.testing.io, 2);
        if (hold_shards) {
            for (cache.shards) |*shard| while (!shard.mutex.tryLock()) std.atomic.spinLoopHint();
        } else cache.admission_mutex.lockUncancelable(std.testing.io);
        const Rescue = struct {
            cache: *FdCache,
            done: std.atomic.Value(bool) = .init(false),
            rescued: std.atomic.Value(bool) = .init(false),
            fn run(self: *@This()) void {
                for (0..100) |_| {
                    if (self.done.load(.acquire)) break;
                    std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake) catch {};
                }
                if (!self.done.load(.acquire)) self.rescued.store(true, .release);
                if (hold_shards) {
                    for (self.cache.shards) |*shard| shard.mutex.unlock();
                } else self.cache.admission_mutex.unlock(std.testing.io);
            }
        };
        var rescue = Rescue{ .cache = cache };
        var future = try std.testing.io.concurrent(Rescue.run, .{&rescue});
        defer {
            rescue.done.store(true, .release);
            future.await(std.testing.io);
        }
        const reserved = cache.tryReserveDescriptors(std.testing.io, 1);
        rescue.done.store(true, .release);
        if (reserved) cache.releaseDescriptors(std.testing.io, 1);
        try std.testing.expect(!reserved);
        try std.testing.expect(!rescue.rescued.load(.acquire));
        try std.testing.expectEqual(@as(usize, 2), cache.admitted_descriptors.load(.acquire));
    }
}

test "native fd admission try optional read window evicts idle entries and preserves capacity" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    var tmp = try TestDirectory.init("fd-try-window");
    defer tmp.cleanup();
    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 2);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(std.testing.allocator, .threaded, &pool);
    defer native.deinit();
    const path = tmp.path();
    try native.storage().writeFileAbsolute(path, "value");
    const first = try pool.fd_cache.retain(native.state.cache_namespace, std.testing.io, path);
    pool.fd_cache.release(std.testing.io, first);
    const second = try pool.fd_cache.retain(native.state.cache_namespace + 1, std.testing.io, path);
    pool.fd_cache.release(std.testing.io, second);
    try std.testing.expectEqual(@as(usize, 2), pool.snapshotStats().fd_cache_entries);
    const readers = (try native.storage().tryBeginColdRandomReads(std.testing.allocator, &.{path})) orelse return error.TestExpectedOptionalReaders;
    defer {
        for (readers) |*reader| reader.deinit();
        std.testing.allocator.free(readers);
    }
    try std.testing.expectEqual(@as(usize, 1), readers.len);
    try std.testing.expectEqual(@as(usize, 2), pool.snapshotStats().fd_admitted_descriptors);
    try std.testing.expectEqual(@as(usize, 1), pool.snapshotStats().fd_cache_entries);
    const bytes = try readers[0].readRangeAlloc(std.testing.allocator, 0, 5);
    defer std.testing.allocator.free(bytes);
    try std.testing.expectEqualStrings("value", bytes);
}

test "native fd admission try optional read window allocation failure releases every permit" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    var tmp = try TestDirectory.init("fd-try-window-oom");
    defer tmp.cleanup();
    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 2);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(std.testing.allocator, .threaded, &pool);
    defer native.deinit();
    try native.storage().writeFileAbsolute(tmp.path(), "value");
    const Fixture = struct {
        fn run(allocator: Allocator, storage: Storage, path: []const u8) !void {
            const readers = (try storage.tryBeginColdRandomReads(allocator, &.{ path, path })) orelse return error.TestExpectedOptionalReaders;
            defer {
                for (readers) |*reader| reader.deinit();
                allocator.free(readers);
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ native.storage(), tmp.path() });
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
    try std.testing.expectEqual(@as(usize, 1), native.state.refs.load(.acquire));
}

test "native buffered atomic cleanup removes staging and restores cancellation" {
    if (!supports_native_storage) return error.SkipZigTest;
    const a = std.testing.allocator;
    var native_storage = try NativeStorage.init(a, .threaded);
    defer native_storage.deinit();
    const io = native_storage.state.threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/published", .{tmp.sub_path});
    defer a.free(path);
    try tmp.dir.writeFile(io, .{ .sub_path = "published", .data = "retained" });
    var sink = try NativeBufferedAtomicWriteSink.create(a, path, native_storage.state);
    var active = true;
    defer if (active) sink.abort();
    const writer: *NativeBufferedAtomicWriteSink = @ptrCast(@alignCast(sink.ptr));
    const staging = try a.dupe(u8, writer.tmp_path);
    defer a.free(staging);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = staging, .data = "partial" });
    const State = struct {
        started: std.Io.Event = .unset,
        gate: std.Io.Event = .unset,
        restored: bool = false,
        fn run(i: std.Io, self: *@This(), output: *AtomicWriteSink) void {
            self.started.set(i);
            self.gate.wait(i) catch i.recancel();
            output.abort();
            i.checkCancel() catch |err| {
                self.restored = err == error.Canceled;
            };
        }
    };
    var state: State = .{};
    var task = try io.concurrent(State.run, .{ io, &state, &sink });
    active = false;
    state.started.waitUncancelable(io);
    task.cancel(io);
    try std.testing.expect(state.restored);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, staging, .{}));
    const retained = try tmp.dir.readFileAlloc(io, "published", a, .limited(32));
    defer a.free(retained);
    try std.testing.expectEqualStrings("retained", retained);
}

test "memory storage rename preserves both paths on allocation failure" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var memory = MemoryStorage.init(a);
            defer memory.deinit();
            const storage = memory.storage();
            try storage.writeFileAbsolute("/source", "source bytes");
            try storage.writeFileAbsolute("/destination", "destination bytes");
            storage.renameAbsolute("/source", "/destination") catch |err| {
                try std.testing.expectEqualStrings("source bytes", memory.files.get("/source").?);
                try std.testing.expectEqualStrings("destination bytes", memory.files.get("/destination").?);
                return err;
            };
            try std.testing.expect(!memory.files.contains("/source"));
            try std.testing.expectEqualStrings("source bytes", memory.files.get("/destination").?);
            try storage.renameAbsolute("/destination", "/destination");
            try std.testing.expectEqualStrings("source bytes", memory.files.get("/destination").?);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "storage file errors and returned writers use their owning dispatcher" {
    const Foreign = struct {
        var calls: usize = 0;
        fn dispatch(contract: *const @import("antfly_runtime_abi").native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            calls += 1;
            return Storage.BoundaryAbi.local_dispatch(contract, callback, args, output);
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(path);
    const file = try std.fs.path.join(std.testing.allocator, &.{ path, "data" });
    defer std.testing.allocator.free(file);
    var backing = IoStorage.init(std.testing.io);
    var storage = backing.storage();
    storage.boundary_dispatch = Foreign.dispatch;
    Foreign.calls = 0;
    try std.testing.expectError(error.FileNotFound, storage.fileSize(file));
    try std.testing.expectError(error.FileNotFound, storage.readFileAlloc(std.testing.allocator, file, 64));
    try storage.writeFileAbsolute(file, "hello");
    try std.testing.expectEqual(@as(u64, 5), try storage.fileSize(file));
    try std.testing.expectEqual(@as(usize, 4), Foreign.calls);
    const SinkForeign = struct {
        var calls: usize = 0;
        fn dispatch(contract: *const @import("antfly_runtime_abi").native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            calls += 1;
            return AtomicWriteSink.BoundaryAbi.local_dispatch(contract, callback, args, output);
        }
    };
    var sink = try storage.beginAtomicWrite(std.testing.allocator, file);
    var unfinished = true;
    defer if (unfinished) sink.abort();
    SinkForeign.calls = 0;
    sink.boundary_dispatch = SinkForeign.dispatch;
    try sink.appendSlice("hello");
    try sink.writeAt(0, "h");
    try std.testing.expectEqual(Crc32.hash("hello"), try sink.crc32Prefix(5));
    try std.testing.expectEqual(Crc32.hash("ello"), try sink.crc32Range(1, 4));
    unfinished = false;
    try sink.finish();
    try std.testing.expectEqual(@as(usize, 5), SinkForeign.calls);
    var reader = try storage.beginColdSequentialRead(std.testing.allocator, file);
    defer reader.deinit();
    const ReaderForeign = struct {
        var calls: usize = 0;
        fn dispatch(contract: *const @import("antfly_runtime_abi").native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            calls += 1;
            return ColdSequentialReader.BoundaryAbi.local_dispatch(contract, callback, args, output);
        }
    };
    reader.boundary_dispatch = ReaderForeign.dispatch;
    ReaderForeign.calls = 0;
    var bytes: [5]u8 = undefined;
    try reader.readRangeInto(0, &bytes);
    try std.testing.expectEqualStrings("hello", &bytes);
    try std.testing.expectError(error.EndOfStream, reader.readRangeInto(5, &bytes));
    try std.testing.expectEqual(@as(usize, 2), ReaderForeign.calls);
}

test "native executor memory path locks charge their parent and retire owned stacks" {
    if (comptime !std.Thread.use_pthreads) return error.SkipZigTest;
    const Manager = @import("../resource_manager.zig").ResourceManager;
    var root = Manager.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
    defer root.deinit(std.testing.allocator);
    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 8);
    defer pool.deinit();
    pool.memory_parent = root.memoryParentPort();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/private.lock", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    var file = try openNativePathLockFileWithPool(std.testing.allocator, path, .{ .create_if_missing = true }, &pool);
    var live = true;
    defer if (live) file.close();
    try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
    const Work = struct {
        fn run() u32 {
            return 42;
        }
    };
    const io = file.io_impl.io();
    var task = try io.concurrent(Work.run, .{});
    try std.testing.expectEqual(@as(u32, 42), task.await(io));
    try std.testing.expect(file.memory.?.stackStats().used > 0);
    file.close();
    live = false;
    try std.testing.expectEqual(@as(usize, 0), pool.retainedStates());
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "native executor memory range futures and descriptor permits outlive the storage handle" {
    const Manager = @import("../resource_manager.zig").ResourceManager;
    var root = Manager.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 << 20 } });
    defer root.deinit(std.testing.allocator);
    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 8);
    defer pool.deinit();
    pool.memory_parent = root.memoryParentPort();
    var native = try NativeStorage.initWithPool(std.testing.allocator, .threaded, &pool);
    var live = true;
    defer if (live) native.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/retained.bytes", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    try native.storage().writeFileAbsolute(path, "abcdefgh");
    var future = try native.storage().beginReadFileRangeAlloc(std.testing.allocator, path, 2, 4);
    var pending = true;
    defer if (pending) future.cancel();
    var permit = try native.acquireFdPermit();
    defer permit.release();
    native.deinit();
    live = false;
    pool.closeAdmission();
    try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
    try std.testing.expect(root.snapshot().memory.used_bytes > 0);
    const bytes = try future.wait();
    pending = false;
    defer std.testing.allocator.free(bytes);
    try std.testing.expectEqualStrings("cdef", bytes);
    try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
    permit.release();
    try std.testing.expectEqual(@as(usize, 0), pool.retainedStates());
    try std.testing.expectEqual(@as(u64, 0), root.snapshot().memory.used_bytes);
}

test "LSM streaming block controlled into reads preserve original errors across checked callbacks" {
    const Fixture = struct {
        var dispatched: usize = 0;
        var reads: usize = 0;
        fn dispatch(contract: *const @import("antfly_runtime_abi").native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            dispatched += 1;
            return Storage.BoundaryAbi.local_dispatch(contract, callback, args, output);
        }
        fn read(_: *anyopaque, _: []const u8, _: u64, out: []u8, control: read_admission.Port) !void {
            reads += 1;
            // A provider that ignores rejection must not turn it into success.
            control.checkpoint() catch {};
            @memset(out, 0xab);
        }
    };
    const Budget = struct {
        checks: usize = 0,
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedWindowWait;
        }
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            if (self.checks >= 2) return error.TestOriginalIntoRejection;
        }
    };
    for ([_]bool{ false, true }) |checked| {
        var memory = MemoryStorage.init(std.testing.allocator);
        defer memory.deinit();
        var storage = memory.storage();
        var table = storage.vtable.*;
        table.read_file_range_into_with_admission = Fixture.read;
        storage.vtable = &table;
        if (checked) storage.boundary_dispatch = Fixture.dispatch;
        Fixture.dispatched = 0;
        Fixture.reads = 0;
        var budget: Budget = .{};
        var adapter: read_admission.Adapter(*Budget) = .{ .budget = &budget };
        const payload = adapter.payload();
        var out: [17]u8 = undefined;
        try std.testing.expectError(error.TestOriginalIntoRejection, storage.readFileRangeIntoWithAdmission("immutable-run", 0, &out, .init(&payload)));
        try std.testing.expectEqual(@as(usize, 1), Fixture.reads);
        try std.testing.expectEqual(@as(usize, if (checked) 1 else 0), Fixture.dispatched);
        table.read_file_range_into_with_admission = null;
        budget.checks = 0;
        try std.testing.expectError(error.ExecutionWaitUnsupported, storage.readFileRangeIntoWithAdmission("immutable-run", 0, &out, .init(&payload)));
        try std.testing.expectEqual(@as(usize, 1), Fixture.reads);
    }
}

test "generation tree accounting respects directory boundaries (#1210)" {
    const a = std.testing.allocator;
    var memory = MemoryStorage.init(a);
    defer memory.deinit();
    const storage = memory.storage();
    try storage.writeFileAbsolute("/generation/index/a", "123");
    try storage.writeFileAbsolute("/generation/index/nested/b", "4567");
    try storage.writeFileAbsolute("/generation/index/nested/c", "89");
    try storage.writeFileAbsolute("/generation-other/keep", "unrelated");
    try std.testing.expectEqual(@as(u64, 9), try storage.treeBytes(a, "/generation"));
    try std.testing.expectEqual(@as(u64, 0), try storage.treeBytes(a, "/missing"));
    const dirs = try storage.listDirectoryNamesAlloc(a, "/generation/");
    defer Storage.freeFileNames(a, dirs);
    try std.testing.expectEqual(@as(usize, 1), dirs.len);
    try std.testing.expectEqualStrings("index", dirs[0]);

    if (!supports_native_storage) return;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/generation", .{tmp.sub_path});
    defer a.free(path);
    var native = try NativeStorage.init(a, .threaded);
    defer native.deinit();
    const native_storage = native.storage();
    const nested = try std.fs.path.join(a, &.{ path, "index/nested" });
    defer a.free(nested);
    try native_storage.createDirPath(nested);
    const file = try std.fs.path.join(a, &.{ nested, "payload" });
    defer a.free(file);
    try native_storage.writeFileAbsolute(file, "123456789");
    try std.testing.expectEqual(@as(u64, 9), try native_storage.treeBytes(a, path));
}

const SyncTestControl = struct {
    const cpu = @import("../../common/runnable_scheduler.zig");
    const now = @import("antfly_platform").time.monotonicNs;
    const Mode = enum { success, begin_failure, end_failure, expired };
    execution: ?*cpu.Execution = null,
    scheduler: ?*cpu.Scheduler = null,
    deadline_ns: u64 = 0,
    mode: Mode = .success,
    begins: usize = 0,
    ends: usize = 0,
    release_pool: ?*NativeStoragePool = null,
    held_descriptors: usize = 0,
    fn lifetime(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.execution) |execution| try execution.checkLifetime() else if (now() >= self.deadline_ns) return error.DeadlineExceeded;
        if (self.mode == .expired and self.begins != 0) return error.DeadlineExceeded;
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try lifetime(raw);
        if (comptime supports_posix_fd_cache) if (self.release_pool) |pool| {
            if (!pool.fd_cache.admission_mutex.tryLock()) return error.TestUnexpectedResult;
            pool.fd_cache.admission_mutex.unlock(std.testing.io);
        };
        if (self.execution) |execution| if (self.scheduler.?.state(&execution.entry.?) != .running) return error.InvalidRunnableTransition;
    }
    fn deadline(raw: *anyopaque) !?u64 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return if (self.execution) |execution| execution.entry.?.options.deadline_ns else self.deadline_ns;
    }
    fn begin(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.begins += 1;
        if (self.mode == .begin_failure) return error.NotFound;
        if (self.execution) |execution| try execution.park(.resource_blocked);
        if (self.mode != .expired) NativeSyncTestControl.release.store(true, .release);
        if (self.release_pool) |pool| {
            pool.releaseDescriptorsForTest(std.testing.io, self.held_descriptors);
            self.held_descriptors = 0;
        }
    }
    fn end(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.ends += 1;
        NativeSyncTestControl.release.store(true, .release);
        if (self.mode == .end_failure) return error.Canceled;
        if (self.execution) |execution| try execution.enter(.query);
    }
    fn payload(self: *@This()) read_admission.Payload {
        return .{ .ptr = self, .vtable = &.{ .checkpoint = checkpoint, .check_lifetime = lifetime, .deadline_ns = deadline }, .execution_wait = .{ .ptr = self, .begin = begin, .end = end } };
    }
};

test "controlled physical sync retains accepted file and directory work through original failures and storage close" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const cpu = SyncTestControl.cpu;
    const now = SyncTestControl.now;
    inline for (.{ false, true }) |directory_sync| {
        for (std.enums.values(SyncTestControl.Mode)) |mode| {
            const a = std.testing.allocator;
            var pool = NativeStoragePool.initWithCapacityForTest(a, 8);
            defer pool.deinit();
            var native = try NativeStorage.initWithPool(a, .threaded, &pool);
            var native_live = true;
            defer if (native_live) native.deinit();
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/sync", .{tmp.sub_path});
            defer a.free(path);
            try native.storage().writeFileAbsolute(path, "durable");
            var slots = try cpu.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 });
            defer slots.close();
            var execution: cpu.Execution = .{};
            defer execution.finish();
            try execution.start(&slots, .{ .io = std.testing.io, .native_now_ns = now, .allow_wait = true, .deadline_ns = now() + 5 * std.time.ns_per_s }, .query);
            var health: cpu.Execution = .{};
            defer health.finish();
            try health.start(&slots, .{ .io = std.testing.io, .native_now_ns = now }, .control);
            var control: SyncTestControl = .{ .execution = &execution, .scheduler = &slots };
            var payload = control.payload();
            const original = read_admission.Port.init(&payload);
            NativeSyncTestControl.pause.store(true, .release);
            NativeSyncTestControl.started.store(false, .release);
            NativeSyncTestControl.release.store(false, .release);
            NativeSyncTestControl.calls.store(0, .release);
            defer NativeSyncTestControl.pause.store(false, .release);
            var operation = if (directory_sync) try native.storage().beginSyncParentWithAdmission(path, original) else try native.storage().beginSyncFileContentsWithAdmission(path, original);
            var live = true;
            defer {
                NativeSyncTestControl.release.store(true, .release);
                if (live) operation.deinit();
            }
            const until = now() + 5 * std.time.ns_per_s;
            while (!NativeSyncTestControl.started.load(.acquire)) {
                if (now() >= until) return error.TestUnexpectedResult;
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            }
            const identity = operation.ptr;
            var lease = try native.acquireLease();
            native.deinit();
            native_live = false;
            try std.testing.expectError(error.StorageClosed, lease.storage().beginSyncFileContentsWithAdmission(path, original));
            lease.deinit();
            try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
            control.mode = mode;
            const result = operation.waitWithAdmission(original);
            if (mode == .success) try result else {
                try std.testing.expectError(switch (mode) {
                    .begin_failure => error.NotFound,
                    .end_failure => error.Canceled,
                    .expired => error.DeadlineExceeded,
                    else => unreachable,
                }, result);
                try std.testing.expect(operation.ptr == identity);
                try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
                try std.testing.expectEqual(@as(usize, 1), pool.snapshotStats().fd_admitted_descriptors);
                if (mode == .begin_failure) try std.testing.expectEqual(@as(usize, 0), NativeSyncTestControl.calls.load(.acquire));
                if (mode == .end_failure) try execution.enter(.query);
                control.mode = .success;
                NativeSyncTestControl.release.store(true, .release);
                try operation.waitWithAdmission(original);
            }
            try operation.waitWithAdmission(original);
            try std.testing.expectEqual(@as(usize, 1), NativeSyncTestControl.calls.load(.acquire));
            try std.testing.expectEqual(control.begins, control.ends + @as(usize, if (mode == .begin_failure) 1 else 0));
            try std.testing.expectEqual(cpu.State.running, slots.state(&health.entry.?));
            operation.deinit();
            live = false;
            try std.testing.expectEqual(@as(usize, 0), pool.retainedStates());
            try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
        }
    }
}

test "controlled physical sync descriptor admission rejects original failure and retries without leaking capacity" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 4);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(std.testing.allocator, .threaded, &pool);
    defer native.deinit();
    const io = std.testing.io;
    var held: usize = 0;
    while (pool.fd_cache.tryReserveDescriptors(io, 1)) held += 1;
    try std.testing.expect(held != 0);
    var control: SyncTestControl = .{ .deadline_ns = SyncTestControl.now() + 5 * std.time.ns_per_s, .mode = .begin_failure, .release_pool = &pool, .held_descriptors = held };
    defer if (control.held_descriptors != 0) pool.releaseDescriptorsForTest(io, control.held_descriptors);
    var payload = control.payload();
    const original = read_admission.Port.init(&payload);
    try std.testing.expectError(error.NotFound, native.storage().beginSyncFileContentsWithAdmission("/missing-antfly-physical-sync", original));
    try std.testing.expectEqual(held, pool.snapshotStats().fd_admitted_descriptors);
    control.mode = .success;
    var operation = try native.storage().beginSyncFileContentsWithAdmission("/missing-antfly-physical-sync", original);
    defer operation.deinit();
    try std.testing.expectEqual(@as(usize, 0), control.held_descriptors);
    try std.testing.expectError(error.FileNotFound, operation.waitWithAdmission(original));
    // Physical errors are stable observations; retry never reissues the syscall.
    try std.testing.expectError(error.FileNotFound, operation.waitWithAdmission(original));
}

test "controlled physical sync allocation rejection returns descriptor and state ownership before dispatch" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    for (0..2) |failure_offset| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 4);
        defer pool.deinit();
        var native = try NativeStorage.initWithPool(failing.allocator(), .threaded, &pool);
        defer native.deinit();
        var control: SyncTestControl = .{ .deadline_ns = SyncTestControl.now() + 5 * std.time.ns_per_s };
        var payload = control.payload();
        failing.fail_index = failing.alloc_index + failure_offset;
        const result = native.storage().beginSyncFileContentsWithAdmission("/unissued-sync", .init(&payload));
        failing.fail_index = std.math.maxInt(usize);
        if (result) |value| {
            var operation = value;
            operation.deinit();
            return error.TestUnexpectedResult;
        } else |cause| try std.testing.expectEqual(error.OutOfMemory, cause);
        try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
        try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
    }
}

test "controlled physical sync refuses legacy provider fallback and unbounded original authority" {
    var memory = MemoryStorage.init(std.testing.allocator);
    defer memory.deinit();
    const storage = memory.storage();
    try storage.writeFileAbsolute("/sync", "payload");
    try storage.syncFileContentsAbsolute("/sync");
    var control: SyncTestControl = .{ .deadline_ns = SyncTestControl.now() + 5 * std.time.ns_per_s };
    var payload = control.payload();
    try std.testing.expectError(error.ExecutionWaitUnsupported, storage.beginSyncFileContentsWithAdmission("/sync", .init(&payload)));
    try std.testing.expectError(error.ExecutionWaitUnsupported, storage.beginSyncParentWithAdmission("/sync", .init(&payload)));
    try std.testing.expectEqual(@as(usize, 0), control.begins);
    if (supports_posix_fd_cache) {
        var native = try NativeStorage.init(std.testing.allocator, .threaded);
        defer native.deinit();
        payload.execution_wait = null;
        try std.testing.expectError(error.ExecutionWaitUnsupported, native.storage().beginSyncFileContentsWithAdmission("/unissued", .init(&payload)));
        payload = control.payload();
        const NoDeadline = struct {
            fn get(_: *anyopaque) !?u64 {
                return null;
            }
        };
        payload.vtable = &.{ .checkpoint = SyncTestControl.checkpoint, .check_lifetime = SyncTestControl.lifetime, .deadline_ns = NoDeadline.get };
        try std.testing.expectError(error.ExecutionWaitUnsupported, native.storage().beginSyncFileContentsWithAdmission("/unissued", .init(&payload)));
    }
}

test "controlled physical sync refuses actual executor saturation without inline I/O or leaked permits" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    var pool = NativeStoragePool.initWithCapacityForTest(std.testing.allocator, 4);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(std.testing.allocator, .threaded, &pool);
    defer native.deinit();
    NativeSyncTestControl.pause.store(false, .release);
    NativeSyncTestControl.calls.store(0, .release);
    var control: SyncTestControl = .{ .deadline_ns = SyncTestControl.now() + 5 * std.time.ns_per_s };
    var payload = control.payload();
    const original = read_admission.Port.init(&payload);
    // Change the real executor before it has any concurrent tasks. Refusal is
    // produced by std.Io.Threaded, not by a sync-provider test replacement.
    const previous_limit = native.state.threaded.concurrent_limit;
    native.state.threaded.concurrent_limit = .nothing;
    for (0..2) |_| {
        const result = native.storage().beginSyncFileContentsWithAdmission("/unissued-antfly-sync", original);
        if (result) |value| {
            var operation = value;
            operation.deinit();
            return error.TestUnexpectedResult;
        } else |cause| try std.testing.expectEqual(error.ConcurrencyUnavailable, cause);
        try std.testing.expectEqual(@as(usize, 0), NativeSyncTestControl.calls.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
        try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
    }
    native.state.threaded.concurrent_limit = previous_limit;
    var operation = try native.storage().beginSyncFileContentsWithAdmission("/unissued-antfly-sync", original);
    defer operation.deinit();
    try std.testing.expectError(error.FileNotFound, operation.waitWithAdmission(original));
    try std.testing.expectEqual(@as(usize, 1), NativeSyncTestControl.calls.load(.acquire));
}

test "controlled physical sync metadata read preserves bounded bytes through original failure and owner close" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const cpu = SyncTestControl.cpu;
    const now = SyncTestControl.now;
    for (std.enums.values(SyncTestControl.Mode)) |mode| {
        const a = std.testing.allocator;
        var pool = NativeStoragePool.initWithCapacityForTest(a, 8);
        defer pool.deinit();
        var native = try NativeStorage.initWithPool(a, .threaded, &pool);
        var native_live = true;
        defer if (native_live) native.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/CURRENT", .{tmp.sub_path});
        defer a.free(path);
        try native.storage().writeFileAbsolute(path, "old-control");
        var slots = try cpu.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 });
        defer slots.close();
        var execution: cpu.Execution = .{};
        defer execution.finish();
        try execution.start(&slots, .{ .io = std.testing.io, .native_now_ns = now, .allow_wait = true, .deadline_ns = now() + 5 * std.time.ns_per_s }, .query);
        var control: SyncTestControl = .{ .execution = &execution, .scheduler = &slots };
        var payload = control.payload();
        const original = read_admission.Port.init(&payload);
        NativeSyncTestControl.pause.store(true, .release);
        NativeSyncTestControl.started.store(false, .release);
        NativeSyncTestControl.release.store(false, .release);
        NativeSyncTestControl.reads.store(0, .release);
        defer NativeSyncTestControl.pause.store(false, .release);
        var operation = try native.storage().beginControlFileReadWithAdmission(path, 64, original);
        var live = true;
        defer {
            NativeSyncTestControl.release.store(true, .release);
            if (live) operation.deinit();
        }
        const until = now() + 5 * std.time.ns_per_s;
        while (!NativeSyncTestControl.started.load(.acquire)) {
            if (now() >= until) return error.TestUnexpectedResult;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        native.deinit();
        native_live = false;
        control.mode = mode;
        var result: ControlFileResult = undefined;
        const first = operation.waitWithAdmission(original);
        if (mode == .success) result = try first else {
            try std.testing.expectError(switch (mode) {
                .begin_failure => error.NotFound,
                .end_failure => error.Canceled,
                .expired => error.DeadlineExceeded,
                else => unreachable,
            }, first);
            try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
            try std.testing.expectEqual(@as(usize, 1), pool.snapshotStats().fd_admitted_descriptors);
            if (mode == .begin_failure) try std.testing.expectEqual(@as(usize, 0), NativeSyncTestControl.reads.load(.acquire));
            if (mode == .end_failure) try execution.enter(.query);
            control.mode = .success;
            NativeSyncTestControl.release.store(true, .release);
            result = try operation.waitWithAdmission(original);
        }
        try std.testing.expectEqual(@as(u64, 11), result.file_size);
        try std.testing.expectEqualStrings("old-control", result.bytes);
        var replacement = try NativeStorage.init(a, .threaded);
        defer replacement.deinit();
        try replacement.storage().writeFileAbsolute(path, "replacement");
        const again = try operation.waitWithAdmission(original);
        try std.testing.expect(again.bytes.ptr == result.bytes.ptr);
        try std.testing.expectEqualStrings("old-control", again.bytes);
        try std.testing.expectEqual(@as(usize, 1), NativeSyncTestControl.reads.load(.acquire));
        operation.deinit();
        live = false;
        try std.testing.expectEqual(@as(usize, 0), pool.retainedStates());
        try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
    }
}

test "controlled physical sync metadata size guard rejects hydration and dispatch allocation failures" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const a = std.testing.allocator;
    var pool = NativeStoragePool.initWithCapacityForTest(a, 4);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(a, .threaded, &pool);
    defer native.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/oversized-control", .{tmp.sub_path});
    defer a.free(path);
    const large: [8192]u8 = @splat('x');
    try native.storage().writeFileAbsolute(path, &large);
    var control: SyncTestControl = .{ .deadline_ns = SyncTestControl.now() + 5 * std.time.ns_per_s };
    var payload = control.payload();
    const original = read_admission.Port.init(&payload);
    var read = try native.storage().beginControlFileReadWithAdmission(path, 36, original);
    const result = try read.waitWithAdmission(original);
    try std.testing.expectEqual(@as(u64, large.len), result.file_size);
    try std.testing.expectEqual(@as(usize, 0), result.bytes.len);
    read.deinit();
    for ([_]usize{ 0, max_control_file_bytes + 1 }) |limit|
        try std.testing.expectError(error.ResourceBudgetExceeded, native.storage().beginControlFileReadWithAdmission(path, limit, original));
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
    var memory = MemoryStorage.init(a);
    defer memory.deinit();
    try std.testing.expectError(error.ExecutionWaitUnsupported, memory.storage().beginControlFileReadWithAdmission(path, 36, original));
    const old_limit = native.state.threaded.concurrent_limit;
    native.state.threaded.concurrent_limit = .nothing;
    try std.testing.expectError(error.ConcurrencyUnavailable, native.storage().beginControlFileReadWithAdmission(path, 36, original));
    native.state.threaded.concurrent_limit = old_limit;
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
    // All three caller-side allocations precede physical dispatch.
    for (0..3) |offset| {
        var failing = std.testing.FailingAllocator.init(a, .{});
        var candidate = try NativeStorage.initWithPool(failing.allocator(), .threaded, &pool);
        defer candidate.deinit();
        failing.fail_index = failing.alloc_index + offset;
        const attempt = candidate.storage().beginControlFileReadWithAdmission(path, 36, original);
        failing.fail_index = std.math.maxInt(usize);
        if (attempt) |value| {
            var unexpected = value;
            unexpected.deinit();
            return error.TestUnexpectedResult;
        } else |cause| try std.testing.expectEqual(error.OutOfMemory, cause);
        try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
    }
}

test "controlled physical sync reusable grant preserves failed phases and completes under exhausted resources" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const cpu = SyncTestControl.cpu;
    const now = SyncTestControl.now;
    for (std.enums.values(SyncTestControl.Mode)) |mode| {
        const a = std.testing.allocator;
        var backing = std.testing.FailingAllocator.init(a, .{});
        var pool = NativeStoragePool.initWithCapacityForTest(a, 4);
        defer pool.deinit();
        var native = try NativeStorage.initWithPool(backing.allocator(), .threaded, &pool);
        var native_live = true;
        defer if (native_live) native.deinit();
        const state = native.state;
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/CURRENT", .{tmp.sub_path});
        defer a.free(path);
        try native.storage().writeFileAbsolute(path, "old-control");
        var slots = try cpu.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 });
        defer slots.close();
        var execution: cpu.Execution = .{};
        defer execution.finish();
        try execution.start(&slots, .{ .io = std.testing.io, .native_now_ns = now, .allow_wait = true, .deadline_ns = now() + 10 * std.time.ns_per_s }, .query);
        var health: cpu.Execution = .{};
        defer health.finish();
        try health.start(&slots, .{ .io = std.testing.io, .native_now_ns = now }, .control);
        var control: SyncTestControl = .{ .execution = &execution, .scheduler = &slots };
        var payload = control.payload();
        const original = read_admission.Port.init(&payload);
        var grant = (try native.storage().tryPrepareDurabilityWithAdmission(original)) orelse return error.TestUnexpectedResult;
        var grant_live = true;
        defer {
            NativeSyncTestControl.release.store(true, .release);
            NativeSyncTestControl.pause.store(false, .release);
            execution.finish();
            if (grant_live) grant.deinit();
        }
        // Force every later stage to use only its prepared resources. Neither
        // a new allocation nor a fresh worker/descriptor can be admitted.
        state.threaded.concurrent_limit = .nothing;
        backing.fail_index = backing.alloc_index;
        backing.resize_fail_index = backing.resize_index;
        const allocation_index = backing.alloc_index;
        const resize_index = backing.resize_index;
        var held: usize = 0;
        while (pool.fd_cache.tryReserveDescriptors(std.testing.io, 1)) held += 1;
        defer pool.releaseDescriptorsForTest(std.testing.io, held);
        NativeSyncTestControl.pause.store(true, .release);
        NativeSyncTestControl.started.store(false, .release);
        NativeSyncTestControl.release.store(false, .release);
        NativeSyncTestControl.reads.store(0, .release);
        NativeSyncTestControl.calls.store(0, .release);
        var read = try grant.beginControlRead(path, 36, original);
        const until = now() + 5 * std.time.ns_per_s;
        while (!NativeSyncTestControl.started.load(.acquire)) {
            if (now() >= until) return error.TestUnexpectedResult;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        native.deinit();
        native_live = false;
        control.mode = mode;
        const first = read.waitWithAdmission(original);
        var result: ControlFileResult = undefined;
        if (mode == .success) result = try first else {
            try std.testing.expectError(switch (mode) {
                .begin_failure => error.NotFound,
                .end_failure => error.Canceled,
                .expired => error.DeadlineExceeded,
                .success => unreachable,
            }, first);
            try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
            try std.testing.expectEqual(held + 1, pool.snapshotStats().fd_admitted_descriptors);
            if (mode == .end_failure) try execution.enter(.query);
            control.mode = .success;
            NativeSyncTestControl.release.store(true, .release);
            result = try read.waitWithAdmission(original);
        }
        try std.testing.expectEqualStrings("old-control", result.bytes);
        try std.testing.expectEqual(@as(u64, 11), result.file_size);
        // The pending phase owns its result until explicit retirement. A new
        // phase must neither overwrite it nor accidentally issue more I/O.
        try std.testing.expectError(error.ResourceBudgetExceeded, grant.beginSyncContents(path, original));
        const again = try read.waitWithAdmission(original);
        try std.testing.expect(again.bytes.ptr == result.bytes.ptr);
        try std.testing.expectEqualStrings("old-control", again.bytes);
        try std.testing.expectEqual(@as(usize, 1), NativeSyncTestControl.reads.load(.acquire));
        read.deinit();
        NativeSyncTestControl.pause.store(false, .release);
        // Immediate producer reuse exercises the completion/wake race, while
        // alternating data results and both types of physical durability work.
        for (0..64) |_| {
            var next_read = try grant.beginControlRead(path, 36, original);
            try std.testing.expectEqualStrings("old-control", (try next_read.waitWithAdmission(original)).bytes);
            next_read.deinit();
            var contents = try grant.beginSyncContents(path, original);
            try contents.waitWithAdmission(original);
            contents.deinit();
            var parent = try grant.beginSyncParent(path, original);
            try parent.waitWithAdmission(original);
            parent.deinit();
        }
        try std.testing.expectEqual(@as(usize, 65), NativeSyncTestControl.reads.load(.acquire));
        try std.testing.expectEqual(@as(usize, 128), NativeSyncTestControl.calls.load(.acquire));
        try std.testing.expectEqual(allocation_index, backing.alloc_index);
        try std.testing.expectEqual(resize_index, backing.resize_index);
        try std.testing.expectEqual(cpu.State.running, slots.state(&health.entry.?));
        grant.deinit();
        grant_live = false;
        try std.testing.expectEqual(@as(usize, 0), pool.retainedStates());
        try std.testing.expectEqual(held, pool.snapshotStats().fd_admitted_descriptors);
        try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
    }
}

test "controlled physical sync reusable grant preparation rolls back capacity and refuses unsupported authority" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const a = std.testing.allocator;
    var pool = NativeStoragePool.initWithCapacityForTest(a, 4);
    defer pool.deinit();
    var backing = std.testing.FailingAllocator.init(a, .{});
    var native = try NativeStorage.initWithPool(backing.allocator(), .threaded, &pool);
    defer native.deinit();
    var control: SyncTestControl = .{ .deadline_ns = SyncTestControl.now() + 5 * std.time.ns_per_s };
    var payload = control.payload();
    const original = read_admission.Port.init(&payload);
    var unbounded = payload;
    unbounded.execution_wait = null;
    try std.testing.expectError(error.ExecutionWaitUnsupported, native.storage().tryPrepareDurabilityWithAdmission(.init(&unbounded)));
    var memory = MemoryStorage.init(a);
    defer memory.deinit();
    try std.testing.expectError(error.ExecutionWaitUnsupported, memory.storage().tryPrepareDurabilityWithAdmission(original));
    // Busy admission metadata must return promptly rather than blocking the
    // CPU or escaping with a retained state/descriptor.
    try std.testing.expect(pool.fd_cache.admission_mutex.tryLock());
    {
        defer pool.fd_cache.admission_mutex.unlock(std.testing.io);
        try std.testing.expect(try native.storage().tryPrepareDurabilityWithAdmission(original) == null);
    }
    var queued: FdCache.AdmissionWaiter = .{ .count = 1 };
    pool.fd_cache.enqueueAdmissionWaiter(&queued);
    {
        defer pool.fd_cache.removeAdmissionWaiter(&queued);
        try std.testing.expect(try native.storage().tryPrepareDurabilityWithAdmission(original) == null);
    }
    var held: usize = 0;
    while (pool.fd_cache.tryReserveDescriptors(std.testing.io, 1)) held += 1;
    try std.testing.expect(try native.storage().tryPrepareDurabilityWithAdmission(original) == null);
    pool.releaseDescriptorsForTest(std.testing.io, held);
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
    backing.fail_index = backing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, native.storage().tryPrepareDurabilityWithAdmission(original));
    backing.fail_index = std.math.maxInt(usize);
    const concurrent_limit = native.state.threaded.concurrent_limit;
    native.state.threaded.concurrent_limit = .nothing;
    try std.testing.expectError(error.ConcurrencyUnavailable, native.storage().tryPrepareDurabilityWithAdmission(original));
    native.state.threaded.concurrent_limit = concurrent_limit;
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
    try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
    var grant = (try native.storage().tryPrepareDurabilityWithAdmission(original)) orelse return error.TestUnexpectedResult;
    grant.deinit();
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
}

test "controlled physical sync reusable grant joins accepted work before retiring its worker" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const a = std.testing.allocator;
    var pool = NativeStoragePool.initWithCapacityForTest(a, 4);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(a, .threaded, &pool);
    var native_live = true;
    defer if (native_live) native.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/sync", .{tmp.sub_path});
    defer a.free(path);
    try native.storage().writeFileAbsolute(path, "durable");
    var control: SyncTestControl = .{ .deadline_ns = SyncTestControl.now() + 5 * std.time.ns_per_s };
    var payload = control.payload();
    const original = read_admission.Port.init(&payload);
    const Join = struct {
        grant: DurabilityResources,
        joining: std.Io.Event = .unset,
        retired: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.joining.set(std.testing.io);
            self.grant.deinit();
            self.retired.store(true, .release);
        }
    };
    var join: Join = .{ .grant = (try native.storage().tryPrepareDurabilityWithAdmission(original)) orelse return error.TestUnexpectedResult };
    NativeSyncTestControl.pause.store(true, .release);
    NativeSyncTestControl.started.store(false, .release);
    NativeSyncTestControl.release.store(false, .release);
    NativeSyncTestControl.calls.store(0, .release);
    var joined = false;
    defer {
        NativeSyncTestControl.release.store(true, .release);
        NativeSyncTestControl.pause.store(false, .release);
        if (!joined) join.grant.deinit();
    }
    // Parent retirement consumes this accepted child. It must finish the
    // physical operation even when no consumer ever attempts an admitted wait.
    _ = try join.grant.beginSyncContents(path, original);
    const until = SyncTestControl.now() + 5 * std.time.ns_per_s;
    while (!NativeSyncTestControl.started.load(.acquire)) {
        if (SyncTestControl.now() >= until) return error.TestUnexpectedResult;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    native.deinit();
    native_live = false;
    var thread = try std.testing.io.concurrent(Join.run, .{&join});
    defer {
        NativeSyncTestControl.release.store(true, .release);
        thread.await(std.testing.io);
    }
    joined = true;
    try join.joining.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expect(!join.retired.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
    try std.testing.expectEqual(@as(usize, 0), NativeSyncTestControl.calls.load(.acquire));
    NativeSyncTestControl.release.store(true, .release);
    thread.await(std.testing.io);
    try std.testing.expect(join.retired.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), NativeSyncTestControl.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), pool.retainedStates());
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
}

test "controlled physical sync reusable grant bounds metadata and reuses the slot after physical failure" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const a = std.testing.allocator;
    var pool = NativeStoragePool.initWithCapacityForTest(a, 4);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(a, .threaded, &pool);
    defer native.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/CURRENT", .{tmp.sub_path});
    defer a.free(path);
    const missing = try std.fmt.allocPrint(a, "{s}.missing", .{path});
    defer a.free(missing);
    const too_large: [8192]u8 = @splat(7);
    try native.storage().writeFileAbsolute(path, &too_large);
    var control: SyncTestControl = .{ .deadline_ns = SyncTestControl.now() + 5 * std.time.ns_per_s };
    var payload = control.payload();
    const original = read_admission.Port.init(&payload);
    var grant = (try native.storage().tryPrepareDurabilityWithAdmission(original)) orelse return error.TestUnexpectedResult;
    defer grant.deinit();
    const too_long: [std.fs.max_path_bytes]u8 = @splat('x');
    try std.testing.expectError(error.NameTooLong, grant.beginSyncContents(&too_long, original));
    for ([_]usize{ 0, max_control_file_bytes + 1 }) |limit|
        try std.testing.expectError(error.ResourceBudgetExceeded, grant.beginControlRead(path, limit, original));
    var read = try grant.beginControlRead(path, 36, original);
    const oversized = try read.waitWithAdmission(original);
    try std.testing.expectEqual(@as(u64, 8192), oversized.file_size);
    try std.testing.expectEqual(@as(usize, 0), oversized.bytes.len);
    read.deinit();
    var failed = try grant.beginSyncContents(missing, original);
    for (0..3) |_| try std.testing.expectError(error.FileNotFound, failed.waitWithAdmission(original));
    failed.deinit();
    // A failed physical stage is a stable observation until retired. Once
    // retired, it cannot poison the next request's status or result buffer.
    var sync = try grant.beginSyncContents(path, original);
    try sync.waitWithAdmission(original);
    sync.deinit();
    control.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, grant.beginSyncContents(path, original));
    control.deadline_ns = SyncTestControl.now() + 5 * std.time.ns_per_s;
    var parent = try grant.beginSyncParent(path, original);
    try parent.waitWithAdmission(original);
    parent.deinit();
}

test "controlled physical sync prepared append owns bytes and survives rejected waits with exhausted resources" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const cpu = SyncTestControl.cpu;
    const now = SyncTestControl.now;
    for (std.enums.values(SyncTestControl.Mode)) |mode| {
        const a = std.testing.allocator;
        var backing = std.testing.FailingAllocator.init(a, .{});
        var pool = NativeStoragePool.initWithCapacityForTest(a, 4);
        defer pool.deinit();
        var native = try NativeStorage.initWithPool(backing.allocator(), .threaded, &pool);
        var native_live = true;
        defer if (native_live) native.deinit();
        const state = native.state;
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/append", .{tmp.sub_path});
        defer a.free(path);
        try native.storage().writeFileAbsolute(path, "prefix:");
        var slots = try cpu.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 });
        defer slots.close();
        var execution: cpu.Execution = .{};
        defer execution.finish();
        try execution.start(&slots, .{ .io = std.testing.io, .native_now_ns = now, .allow_wait = true, .deadline_ns = now() + 10 * std.time.ns_per_s }, .query);
        var health: cpu.Execution = .{};
        defer health.finish();
        try health.start(&slots, .{ .io = std.testing.io, .native_now_ns = now }, .control);
        var control: SyncTestControl = .{ .execution = &execution, .scheduler = &slots };
        var payload = control.payload();
        const original = read_admission.Port.init(&payload);
        const input = try a.alloc(u8, 128 * 1024);
        defer a.free(input);
        @memset(input, 'a');
        var grant = (try native.storage().tryPrepareAppendWithAdmission(input, original)) orelse return error.TestUnexpectedResult;
        var grant_live = true;
        defer {
            NativeSyncTestControl.release.store(true, .release);
            NativeSyncTestControl.pause.store(false, .release);
            execution.finish();
            if (grant_live) grant.deinit();
        }
        // The input borrow ends at preparation, before publication/dispatch.
        @memset(input, 'b');
        state.threaded.concurrent_limit = .nothing;
        backing.fail_index = backing.alloc_index;
        backing.resize_fail_index = backing.resize_index;
        const allocations = backing.alloc_index;
        const resizes = backing.resize_index;
        var held: usize = 0;
        while (pool.fd_cache.tryReserveDescriptors(std.testing.io, 1)) held += 1;
        defer pool.releaseDescriptorsForTest(std.testing.io, held);
        NativeSyncTestControl.pause.store(true, .release);
        NativeSyncTestControl.started.store(false, .release);
        NativeSyncTestControl.release.store(false, .release);
        NativeSyncTestControl.appends.store(0, .release);
        NativeSyncTestControl.calls.store(0, .release);
        var operation = try grant.beginPreparedAppend(path, false, original);
        native.deinit();
        native_live = false;
        control.mode = mode;
        if (mode != .success) {
            const expected = switch (mode) {
                .begin_failure => error.NotFound,
                .end_failure => error.Canceled,
                .expired => error.DeadlineExceeded,
                .success => unreachable,
            };
            try std.testing.expectError(expected, operation.waitWithAdmission(original));
            if (mode == .end_failure) try execution.enter(.query);
            control.mode = .success;
            NativeSyncTestControl.release.store(true, .release);
        }
        try operation.waitWithAdmission(original);
        try operation.waitWithAdmission(original);
        try std.testing.expectError(error.ResourceBudgetExceeded, grant.beginPreparedAppend(path, false, original));
        operation.deinit();
        NativeSyncTestControl.pause.store(false, .release);
        // Append alone never implies a durability barrier. Both barriers can
        // follow on the very same prepared worker and descriptor grant.
        try std.testing.expectEqual(@as(usize, 0), NativeSyncTestControl.calls.load(.acquire));
        var contents = try grant.beginSyncContents(path, original);
        try contents.waitWithAdmission(original);
        contents.deinit();
        var parent = try grant.beginSyncParent(path, original);
        try parent.waitWithAdmission(original);
        parent.deinit();
        try std.testing.expectEqual(@as(usize, 1), NativeSyncTestControl.appends.load(.acquire));
        try std.testing.expectEqual(@as(usize, 2), NativeSyncTestControl.calls.load(.acquire));
        try std.testing.expectEqual(allocations, backing.alloc_index);
        try std.testing.expectEqual(resizes, backing.resize_index);
        try std.testing.expectEqual(cpu.State.running, slots.state(&health.entry.?));
        grant.deinit();
        grant_live = false;
        try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
        try std.testing.expectEqual(@as(usize, 0), pool.retainedStates());
        try std.testing.expectEqual(held, pool.snapshotStats().fd_admitted_descriptors);
        const actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, a, .limited(256 * 1024));
        defer a.free(actual);
        try std.testing.expectEqualStrings("prefix:", actual[0..7]);
        try std.testing.expectEqual(@as(usize, 7 + 128 * 1024), actual.len);
        for (actual[7..]) |byte| try std.testing.expectEqual(@as(u8, 'a'), byte);
    }
}

test "controlled physical sync prepared append requires explicit creation and never fabricates missing parents" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const a = std.testing.allocator;
    var native = try NativeStorage.init(a, .threaded);
    defer native.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/new", .{tmp.sub_path});
    defer a.free(path);
    const missing_parent = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/absent/new", .{tmp.sub_path});
    defer a.free(missing_parent);
    var control: SyncTestControl = .{ .deadline_ns = SyncTestControl.now() + 10 * std.time.ns_per_s };
    var payload = control.payload();
    const original = read_admission.Port.init(&payload);
    var ordinary = (try native.storage().tryPrepareDurabilityWithAdmission(original)).?;
    defer ordinary.deinit();
    try std.testing.expectError(error.ExecutionWaitUnsupported, ordinary.beginPreparedAppend(path, true, original));
    var grant = (try native.storage().tryPrepareAppendWithAdmission("record", original)).?;
    defer grant.deinit();
    var missing = try grant.beginPreparedAppend(path, false, original);
    for (0..2) |_| try std.testing.expectError(error.FileNotFound, missing.waitWithAdmission(original));
    missing.deinit();
    var bad_parent = try grant.beginPreparedAppend(missing_parent, true, original);
    try std.testing.expectError(error.FileNotFound, bad_parent.waitWithAdmission(original));
    bad_parent.deinit();
    var created = try grant.beginPreparedAppend(path, true, original);
    try created.waitWithAdmission(original);
    created.deinit();
    const actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, a, .limited(100));
    defer a.free(actual);
    try std.testing.expectEqualStrings("record", actual);
}

test "controlled physical sync prepared append joins accepted write before freeing its bytes and worker" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const a = std.testing.allocator;
    var pool = NativeStoragePool.initWithCapacityForTest(a, 4);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(a, .threaded, &pool);
    var native_live = true;
    defer if (native_live) native.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/sync", .{tmp.sub_path});
    defer a.free(path);
    try native.storage().writeFileAbsolute(path, "durable");
    var control: SyncTestControl = .{ .deadline_ns = SyncTestControl.now() + 5 * std.time.ns_per_s };
    var payload = control.payload();
    const original = read_admission.Port.init(&payload);
    const Join = struct {
        grant: DurabilityResources,
        joining: std.Io.Event = .unset,
        retired: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.joining.set(std.testing.io);
            self.grant.deinit();
            self.retired.store(true, .release);
        }
    };
    var join: Join = .{ .grant = (try native.storage().tryPrepareAppendWithAdmission("record", original)) orelse return error.TestUnexpectedResult };
    NativeSyncTestControl.pause.store(true, .release);
    NativeSyncTestControl.started.store(false, .release);
    NativeSyncTestControl.release.store(false, .release);
    NativeSyncTestControl.appends.store(0, .release);
    var joined = false;
    defer {
        NativeSyncTestControl.release.store(true, .release);
        NativeSyncTestControl.pause.store(false, .release);
        if (!joined) join.grant.deinit();
    }
    // Parent retirement consumes this accepted child. It must finish the
    // physical operation even when no consumer ever attempts an admitted wait.
    _ = try join.grant.beginPreparedAppend(path, false, original);
    const until = SyncTestControl.now() + 5 * std.time.ns_per_s;
    while (!NativeSyncTestControl.started.load(.acquire)) {
        if (SyncTestControl.now() >= until) return error.TestUnexpectedResult;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    native.deinit();
    native_live = false;
    var thread = try std.testing.io.concurrent(Join.run, .{&join});
    defer {
        NativeSyncTestControl.release.store(true, .release);
        thread.await(std.testing.io);
    }
    joined = true;
    try join.joining.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expect(!join.retired.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), pool.retainedStates());
    try std.testing.expectEqual(@as(usize, 0), NativeSyncTestControl.appends.load(.acquire));
    NativeSyncTestControl.release.store(true, .release);
    thread.await(std.testing.io);
    try std.testing.expect(join.retired.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), NativeSyncTestControl.appends.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), pool.retainedStates());
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
    const actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, a, .limited(100));
    defer a.free(actual);
    try std.testing.expectEqualStrings("durablerecord", actual);
}

test "controlled physical sync prepared append rolls back partial copies allocation dispatch and descriptor refusal" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const a = std.testing.allocator;
    var backing = std.testing.FailingAllocator.init(a, .{});
    var pool = NativeStoragePool.initWithCapacityForTest(a, 4);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(backing.allocator(), .threaded, &pool);
    defer native.deinit();
    const Control = struct {
        checks: usize = 0,
        backing: *std.testing.FailingAllocator,
        frozen_threshold: usize,
        reject: bool = true,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.reject and self.backing.allocated_bytes - self.backing.freed_bytes >= self.frozen_threshold) {
                self.checks += 1;
                if (self.checks >= 2) return error.SessionLeaseLost;
            }
        }
        fn deadline(_: *anyopaque) !?u64 {
            return std.math.maxInt(u64);
        }
        fn begin(_: *anyopaque) !void {}
        fn end(_: *anyopaque) !void {}
    };
    const live_bytes = backing.allocated_bytes - backing.freed_bytes;
    var control: Control = .{ .backing = &backing, .frozen_threshold = live_bytes + 70 * 1024 };
    const payload: read_admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
    const original = read_admission.Port.init(&payload);
    const bytes: [70 * 1024]u8 = @splat('r');
    try std.testing.expectError(error.SessionLeaseLost, native.storage().tryPrepareAppendWithAdmission(&bytes, original));
    try std.testing.expectEqual(@as(usize, 2), control.checks);
    try std.testing.expectEqual(live_bytes, backing.allocated_bytes - backing.freed_bytes);
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
    control.reject = false;
    for (0..2) |allocation| {
        backing.fail_index = backing.alloc_index + allocation;
        try std.testing.expectError(error.OutOfMemory, native.storage().tryPrepareAppendWithAdmission(&bytes, original));
        backing.fail_index = std.math.maxInt(usize);
        try std.testing.expectEqual(live_bytes, backing.allocated_bytes - backing.freed_bytes);
        try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
    }
    const concurrent_limit = native.state.threaded.concurrent_limit;
    native.state.threaded.concurrent_limit = .nothing;
    try std.testing.expectError(error.ConcurrencyUnavailable, native.storage().tryPrepareAppendWithAdmission(&bytes, original));
    native.state.threaded.concurrent_limit = concurrent_limit;
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
    var held: usize = 0;
    while (pool.fd_cache.tryReserveDescriptors(std.testing.io, 1)) held += 1;
    try std.testing.expect(try native.storage().tryPrepareAppendWithAdmission(&bytes, original) == null);
    pool.releaseDescriptorsForTest(std.testing.io, held);
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
    var grant = (try native.storage().tryPrepareAppendWithAdmission(&bytes, original)).?;
    grant.deinit();
    try std.testing.expectEqual(@as(usize, 0), pool.snapshotStats().fd_admitted_descriptors);
    var memory = MemoryStorage.init(a);
    defer memory.deinit();
    try std.testing.expectError(error.ExecutionWaitUnsupported, memory.storage().tryPrepareAppendWithAdmission(&bytes, original));
}

test "controlled physical sync prepared append preserves an ambiguous partial write without replay" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const a = std.testing.allocator;
    var native = try NativeStorage.init(a, .threaded);
    defer native.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/partial", .{tmp.sub_path});
    defer a.free(path);
    try native.storage().writeFileAbsolute(path, "prefix:");
    var control: SyncTestControl = .{ .deadline_ns = SyncTestControl.now() + 5 * std.time.ns_per_s };
    var payload = control.payload();
    const original = read_admission.Port.init(&payload);
    var grant = (try native.storage().tryPrepareAppendWithAdmission("record", original)).?;
    defer grant.deinit();
    NativeSyncTestControl.appends.store(0, .release);
    NativeSyncTestControl.append_failure_after.store(3, .release);
    defer NativeSyncTestControl.append_failure_after.store(std.math.maxInt(usize), .release);
    var operation = try grant.beginPreparedAppend(path, false, original);
    defer operation.deinit();
    try std.testing.expectError(error.NoSpaceLeft, operation.waitWithAdmission(original));
    NativeSyncTestControl.append_failure_after.store(std.math.maxInt(usize), .release);
    for (0..3) |_| try std.testing.expectError(error.NoSpaceLeft, operation.waitWithAdmission(original));
    try std.testing.expectEqual(@as(usize, 1), NativeSyncTestControl.appends.load(.acquire));
    const actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, a, .limited(100));
    defer a.free(actual);
    try std.testing.expectEqualStrings("prefix:rec", actual);
}

test "controlled physical sync control replacement fences cached inodes with admitted retry and retained readers" {
    if (!supports_posix_fd_cache) return error.SkipZigTest;
    const a = std.testing.allocator;
    var pool = NativeStoragePool.initWithCapacityForTest(a, 16);
    defer pool.deinit();
    var native = try NativeStorage.initWithPool(a, .threaded, &pool);
    defer native.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/control", .{tmp.sub_path});
    defer a.free(path);
    try native.storage().writeFileAbsolute(path, "old");
    const cache = native.state.fdCache();
    const pinned = try cache.retain(native.state.cache_namespace, std.testing.io, path);
    defer cache.release(std.testing.io, pinned);
    const shard = cache.shardForHash(namespacedPathHash(native.state.cache_namespace, path));
    const Control = struct {
        implementation: ?*NativeDurabilityResources = null,
        mutex: *std.atomic.Mutex,
        block: bool = true,
        held: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.implementation) |impl| if (self.block and impl.phase.load(.acquire) == .completed and !self.held) {
                if (!self.mutex.tryLock()) return error.TestUnexpectedResult;
                self.held = true;
            };
        }
        fn deadline(_: *anyopaque) !?u64 {
            return std.math.maxInt(u64);
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.held) return error.NotFound;
        }
        fn end(_: *anyopaque) !void {}
    };
    var control: Control = .{ .mutex = &shard.mutex };
    const payload: read_admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
    const original = read_admission.Port.init(&payload);
    var grant = (try native.storage().tryPrepareDurabilityWithAdmission(original)).?;
    defer grant.deinit();
    control.implementation = @ptrCast(@alignCast(grant.ptr));
    var operation = try grant.beginControlReplace(path, "new", original);
    defer operation.deinit();
    // Release an injected guard before either mandatory owner retirement.
    defer if (control.held) control.mutex.unlock();
    try std.testing.expectError(error.NotFound, operation.waitWithAdmission(original));
    try std.testing.expect(control.held and control.implementation.?.invalidation_pending);
    control.mutex.unlock();
    control.held = false;
    control.block = false;
    try operation.waitWithAdmission(original);
    try std.testing.expect(!control.implementation.?.invalidation_pending);
    var fresh: [3]u8 = undefined;
    try native.storage().readFileRangeInto(a, path, 0, &fresh);
    try std.testing.expectEqualStrings("new", &fresh);
    try readAllAtOffset(pinned.fd, &fresh, 0);
    try std.testing.expectEqualStrings("old", &fresh);
}
