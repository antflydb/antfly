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
const platform_sync = @import("antfly_platform").sync;
const Allocator = std.mem.Allocator;
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const wal_types = @import("types.zig");
const wal_store = @import("store.zig");
const record_codec = @import("object_store.zig");
const Execution = @import("antfly_local_sources").common_request_admission.runnable_scheduler.Execution;

pub const FsStore = struct {
    alloc: Allocator,
    root_dir: []u8,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn init(alloc: Allocator, root_dir: []const u8) !FsStore {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        try fs_paths.createDirPathPortable(io_impl.io(), root_dir);
        return .{
            .alloc = alloc,
            .root_dir = try alloc.dupe(u8, root_dir),
        };
    }

    pub fn deinit(self: *FsStore) void {
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn walStore(self: *FsStore) wal_store.WalStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn append(self: *FsStore, namespace: []const u8, timestamp_ns: u64, payload: []const u8) !u64 {
        return self.appendControlled(namespace, timestamp_ns, payload, null);
    }

    pub fn appendWithExecution(self: *FsStore, namespace: []const u8, timestamp_ns: u64, payload: []const u8, execution: *Execution) !u64 {
        return self.appendControlled(namespace, timestamp_ns, payload, execution);
    }

    fn appendControlled(self: *FsStore, namespace: []const u8, timestamp_ns: u64, payload: []const u8, execution: ?*Execution) !u64 {
        if (execution) |active| try lockAtomicWithExecution(&self.mutex, active) else lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);

        const log_path = try logPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(log_path);
        const next_path = try nextLsnPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(next_path);
        try ensureParentDir(log_path);
        const current = try scanLog(log_path, null);
        const lsn = if (current) |info|
            try nextAfter(info.latest)
        else if (try readNextState(self.alloc, next_path)) |state|
            state.next_lsn
        else
            1;
        const encoded = try record_codec.encodeRecordAlloc(self.alloc, lsn, timestamp_ns, payload);
        defer self.alloc.free(encoded);
        const previous_bytes = if (current) |info| info.bytes else 0;
        var next = try PreparedNextState.init(self.alloc, next_path, try nextAfter(lsn), previous_bytes + encoded.len);
        defer next.deinit(self.alloc);
        try writeAppendedLogStreaming(self.alloc, log_path, previous_bytes, encoded);
        try next.write(next_path);
        return lsn;
    }

    pub fn appendIdempotent(
        self: *FsStore,
        namespace: []const u8,
        timestamp_ns: u64,
        payload: []const u8,
        operation_id: []const u8,
    ) !u64 {
        return (try self.appendIdempotentMaybeLatest(namespace, timestamp_ns, payload, operation_id, null)).?;
    }

    pub fn appendIdempotentIfLatest(
        self: *FsStore,
        namespace: []const u8,
        timestamp_ns: u64,
        payload: []const u8,
        operation_id: []const u8,
        expected_latest_lsn: u64,
    ) !?u64 {
        return try self.appendIdempotentMaybeLatest(
            namespace,
            timestamp_ns,
            payload,
            operation_id,
            expected_latest_lsn,
        );
    }

    fn appendIdempotentMaybeLatest(
        self: *FsStore,
        namespace: []const u8,
        timestamp_ns: u64,
        payload: []const u8,
        operation_id: []const u8,
        expected_latest_lsn: ?u64,
    ) !?u64 {
        if (operation_id.len == 0 or operation_id.len > std.math.maxInt(u16))
            return error.InvalidWalOperationId;
        if (payload.len > record_codec.idempotent_payload_max) return error.WalRecordTooLarge;

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);
        const log_path = try logPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(log_path);
        const next_path = try nextLsnPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(next_path);
        try ensureParentDir(log_path);
        const current = try scanLog(log_path, .{ .operation_id = operation_id, .timestamp_ns = timestamp_ns, .payload = payload });
        if (current) |info| if (info.existing_lsn) |existing| {
            try writeNextState(self.alloc, next_path, try nextAfter(info.latest), info.bytes);
            return existing;
        };
        const lsn = if (current) |info|
            try nextAfter(info.latest)
        else if (try readNextState(self.alloc, next_path)) |state|
            state.next_lsn
        else
            1;
        if (expected_latest_lsn) |expected| {
            if (lsn - 1 != expected) return null;
        }
        const encoded = try record_codec.encodeIdempotentRecordAlloc(self.alloc, lsn, timestamp_ns, payload, operation_id);
        defer self.alloc.free(encoded);
        const previous_bytes = if (current) |info| info.bytes else 0;
        var next = try PreparedNextState.init(self.alloc, next_path, try nextAfter(lsn), previous_bytes + encoded.len);
        defer next.deinit(self.alloc);
        try writeAppendedLogStreaming(self.alloc, log_path, previous_bytes, encoded);
        try next.write(next_path);
        return lsn;
    }

    pub fn readFromAlloc(self: *FsStore, alloc: Allocator, namespace: []const u8, start_lsn: u64) ![]wal_types.Record {
        const log_path = try logPathAlloc(alloc, self.root_dir, namespace);
        defer alloc.free(log_path);
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        const file = std.Io.Dir.cwd().openFile(io, log_path, .{}) catch |err| switch (err) {
            error.FileNotFound => return alloc.alloc(wal_types.Record, 0),
            else => return err,
        };
        defer file.close(io);
        var scanner: LogScanner = .{ .file = file, .io = io, .length = try file.length(io) };
        var out: std.ArrayList(wal_types.Record) = .empty;
        errdefer {
            for (out.items) |record| {
                alloc.free(record.payload);
                if (record.operation_id) |id| alloc.free(id);
            }
            out.deinit(alloc);
        }
        while (try scanner.next()) |record| {
            if (record.lsn < start_lsn) continue;
            const payload = try alloc.alloc(u8, record.payload_len);
            errdefer alloc.free(payload);
            try readExact(file, io, payload, record.payload_offset);
            const id = if (record.operation_id_len != 0) blk: {
                const owned = try alloc.alloc(u8, record.operation_id_len);
                errdefer alloc.free(owned);
                try readExact(file, io, owned, record.operation_id_offset);
                break :blk owned;
            } else null;
            errdefer if (id) |owned| alloc.free(owned);
            try out.append(alloc, .{ .lsn = record.lsn, .timestamp_ns = record.timestamp_ns, .payload = payload, .operation_id = id });
        }
        return out.toOwnedSlice(alloc);
    }

    pub fn latestLsn(self: *FsStore, namespace: []const u8) !u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);
        const log_path = try logPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(log_path);
        const next_path = try nextLsnPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(next_path);
        const next_state = try readNextState(self.alloc, next_path);
        if (!fileExists(log_path)) return if (next_state) |state| state.next_lsn -| 1 else 0;
        if (next_state) |state| {
            if (state.log_byte_len) |expected_len| {
                if (try fileSize(log_path) == expected_len) return state.next_lsn -| 1;
            }
        }
        const info = (try scanLog(log_path, null)) orelse return error.InvalidWal;
        const latest = info.latest;
        try writeNextState(self.alloc, next_path, try nextAfter(latest), info.bytes);
        return latest;
    }

    pub fn truncatePrefix(self: *FsStore, namespace: []const u8, keep_from_lsn: u64) !u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);

        const log_path = try logPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(log_path);
        const next_path = try nextLsnPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(next_path);
        if (!fileExists(log_path)) return 0;

        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        const source = try std.Io.Dir.cwd().openFile(io, log_path, .{});
        defer source.close(io);
        var scanner: LogScanner = .{ .file = source, .io = io, .length = try source.length(io) };
        const tmp_path = try temporaryPath(self.alloc, log_path);
        defer self.alloc.free(tmp_path);
        defer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
        var output = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
        var output_open = true;
        defer if (output_open) output.close(io);
        var removed: u64 = 0;
        var latest: u64 = 0;
        var output_bytes: u64 = 0;
        var scratch: [32 * 1024]u8 = undefined;
        while (try scanner.next()) |record| {
            latest = record.lsn;
            if (record.lsn < keep_from_lsn) {
                removed += 1;
                continue;
            }
            try copyRange(source, output, io, record.offset, output_bytes, record.next_offset - record.offset, &scratch);
            output_bytes += record.next_offset - record.offset;
        }
        const next_lsn = try nextAfter(latest);
        output.close(io);
        output_open = false;
        var next = try PreparedNextState.init(self.alloc, next_path, next_lsn, output_bytes);
        defer next.deinit(self.alloc);
        if (output_bytes == 0) {
            // Keep the durable next LSN before removing the last log record.
            try next.write(next_path);
            deleteFileIfExists(log_path);
        } else {
            try renameTemporary(io, tmp_path, log_path);
            try next.write(next_path);
        }
        return removed;
    }

    const vtable: wal_store.WalStore.VTable = .{
        .deinit = erasedDeinit,
        .append = erasedAppend,
        .append_with_execution = erasedAppendWithExecution,
        .append_idempotent = erasedAppendIdempotent,
        .append_idempotent_if_latest = erasedAppendIdempotentIfLatest,
        .read_from_alloc = erasedReadFromAlloc,
        .latest_lsn = erasedLatestLsn,
        .truncate_prefix = erasedTruncatePrefix,
    };

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedAppend(ptr: *anyopaque, namespace: []const u8, timestamp_ns: u64, payload: []const u8) !u64 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.append(namespace, timestamp_ns, payload);
    }

    fn erasedAppendWithExecution(ptr: *anyopaque, namespace: []const u8, timestamp_ns: u64, payload: []const u8, execution: *Execution) !u64 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return self.appendWithExecution(namespace, timestamp_ns, payload, execution);
    }

    fn erasedAppendIdempotent(ptr: *anyopaque, namespace: []const u8, timestamp_ns: u64, payload: []const u8, operation_id: []const u8) !u64 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.appendIdempotent(namespace, timestamp_ns, payload, operation_id);
    }

    fn erasedAppendIdempotentIfLatest(
        ptr: *anyopaque,
        namespace: []const u8,
        timestamp_ns: u64,
        payload: []const u8,
        operation_id: []const u8,
        expected_latest_lsn: u64,
    ) !?u64 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.appendIdempotentIfLatest(
            namespace,
            timestamp_ns,
            payload,
            operation_id,
            expected_latest_lsn,
        );
    }

    fn erasedReadFromAlloc(ptr: *anyopaque, alloc: Allocator, namespace: []const u8, start_lsn: u64) ![]wal_types.Record {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.readFromAlloc(alloc, namespace, start_lsn);
    }

    fn erasedLatestLsn(ptr: *anyopaque, namespace: []const u8) !u64 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.latestLsn(namespace);
    }

    fn erasedTruncatePrefix(ptr: *anyopaque, namespace: []const u8, keep_from_lsn: u64) !u64 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.truncatePrefix(namespace, keep_from_lsn);
    }
};

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn fileExists(path: []const u8) bool {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    _ = std.Io.Dir.cwd().statFile(io_impl.io(), path, .{}) catch return false;
    return true;
}

fn fileSize(path: []const u8) !u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return (try std.Io.Dir.cwd().statFile(io_impl.io(), path, .{})).size;
}

/// No serialization lease is held across checked CPU reentry. Waking only
/// grants permission to retry: CPU must be reacquired before claiming the mutex.
fn lockAtomicWithExecution(mutex: *std.atomic.Mutex, execution: *Execution) !void {
    const entry = execution.entry orelse return error.InvalidRunnableTransition;
    const io = entry.options.io;
    const class = try execution.retainedClass();
    while (true) {
        try execution.checkpoint();
        if (mutex.tryLock()) return;
        try execution.park(.resource_blocked);
        const waited = io.sleep(.fromMilliseconds(1), .awake);
        // A failed sleep still requires the original checked reentry. Failure
        // leaves the mutex unowned, so no result or lock can escape admission.
        try execution.enter(class);
        try waited;
    }
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

fn ensureParentDir(path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), parent);
}

fn deleteFileIfExists(path: []const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteFile(io_impl.io(), path) catch {};
}

const RecordLocation = struct {
    offset: u64,
    next_offset: u64,
    lsn: u64,
    timestamp_ns: u64,
    payload_offset: u64,
    payload_len: usize,
    operation_id_offset: u64 = 0,
    operation_id_len: usize = 0,
};

/// The on-disk format is unchanged. Scan only fixed headers and verify every
/// range against the pinned file length; payload size never drives scratch.
const LogScanner = struct {
    file: std.Io.File,
    io: std.Io,
    length: u64,
    offset: u64 = 0,
    buffered_offset: u64 = 0,
    buffered_len: usize = 0,
    buffer: [8192]u8 = undefined,
    fn read(self: *LogScanner, bytes: []u8, offset: u64) !void {
        if (offset > self.length or bytes.len > self.length - offset) return error.InvalidWal;
        if (offset < self.buffered_offset or offset - self.buffered_offset > self.buffered_len or
            bytes.len > self.buffered_len -| (offset - self.buffered_offset))
        {
            self.buffered_offset = offset;
            self.buffered_len = @intCast(@min(self.length - offset, self.buffer.len));
            try readExact(self.file, self.io, self.buffer[0..self.buffered_len], offset);
        }
        @memcpy(bytes, self.buffer[@intCast(offset - self.buffered_offset)..][0..bytes.len]);
    }
    fn next(self: *LogScanner) !?RecordLocation {
        if (self.offset == self.length) return null;
        if (self.offset > self.length or self.length - self.offset < 20) return error.InvalidWal;
        var header: [20]u8 = undefined;
        try self.read(&header, self.offset);
        const tag = std.mem.readInt(u32, header[16..20], .little);
        var record: RecordLocation = .{
            .offset = self.offset,
            .next_offset = 0,
            .lsn = std.mem.readInt(u64, header[0..8], .little),
            .timestamp_ns = std.mem.readInt(u64, header[8..16], .little),
            .payload_offset = self.offset + 20,
            .payload_len = @intCast(tag & ~record_codec.idempotent_record_flag),
        };
        if (tag & record_codec.idempotent_record_flag != 0) {
            if (self.length - record.payload_offset < 2) return error.InvalidWal;
            var id_size: [2]u8 = undefined;
            try self.read(&id_size, record.payload_offset);
            record.operation_id_len = std.mem.readInt(u16, &id_size, .little);
            record.operation_id_offset = record.payload_offset + 2;
            if (record.operation_id_len == 0 or record.operation_id_len > self.length - record.operation_id_offset) return error.InvalidWal;
            record.payload_offset = record.operation_id_offset + record.operation_id_len;
        }
        if (record.payload_len > self.length - record.payload_offset) return error.InvalidWal;
        record.next_offset = record.payload_offset + record.payload_len;
        self.offset = record.next_offset;
        return record;
    }
};

fn readExact(file: std.Io.File, io: std.Io, bytes: []u8, offset: u64) !void {
    if (try file.readPositionalAll(io, bytes, offset) != bytes.len) return error.InvalidWal;
}

const OperationMatch = struct { operation_id: []const u8, timestamp_ns: u64, payload: []const u8 };
const LogInfo = struct { latest: u64 = 0, bytes: u64, existing_lsn: ?u64 = null };
fn scanLog(path: []const u8, match: ?OperationMatch) !?LogInfo {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    const file = std.Io.Dir.cwd().openFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close(io);
    var scanner: LogScanner = .{ .file = file, .io = io, .length = try file.length(io) };
    var info: LogInfo = .{ .bytes = scanner.length };
    while (try scanner.next()) |record| {
        info.latest = record.lsn;
        if (match) |wanted| if (info.existing_lsn == null and record.operation_id_len == wanted.operation_id.len and
            try matchesRange(file, io, record.operation_id_offset, wanted.operation_id))
        {
            if (record.timestamp_ns != wanted.timestamp_ns or record.payload_len != wanted.payload.len or
                !try matchesRange(file, io, record.payload_offset, wanted.payload)) return error.WalIdempotencyConflict;
            info.existing_lsn = record.lsn;
        };
    }
    return info;
}

fn matchesRange(file: std.Io.File, io: std.Io, offset: u64, expected: []const u8) !bool {
    var scratch: [4096]u8 = undefined;
    var position: usize = 0;
    while (position < expected.len) {
        const count = @min(expected.len - position, scratch.len);
        try readExact(file, io, scratch[0..count], offset + position);
        if (!std.mem.eql(u8, scratch[0..count], expected[position..][0..count])) return false;
        position += count;
    }
    return true;
}

fn nextAfter(lsn: u64) !u64 {
    return std.math.add(u64, lsn, 1) catch error.InvalidWal;
}

fn temporaryPath(alloc: Allocator, path: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "{s}.tmp-{d}", .{ path, test_nonce.fetchAdd(1, .monotonic) });
}

fn copyRange(source: std.Io.File, output: std.Io.File, io: std.Io, start: u64, destination: u64, length: u64, scratch: []u8) !void {
    var done: u64 = 0;
    while (done < length) {
        const count: usize = @intCast(@min(length - done, scratch.len));
        try readExact(source, io, scratch[0..count], start + done);
        try output.writePositionalAll(io, scratch[0..count], destination + done);
        done += count;
    }
}

fn renameTemporary(io: std.Io, temporary: []const u8, path: []const u8) !void {
    try std.Io.Dir.rename(std.Io.Dir.cwd(), temporary, std.Io.Dir.cwd(), path, io);
}

fn writeAppendedLogStreaming(alloc: Allocator, path: []const u8, existing_bytes: u64, contents: []const u8) !void {
    const tmp_path = try temporaryPath(alloc, path);
    defer alloc.free(tmp_path);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    defer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
    var output = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
    var output_open = true;
    defer if (output_open) output.close(io);
    if (existing_bytes != 0) {
        const source = try std.Io.Dir.cwd().openFile(io, path, .{});
        defer source.close(io);
        var scratch: [32 * 1024]u8 = undefined;
        try copyRange(source, output, io, 0, 0, existing_bytes, &scratch);
    }
    try output.writePositionalAll(io, contents, existing_bytes);
    output.close(io);
    output_open = false;
    try renameTemporary(io, tmp_path, path);
}

fn writeFileAtomically(alloc: Allocator, path: []const u8, contents: []const u8) !void {
    const tmp_path = try temporaryPath(alloc, path);
    defer alloc.free(tmp_path);
    try writeFileAtomicallyPrepared(path, tmp_path, contents);
}

// The caller prepares all store-allocator memory before publishing the WAL.
fn writeFileAtomicallyPrepared(path: []const u8, tmp_path: []const u8, contents: []const u8) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    defer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    {
        var file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
        defer file.close(io);

        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(contents);
        try writer.end();
    }

    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.renameAbsolute(tmp_path, path, io) catch |err| {
            std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {};
            return err;
        };
    } else {
        std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
            std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {};
            return err;
        };
    }
}

fn logPathAlloc(alloc: Allocator, root_dir: []const u8, namespace: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, namespace, "wal.log" });
}

fn nextLsnPathAlloc(alloc: Allocator, root_dir: []const u8, namespace: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, namespace, "NEXT_LSN" });
}

fn openNamespaceLock(
    alloc: Allocator,
    root_dir: []const u8,
    namespace: []const u8,
    io: std.Io,
) !std.Io.File {
    const path = try std.fs.path.join(alloc, &.{ root_dir, namespace, ".wal.lock" });
    defer alloc.free(path);
    try ensureParentDir(path);
    return try fs_paths.createFilePortable(io, path, .{ .truncate = false, .read = true });
}

const NextState = struct {
    next_lsn: u64,
    log_byte_len: ?u64,
};

fn readNextState(_: Allocator, path: []const u8) !?NextState {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    const file = std.Io.Dir.cwd().openFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close(io);
    const length = try file.length(io);
    var scratch: [128]u8 = undefined;
    if (length > scratch.len) return error.InvalidWal;
    const raw = scratch[0..@intCast(length)];
    try readExact(file, io, raw, 0);
    var fields = std.mem.tokenizeAny(u8, raw, " \t\r\n");
    const next_lsn = try std.fmt.parseInt(u64, fields.next() orelse return error.InvalidWal, 10);
    const log_byte_len = if (fields.next()) |value| try std.fmt.parseInt(u64, value, 10) else null;
    if (fields.next() != null) return error.InvalidWal;
    return .{ .next_lsn = next_lsn, .log_byte_len = log_byte_len };
}

/// Sidecar memory is reserved before a log rename can make a mutation visible.
/// Two decimal u64 values and a space fit in the inline payload.
const PreparedNextState = struct {
    temporary_path: []u8,
    payload: [41]u8 = undefined,
    payload_len: usize,

    fn init(alloc: Allocator, path: []const u8, next_lsn: u64, log_byte_len: u64) !PreparedNextState {
        var prepared: PreparedNextState = .{ .temporary_path = try temporaryPath(alloc, path), .payload_len = 0 };
        errdefer alloc.free(prepared.temporary_path);
        const text = try std.fmt.bufPrint(&prepared.payload, "{d} {d}", .{ next_lsn, log_byte_len });
        prepared.payload_len = text.len;
        return prepared;
    }
    fn deinit(self: *PreparedNextState, alloc: Allocator) void {
        alloc.free(self.temporary_path);
        self.* = undefined;
    }
    fn write(self: *const PreparedNextState, path: []const u8) !void {
        try writeFileAtomicallyPrepared(path, self.temporary_path, self.payload[0..self.payload_len]);
    }
};

fn writeNextState(alloc: Allocator, path: []const u8, next_lsn: u64, log_byte_len: u64) !void {
    var prepared = try PreparedNextState.init(alloc, path, next_lsn, log_byte_len);
    defer prepared.deinit(alloc);
    try prepared.write(path);
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-wal-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "fs wal store append and readFromAlloc preserve order and lsn" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "append");
    defer cleanupTmp(path);

    var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store.deinit();

    try std.testing.expectEqual(@as(u64, 1), try store.append("docs", 100, "one"));
    try std.testing.expectEqual(@as(u64, 2), try store.append("docs", 200, "two"));

    const all = try store.readFromAlloc(std.testing.allocator, "docs", 1);
    defer wal_types.freeRecords(std.testing.allocator, all);
    try std.testing.expectEqual(@as(usize, 2), all.len);
    try std.testing.expectEqual(@as(u64, 1), all[0].lsn);
    try std.testing.expectEqual(@as(u64, 2), all[1].lsn);
    try std.testing.expectEqualStrings("one", all[0].payload);
    try std.testing.expectEqualStrings("two", all[1].payload);

    const tail = try store.readFromAlloc(std.testing.allocator, "docs", 2);
    defer wal_types.freeRecords(std.testing.allocator, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    try std.testing.expectEqual(@as(u64, 2), tail[0].lsn);
    try std.testing.expectEqualStrings("two", tail[0].payload);
}

test "fs wal store survives reopen" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "reopen");
    defer cleanupTmp(path);

    {
        var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
        defer store.deinit();
        _ = try store.append("docs", 300, "three");
        _ = try store.append("docs", 400, "four");
    }

    {
        var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
        defer store.deinit();
        try std.testing.expectEqual(@as(u64, 3), try store.append("docs", 500, "five"));
        const all = try store.readFromAlloc(std.testing.allocator, "docs", 1);
        defer wal_types.freeRecords(std.testing.allocator, all);
        try std.testing.expectEqual(@as(usize, 3), all.len);
        try std.testing.expectEqualStrings("five", all[2].payload);
    }
}

test "serverless fs wal store idempotent append survives reopen and truncation" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "idempotent-reopen");
    defer cleanupTmp(path);

    {
        var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
        defer store.deinit();
        try std.testing.expectEqual(@as(u64, 1), try store.append("docs", 100, "ordinary"));
        try std.testing.expectEqual(@as(u64, 2), try store.appendIdempotent("docs", 200, "derived", "enrich/1"));
    }
    {
        var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
        defer store.deinit();
        try std.testing.expectEqual(@as(u64, 2), try store.appendIdempotent("docs", 200, "derived", "enrich/1"));
        try std.testing.expectError(error.WalIdempotencyConflict, store.appendIdempotent("docs", 200, "different", "enrich/1"));
        try std.testing.expectEqual(@as(u64, 1), try store.truncatePrefix("docs", 2));
        const records = try store.readFromAlloc(std.testing.allocator, "docs", 1);
        defer wal_types.freeRecords(std.testing.allocator, records);
        try std.testing.expectEqual(@as(usize, 1), records.len);
        try std.testing.expectEqualStrings("enrich/1", records[0].operation_id.?);
        try std.testing.expectEqual(@as(u64, 2), try store.appendIdempotent("docs", 200, "derived", "enrich/1"));
    }
}

test "serverless fs WAL conditional idempotent append fences a changed latest LSN" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "conditional-idempotent");
    defer cleanupTmp(path);

    var impl = try FsStore.init(std.testing.allocator, std.mem.span(path));
    var store = impl.walStore();
    defer store.deinit();
    try std.testing.expectEqual(@as(u64, 1), try store.append("docs", 100, "user-one"));
    try std.testing.expectEqual(
        @as(?u64, null),
        try store.appendIdempotentIfLatest("docs", 101, "stale-derived", "enrich/1", 0),
    );
    try std.testing.expectEqual(
        @as(?u64, 2),
        try store.appendIdempotentIfLatest("docs", 101, "derived", "enrich/1", 1),
    );
    try std.testing.expectEqual(@as(u64, 3), try store.append("docs", 102, "user-two"));
    try std.testing.expectEqual(
        @as(?u64, 2),
        try store.appendIdempotentIfLatest("docs", 101, "derived", "enrich/1", 1),
    );
    try std.testing.expectEqual(
        @as(?u64, null),
        try store.appendIdempotentIfLatest("docs", 103, "stale-derived-two", "enrich/2", 2),
    );
    try std.testing.expectEqual(@as(u64, 3), try store.latestLsn("docs"));
}

test "serverless fs WAL conditional append is atomic across store instances" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "cross-instance-conditional");
    defer cleanupTmp(path);

    var store_a = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store_a.deinit();
    var store_b = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store_b.deinit();
    try std.testing.expectEqual(@as(u64, 1), try store_a.append("docs", 100, "seed"));

    const Race = struct {
        io: std.Io,
        stores: [2]*FsStore,
        ready_count: std.atomic.Value(u32) = .init(0),
        ready: std.Io.Event = .unset,
        start: std.Io.Event = .unset,
        winners: std.atomic.Value(u32) = .init(0),
        errors: std.atomic.Value(u32) = .init(0),

        fn run(self: *@This(), index: usize) void {
            if (self.ready_count.fetchAdd(1, .release) == 1) self.ready.set(self.io);
            self.start.waitUncancelable(self.io);
            var operation_buf: [128]u8 = undefined;
            const operation_id = @import("../enrichment/operation_id.zig").format(&operation_buf, 1, 1, index, 1) catch {
                _ = self.errors.fetchAdd(1, .monotonic);
                return;
            };
            const payload = if (index == 0) "derived-a" else "derived-b";
            const appended = self.stores[index].appendIdempotentIfLatest(
                "docs",
                101 + @as(u64, @intCast(index)),
                payload,
                operation_id,
                1,
            ) catch {
                _ = self.errors.fetchAdd(1, .monotonic);
                return;
            };
            if (appended != null) _ = self.winners.fetchAdd(1, .monotonic);
        }
    };

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    var race = Race{ .io = io, .stores = .{ &store_a, &store_b } };
    var group: std.Io.Group = .init;
    errdefer group.cancel(io);
    group.async(io, Race.run, .{ &race, 0 });
    group.async(io, Race.run, .{ &race, 1 });
    race.ready.waitUncancelable(io);
    race.start.set(io);
    try group.await(io);

    try std.testing.expectEqual(@as(u32, 0), race.errors.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), race.winners.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 2), try store_a.latestLsn("docs"));
    const records = try store_a.readFromAlloc(std.testing.allocator, "docs", 1);
    defer wal_types.freeRecords(std.testing.allocator, records);
    try std.testing.expectEqual(@as(usize, 2), records.len);
}

test "fs wal store erased interface works" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "erased");
    defer cleanupTmp(path);

    var fs = try FsStore.init(std.testing.allocator, std.mem.span(path));
    var runtime = fs.walStore();
    defer runtime.deinit();

    try std.testing.expectEqual(@as(u64, 1), try runtime.append("docs", 1000, "payload"));
    const rows = try runtime.readFromAlloc("docs", 1);
    defer wal_types.freeRecords(std.testing.allocator, rows);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("payload", rows[0].payload);
    try std.testing.expectEqual(@as(u64, 1), try runtime.latestLsn("docs"));
}

test "fs wal store truncates older records while preserving LSN continuity" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "truncate");
    defer cleanupTmp(path);

    var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store.deinit();

    _ = try store.append("docs", 100, "one");
    _ = try store.append("docs", 200, "two");
    _ = try store.append("docs", 300, "three");

    try std.testing.expectEqual(@as(u64, 2), try store.truncatePrefix("docs", 3));

    const tail = try store.readFromAlloc(std.testing.allocator, "docs", 1);
    defer wal_types.freeRecords(std.testing.allocator, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    try std.testing.expectEqual(@as(u64, 3), tail[0].lsn);
    try std.testing.expectEqualStrings("three", tail[0].payload);
    try std.testing.expectEqual(@as(u64, 3), try store.latestLsn("docs"));
    try std.testing.expectEqual(@as(u64, 4), try store.append("docs", 400, "four"));
}

test "fs wal allocation failure cannot reject a newly published append or truncation" {
    const Fixture = struct {
        const Mode = enum { ordinary, idempotent, conditional, truncate };
        fn run(alloc: Allocator, mode: Mode) !void {
            const a = std.testing.allocator;
            var path_buf: [256]u8 = undefined;
            const root = tmpPath(&path_buf, "publication-memory");
            defer cleanupTmp(root);
            {
                var seed = try FsStore.init(a, std.mem.span(root));
                defer seed.deinit();
                _ = try seed.append("docs", 1, "one");
                _ = try seed.append("docs", 2, "two");
            }
            // Injection covers only operation-owned memory, not seed/verification.
            const result: anyerror!u64 = operation: {
                var store = FsStore.init(alloc, std.mem.span(root)) catch |err| break :operation err;
                defer store.deinit();
                break :operation switch (mode) {
                    .ordinary => store.append("docs", 3, "accepted"),
                    .idempotent => store.appendIdempotent("docs", 3, "accepted", "operation"),
                    .conditional => (store.appendIdempotentIfLatest("docs", 3, "accepted", "operation", 2) catch |err| break :operation err).?,
                    .truncate => store.truncatePrefix("docs", 2),
                };
            };
            var observer = try FsStore.init(a, std.mem.span(root));
            defer observer.deinit();
            const records = try observer.readFromAlloc(a, "docs", 1);
            defer wal_types.freeRecords(a, records);
            if (result) |value| {
                try std.testing.expectEqual(@as(u64, if (mode == .truncate) 1 else 3), value);
                try std.testing.expectEqual(@as(usize, if (mode == .truncate) 1 else 3), records.len);
                try std.testing.expectEqualStrings(if (mode == .truncate) "two" else "accepted", records[records.len - 1].payload);
                try std.testing.expectEqual(@as(u64, if (mode == .truncate) 2 else 3), try observer.latestLsn("docs"));
            } else |err| {
                try std.testing.expectEqual(error.OutOfMemory, err);
                try std.testing.expectEqual(@as(usize, 2), records.len);
                try std.testing.expectEqualStrings("one", records[0].payload);
                try std.testing.expectEqualStrings("two", records[1].payload);
                try std.testing.expectEqual(@as(u64, 2), try observer.latestLsn("docs"));
                return err;
            }
        }
    };
    inline for (std.meta.tags(Fixture.Mode)) |mode| {
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{mode});
    }
}

test "fs wal temporary paths respect caller memory before replacement" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const root = tmpPath(&path_buf, "temporary-memory");
    defer cleanupTmp(root);
    var store = try FsStore.init(a, std.mem.span(root));
    defer store.deinit();
    try std.testing.expectEqual(@as(u64, 1), try store.append("docs", 1, "accepted"));
    const log_path = try logPathAlloc(a, std.mem.span(root), "docs");
    defer a.free(log_path);
    const next_path = try nextLsnPathAlloc(a, std.mem.span(root), "docs");
    defer a.free(next_path);
    var budget = @import("antfly_local_sources").storage_test_allocator.BoundedAllocator.init(a, 0);
    defer std.debug.assert(budget.deinit() == 0);
    try std.testing.expectError(error.OutOfMemory, writeAppendedLogStreaming(budget.allocator(), log_path, 0, "replacement"));
    try std.testing.expectError(error.OutOfMemory, writeFileAtomically(budget.allocator(), next_path, "99"));
    try std.testing.expectEqual(@as(u64, 1), try store.latestLsn("docs"));
    const records = try store.readFromAlloc(a, "docs", 1);
    defer wal_types.freeRecords(a, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqualStrings("accepted", records[0].payload);
}

test "fs wal streaming operations keep heap independent of existing log size" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const root = tmpPath(&path_buf, "streaming-memory");
    defer cleanupTmp(root);
    const large = try a.alloc(u8, 1024 * 1024);
    defer a.free(large);
    for (large, 0..) |*byte, i| byte.* = @intCast(i % 251);
    {
        var seed = try FsStore.init(a, std.mem.span(root));
        defer seed.deinit();
        try std.testing.expectEqual(@as(u64, 1), try seed.append("docs", 1, large));
        try std.testing.expectEqual(@as(u64, 2), try seed.appendIdempotent("docs", 2, large, "large-operation"));
    }
    var budget = @import("antfly_local_sources").storage_test_allocator.BoundedAllocator.init(a, 16 * 1024);
    defer std.debug.assert(budget.deinit() == 0);
    {
        var store = try FsStore.init(budget.allocator(), std.mem.span(root));
        defer store.deinit();
        const baseline = budget.live;
        // Duplicate comparison streams even a payload exceeding this budget.
        try std.testing.expectEqual(@as(u64, 2), try store.appendIdempotent("docs", 2, large, "large-operation"));
        large[0] ^= 1;
        try std.testing.expectError(error.WalIdempotencyConflict, store.appendIdempotent("docs", 2, large, "large-operation"));
        large[0] ^= 1;
        try std.testing.expectEqual(@as(u64, 3), try store.append("docs", 3, "tail"));
        const next_path = try nextLsnPathAlloc(a, std.mem.span(root), "docs");
        defer a.free(next_path);
        // Force legacy/stale sidecar recovery through the complete log scanner.
        try writeFileAtomically(a, next_path, "1");
        try std.testing.expectEqual(@as(u64, 3), try store.latestLsn("docs"));
        const tail = try store.readFromAlloc(budget.allocator(), "docs", 3);
        try std.testing.expectEqual(@as(usize, 1), tail.len);
        try std.testing.expectEqualStrings("tail", tail[0].payload);
        wal_types.freeRecords(budget.allocator(), tail);
        // Returned payloads obey the caller budget and failure leaks no prefix.
        try std.testing.expectError(error.OutOfMemory, store.readFromAlloc(budget.allocator(), "docs", 1));
        try std.testing.expectEqual(baseline, budget.live);
        try std.testing.expectEqual(@as(u64, 1), try store.truncatePrefix("docs", 2));
        const retained = try store.readFromAlloc(a, "docs", 1);
        defer wal_types.freeRecords(a, retained);
        try std.testing.expectEqual(@as(usize, 2), retained.len);
        try std.testing.expectEqualStrings("large-operation", retained[0].operation_id.?);
        try std.testing.expectEqualSlices(u8, large, retained[0].payload);
        try std.testing.expectEqualStrings("tail", retained[1].payload);
        try std.testing.expectEqual(@as(u64, 2), try store.truncatePrefix("docs", 4));
        try std.testing.expectEqual(@as(u64, 3), try store.latestLsn("docs"));
        try std.testing.expectEqual(@as(u64, 4), try store.append("docs", 4, "after-truncate"));
        try std.testing.expectError(error.OutOfMemory, store.append("docs", 5, large));
        try std.testing.expectEqual(@as(u64, 4), try store.latestLsn("docs"));
        try std.testing.expectEqual(baseline, budget.live);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

test "fs wal streaming scanner rejects malformed records without replacing the log" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const root = tmpPath(&path_buf, "streaming-corruption");
    defer cleanupTmp(root);
    var store = try FsStore.init(a, std.mem.span(root));
    defer store.deinit();
    const normal = try record_codec.encodeRecordAlloc(a, 1, 1, "abc");
    defer a.free(normal);
    var id_header: [22]u8 = @splat(0);
    std.mem.writeInt(u32, id_header[16..20], record_codec.idempotent_record_flag, .little);
    var missing_id = id_header;
    std.mem.writeInt(u16, missing_id[20..22], 7, .little);
    const invalid = [_][]const u8{ normal[0..19], normal[0 .. normal.len - 1], id_header[0..20], &id_header, &missing_id };
    const log_path = try logPathAlloc(a, std.mem.span(root), "docs");
    defer a.free(log_path);
    try ensureParentDir(log_path);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    for (invalid) |raw| {
        try std.testing.expectError(error.InvalidWal, record_codec.lastLsn(raw));
        try writeFileAtomically(a, log_path, raw);
        try std.testing.expectError(error.InvalidWal, store.append("docs", 2, "new"));
        const unchanged = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), log_path, a, .limited(128));
        defer a.free(unchanged);
        try std.testing.expectEqualSlices(u8, raw, unchanged);
    }
}
