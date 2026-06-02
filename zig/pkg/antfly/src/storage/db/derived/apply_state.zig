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
const Allocator = std.mem.Allocator;
const backend_erased = @import("../../backend_erased.zig");
const docstore_mod = @import("../../docstore.zig");
const lsm_backend = @import("../../lsm_backend.zig");
const mem_backend = @import("../../mem_backend.zig");
const fs_paths = @import("../../../common/fs_paths.zig");
const platform_time = @import("../../../platform/time.zig");

const metadata_prefix = "\x00\x00__metadata__:derived_apply:";
const checkpoint_file_name = "derived_apply.checkpoint";
const checkpoint_magic = "AFAPSEQ1";
const checkpoint_max_bytes: usize = 16 * 1024 * 1024;

const checkpoint_lock_alloc = std.heap.page_allocator;
var checkpoint_lock_registry_mutex: std.atomic.Mutex = .unlocked;
var checkpoint_locks: std.StringHashMapUnmanaged(*CheckpointFileLock) = .empty;

fn lockAtomicMutex(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.Thread.yield() catch {};
}

const CheckpointFileLock = struct {
    path: []u8,
    mutex: std.atomic.Mutex = .unlocked,
    refs: usize = 0,
};

const CheckpointWriteGuard = struct {
    lock: *CheckpointFileLock,

    fn release(self: *@This()) void {
        self.lock.mutex.unlock();
        releaseCheckpointFileLock(self.lock);
    }
};

fn acquireCheckpointFileLock(path: []const u8) !CheckpointWriteGuard {
    const lock = try retainCheckpointFileLock(path);
    lockAtomicMutex(&lock.mutex);
    return .{ .lock = lock };
}

fn retainCheckpointFileLock(path: []const u8) !*CheckpointFileLock {
    lockAtomicMutex(&checkpoint_lock_registry_mutex);
    defer checkpoint_lock_registry_mutex.unlock();

    const gop = try checkpoint_locks.getOrPut(checkpoint_lock_alloc, path);
    if (!gop.found_existing) {
        errdefer _ = checkpoint_locks.remove(path);

        const owned_path = try checkpoint_lock_alloc.dupe(u8, path);
        errdefer checkpoint_lock_alloc.free(owned_path);

        const lock = try checkpoint_lock_alloc.create(CheckpointFileLock);
        lock.* = .{
            .path = owned_path,
        };
        gop.key_ptr.* = owned_path;
        gop.value_ptr.* = lock;
    }

    const lock = gop.value_ptr.*;
    lock.refs += 1;
    return lock;
}

fn releaseCheckpointFileLock(lock: *CheckpointFileLock) void {
    lockAtomicMutex(&checkpoint_lock_registry_mutex);
    defer checkpoint_lock_registry_mutex.unlock();

    std.debug.assert(lock.refs > 0);
    lock.refs -= 1;
    if (lock.refs != 0) return;

    const removed = checkpoint_locks.fetchRemove(lock.path) orelse {
        std.debug.panic("missing derived apply checkpoint lock for {s}", .{lock.path});
    };
    std.debug.assert(removed.value == lock);
    checkpoint_lock_alloc.free(lock.path);
    checkpoint_lock_alloc.destroy(lock);
}

pub const AppliedSequenceUpdate = struct {
    index_name: []const u8,
    sequence: u64,
};

pub fn checkpointPathAlloc(alloc: Allocator, db_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ db_path, checkpoint_file_name });
}

pub fn loadAppliedSequence(alloc: Allocator, store: anytype, index_name: []const u8) !u64 {
    const key = try std.fmt.allocPrint(alloc, "{s}{s}", .{ metadata_prefix, index_name });
    defer alloc.free(key);

    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const borrowed = txn.get(key) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    const raw = try alloc.dupe(u8, borrowed);
    defer alloc.free(raw);

    if (raw.len != 8) return error.InvalidDerivedApplyState;
    return std.mem.readInt(u64, raw[0..8], .little);
}

pub fn loadAppliedSequenceWithCheckpoint(
    alloc: Allocator,
    store: anytype,
    checkpoint_path: ?[]const u8,
    index_name: []const u8,
) !u64 {
    if (comptime builtin.os.tag == .freestanding) return try loadAppliedSequence(alloc, store, index_name);
    const path = checkpoint_path orelse return try loadAppliedSequence(alloc, store, index_name);
    const checkpoint = loadCheckpointSequence(alloc, path, index_name) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    return checkpoint orelse 0;
}

pub fn saveAppliedSequence(store: anytype, index_name: []const u8, sequence: u64) !void {
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try saveAppliedSequenceTxn(&txn, index_name, sequence);
    try txn.commit();
}

pub fn saveAppliedSequenceTxn(txn: anytype, index_name: []const u8, sequence: u64) !void {
    var mutable_txn = txn;
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, sequence, .little);

    var key_buf: [256]u8 = undefined;
    const key = try std.fmt.bufPrint(&key_buf, "{s}{s}", .{ metadata_prefix, index_name });
    try mutable_txn.put(key, &buf);
}

pub fn saveAppliedSequences(store: anytype, updates: []const AppliedSequenceUpdate) !void {
    if (updates.len == 0) return;

    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    for (updates) |update| {
        try saveAppliedSequenceTxn(&txn, update.index_name, update.sequence);
    }
    try txn.commit();
}

pub fn saveAppliedSequenceWithCheckpoint(
    alloc: Allocator,
    store: anytype,
    checkpoint_path: ?[]const u8,
    index_name: []const u8,
    sequence: u64,
) !void {
    if (comptime builtin.os.tag == .freestanding) return try saveAppliedSequence(store, index_name, sequence);
    if (checkpoint_path) |path| {
        try setAppliedSequencesCheckpoint(alloc, path, &[_]AppliedSequenceUpdate{.{
            .index_name = index_name,
            .sequence = sequence,
        }});
        return;
    }
    try saveAppliedSequence(store, index_name, sequence);
}

pub fn saveAppliedSequencesWithCheckpoint(
    alloc: Allocator,
    store: anytype,
    checkpoint_path: ?[]const u8,
    updates: []const AppliedSequenceUpdate,
) !void {
    if (updates.len == 0) return;
    if (comptime builtin.os.tag == .freestanding) return try saveAppliedSequences(store, updates);
    if (checkpoint_path) |path| {
        try saveAppliedSequencesCheckpoint(alloc, path, updates);
        return;
    }
    try saveAppliedSequences(store, updates);
}

pub fn clearAppliedSequence(store: anytype, index_name: []const u8) !void {
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try clearAppliedSequenceTxn(&txn, index_name);
    try txn.commit();
}

pub fn clearAppliedSequenceWithCheckpoint(
    alloc: Allocator,
    store: anytype,
    checkpoint_path: ?[]const u8,
    index_name: []const u8,
) !void {
    if (comptime builtin.os.tag == .freestanding) return try clearAppliedSequence(store, index_name);
    if (checkpoint_path) |path| {
        try clearAppliedSequenceCheckpoint(alloc, path, index_name);
        return;
    }
    try clearAppliedSequence(store, index_name);
}

pub fn clearAppliedSequenceTxn(txn: anytype, index_name: []const u8) !void {
    var mutable_txn = txn;
    var key_buf: [256]u8 = undefined;
    const key = try std.fmt.bufPrint(&key_buf, "{s}{s}", .{ metadata_prefix, index_name });
    mutable_txn.delete(key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = false };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

const CheckpointMap = struct {
    map: std.StringHashMapUnmanaged(u64) = .empty,

    fn deinit(self: *@This(), alloc: Allocator) void {
        var it = self.map.iterator();
        while (it.next()) |entry| alloc.free(@constCast(entry.key_ptr.*));
        self.map.deinit(alloc);
        self.* = .{};
    }

    fn putMax(self: *@This(), alloc: Allocator, name: []const u8, sequence: u64) !void {
        const gop = try self.map.getOrPut(alloc, name);
        if (gop.found_existing) {
            gop.value_ptr.* = @max(gop.value_ptr.*, sequence);
            return;
        }
        errdefer _ = self.map.remove(name);
        gop.key_ptr.* = try alloc.dupe(u8, name);
        gop.value_ptr.* = sequence;
    }

    fn put(self: *@This(), alloc: Allocator, name: []const u8, sequence: u64) !void {
        const gop = try self.map.getOrPut(alloc, name);
        if (gop.found_existing) {
            gop.value_ptr.* = sequence;
            return;
        }
        errdefer _ = self.map.remove(name);
        gop.key_ptr.* = try alloc.dupe(u8, name);
        gop.value_ptr.* = sequence;
    }

    fn remove(self: *@This(), alloc: Allocator, name: []const u8) void {
        const removed = self.map.fetchRemove(name) orelse return;
        alloc.free(@constCast(removed.key));
    }
};

fn loadCheckpointSequence(alloc: Allocator, path: []const u8, index_name: []const u8) !?u64 {
    var checkpoint = try loadCheckpoint(alloc, path);
    defer checkpoint.deinit(alloc);
    return checkpoint.map.get(index_name);
}

fn saveAppliedSequencesCheckpoint(alloc: Allocator, path: []const u8, updates: []const AppliedSequenceUpdate) !void {
    var guard = try acquireCheckpointFileLock(path);
    defer guard.release();

    var checkpoint = loadCheckpoint(alloc, path) catch |err| switch (err) {
        error.FileNotFound => CheckpointMap{},
        else => return err,
    };
    defer checkpoint.deinit(alloc);

    for (updates) |update| {
        try checkpoint.putMax(alloc, update.index_name, update.sequence);
    }
    try writeCheckpointAtomically(alloc, path, &checkpoint);
}

fn setAppliedSequencesCheckpoint(alloc: Allocator, path: []const u8, updates: []const AppliedSequenceUpdate) !void {
    var guard = try acquireCheckpointFileLock(path);
    defer guard.release();

    var checkpoint = loadCheckpoint(alloc, path) catch |err| switch (err) {
        error.FileNotFound => CheckpointMap{},
        else => return err,
    };
    defer checkpoint.deinit(alloc);

    for (updates) |update| {
        try checkpoint.put(alloc, update.index_name, update.sequence);
    }
    try writeCheckpointAtomically(alloc, path, &checkpoint);
}

fn clearAppliedSequenceCheckpoint(alloc: Allocator, path: []const u8, index_name: []const u8) !void {
    var guard = try acquireCheckpointFileLock(path);
    defer guard.release();

    var checkpoint = loadCheckpoint(alloc, path) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer checkpoint.deinit(alloc);

    checkpoint.remove(alloc, index_name);
    try writeCheckpointAtomically(alloc, path, &checkpoint);
}

fn loadCheckpoint(alloc: Allocator, path: []const u8) !CheckpointMap {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const raw = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(checkpoint_max_bytes));
    defer alloc.free(raw);
    return try decodeCheckpoint(alloc, raw);
}

fn decodeCheckpoint(alloc: Allocator, raw: []const u8) !CheckpointMap {
    if (raw.len < checkpoint_magic.len + 4) return error.InvalidDerivedApplyState;
    if (!std.mem.eql(u8, raw[0..checkpoint_magic.len], checkpoint_magic)) return error.InvalidDerivedApplyState;
    var pos: usize = checkpoint_magic.len;
    const count = try readCheckpointInt(raw, &pos, u32);

    var checkpoint = CheckpointMap{};
    errdefer checkpoint.deinit(alloc);
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const name_len = try readCheckpointInt(raw, &pos, u32);
        const sequence = try readCheckpointInt(raw, &pos, u64);
        if (name_len == 0 or name_len > std.math.maxInt(u16)) return error.InvalidDerivedApplyState;
        if (pos + name_len > raw.len) return error.InvalidDerivedApplyState;
        const name = raw[pos .. pos + name_len];
        pos += name_len;
        try checkpoint.putMax(alloc, name, sequence);
    }
    if (pos != raw.len) return error.InvalidDerivedApplyState;
    return checkpoint;
}

fn readCheckpointInt(raw: []const u8, pos: *usize, comptime T: type) !T {
    const size = @sizeOf(T);
    if (pos.* + size > raw.len) return error.InvalidDerivedApplyState;
    const out = std.mem.readInt(T, raw[pos.* .. pos.* + size][0..size], .little);
    pos.* += size;
    return out;
}

fn encodeCheckpoint(alloc: Allocator, checkpoint: *const CheckpointMap) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, checkpoint_magic);
    try appendCheckpointInt(alloc, &out, u32, @intCast(checkpoint.map.count()));
    var it = checkpoint.map.iterator();
    while (it.next()) |entry| {
        if (entry.key_ptr.*.len == 0 or entry.key_ptr.*.len > std.math.maxInt(u16)) return error.InvalidDerivedApplyState;
        try appendCheckpointInt(alloc, &out, u32, @intCast(entry.key_ptr.*.len));
        try appendCheckpointInt(alloc, &out, u64, entry.value_ptr.*);
        try out.appendSlice(alloc, entry.key_ptr.*);
    }
    return try out.toOwnedSlice(alloc);
}

fn appendCheckpointInt(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime T: type,
    value: T,
) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn writeCheckpointAtomically(alloc: Allocator, path: []const u8, checkpoint: *const CheckpointMap) !void {
    const encoded = try encodeCheckpoint(alloc, checkpoint);
    defer alloc.free(encoded);

    if (std.fs.path.dirname(path)) |parent| {
        var io_parent = std.Io.Threaded.init(alloc, .{});
        defer io_parent.deinit();
        try fs_paths.createDirPathPortable(io_parent.io(), parent);
    }

    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{d}", .{ path, platform_time.monotonicNs() });
    defer alloc.free(tmp_path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(encoded);
        try writer.end();
        try file.sync(io);
    }
    std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
        return err;
    };
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse ".");
}

test "derived apply state works with memory backend store" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
    try saveAppliedSequence(runtime, "idx", 27);
    try std.testing.expectEqual(@as(u64, 27), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
}

test "derived apply state works with lsm backend store" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
    try saveAppliedSequence(runtime, "idx", 31);
    try std.testing.expectEqual(@as(u64, 31), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
}

test "derived apply state lsm point load does not clone mutable snapshot" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 1024 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveAppliedSequence(runtime, "idx", 41);
    const before = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 41), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
    const after = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(before.mutable_snapshot_clone_calls, after.mutable_snapshot_clone_calls);
}

test "derived apply state keeps latest lsm value across many flushed overwrites" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});

    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{
            .flush_threshold = 1,
            .compact_threshold_runs = 4,
            .level_target_runs_base = 2,
            .level_target_runs_multiplier = 2,
            .level_target_bytes_base = 0,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        try saveAppliedSequence(runtime, "idx", 0);
        var sequence: u64 = 1;
        while (sequence <= 1024) : (sequence += 1) {
            try saveAppliedSequence(runtime, "idx", sequence);
        }
        try std.testing.expectEqual(@as(u64, 1024), try loadAppliedSequence(alloc, runtime, "idx"));
    }

    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{
            .flush_threshold = 1,
            .compact_threshold_runs = 4,
            .level_target_runs_base = 2,
            .level_target_runs_multiplier = 2,
            .level_target_bytes_base = 0,
        });
        defer backend.close();

        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();

        try std.testing.expectEqual(@as(u64, 1024), try loadAppliedSequence(alloc, runtime, "idx"));
    }
}

test "derived apply state clear removes persisted sequence" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveAppliedSequence(runtime, "idx", 41);
    try clearAppliedSequence(runtime, "idx");
    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "idx"));
}

test "derived apply checkpoint is authoritative when configured" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const db_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const checkpoint_path = try checkpointPathAlloc(alloc, db_path);
    defer alloc.free(checkpoint_path);

    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    try saveAppliedSequence(runtime, "legacy_idx", 7);
    try std.testing.expectEqual(
        @as(u64, 0),
        try loadAppliedSequenceWithCheckpoint(alloc, runtime, checkpoint_path, "legacy_idx"),
    );
    try std.testing.expectEqual(
        @as(u64, 7),
        try loadAppliedSequenceWithCheckpoint(alloc, runtime, null, "legacy_idx"),
    );

    try saveAppliedSequencesWithCheckpoint(alloc, runtime, checkpoint_path, &[_]AppliedSequenceUpdate{
        .{ .index_name = "dense_idx", .sequence = 10 },
        .{ .index_name = "sparse_idx", .sequence = 3 },
    });
    try saveAppliedSequencesWithCheckpoint(alloc, runtime, checkpoint_path, &[_]AppliedSequenceUpdate{
        .{ .index_name = "dense_idx", .sequence = 12 },
        .{ .index_name = "sparse_idx", .sequence = 2 },
    });

    try std.testing.expectEqual(
        @as(u64, 12),
        try loadAppliedSequenceWithCheckpoint(alloc, runtime, checkpoint_path, "dense_idx"),
    );
    try std.testing.expectEqual(
        @as(u64, 3),
        try loadAppliedSequenceWithCheckpoint(alloc, runtime, checkpoint_path, "sparse_idx"),
    );
    try std.testing.expectEqual(
        @as(u64, 0),
        try loadAppliedSequenceWithCheckpoint(alloc, runtime, checkpoint_path, "legacy_idx"),
    );
}

test "derived apply checkpoint clear removes sidecar entry" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const db_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const checkpoint_path = try checkpointPathAlloc(alloc, db_path);
    defer alloc.free(checkpoint_path);

    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    try saveAppliedSequenceWithCheckpoint(alloc, runtime, checkpoint_path, "dense_idx", 22);
    try std.testing.expectEqual(
        @as(u64, 22),
        try loadAppliedSequenceWithCheckpoint(alloc, runtime, checkpoint_path, "dense_idx"),
    );
    try clearAppliedSequenceWithCheckpoint(alloc, runtime, checkpoint_path, "dense_idx");
    try std.testing.expectEqual(
        @as(u64, 0),
        try loadAppliedSequenceWithCheckpoint(alloc, runtime, checkpoint_path, "dense_idx"),
    );
}

test "derived apply checkpoint write locks are scoped per checkpoint path" {
    var guard_a = try acquireCheckpointFileLock(".zig-cache/tmp/derived-apply-a.checkpoint");
    defer guard_a.release();

    var guard_b = try acquireCheckpointFileLock(".zig-cache/tmp/derived-apply-b.checkpoint");
    defer guard_b.release();

    try std.testing.expect(guard_a.lock != guard_b.lock);
}

test "derived apply checkpoint serializes concurrent sidecar writers" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const db_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const checkpoint_path = try checkpointPathAlloc(alloc, db_path);
    defer alloc.free(checkpoint_path);

    const worker_count = 6;
    const Barrier = struct {
        mutex: std.atomic.Mutex = .unlocked,
        waiting: usize = 0,
        open: bool = false,

        fn wait(self: *@This(), total: usize) void {
            var registered = false;
            while (true) {
                lockAtomicMutex(&self.mutex);
                if (!registered) {
                    self.waiting += 1;
                    registered = true;
                    if (self.waiting == total) self.open = true;
                }
                const ready = self.open;
                self.mutex.unlock();
                if (ready) return;
                std.Thread.yield() catch {};
            }
        }
    };

    const Worker = struct {
        alloc: Allocator,
        path: []const u8,
        name: []const u8,
        sequence: u64,
        barrier: *Barrier,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.barrier.wait(worker_count);
            saveAppliedSequencesCheckpoint(self.alloc, self.path, &[_]AppliedSequenceUpdate{.{
                .index_name = self.name,
                .sequence = self.sequence,
            }}) catch |err| {
                self.err = err;
            };
        }
    };

    var barrier = Barrier{};
    var workers: [worker_count]Worker = undefined;
    var threads: [worker_count]std.Thread = undefined;
    const names = [_][]const u8{ "idx0", "idx1", "idx2", "idx3", "idx4", "idx5" };
    for (&workers, 0..) |*worker, i| {
        worker.* = .{
            .alloc = alloc,
            .path = checkpoint_path,
            .name = names[i],
            .sequence = @intCast(i + 1),
            .barrier = &barrier,
        };
        threads[i] = try std.Thread.spawn(.{}, Worker.run, .{worker});
    }
    for (&threads) |thread| thread.join();
    for (&workers) |*worker| {
        if (worker.err) |err| return err;
    }

    for (names, 0..) |name, i| {
        try std.testing.expectEqual(@as(u64, @intCast(i + 1)), try loadCheckpointSequence(alloc, checkpoint_path, name));
    }
}
