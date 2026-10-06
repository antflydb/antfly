// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations.

const std = @import("std");
const Backend = @import("../lsm_backend.zig").Backend;
const storage_io = @import("storage_io.zig");

fn write(backend: *Backend, value: []const u8) !void {
    {
        var txn = try backend.beginWrite();
        errdefer txn.abort();
        try txn.put(.{}, "row", value);
        try txn.commit();
    }
    try backend.sync(true);
}

test "lsm physical usage tolerates obsolete deletion but preserves I/O errors" {
    const Mode = enum { reclaim, missing_active, denied_active, denied_obsolete };
    const Hook = struct {
        var backend: *Backend = undefined;
        var mode: Mode = .reclaim;
        const old = "/usage/obsolete.tbl";
        const later = "/usage/later.tbl";

        fn size(ptr: *anyopaque, path: []const u8) !u64 {
            const memory: *storage_io.MemoryStorage = @ptrCast(@alignCast(ptr));
            // Real filesystem work must never run under the writer mutex.
            try std.testing.expect(backend.mu.tryLock());
            backend.mu.unlock();
            try std.testing.expect(backend.active_readers != 0);
            if (std.mem.eql(u8, path, old)) {
                if (mode == .denied_obsolete) return error.AccessDenied;
                // Force the exact capture -> unlink -> stat ordering, without
                // timing or background threads. Mutate the live ledger too;
                // measurement must continue over its retained immutable root.
                try memory.storage().deleteFileAbsolute(old);
                try memory.storage().writeFileAbsolute(later, "not in captured inventory");
                try std.testing.expect(backend.mu.tryLock());
                defer backend.mu.unlock();
                try backend.obsolete_paths.ensureUnusedCapacity(backend.allocator, 1);
                backend.obsolete_paths.removePrepared(old);
                try backend.queueObsoleteFilePath(try backend.allocator.dupe(u8, later));
            } else switch (mode) {
                .missing_active => return error.FileNotFound,
                .denied_active => return error.AccessDenied,
                else => {},
            }
            return memory.storage().fileSize(path);
        }
    };
    const alloc = std.testing.allocator;
    inline for (std.meta.tags(Mode)) |mode| {
        var memory = storage_io.MemoryStorage.init(alloc);
        defer memory.deinit();
        var backend = try Backend.open(alloc, "/usage", .{ .storage = memory.storage(), .wal_enabled = false, .flush_threshold = 1 });
        defer backend.close();
        try write(&backend, "original");
        try std.testing.expectEqual(@as(usize, 1), backend.runs.count());
        const active_size = try memory.storage().fileSize(backend.runs.at(0).path.?);
        try memory.storage().writeFileAbsolute(Hook.old, "obsolete");
        try backend.queueObsoleteFilePath(try alloc.dupe(u8, Hook.old));
        const before = backend.snapshotMaintenanceStats();
        var vtable = memory.storage().vtable.*;
        vtable.file_size = Hook.size;
        backend.storage = .{ .ptr = &memory, .vtable = &vtable };
        defer backend.storage = memory.storage();
        Hook.backend = &backend;
        Hook.mode = mode;
        switch (mode) {
            .reclaim => {
                const result = try backend.measurePhysicalUsage();
                try std.testing.expectEqual(active_size, result.active_sst_bytes);
                try std.testing.expectEqual(@as(u64, 0), result.obsolete_file_bytes);
                try std.testing.expectEqual(@as(u64, 1), result.missing_obsolete_files);
                try std.testing.expectEqual(active_size, try result.totalBytes());
            },
            .missing_active => try std.testing.expectError(error.FileNotFound, backend.measurePhysicalUsage()),
            .denied_active, .denied_obsolete => try std.testing.expectError(error.AccessDenied, backend.measurePhysicalUsage()),
        }
        const after = backend.snapshotMaintenanceStats();
        try std.testing.expectEqual(before.active_readers, after.active_readers);
        try std.testing.expectEqual(before.mutable_snapshot_clone_calls, after.mutable_snapshot_clone_calls);
        backend.drainRetiredLedgers();
        try std.testing.expect(backend.ledger_snapshots == null);
    }
}

test "lsm physical usage pins active files across concurrent publication" {
    const Hook = struct {
        var backend: *Backend = undefined;
        var armed = true;
        var pinned = false;
        fn size(ptr: *anyopaque, path: []const u8) !u64 {
            const memory: *storage_io.MemoryStorage = @ptrCast(@alignCast(ptr));
            if (armed) {
                armed = false;
                try write(backend, "replacement with a different size");
                {
                    const runtime = @import("runtime.zig");
                    const locked = runtime.lockBackend(Backend, backend);
                    defer runtime.unlockBackend(Backend, backend, locked);
                    try @import("compaction.zig").compactAllRuns(Backend, backend);
                }
                try std.testing.expect(backend.obsolete_paths.contains(path));
                for (0..32) |_| {
                    backend.obsolete_reclaim_retry_at_ns = 0;
                    _ = try backend.runMaintenanceStep();
                }
                // The old run remains readable even after it has been retired
                // by publication and cleanup has had a chance to reclaim it.
                pinned = (try memory.storage().fileSize(path)) != 0;
            }
            return memory.storage().fileSize(path);
        }
    };
    const alloc = std.testing.allocator;
    var memory = storage_io.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = try Backend.open(alloc, "/usage-publication", .{
        .storage = memory.storage(),
        .wal_enabled = false,
        .flush_threshold = 1,
        .compact_threshold_runs = 2,
        .obsolete_retention_ns = 0,
    });
    defer backend.close();
    try write(&backend, "original");
    const old = try alloc.dupe(u8, backend.runs.at(0).path.?);
    defer alloc.free(old);
    const old_size = try memory.storage().fileSize(old);
    var vtable = memory.storage().vtable.*;
    vtable.file_size = Hook.size;
    backend.storage = .{ .ptr = &memory, .vtable = &vtable };
    defer backend.storage = memory.storage();
    Hook.backend = &backend;
    Hook.armed = true;
    Hook.pinned = false;
    const result = try backend.measurePhysicalUsage();
    try std.testing.expect(Hook.pinned);
    try std.testing.expectEqual(old_size, result.active_sst_bytes);
    // The newly published files are not mixed into the captured inventory.
    try std.testing.expectEqual(old_size, try result.totalBytes());
    for (0..128) |_| {
        backend.obsolete_reclaim_retry_at_ns = 0;
        _ = try backend.runMaintenanceStep();
    }
    try std.testing.expectError(error.FileNotFound, memory.storage().fileSize(old));
}

test "lsm physical usage counts active ledger overlap once and checks overflow" {
    const alloc = std.testing.allocator;
    var memory = storage_io.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = try Backend.open(alloc, "/usage-overlap", .{ .storage = memory.storage(), .wal_enabled = false, .flush_threshold = 1 });
    defer backend.close();
    try write(&backend, "original");
    const path = backend.runs.at(0).path.?;
    const size = try memory.storage().fileSize(path);
    try backend.queueObsoleteFilePath(try alloc.dupe(u8, path));
    const result = try backend.measurePhysicalUsage();
    try std.testing.expectEqual(size, try result.totalBytes());
    try std.testing.expectEqual(@as(u64, 0), result.obsolete_file_bytes);
    try std.testing.expectError(error.Overflow, (Backend.PhysicalUsage{ .active_sst_bytes = std.math.maxInt(u64), .wal_retained_bytes = 1 }).totalBytes());
}

test "lsm physical usage capture unwinds allocation failures" {
    const Fixture = struct {
        fn check(alloc: std.mem.Allocator) !void {
            var memory = storage_io.MemoryStorage.init(alloc);
            defer memory.deinit();
            var backend = Backend.init(alloc, .{ .wal_enabled = false });
            defer backend.close();
            backend.storage = memory.storage();
            const readers = backend.active_readers;
            defer std.debug.assert(backend.active_readers == readers);
            const result = try backend.measurePhysicalUsage();
            try std.testing.expectEqual(@as(u64, 0), try result.totalBytes());
            backend.drainRetiredLedgers();
            try std.testing.expect(backend.ledger_snapshots == null);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}

test "lsm physical usage includes retained WAL and existing obsolete bytes" {
    const alloc = std.testing.allocator;
    var memory = storage_io.MemoryStorage.init(alloc);
    defer memory.deinit();
    var backend = try Backend.open(alloc, "/usage-wal", .{ .storage = memory.storage(), .flush_threshold = 1000 });
    defer backend.close();
    {
        var txn = try backend.beginWrite();
        errdefer txn.abort();
        try txn.put(.{}, "row", "retained in WAL");
        try txn.commit();
    }
    const wal_bytes = backend.snapshotMaintenanceStats().wal_retained_bytes;
    try std.testing.expect(wal_bytes > 0);
    const path = "/usage-wal/obsolete-manifest";
    try memory.storage().writeFileAbsolute(path, "retained");
    try backend.queueObsoleteFilePath(try alloc.dupe(u8, path));
    const result = try backend.measurePhysicalUsage();
    try std.testing.expectEqual(wal_bytes, result.wal_retained_bytes);
    try std.testing.expectEqual(@as(u64, 8), result.obsolete_file_bytes);
    try std.testing.expectEqual(@as(u64, 0), result.active_sst_bytes);
    try std.testing.expectEqual(@as(u64, 0), result.missing_obsolete_files);
    try std.testing.expectEqual(wal_bytes + 8, try result.totalBytes());
}
