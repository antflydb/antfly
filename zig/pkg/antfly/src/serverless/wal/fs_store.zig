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
const Allocator = std.mem.Allocator;
const fs_paths = @import("../../common/fs_paths.zig");
const wal_types = @import("types.zig");
const wal_store = @import("store.zig");

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
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const log_path = try logPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(log_path);
        const next_path = try nextLsnPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(next_path);

        try ensureParentDir(log_path);
        const lsn = try currentNextLsn(self.alloc, next_path);
        const encoded = try encodeRecordAlloc(self.alloc, lsn, timestamp_ns, payload);
        defer self.alloc.free(encoded);

        try appendFile(log_path, encoded);
        const next_payload = try std.fmt.allocPrint(self.alloc, "{d}", .{lsn + 1});
        defer self.alloc.free(next_payload);
        try writeFileAtomically(next_path, next_payload);
        return lsn;
    }

    pub fn readFromAlloc(self: *FsStore, alloc: Allocator, namespace: []const u8, start_lsn: u64) ![]wal_types.Record {
        const log_path = try logPathAlloc(alloc, self.root_dir, namespace);
        defer alloc.free(log_path);
        if (!fileExists(log_path)) return try alloc.alloc(wal_types.Record, 0);

        const raw = try readFileAlloc(alloc, log_path);
        defer alloc.free(raw);

        var cursor: usize = 0;
        var out = std.ArrayListUnmanaged(wal_types.Record).empty;
        errdefer {
            wal_types.freeRecords(alloc, out.items);
            out = .empty;
        }

        while (cursor < raw.len) {
            if (cursor + 8 + 8 + 4 > raw.len) return error.InvalidWal;
            const lsn = std.mem.readInt(u64, raw[cursor..][0..8], .little);
            cursor += 8;
            const timestamp_ns = std.mem.readInt(u64, raw[cursor..][0..8], .little);
            cursor += 8;
            const payload_len = std.mem.readInt(u32, raw[cursor..][0..4], .little);
            cursor += 4;
            if (cursor + payload_len > raw.len) return error.InvalidWal;

            if (lsn >= start_lsn) {
                try out.append(alloc, .{
                    .lsn = lsn,
                    .timestamp_ns = timestamp_ns,
                    .payload = try alloc.dupe(u8, raw[cursor .. cursor + payload_len]),
                });
            }
            cursor += payload_len;
        }

        return try out.toOwnedSlice(alloc);
    }

    pub fn latestLsn(self: *FsStore, namespace: []const u8) !u64 {
        const next_path = try nextLsnPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(next_path);
        if (!fileExists(next_path)) return 0;
        const next_lsn = try currentNextLsn(self.alloc, next_path);
        return next_lsn -| 1;
    }

    pub fn truncatePrefix(self: *FsStore, namespace: []const u8, keep_from_lsn: u64) !u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const log_path = try logPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(log_path);
        if (!fileExists(log_path)) return 0;

        const records = try self.readFromAlloc(self.alloc, namespace, 1);
        defer wal_types.freeRecords(self.alloc, records);

        var kept = std.ArrayListUnmanaged(wal_types.Record).empty;
        defer {
            for (kept.items) |record| self.alloc.free(record.payload);
            kept.deinit(self.alloc);
        }

        var removed: u64 = 0;
        for (records) |record| {
            if (record.lsn < keep_from_lsn) {
                removed += 1;
                continue;
            }
            try kept.append(self.alloc, .{
                .lsn = record.lsn,
                .timestamp_ns = record.timestamp_ns,
                .payload = try self.alloc.dupe(u8, record.payload),
            });
        }

        if (kept.items.len == 0) {
            deleteFileIfExists(log_path);
            return removed;
        }

        const encoded = try encodeRecordsAlloc(self.alloc, kept.items);
        defer self.alloc.free(encoded);
        try writeFileAtomically(log_path, encoded);
        return removed;
    }

    const vtable: wal_store.WalStore.VTable = .{
        .deinit = erasedDeinit,
        .append = erasedAppend,
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

fn encodeRecordAlloc(alloc: Allocator, lsn: u64, timestamp_ns: u64, payload: []const u8) ![]u8 {
    const buf = try alloc.alloc(u8, 8 + 8 + 4 + payload.len);
    var pos: usize = 0;
    std.mem.writeInt(u64, buf[pos..][0..8], lsn, .little);
    pos += 8;
    std.mem.writeInt(u64, buf[pos..][0..8], timestamp_ns, .little);
    pos += 8;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(payload.len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..payload.len], payload);
    return buf;
}

fn encodeRecordsAlloc(alloc: Allocator, records: []const wal_types.Record) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    for (records) |record| {
        const encoded = try encodeRecordAlloc(alloc, record.lsn, record.timestamp_ns, record.payload);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    return try out.toOwnedSlice(alloc);
}

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn fileExists(path: []const u8) bool {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    _ = std.Io.Dir.cwd().statFile(io_impl.io(), path, .{}) catch return false;
    return true;
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn readFileAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(std.math.maxInt(usize)));
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

fn appendFile(path: []const u8, contents: []const u8) !void {
    const alloc = std.heap.page_allocator;
    const existing = if (fileExists(path)) try readFileAlloc(alloc, path) else null;
    defer if (existing) |bytes| alloc.free(bytes);

    const combined = if (existing) |bytes|
        try std.mem.concat(alloc, u8, &.{ bytes, contents })
    else
        try alloc.dupe(u8, contents);
    defer alloc.free(combined);

    try writeFileAtomically(path, combined);
}

fn writeFileAtomically(path: []const u8, contents: []const u8) !void {
    const tmp_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-{d}", .{ path, test_nonce.fetchAdd(1, .monotonic) });
    defer std.heap.page_allocator.free(tmp_path);

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();

    {
        var file = try std.Io.Dir.createFileAbsolute(io, tmp_path, .{ .truncate = true });
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

fn currentNextLsn(alloc: Allocator, next_path: []const u8) !u64 {
    if (!fileExists(next_path)) return 1;
    const raw = try readFileAlloc(alloc, next_path);
    defer alloc.free(raw);
    return try std.fmt.parseInt(u64, std.mem.trim(u8, raw, " \t\r\n"), 10);
}

fn logPathAlloc(alloc: Allocator, root_dir: []const u8, namespace: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, namespace, "wal.log" });
}

fn nextLsnPathAlloc(alloc: Allocator, root_dir: []const u8, namespace: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, namespace, "NEXT_LSN" });
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
