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
const platform_sync = @import("antfly_platform").sync;
const Allocator = std.mem.Allocator;
const fs_paths = @import("../../common/fs_paths.zig");
const wal_types = @import("types.zig");
const wal_store = @import("store.zig");
const record_codec = @import("object_store.zig");

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
        const current = if (fileExists(log_path)) try readFileAlloc(self.alloc, log_path) else null;
        defer if (current) |raw| self.alloc.free(raw);
        const lsn = if (current) |raw|
            (try record_codec.lastLsn(raw)) + 1
        else if (try readNextState(self.alloc, next_path)) |state|
            state.next_lsn
        else
            1;
        const encoded = try record_codec.encodeRecordAlloc(self.alloc, lsn, timestamp_ns, payload);
        defer self.alloc.free(encoded);
        try writeAppendedLogAtomically(self.alloc, log_path, current, encoded);
        try writeNextState(self.alloc, next_path, lsn + 1, (if (current) |raw| raw.len else 0) + encoded.len);
        return lsn;
    }

    pub fn appendIdempotent(
        self: *FsStore,
        namespace: []const u8,
        timestamp_ns: u64,
        payload: []const u8,
        operation_id: []const u8,
    ) !u64 {
        if (operation_id.len == 0 or operation_id.len > std.math.maxInt(u16))
            return error.InvalidWalOperationId;
        if (payload.len > record_codec.idempotent_payload_max) return error.WalRecordTooLarge;

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const log_path = try logPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(log_path);
        const next_path = try nextLsnPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(next_path);
        try ensureParentDir(log_path);
        const current = if (fileExists(log_path)) try readFileAlloc(self.alloc, log_path) else null;
        defer if (current) |raw| self.alloc.free(raw);
        if (current) |raw| {
            if (try record_codec.findOperation(raw, operation_id)) |existing| {
                if (existing.timestamp_ns != timestamp_ns or !std.mem.eql(u8, existing.payload, payload))
                    return error.WalIdempotencyConflict;
                try writeNextState(self.alloc, next_path, (try record_codec.lastLsn(raw)) + 1, raw.len);
                return existing.lsn;
            }
        }

        const lsn = if (current) |raw|
            (try record_codec.lastLsn(raw)) + 1
        else if (try readNextState(self.alloc, next_path)) |state|
            state.next_lsn
        else
            1;
        const encoded = try record_codec.encodeIdempotentRecordAlloc(self.alloc, lsn, timestamp_ns, payload, operation_id);
        defer self.alloc.free(encoded);
        try writeAppendedLogAtomically(self.alloc, log_path, current, encoded);
        try writeNextState(self.alloc, next_path, lsn + 1, (if (current) |raw| raw.len else 0) + encoded.len);
        return lsn;
    }

    pub fn readFromAlloc(self: *FsStore, alloc: Allocator, namespace: []const u8, start_lsn: u64) ![]wal_types.Record {
        const log_path = try logPathAlloc(alloc, self.root_dir, namespace);
        defer alloc.free(log_path);
        if (!fileExists(log_path)) return try alloc.alloc(wal_types.Record, 0);

        const raw = try readFileAlloc(alloc, log_path);
        defer alloc.free(raw);

        return try record_codec.decodeRecordsFromAlloc(alloc, raw, start_lsn);
    }

    pub fn latestLsn(self: *FsStore, namespace: []const u8) !u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
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
        const raw = try readFileAlloc(self.alloc, log_path);
        defer self.alloc.free(raw);
        const latest = try record_codec.lastLsn(raw);
        try writeNextState(self.alloc, next_path, latest + 1, raw.len);
        return latest;
    }

    pub fn truncatePrefix(self: *FsStore, namespace: []const u8, keep_from_lsn: u64) !u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const log_path = try logPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(log_path);
        const next_path = try nextLsnPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(next_path);
        if (!fileExists(log_path)) return 0;

        const records = try self.readFromAlloc(self.alloc, namespace, 1);
        defer wal_types.freeRecords(self.alloc, records);
        const next_lsn = if (records.len > 0) records[records.len - 1].lsn + 1 else 1;

        var kept = std.ArrayListUnmanaged(wal_types.Record).empty;
        defer {
            for (kept.items) |record| {
                self.alloc.free(record.payload);
                if (record.operation_id) |operation_id| self.alloc.free(operation_id);
            }
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
                .operation_id = if (record.operation_id) |operation_id| try self.alloc.dupe(u8, operation_id) else null,
            });
        }

        if (kept.items.len == 0) {
            deleteFileIfExists(log_path);
            try writeNextState(self.alloc, next_path, next_lsn, 0);
            return removed;
        }

        const encoded = try record_codec.encodeRecordsAlloc(self.alloc, kept.items);
        defer self.alloc.free(encoded);
        try writeFileAtomically(log_path, encoded);
        try writeNextState(self.alloc, next_path, next_lsn, encoded.len);
        return removed;
    }

    const vtable: wal_store.WalStore.VTable = .{
        .deinit = erasedDeinit,
        .append = erasedAppend,
        .append_idempotent = erasedAppendIdempotent,
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

    fn erasedAppendIdempotent(ptr: *anyopaque, namespace: []const u8, timestamp_ns: u64, payload: []const u8, operation_id: []const u8) !u64 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.appendIdempotent(namespace, timestamp_ns, payload, operation_id);
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

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
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

fn writeAppendedLogAtomically(alloc: Allocator, path: []const u8, existing: ?[]const u8, contents: []const u8) !void {
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

fn logPathAlloc(alloc: Allocator, root_dir: []const u8, namespace: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, namespace, "wal.log" });
}

fn nextLsnPathAlloc(alloc: Allocator, root_dir: []const u8, namespace: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, namespace, "NEXT_LSN" });
}

const NextState = struct {
    next_lsn: u64,
    log_byte_len: ?u64,
};

fn readNextState(alloc: Allocator, path: []const u8) !?NextState {
    if (!fileExists(path)) return null;
    const raw = try readFileAlloc(alloc, path);
    defer alloc.free(raw);
    var fields = std.mem.tokenizeAny(u8, raw, " \t\r\n");
    const next_lsn = try std.fmt.parseInt(u64, fields.next() orelse return error.InvalidWal, 10);
    const log_byte_len = if (fields.next()) |value| try std.fmt.parseInt(u64, value, 10) else null;
    if (fields.next() != null) return error.InvalidWal;
    return .{ .next_lsn = next_lsn, .log_byte_len = log_byte_len };
}

fn writeNextState(alloc: Allocator, path: []const u8, next_lsn: u64, log_byte_len: usize) !void {
    const payload = try std.fmt.allocPrint(alloc, "{d} {d}", .{ next_lsn, log_byte_len });
    defer alloc.free(payload);
    try writeFileAtomically(path, payload);
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
