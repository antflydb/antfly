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
const lsm_backend = @import("../../lsm_backend/mod.zig");
const platform_time = @import("../../../platform/time.zig");
const supports_native_derived_log = builtin.os.tag != .freestanding;
const wal = if (supports_native_derived_log) @import("../../wal.zig") else struct {
    pub const CommitBackend = enum {
        adaptive,
    };
    pub const StorageBackend = enum {
        lmdb,
        lsm,
        lsm_memory,
    };

    pub const CommitStats = struct {};

    pub const WalStats = struct {
        append_calls: u64 = 0,
        append_batch_calls: u64 = 0,
        logical_entries: u64 = 0,
        physical_commits: u64 = 0,
        grouped_commits: u64 = 0,
        grouped_requests: u64 = 0,
        max_requests_per_commit: u64 = 0,
        max_entries_per_commit: u64 = 0,
        total_wait_ns: u64 = 0,
        total_coalesce_ns: u64 = 0,
        total_txn_open_ns: u64 = 0,
        total_put_ns: u64 = 0,
        total_commit_ns: u64 = 0,
    };

    pub const FullStats = struct {
        wal: WalStats = .{},
        commit: ?CommitStats = null,
    };
};

pub const OpenOptions = struct {
    map_size: usize = 64 * 1024 * 1024,
    no_sync: bool = false,
    artificial_sync_delay_ns: u64 = 0,
    group_commit_window_ns: u64 = 0,
    group_commit_max_requests: usize = 64,
    commit_backend: wal.CommitBackend = .adaptive,
    backend: wal.StorageBackend = .lsm,
    storage: ?lsm_backend.Storage = null,
    lsm_options: lsm_backend.Options = .{},
};

pub const StorageBackend = wal.StorageBackend;

pub const Entry = struct {
    sequence: u64,
    payload: []u8,

    pub fn deinit(self: *Entry, alloc: Allocator) void {
        alloc.free(self.payload);
        self.* = undefined;
    }
};

pub const EntryView = struct {
    sequence: u64,
    payload: []const u8,
};

pub const FullStats = wal.FullStats;

pub const DerivedLog = if (!supports_native_derived_log) struct {
    alloc: Allocator,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    next_sequence: u64 = 1,
    stats_value: wal.WalStats = .{},

    pub fn open(_: [*:0]const u8, _: OpenOptions) !DerivedLog {
        return .{
            .alloc = std.heap.page_allocator,
        };
    }

    pub fn close(self: *DerivedLog) void {
        for (self.entries.items) |*entry| entry.deinit(self.alloc);
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn appendOpaque(self: *DerivedLog, payload: []const u8) !u64 {
        const sequence = self.next_sequence;
        self.next_sequence += 1;
        try self.entries.append(self.alloc, .{
            .sequence = sequence,
            .payload = try self.alloc.dupe(u8, payload),
        });
        self.stats_value.append_calls += 1;
        self.stats_value.logical_entries += 1;
        self.stats_value.physical_commits += 1;
        return sequence;
    }

    pub fn lastSequence(self: *DerivedLog) u64 {
        return self.next_sequence - 1;
    }

    pub fn nextAppendSequence(self: *DerivedLog) u64 {
        return self.next_sequence;
    }

    pub fn statsSnapshot(self: *DerivedLog) wal.WalStats {
        return self.stats_value;
    }

    pub fn walStatsSnapshot(self: *DerivedLog) wal.WalStats {
        return self.stats_value;
    }

    pub fn commitStatsSnapshot(_: *DerivedLog) ?wal.CommitStats {
        return null;
    }

    pub fn fullStatsSnapshot(self: *DerivedLog) FullStats {
        return .{
            .wal = self.stats_value,
            .commit = null,
        };
    }

    pub fn iterateOpaqueFrom(self: *DerivedLog, alloc: Allocator, from_sequence: u64) ![]Entry {
        var results = std.ArrayListUnmanaged(Entry).empty;
        errdefer {
            for (results.items) |*entry| entry.deinit(alloc);
            results.deinit(alloc);
        }

        for (self.entries.items) |entry| {
            if (entry.sequence < from_sequence) continue;
            try results.append(alloc, .{
                .sequence = entry.sequence,
                .payload = try alloc.dupe(u8, entry.payload),
            });
        }

        return try results.toOwnedSlice(alloc);
    }

    pub fn truncate(self: *DerivedLog, up_to_sequence: u64) !void {
        var write_index: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.sequence <= up_to_sequence) {
                var doomed = entry;
                doomed.deinit(self.alloc);
                continue;
            }
            if (write_index != self.entries.items.len) {
                self.entries.items[write_index] = entry;
            }
            write_index += 1;
        }
        self.entries.items.len = write_index;
    }
} else struct {
    wal_impl: wal.WAL,

    pub fn open(path: [*:0]const u8, opts: OpenOptions) !DerivedLog {
        return .{
            .wal_impl = try wal.WAL.open(path, .{
                .map_size = opts.map_size,
                .no_sync = opts.no_sync,
                .artificial_sync_delay_ns = opts.artificial_sync_delay_ns,
                .group_commit_window_ns = opts.group_commit_window_ns,
                .group_commit_max_requests = opts.group_commit_max_requests,
                .commit_backend = opts.commit_backend,
                .backend = opts.backend,
                .storage = opts.storage,
                .lsm_options = opts.lsm_options,
            }),
        };
    }

    pub fn close(self: *DerivedLog) void {
        self.wal_impl.close();
        self.* = undefined;
    }

    pub fn appendOpaque(self: *DerivedLog, payload: []const u8) !u64 {
        return try self.wal_impl.append(payload);
    }

    pub fn lastSequence(self: *DerivedLog) u64 {
        return self.wal_impl.next_lsn - 1;
    }

    pub fn nextAppendSequence(self: *DerivedLog) u64 {
        return self.wal_impl.next_lsn;
    }

    pub fn statsSnapshot(self: *DerivedLog) wal.WalStats {
        return self.wal_impl.statsSnapshot();
    }

    pub fn walStatsSnapshot(self: *DerivedLog) wal.WalStats {
        return self.wal_impl.statsSnapshot();
    }

    pub fn commitStatsSnapshot(self: *DerivedLog) ?wal.CommitStats {
        return self.wal_impl.commitStatsSnapshot();
    }

    pub fn fullStatsSnapshot(self: *DerivedLog) FullStats {
        return self.wal_impl.fullStatsSnapshot();
    }

    pub fn iterateOpaqueFrom(self: *DerivedLog, alloc: Allocator, from_sequence: u64) ![]Entry {
        const wal_entries = try self.wal_impl.iterateFrom(alloc, from_sequence);
        errdefer {
            for (wal_entries) |entry| alloc.free(entry.data);
            alloc.free(wal_entries);
        }

        var entries = try alloc.alloc(Entry, wal_entries.len);
        var initialized: usize = 0;
        errdefer {
            for (entries[0..initialized]) |*entry| entry.deinit(alloc);
            alloc.free(entries);
        }

        for (wal_entries, 0..) |entry, i| {
            entries[i] = .{
                .sequence = entry.lsn,
                .payload = @constCast(entry.data),
            };
            initialized += 1;
        }

        alloc.free(wal_entries);
        return entries;
    }

    pub fn iterateOpaqueFromStreamingWithContext(
        self: *DerivedLog,
        from_sequence: u64,
        context: anytype,
        comptime callback: fn (@TypeOf(context), EntryView) anyerror!wal.WAL.ScanAction,
    ) !void {
        try self.wal_impl.iterateFromStreamingWithContext(from_sequence, context, struct {
            fn adapted(ctx: @TypeOf(context), wal_entry: wal.WalEntry) anyerror!wal.WAL.ScanAction {
                return try callback(ctx, .{
                    .sequence = wal_entry.lsn,
                    .payload = wal_entry.data,
                });
            }
        }.adapted);
    }

    pub fn truncate(self: *DerivedLog, up_to_sequence: u64) !void {
        try self.wal_impl.truncate(up_to_sequence);
    }
};

var derived_log_tmp_nonce: u64 = 0;

fn derivedLogTmpPath(buf: []u8) [*:0]const u8 {
    const base = "/tmp/antfly-derived-log-test-";
    const ts = platform_time.monotonicNs();
    const nonce = @atomicRmw(u64, &derived_log_tmp_nonce, .Add, 1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "{s}{d}-{d}\x00", .{ base, ts, nonce }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupDerivedLogDir(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "derived log defaults to adaptive commit backend" {
    const opts = OpenOptions{};
    try std.testing.expectEqual(wal.CommitBackend.adaptive, opts.commit_backend);
}

test "derived log propagates wal group commit settings" {
    const Barrier = struct {
        mutex: std.atomic.Mutex = .unlocked,
        waiting: usize = 0,
        open: bool = false,

        fn wait(self: *@This(), total: usize) void {
            var registered = false;
            while (true) {
                while (!self.mutex.tryLock()) std.Thread.yield() catch {};
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
        log: *DerivedLog,
        barrier: *Barrier,
        payload: []const u8,
        result: u64 = 0,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.barrier.wait(2);
            self.result = self.log.appendOpaque(self.payload) catch |err| {
                self.err = err;
                return;
            };
        }
    };

    var attempt: usize = 0;
    while (attempt < 8) : (attempt += 1) {
        var buf: [256]u8 = undefined;
        const path = derivedLogTmpPath(&buf);
        defer cleanupDerivedLogDir(path);

        var log = try DerivedLog.open(path, .{
            .group_commit_window_ns = 10 * std.time.ns_per_ms,
            .group_commit_max_requests = 8,
        });
        defer log.close();

        var barrier = Barrier{};
        var worker_a = Worker{ .log = &log, .barrier = &barrier, .payload = "alpha" };
        var worker_b = Worker{ .log = &log, .barrier = &barrier, .payload = "beta" };

        const thread_a = try std.Thread.spawn(.{}, Worker.run, .{&worker_a});
        const thread_b = try std.Thread.spawn(.{}, Worker.run, .{&worker_b});
        thread_a.join();
        thread_b.join();

        if (worker_a.err) |err| return err;
        if (worker_b.err) |err| return err;

        const min_seq = @min(worker_a.result, worker_b.result);
        const max_seq = @max(worker_a.result, worker_b.result);
        try std.testing.expectEqual(@as(u64, 1), min_seq);
        try std.testing.expectEqual(@as(u64, 2), max_seq);

        const stats = log.statsSnapshot();
        try std.testing.expectEqual(@as(u64, 2), stats.append_calls);
        try std.testing.expectEqual(@as(u64, 2), stats.logical_entries);
        if (stats.grouped_commits > 0) {
            try std.testing.expectEqual(@as(u64, 1), stats.physical_commits);
            try std.testing.expectEqual(@as(u64, 1), stats.grouped_commits);
            try std.testing.expectEqual(@as(u64, 2), stats.grouped_requests);
            return;
        }
    }

    return error.TestExpectedEqual;
}

test "derived log exposes wal and lmdb stats when available" {
    var buf: [256]u8 = undefined;
    const path = derivedLogTmpPath(&buf);
    defer cleanupDerivedLogDir(path);

    var log = try DerivedLog.open(path, .{ .backend = .lmdb });
    defer log.close();

    _ = try log.appendOpaque("payload");

    const stats = log.fullStatsSnapshot();
    try std.testing.expectEqual(@as(u64, 1), stats.wal.append_calls);
    if (stats.commit) |commit| {
        try std.testing.expect(commit.publish_calls >= 1);
        try std.testing.expect(commit.full_publish_calls >= 1);
        try std.testing.expect(commit.page_images_written > 0);
        try std.testing.expect(commit.bytes_written > 0);
        try std.testing.expect(commit.total_publish_ns > 0);
    }
}

test "derived log defaults to lsm backend" {
    try std.testing.expectEqual(StorageBackend.lsm, (OpenOptions{}).backend);
}

test "derived log can use in-memory lsm backend" {
    var buf: [256]u8 = undefined;
    const path = derivedLogTmpPath(&buf);

    var log = try DerivedLog.open(path, .{ .backend = .lsm_memory });
    defer log.close();

    try std.testing.expectEqual(@as(u64, 1), try log.appendOpaque("derived-memory"));
    const entries = try log.iterateOpaqueFrom(std.testing.allocator, 1);
    defer {
        for (entries) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqualStrings("derived-memory", entries[0].payload);
}

test "derived log can use lsm backend" {
    var buf: [256]u8 = undefined;
    const path = derivedLogTmpPath(&buf);
    defer cleanupDerivedLogDir(path);

    {
        var log = try DerivedLog.open(path, .{ .backend = .lsm });
        defer log.close();

        try std.testing.expectEqual(@as(u64, 1), try log.appendOpaque("derived-a"));
        try std.testing.expectEqual(@as(u64, 2), try log.appendOpaque("derived-b"));
        try log.truncate(1);
    }

    {
        var log = try DerivedLog.open(path, .{ .backend = .lsm });
        defer log.close();

        const entries = try log.iterateOpaqueFrom(std.testing.allocator, 1);
        defer {
            for (entries) |*entry| entry.deinit(std.testing.allocator);
            std.testing.allocator.free(entries);
        }

        try std.testing.expectEqual(@as(usize, 1), entries.len);
        try std.testing.expectEqual(@as(u64, 2), entries[0].sequence);
        try std.testing.expectEqualStrings("derived-b", entries[0].payload);
    }
}
