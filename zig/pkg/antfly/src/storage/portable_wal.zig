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
const lsm_backend = @import("lsm_backend/mod.zig");
const storage_io = @import("lsm_backend/storage_io.zig");
const storage_sim = @import("sim_runtime.zig");

pub const CommitStats = struct {};

pub const CommitBackend = enum {
    sync,
    worker_thread,
    async_io,
    adaptive,
};

pub const StorageBackend = enum {
    lmdb,
    lsm,
    lsm_memory,
};

pub const WalOptions = struct {
    map_size: usize = 64 * 1024 * 1024,
    no_sync: bool = false,
    artificial_sync_delay_ns: u64 = 0,
    group_commit_window_ns: u64 = 0,
    group_commit_max_requests: usize = 64,
    commit_backend: CommitBackend = .adaptive,
    backend: ?StorageBackend = null,
    storage: ?storage_io.Storage = null,
    lsm_options: lsm_backend.Options = .{},
    clock: storage_sim.Clock = storage_sim.real_clock,
    commit_scheduler: storage_sim.CompletionScheduler = storage_sim.real_completion_scheduler,
    model_commit_backend_completions: bool = false,

    pub fn resolvedBackend(self: WalOptions) StorageBackend {
        return self.backend orelse .lsm;
    }
};

pub const WalEntry = struct {
    lsn: u64,
    data: []const u8,
};

pub const BatchAppendResult = struct {
    first_lsn: u64,
    count: usize,

    pub fn isEmpty(self: BatchAppendResult) bool {
        return self.count == 0;
    }

    pub fn lsnAt(self: BatchAppendResult, index: usize) u64 {
        std.debug.assert(index < self.count);
        return self.first_lsn + index;
    }

    pub fn lastLsn(self: BatchAppendResult) ?u64 {
        if (self.count == 0) return null;
        return self.first_lsn + @as(u64, @intCast(self.count - 1));
    }
};

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
    wal: WalStats,
    commit: ?CommitStats,
};

pub const ScanAction = enum {
    @"continue",
    stop,
};

pub const WAL = struct {
    alloc: Allocator,
    storage: storage_io.Storage,
    root_path: []u8,
    log_path: []u8,
    next_lsn: u64,
    log_bytes: []u8,
    mutex: std.atomic.Mutex = .unlocked,
    stats: WalStats = .{},

    pub fn open(path: [*:0]const u8, opts: WalOptions) !WAL {
        const storage = opts.storage orelse return error.MissingStorageBackend;
        const alloc = std.heap.page_allocator;
        const root_path = try alloc.dupe(u8, std.mem.span(path));
        errdefer alloc.free(root_path);
        const log_path = try std.fmt.allocPrint(alloc, "{s}/log.bin", .{root_path});
        errdefer alloc.free(log_path);

        try storage.createDirPath(root_path);

        const log_bytes = storage.readFileAlloc(alloc, log_path, std.math.maxInt(usize)) catch |err| switch (err) {
            error.FileNotFound => try alloc.alloc(u8, 0),
            else => return err,
        };
        errdefer alloc.free(log_bytes);

        return .{
            .alloc = alloc,
            .storage = storage,
            .root_path = root_path,
            .log_path = log_path,
            .next_lsn = computeNextLsn(log_bytes),
            .log_bytes = log_bytes,
        };
    }

    pub fn close(self: *WAL) void {
        self.alloc.free(self.root_path);
        self.alloc.free(self.log_path);
        self.alloc.free(self.log_bytes);
        self.* = undefined;
    }

    pub fn append(self: *WAL, data: []const u8) !u64 {
        const result = try self.appendBatch(&.{data});
        return result.first_lsn;
    }

    pub fn appendBatch(self: *WAL, entries: []const []const u8) !BatchAppendResult {
        if (entries.len == 0) {
            return .{ .first_lsn = self.next_lsn, .count = 0 };
        }

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const first_lsn = self.next_lsn;

        var bytes: std.ArrayListUnmanaged(u8) = .empty;
        defer bytes.deinit(self.alloc);
        try bytes.appendSlice(self.alloc, self.log_bytes);

        for (entries, 0..) |entry, i| {
            const lsn = first_lsn + @as(u64, @intCast(i));
            try appendEncodedEntry(self.alloc, &bytes, lsn, entry);
        }

        const owned = try bytes.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(owned);
        try self.storage.writeFileAbsolute(self.log_path, owned);

        self.alloc.free(self.log_bytes);
        self.log_bytes = owned;
        self.next_lsn = first_lsn + @as(u64, @intCast(entries.len));

        if (entries.len == 1) {
            self.stats.append_calls += 1;
        } else {
            self.stats.append_batch_calls += 1;
        }
        self.stats.logical_entries += @intCast(entries.len);
        self.stats.physical_commits += 1;
        if (entries.len > 1) {
            self.stats.grouped_commits += 1;
            self.stats.grouped_requests += @intCast(entries.len);
        }
        self.stats.max_entries_per_commit = @max(self.stats.max_entries_per_commit, @as(u64, @intCast(entries.len)));
        self.stats.max_requests_per_commit = @max(self.stats.max_requests_per_commit, @as(u64, @intCast(entries.len)));

        return .{
            .first_lsn = first_lsn,
            .count = entries.len,
        };
    }

    pub fn statsSnapshot(self: *WAL) WalStats {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.stats;
    }

    pub fn commitStatsSnapshot(_: *WAL) ?CommitStats {
        return null;
    }

    pub fn fullStatsSnapshot(self: *WAL) FullStats {
        return .{
            .wal = self.statsSnapshot(),
            .commit = null,
        };
    }

    pub fn truncate(self: *WAL, up_to_lsn: u64) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var bytes: std.ArrayListUnmanaged(u8) = .empty;
        defer bytes.deinit(self.alloc);

        var it = EntryIterator.init(self.log_bytes);
        while (try it.next()) |entry| {
            if (entry.lsn <= up_to_lsn) continue;
            try appendEncodedEntry(self.alloc, &bytes, entry.lsn, entry.data);
        }

        const owned = try bytes.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(owned);
        try self.storage.writeFileAbsolute(self.log_path, owned);

        self.alloc.free(self.log_bytes);
        self.log_bytes = owned;
        self.next_lsn = computeNextLsn(self.log_bytes);
    }

    pub fn iterateFrom(self: *WAL, alloc: Allocator, from_lsn: u64) ![]WalEntry {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var results: std.ArrayListUnmanaged(WalEntry) = .empty;
        errdefer {
            for (results.items) |entry| alloc.free(@constCast(entry.data));
            results.deinit(alloc);
        }

        var it = EntryIterator.init(self.log_bytes);
        while (try it.next()) |entry| {
            if (entry.lsn < from_lsn) continue;
            try results.append(alloc, .{
                .lsn = entry.lsn,
                .data = try alloc.dupe(u8, entry.data),
            });
        }

        return try results.toOwnedSlice(alloc);
    }

    pub fn iterateFromStreaming(
        self: *WAL,
        from_lsn: u64,
        callback: *const fn (entry: WalEntry) anyerror!ScanAction,
    ) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var it = EntryIterator.init(self.log_bytes);
        while (try it.next()) |entry| {
            if (entry.lsn < from_lsn) continue;
            const action = try callback(.{
                .lsn = entry.lsn,
                .data = entry.data,
            });
            if (action == .stop) return;
        }
    }

    pub fn lastLsn(self: *const WAL) u64 {
        if (self.next_lsn <= 1) return 0;
        return self.next_lsn - 1;
    }

    pub fn sync(_: *WAL, _: bool) !void {}
};

const ParsedEntry = struct {
    lsn: u64,
    data: []const u8,
};

const EntryIterator = struct {
    bytes: []const u8,
    pos: usize = 0,

    fn init(bytes: []const u8) EntryIterator {
        return .{ .bytes = bytes };
    }

    fn next(self: *EntryIterator) !?ParsedEntry {
        if (self.pos >= self.bytes.len) return null;
        if (self.bytes.len - self.pos < 16) return error.CorruptWal;

        const lsn = std.mem.readInt(u64, self.bytes[self.pos..][0..8], .little);
        self.pos += 8;

        const data_len_u32 = std.mem.readInt(u32, self.bytes[self.pos..][0..4], .little);
        self.pos += 4;
        const data_len: usize = @intCast(data_len_u32);

        if (self.bytes.len - self.pos < data_len + 4) return error.CorruptWal;
        const data = self.bytes[self.pos .. self.pos + data_len];
        self.pos += data_len;

        const stored_crc = std.mem.readInt(u32, self.bytes[self.pos..][0..4], .little);
        self.pos += 4;

        var crc = std.hash.Crc32.init();
        crc.update(std.mem.asBytes(&data_len_u32));
        crc.update(data);
        if (crc.final() != stored_crc) return error.CorruptWal;

        return .{
            .lsn = lsn,
            .data = data,
        };
    }
};

fn appendEncodedEntry(alloc: Allocator, bytes: *std.ArrayListUnmanaged(u8), lsn: u64, data: []const u8) !void {
    const data_len_u32: u32 = @intCast(data.len);
    try bytes.ensureUnusedCapacity(alloc, 8 + 4 + data.len + 4);
    bytes.appendSliceAssumeCapacity(std.mem.asBytes(&lsn));
    bytes.appendSliceAssumeCapacity(std.mem.asBytes(&data_len_u32));
    bytes.appendSliceAssumeCapacity(data);

    var crc = std.hash.Crc32.init();
    crc.update(std.mem.asBytes(&data_len_u32));
    crc.update(data);
    const stored_crc = crc.final();
    bytes.appendSliceAssumeCapacity(std.mem.asBytes(&stored_crc));
}

fn computeNextLsn(bytes: []const u8) u64 {
    var it = EntryIterator.init(bytes);
    var next_lsn: u64 = 1;
    while (true) {
        const maybe_entry = it.next() catch break;
        const entry = maybe_entry orelse break;
        next_lsn = entry.lsn + 1;
    }
    return next_lsn;
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}
