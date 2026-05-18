// Copyright 2026 Antfly, Inc.
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
const antfly = @import("antfly-zig");

const db_mod = antfly.db;
const apply_rw_lock_mod = db_mod.apply_rw_lock;
const platform_time = antfly.platform_time;

const Config = struct {
    docs: usize = 5_000,
    write_batches: usize = 200,
    batch_size: usize = 50,
    search_threads: usize = 4,
    body_repeat: usize = 16,
    sync_level: db_mod.types.SyncLevel = .write,
};

const SearchWorker = struct {
    db: *db_mod.DB,
    start: *std.atomic.Value(u8),
    stop: *std.atomic.Value(u8),
    completed: *std.atomic.Value(u64),
    failed: *std.atomic.Value(u64),

    fn run(self: *@This()) void {
        while (self.start.load(.monotonic) == 0) {
            std.Thread.yield() catch {};
        }
        while (self.stop.load(.monotonic) == 0) {
            var result = self.db.search(std.heap.c_allocator, .{
                .index_name = "ft_idx",
                .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
                .limit = 10,
            }) catch {
                _ = self.failed.fetchAdd(1, .monotonic);
                return;
            };
            result.deinit();
            _ = self.completed.fetchAdd(1, .monotonic);
        }
    }
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);

    var db = try db_mod.DB.open(alloc, path, .{
        .start_index_workers = false,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_idx",
        .kind = .full_text,
        .config_json = "{\"field\":\"body\"}",
    });
    try preloadDocs(alloc, &db, cfg);

    var start = std.atomic.Value(u8).init(0);
    var stop = std.atomic.Value(u8).init(0);
    var completed = std.atomic.Value(u64).init(0);
    var failed = std.atomic.Value(u64).init(0);

    const workers = try alloc.alloc(SearchWorker, cfg.search_threads);
    defer alloc.free(workers);
    const threads = try alloc.alloc(std.Thread, cfg.search_threads);
    defer alloc.free(threads);

    for (workers, 0..) |*worker, i| {
        worker.* = .{
            .db = &db,
            .start = &start,
            .stop = &stop,
            .completed = &completed,
            .failed = &failed,
        };
        threads[i] = try std.Thread.spawn(.{}, SearchWorker.run, .{worker});
    }

    const before = db.snapshotApplyLockStats();
    start.store(1, .monotonic);

    const started_ns = nowNs();
    for (0..cfg.write_batches) |batch_idx| {
        const writes = try buildWrites(alloc, cfg, batch_idx);
        defer freeWrites(alloc, writes);
        try db.batch(.{
            .writes = writes,
            .sync_level = cfg.sync_level,
        });
    }
    const elapsed_ns = elapsedSince(started_ns);

    stop.store(1, .monotonic);
    for (threads) |thread| thread.join();

    const after = db.snapshotApplyLockStats();
    printSummary(cfg, elapsed_ns, completed.load(.monotonic), failed.load(.monotonic), deltaStats(after, before));
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, "--docs");
        } else if (std.mem.eql(u8, arg, "--write-batches")) {
            cfg.write_batches = try parseNextUsize(&args, "--write-batches");
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, "--batch-size");
        } else if (std.mem.eql(u8, arg, "--search-threads")) {
            cfg.search_threads = try parseNextUsize(&args, "--search-threads");
        } else if (std.mem.eql(u8, arg, "--body-repeat")) {
            cfg.body_repeat = try parseNextUsize(&args, "--body-repeat");
        } else if (std.mem.eql(u8, arg, "--sync-level")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.sync_level = db_mod.types.parsePublicSyncLevelText(raw) orelse return error.InvalidArgument;
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.write_batches == 0 or cfg.batch_size == 0 or cfg.search_threads == 0 or cfg.body_repeat == 0) {
        return error.InvalidArgument;
    }
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(usize, raw, 10);
}

fn preloadDocs(alloc: std.mem.Allocator, db: *db_mod.DB, cfg: Config) !void {
    var start_doc: usize = 0;
    while (start_doc < cfg.docs) : (start_doc += cfg.batch_size) {
        const end_doc = @min(start_doc + cfg.batch_size, cfg.docs);
        const writes = try alloc.alloc(db_mod.types.BatchWrite, end_doc - start_doc);
        defer freeWrites(alloc, writes);
        for (writes, start_doc..) |*write, doc_idx| {
            write.* = try makeBatchWrite(alloc, doc_idx, 0, cfg);
        }
        try db.batch(.{
            .writes = writes,
            .sync_level = .full_text,
        });
    }
}

fn buildWrites(alloc: std.mem.Allocator, cfg: Config, batch_idx: usize) ![]db_mod.types.BatchWrite {
    const writes = try alloc.alloc(db_mod.types.BatchWrite, cfg.batch_size);
    errdefer freeWrites(alloc, writes);
    for (writes, 0..) |*write, i| {
        const doc_idx = (batch_idx * cfg.batch_size + i) % cfg.docs;
        write.* = try makeBatchWrite(alloc, doc_idx, batch_idx + 1, cfg);
    }
    return writes;
}

fn makeBatchWrite(alloc: std.mem.Allocator, doc_idx: usize, pass_idx: usize, cfg: Config) !db_mod.types.BatchWrite {
    const key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx});
    const value = try encodeDocumentJsonAlloc(alloc, doc_idx, pass_idx, cfg);
    return .{ .key = key, .value = value };
}

fn encodeDocumentJsonAlloc(alloc: std.mem.Allocator, doc_idx: usize, pass_idx: usize, cfg: Config) ![]u8 {
    const body = try generatedBodyTextAlloc(alloc, doc_idx, pass_idx, cfg.body_repeat);
    defer alloc.free(body);
    return try std.fmt.allocPrint(
        alloc,
        "{{\"title\":\"doc-{d}\",\"body\":\"{s}\"}}",
        .{ doc_idx, body },
    );
}

fn generatedBodyTextAlloc(alloc: std.mem.Allocator, doc_idx: usize, pass_idx: usize, body_repeat: usize) ![]u8 {
    const topic = switch (doc_idx % 8) {
        0 => "alpha",
        1 => "beta",
        2 => "gamma",
        3 => "delta",
        4 => "epsilon",
        5 => "zeta",
        6 => "eta",
        else => "theta",
    };
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(alloc);
    const prefix = try std.fmt.allocPrint(alloc, "document {d} pass {d} topic {s}", .{ doc_idx, pass_idx, topic });
    defer alloc.free(prefix);
    try buf.appendSlice(alloc, prefix);
    for (0..body_repeat) |repeat_idx| {
        const segment = try std.fmt.allocPrint(alloc, " repeated context {s} token {d}", .{ topic, repeat_idx });
        defer alloc.free(segment);
        try buf.appendSlice(alloc, segment);
    }
    return try buf.toOwnedSlice(alloc);
}

fn freeWrites(alloc: std.mem.Allocator, writes: []db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(write.key);
        alloc.free(write.value);
    }
    alloc.free(writes);
}

fn deltaStats(after: apply_rw_lock_mod.ApplyRwLock.Stats, before: apply_rw_lock_mod.ApplyRwLock.Stats) apply_rw_lock_mod.ApplyRwLock.Stats {
    return .{
        .shared_lock_calls = after.shared_lock_calls -| before.shared_lock_calls,
        .shared_contended_calls = after.shared_contended_calls -| before.shared_contended_calls,
        .shared_wait_ns = after.shared_wait_ns -| before.shared_wait_ns,
        .shared_max_wait_ns = after.shared_max_wait_ns,
        .exclusive_lock_calls = after.exclusive_lock_calls -| before.exclusive_lock_calls,
        .exclusive_contended_calls = after.exclusive_contended_calls -| before.exclusive_contended_calls,
        .exclusive_wait_ns = after.exclusive_wait_ns -| before.exclusive_wait_ns,
        .exclusive_max_wait_ns = after.exclusive_max_wait_ns,
    };
}

fn printSummary(cfg: Config, elapsed_ns: u64, searches: u64, failed: u64, stats: apply_rw_lock_mod.ApplyRwLock.Stats) void {
    std.debug.print(
        "rw_lock_bench docs={d} write_batches={d} batch_size={d} search_threads={d} sync={s} elapsed_ms={d:.3} searches={d} failed={d}\n",
        .{
            cfg.docs,
            cfg.write_batches,
            cfg.batch_size,
            cfg.search_threads,
            db_mod.types.publicSyncLevelText(cfg.sync_level),
            nsToMsFloat(elapsed_ns),
            searches,
            failed,
        },
    );
    std.debug.print(
        "rw_lock_bench_apply_lock shared_calls={d} shared_contended={d} shared_wait_ms={d:.3} shared_max_wait_ms={d:.3} exclusive_calls={d} exclusive_contended={d} exclusive_wait_ms={d:.3} exclusive_max_wait_ms={d:.3}\n",
        .{
            stats.shared_lock_calls,
            stats.shared_contended_calls,
            nsToMsFloat(stats.shared_wait_ns),
            nsToMsFloat(stats.shared_max_wait_ns),
            stats.exclusive_lock_calls,
            stats.exclusive_contended_calls,
            nsToMsFloat(stats.exclusive_wait_ns),
            nsToMsFloat(stats.exclusive_max_wait_ns),
        },
    );
}

fn nowNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    return nowNs() - start_ns;
}

fn nsToMsFloat(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
}

fn tempPath(buf: []u8) []u8 {
    return std.fmt.bufPrint(buf, "/tmp/antfly-rw-lock-bench-{d}", .{platform_time.monotonicNs()}) catch unreachable;
}
