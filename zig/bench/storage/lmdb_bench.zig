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
const lmdb = @import("lmdb");
const lmdb_engine = @import("lmdb_engine");

const Config = struct {
    samples: usize = 1,
    cycles: usize = 8,
    keys: usize = 512,
    dups: usize = 32,
    named_keys: usize = 128,
    write_map: bool = false,
    map_async: bool = false,
    fixed_map: bool = false,
    commit_backend: lmdb.CommitBackend = .sync,
};

const WorkloadResult = struct {
    name: []const u8,
    ops: usize,
    ns: u64,
};

const KvRoundtripMetrics = struct {
    result: WorkloadResult,
    write_open_ns: u64 = 0,
    put_loop_ns: u64 = 0,
    commit_ns: u64 = 0,
    read_open_ns: u64 = 0,
    read_get_ns: u64 = 0,
    publish_ns: u64 = 0,
    page_write_ns: u64 = 0,
    data_sync_ns: u64 = 0,
    meta_write_ns: u64 = 0,
    meta_sync_ns: u64 = 0,
};

const RangeScanMetrics = struct {
    result: WorkloadResult,
    read_open_ns: u64 = 0,
    seek_ns: u64 = 0,
    scan_loop_ns: u64 = 0,
};

const NsStats = struct {
    total: u128 = 0,
    min: u64 = std.math.maxInt(u64),
    max: u64 = 0,
    count: usize = 0,

    fn add(self: *NsStats, ns: u64) void {
        self.total += ns;
        self.min = @min(self.min, ns);
        self.max = @max(self.max, ns);
        self.count += 1;
    }

    fn avg(self: NsStats) u64 {
        if (self.count == 0) return 0;
        return @intCast(self.total / self.count);
    }
};

const Summary = struct {
    kv_total: NsStats = .{},
    kv_write_open: NsStats = .{},
    kv_put_loop: NsStats = .{},
    kv_commit: NsStats = .{},
    kv_read_open: NsStats = .{},
    kv_read_get: NsStats = .{},
    kv_publish: NsStats = .{},
    kv_page_write: NsStats = .{},
    kv_data_sync: NsStats = .{},
    kv_meta_write: NsStats = .{},
    kv_meta_sync: NsStats = .{},
    range_warm_total: NsStats = .{},
    range_warm_open: NsStats = .{},
    range_warm_seek: NsStats = .{},
    range_warm_scan_loop: NsStats = .{},
    range_reopen_total: NsStats = .{},
    range_reopen_open: NsStats = .{},
    range_reopen_seek: NsStats = .{},
    range_reopen_scan_loop: NsStats = .{},
    dupsort_total: NsStats = .{},
    nested_total: NsStats = .{},
};

fn nanotime() u64 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

pub fn main(init: std.process.Init) !void {
    const cfg = try parseArgs(init.gpa, init.minimal.args);
    if (cfg.map_async and !cfg.write_map) return error.InvalidArgument;

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    try stdout_writer.interface.print(
        "LMDB benchmark backend={s} samples={d} cycles={d} keys={d} dups={d} named_keys={d} write_map={} map_async={} fixed_map={} commit_backend={s}\n",
        .{
            @tagName(lmdb_engine.selected_backend),
            cfg.samples,
            cfg.cycles,
            cfg.keys,
            cfg.dups,
            cfg.named_keys,
            cfg.write_map,
            cfg.map_async,
            cfg.fixed_map,
            @tagName(cfg.commit_backend),
        },
    );

    var summary = Summary{};
    var sample_index: usize = 0;
    while (sample_index < cfg.samples) : (sample_index += 1) {
        const kv = try runKvRoundtrip(cfg);
        try printResult(&stdout_writer.interface, kv.result);
        try printPhaseResult(&stdout_writer.interface, "kv_roundtrip", "write_open", kv.write_open_ns);
        try printPhaseResult(&stdout_writer.interface, "kv_roundtrip", "put_loop", kv.put_loop_ns);
        try printPhaseResult(&stdout_writer.interface, "kv_roundtrip", "commit", kv.commit_ns);
        try printPhaseResult(&stdout_writer.interface, "kv_roundtrip", "read_open", kv.read_open_ns);
        try printPhaseResult(&stdout_writer.interface, "kv_roundtrip", "read_get", kv.read_get_ns);
        try printPhaseResult(&stdout_writer.interface, "kv_roundtrip", "publish", kv.publish_ns);
        try printPhaseResult(&stdout_writer.interface, "kv_roundtrip", "page_write", kv.page_write_ns);
        try printPhaseResult(&stdout_writer.interface, "kv_roundtrip", "data_sync", kv.data_sync_ns);
        try printPhaseResult(&stdout_writer.interface, "kv_roundtrip", "meta_write", kv.meta_write_ns);
        try printPhaseResult(&stdout_writer.interface, "kv_roundtrip", "meta_sync", kv.meta_sync_ns);
        summary.kv_total.add(kv.result.ns);
        summary.kv_write_open.add(kv.write_open_ns);
        summary.kv_put_loop.add(kv.put_loop_ns);
        summary.kv_commit.add(kv.commit_ns);
        summary.kv_read_open.add(kv.read_open_ns);
        summary.kv_read_get.add(kv.read_get_ns);
        summary.kv_publish.add(kv.publish_ns);
        summary.kv_page_write.add(kv.page_write_ns);
        summary.kv_data_sync.add(kv.data_sync_ns);
        summary.kv_meta_write.add(kv.meta_write_ns);
        summary.kv_meta_sync.add(kv.meta_sync_ns);

        const dupsort = try runNamedDupsort(cfg);
        try printResult(&stdout_writer.interface, dupsort);
        summary.dupsort_total.add(dupsort.ns);

        const range_warm = try runRangeScanWarm(cfg);
        try printResult(&stdout_writer.interface, range_warm.result);
        try printPhaseResult(&stdout_writer.interface, "range_scan_warm", "read_open", range_warm.read_open_ns);
        try printPhaseResult(&stdout_writer.interface, "range_scan_warm", "seek", range_warm.seek_ns);
        try printPhaseResult(&stdout_writer.interface, "range_scan_warm", "scan_loop", range_warm.scan_loop_ns);
        summary.range_warm_total.add(range_warm.result.ns);
        summary.range_warm_open.add(range_warm.read_open_ns);
        summary.range_warm_seek.add(range_warm.seek_ns);
        summary.range_warm_scan_loop.add(range_warm.scan_loop_ns);

        const range_reopen = try runRangeScanReopen(cfg);
        try printResult(&stdout_writer.interface, range_reopen.result);
        try printPhaseResult(&stdout_writer.interface, "range_scan_reopen", "read_open", range_reopen.read_open_ns);
        try printPhaseResult(&stdout_writer.interface, "range_scan_reopen", "seek", range_reopen.seek_ns);
        try printPhaseResult(&stdout_writer.interface, "range_scan_reopen", "scan_loop", range_reopen.scan_loop_ns);
        summary.range_reopen_total.add(range_reopen.result.ns);
        summary.range_reopen_open.add(range_reopen.read_open_ns);
        summary.range_reopen_seek.add(range_reopen.seek_ns);
        summary.range_reopen_scan_loop.add(range_reopen.scan_loop_ns);

        const nested = try runNestedReopen(cfg);
        try printResult(&stdout_writer.interface, nested);
        summary.nested_total.add(nested.ns);
    }

    if (cfg.samples > 1) {
        try printSummary(&stdout_writer.interface, "kv_roundtrip", summary.kv_total);
        try printSummary(&stdout_writer.interface, "range_scan_warm", summary.range_warm_total);
        try printSummary(&stdout_writer.interface, "range_scan_reopen", summary.range_reopen_total);
        try printSummary(&stdout_writer.interface, "named_dupsort", summary.dupsort_total);
        try printSummary(&stdout_writer.interface, "nested_reopen", summary.nested_total);
        try printPhaseSummary(&stdout_writer.interface, "kv_roundtrip", "write_open", summary.kv_write_open);
        try printPhaseSummary(&stdout_writer.interface, "kv_roundtrip", "put_loop", summary.kv_put_loop);
        try printPhaseSummary(&stdout_writer.interface, "kv_roundtrip", "commit", summary.kv_commit);
        try printPhaseSummary(&stdout_writer.interface, "kv_roundtrip", "read_open", summary.kv_read_open);
        try printPhaseSummary(&stdout_writer.interface, "kv_roundtrip", "read_get", summary.kv_read_get);
        try printPhaseSummary(&stdout_writer.interface, "kv_roundtrip", "publish", summary.kv_publish);
        try printPhaseSummary(&stdout_writer.interface, "kv_roundtrip", "page_write", summary.kv_page_write);
        try printPhaseSummary(&stdout_writer.interface, "kv_roundtrip", "data_sync", summary.kv_data_sync);
        try printPhaseSummary(&stdout_writer.interface, "kv_roundtrip", "meta_write", summary.kv_meta_write);
        try printPhaseSummary(&stdout_writer.interface, "kv_roundtrip", "meta_sync", summary.kv_meta_sync);
        try printPhaseSummary(&stdout_writer.interface, "range_scan_warm", "read_open", summary.range_warm_open);
        try printPhaseSummary(&stdout_writer.interface, "range_scan_warm", "seek", summary.range_warm_seek);
        try printPhaseSummary(&stdout_writer.interface, "range_scan_warm", "scan_loop", summary.range_warm_scan_loop);
        try printPhaseSummary(&stdout_writer.interface, "range_scan_reopen", "read_open", summary.range_reopen_open);
        try printPhaseSummary(&stdout_writer.interface, "range_scan_reopen", "seek", summary.range_reopen_seek);
        try printPhaseSummary(&stdout_writer.interface, "range_scan_reopen", "scan_loop", summary.range_reopen_scan_loop);
    }
    try stdout_writer.flush();
}

fn printResult(writer: anytype, result: WorkloadResult) !void {
    const secs = @as(f64, @floatFromInt(result.ns)) / 1e9;
    const ops_per_sec = @as(f64, @floatFromInt(result.ops)) / secs;
    try writer.print(
        "{{\"backend\":\"{s}\",\"workload\":\"{s}\",\"ops\":{d},\"ns\":{d},\"ops_per_sec\":{d:.2}}}\n",
        .{ @tagName(lmdb_engine.selected_backend), result.name, result.ops, result.ns, ops_per_sec },
    );
}

fn printPhaseResult(writer: anytype, workload: []const u8, phase: []const u8, ns: u64) !void {
    try writer.print(
        "{{\"backend\":\"{s}\",\"workload\":\"{s}\",\"phase\":\"{s}\",\"ns\":{d}}}\n",
        .{ @tagName(lmdb_engine.selected_backend), workload, phase, ns },
    );
}

fn printSummary(writer: anytype, workload: []const u8, stats: NsStats) !void {
    try writer.print(
        "{{\"backend\":\"{s}\",\"workload\":\"{s}\",\"summary\":true,\"samples\":{d},\"avg_ns\":{d},\"min_ns\":{d},\"max_ns\":{d}}}\n",
        .{ @tagName(lmdb_engine.selected_backend), workload, stats.count, stats.avg(), stats.min, stats.max },
    );
}

fn printPhaseSummary(writer: anytype, workload: []const u8, phase: []const u8, stats: NsStats) !void {
    try writer.print(
        "{{\"backend\":\"{s}\",\"workload\":\"{s}\",\"phase\":\"{s}\",\"summary\":true,\"samples\":{d},\"avg_ns\":{d},\"min_ns\":{d},\"max_ns\":{d}}}\n",
        .{ @tagName(lmdb_engine.selected_backend), workload, phase, stats.count, stats.avg(), stats.min, stats.max },
    );
}

fn parseArgs(alloc: std.mem.Allocator, proc_args: std.process.Args) !Config {
    var cfg = Config{};
    var args = try std.process.Args.Iterator.initAllocator(proc_args, alloc);
    defer args.deinit();
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--cycles")) {
            cfg.cycles = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--keys")) {
            cfg.keys = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--dups")) {
            cfg.dups = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--named-keys")) {
            cfg.named_keys = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--write-map")) {
            cfg.write_map = true;
        } else if (std.mem.eql(u8, arg, "--map-async")) {
            cfg.map_async = true;
        } else if (std.mem.eql(u8, arg, "--fixed-map")) {
            cfg.fixed_map = true;
        } else if (std.mem.eql(u8, arg, "--worker-thread")) {
            cfg.commit_backend = .worker_thread;
        } else if (std.mem.eql(u8, arg, "--async-io")) {
            cfg.commit_backend = .async_io;
        } else if (std.mem.eql(u8, arg, "--adaptive")) {
            cfg.commit_backend = .adaptive;
        } else {
            return error.InvalidArgument;
        }
    }

    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const value = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return std.fmt.parseUnsigned(usize, value, 10);
}

fn envOptions(cfg: Config) lmdb.EnvironmentOptions {
    return .{
        .max_dbs = 4,
        .map_size = 64 * 1024 * 1024,
        .write_map = cfg.write_map,
        .map_async = cfg.map_async,
        .fixed_map = cfg.fixed_map,
        .commit_backend = cfg.commit_backend,
    };
}

fn runKvRoundtrip(cfg: Config) !KvRoundtripMetrics {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tempPath(&tmp_buf, "kv");
    defer cleanupTempDir(tmp_path);

    const start = nanotime();
    var ops: usize = 0;
    var metrics = KvRoundtripMetrics{
        .result = .{
            .name = "kv_roundtrip",
            .ops = 0,
            .ns = 0,
        },
    };

    var cycle: usize = 0;
    while (cycle < cfg.cycles) : (cycle += 1) {
        {
            const open_start = nanotime();
            var env = try lmdb.Environment.open(tmp_path, envOptions(cfg));
            metrics.write_open_ns += nanotime() - open_start;
            defer env.close();
            const commit_stats_before = env.commitStatsSnapshot();

            const begin_start = nanotime();
            var txn = try env.begin(.{});
            metrics.write_open_ns += nanotime() - begin_start;
            errdefer txn.abort();

            const dbi = try txn.openDb(null, .{ .create = true });
            var key_buf: [32]u8 = undefined;
            var value_buf: [32]u8 = undefined;
            const put_start = nanotime();
            var i: usize = 0;
            while (i < cfg.keys) : (i += 1) {
                const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>2}-{d:0>4}", .{ cycle, i });
                const value = try std.fmt.bufPrint(&value_buf, "v-{d:0>2}-{d:0>4}", .{ cycle, i });
                try txn.put(dbi, key, value, .{});
                ops += 1;
            }
            metrics.put_loop_ns += nanotime() - put_start;
            const commit_start = nanotime();
            try txn.commit();
            metrics.commit_ns += nanotime() - commit_start;
            if (commit_stats_before) |before| {
                if (env.commitStatsSnapshot()) |after| {
                    metrics.publish_ns += after.total_publish_ns - before.total_publish_ns;
                    metrics.page_write_ns += after.total_page_write_ns - before.total_page_write_ns;
                    metrics.data_sync_ns += after.total_data_sync_ns - before.total_data_sync_ns;
                    metrics.meta_write_ns += after.total_meta_write_ns - before.total_meta_write_ns;
                    metrics.meta_sync_ns += after.total_meta_sync_ns - before.total_meta_sync_ns;
                }
            }
        }

        {
            const open_start = nanotime();
            var env = try lmdb.Environment.open(tmp_path, envOptions(cfg));
            metrics.read_open_ns += nanotime() - open_start;
            defer env.close();

            const begin_start = nanotime();
            var txn = try env.begin(.{ .read_only = true });
            metrics.read_open_ns += nanotime() - begin_start;
            defer txn.abort();

            const dbi = try txn.openDb(null, .{});
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>2}-{d:0>4}", .{ cycle, cfg.keys - 1 });
            const get_start = nanotime();
            _ = try txn.get(dbi, key);
            metrics.read_get_ns += nanotime() - get_start;
            ops += 1;
        }
    }

    metrics.result.ops = ops;
    metrics.result.ns = nanotime() - start;
    return metrics;
}

fn runNamedDupsort(cfg: Config) !WorkloadResult {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tempPath(&tmp_buf, "dups");
    defer cleanupTempDir(tmp_path);

    const start = nanotime();
    var ops: usize = 0;

    var cycle: usize = 0;
    while (cycle < cfg.cycles) : (cycle += 1) {
        {
            var env = try lmdb.Environment.open(tmp_path, envOptions(cfg));
            defer env.close();

            var txn = try env.begin(.{});
            errdefer txn.abort();

            const docs = try txn.openDb("docs", .{ .create = true });
            const dups = try txn.openDb("dups", .{ .create = true, .dup_sort = true });
            var key_buf: [32]u8 = undefined;
            var value_buf: [32]u8 = undefined;

            var i: usize = 0;
            while (i < cfg.named_keys) : (i += 1) {
                const key = try std.fmt.bufPrint(&key_buf, "doc-{d:0>2}-{d:0>4}", .{ cycle, i });
                const value = try std.fmt.bufPrint(&value_buf, "body-{d:0>2}-{d:0>4}", .{ cycle, i });
                try txn.put(docs, key, value, .{});
                ops += 1;
            }

            i = 0;
            while (i < cfg.dups) : (i += 1) {
                const dup_value = try std.fmt.bufPrint(&value_buf, "dup-{d:0>2}-{d:0>4}", .{ cycle, i });
                try txn.put(dups, "dup", dup_value, .{});
                ops += 1;
            }

            if (cycle > 0 and cycle % 3 == 0) {
                var cur = try txn.cursor(dups);
                defer cur.close();
                _ = try cur.seekExact("dup");
                try cur.deleteEntry();
                ops += 1;
            }

            try txn.commit();
        }

        {
            var env = try lmdb.Environment.open(tmp_path, envOptions(cfg));
            defer env.close();

            var txn = try env.begin(.{ .read_only = true });
            defer txn.abort();

            const dups = try txn.openDb("dups", .{ .dup_sort = true });
            var cur = try txn.cursor(dups);
            defer cur.close();
            _ = try cur.seekExact("dup");
            ops += 1;
        }
    }

    return .{ .name = "named_dupsort", .ops = ops, .ns = nanotime() - start };
}

fn rangeScanShape(cfg: Config) struct { total_keys: usize, scan_keys: usize, stride: usize } {
    const total_keys = @max(cfg.cycles * cfg.keys * 32, @as(usize, 131072));
    const scan_keys = @min(total_keys, @max(cfg.keys * 16, @as(usize, 8192)));
    const stride = @max(@as(usize, 1), (total_keys - scan_keys) / @max(cfg.cycles, @as(usize, 1)));
    return .{ .total_keys = total_keys, .scan_keys = scan_keys, .stride = stride };
}

fn populateRangeScanDb(tmp_path: [*:0]const u8, cfg: Config, total_keys: usize) !void {
    var env = try lmdb.Environment.open(tmp_path, envOptions(cfg));
    defer env.close();

    var txn = try env.begin(.{});
    errdefer txn.abort();

    const dbi = try txn.openDb(null, .{ .create = true });
    var key_buf: [32]u8 = undefined;
    var value_buf: [32]u8 = undefined;
    var i: usize = 0;
    while (i < total_keys) : (i += 1) {
        const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>6}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "v-{d:0>6}", .{i});
        try txn.put(dbi, key, value, .{ .append = true });
    }
    try txn.commit();
}

fn runRangeScanWarm(cfg: Config) !RangeScanMetrics {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tempPath(&tmp_buf, "scan");
    defer cleanupTempDir(tmp_path);

    const shape = rangeScanShape(cfg);
    try populateRangeScanDb(tmp_path, cfg, shape.total_keys);

    var ops: usize = 0;
    var metrics = RangeScanMetrics{
        .result = .{
            .name = "range_scan_warm",
            .ops = 0,
            .ns = 0,
        },
    };

    const start = nanotime();
    const open_start = nanotime();
    var env = try lmdb.Environment.open(tmp_path, envOptions(cfg));
    metrics.read_open_ns += nanotime() - open_start;
    defer env.close();

    const begin_start = nanotime();
    var txn = try env.begin(.{ .read_only = true });
    metrics.read_open_ns += nanotime() - begin_start;
    defer txn.abort();

    const dbi = try txn.openDb(null, .{});
    var cycle: usize = 0;
    while (cycle < cfg.cycles) : (cycle += 1) {
        const start_index = @min(cycle * shape.stride, shape.total_keys - shape.scan_keys);
        var key_buf: [32]u8 = undefined;
        const start_key = try std.fmt.bufPrint(&key_buf, "k-{d:0>6}", .{start_index});
        {
            var scanner = try txn.rangeViewScanner(dbi, start_key);
            defer scanner.close();

            const seek_start = nanotime();
            const first_batch = try scanner.nextViewBatch();
            metrics.seek_ns += nanotime() - seek_start;
            const first_count = first_batch.len();
            ops += first_count;

            const scan_start = nanotime();
            var scanned: usize = first_count;
            while (scanned < shape.scan_keys) {
                const batch = scanner.nextViewBatch() catch |err| switch (err) {
                    lmdb.Error.NotFound => break,
                    else => return err,
                };
                const count = @min(batch.len(), shape.scan_keys - scanned);
                scanned += count;
                ops += count;
            }
            metrics.scan_loop_ns += nanotime() - scan_start;
        }
    }

    metrics.result.ops = ops;
    metrics.result.ns = nanotime() - start;
    return metrics;
}

fn runRangeScanReopen(cfg: Config) !RangeScanMetrics {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tempPath(&tmp_buf, "scanr");
    defer cleanupTempDir(tmp_path);

    const shape = rangeScanShape(cfg);
    try populateRangeScanDb(tmp_path, cfg, shape.total_keys);

    var ops: usize = 0;
    var metrics = RangeScanMetrics{
        .result = .{
            .name = "range_scan_reopen",
            .ops = 0,
            .ns = 0,
        },
    };

    const start = nanotime();
    var cycle: usize = 0;
    while (cycle < cfg.cycles) : (cycle += 1) {
        const open_start = nanotime();
        var env = try lmdb.Environment.open(tmp_path, envOptions(cfg));
        metrics.read_open_ns += nanotime() - open_start;
        defer env.close();

        const begin_start = nanotime();
        var txn = try env.begin(.{ .read_only = true });
        metrics.read_open_ns += nanotime() - begin_start;
        defer txn.abort();

        const dbi = try txn.openDb(null, .{});
        const start_index = @min(cycle * shape.stride, shape.total_keys - shape.scan_keys);
        var key_buf: [32]u8 = undefined;
        const start_key = try std.fmt.bufPrint(&key_buf, "k-{d:0>6}", .{start_index});
        {
            var scanner = try txn.rangeViewScanner(dbi, start_key);
            defer scanner.close();

            const seek_start = nanotime();
            const first_batch = try scanner.nextViewBatch();
            metrics.seek_ns += nanotime() - seek_start;
            const first_count = first_batch.len();
            ops += first_count;

            const scan_start = nanotime();
            var scanned: usize = first_count;
            while (scanned < shape.scan_keys) {
                const batch = scanner.nextViewBatch() catch |err| switch (err) {
                    lmdb.Error.NotFound => break,
                    else => return err,
                };
                const count = @min(batch.len(), shape.scan_keys - scanned);
                scanned += count;
                ops += count;
            }
            metrics.scan_loop_ns += nanotime() - scan_start;
        }
    }

    metrics.result.ops = ops;
    metrics.result.ns = nanotime() - start;
    return metrics;
}

fn runNestedReopen(cfg: Config) !WorkloadResult {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tempPath(&tmp_buf, "nested");
    defer cleanupTempDir(tmp_path);

    const start = nanotime();
    var ops: usize = 0;

    var cycle: usize = 0;
    while (cycle < cfg.cycles) : (cycle += 1) {
        {
            var env = try lmdb.Environment.open(tmp_path, envOptions(cfg));
            defer env.close();

            var parent = try env.begin(.{});
            errdefer parent.abort();

            const main_db = try parent.openDb(null, .{ .create = true });
            var value_buf: [32]u8 = undefined;
            const parent_value = try std.fmt.bufPrint(&value_buf, "p-{d:0>2}", .{cycle});
            try parent.put(main_db, "parent", parent_value, .{});
            ops += 1;

            var child = try parent.beginChild();
            var child_done = false;
            defer if (!child_done) child.abort();

            const child_value = try std.fmt.bufPrint(&value_buf, "c-{d:0>2}", .{cycle});
            try child.put(main_db, "child", child_value, .{});
            ops += 1;

            if (cycle % 2 == 0) {
                try child.commit();
                child_done = true;
            } else {
                child.abort();
                child_done = true;
            }

            const keep_value = try std.fmt.bufPrint(&value_buf, "k-{d:0>2}", .{cycle});
            try parent.put(main_db, "keep", keep_value, .{});
            ops += 1;
            try parent.commit();
        }

        {
            var env = try lmdb.Environment.open(tmp_path, envOptions(cfg));
            defer env.close();

            var txn = try env.begin(.{ .read_only = true });
            defer txn.abort();

            const main_db = try txn.openDb(null, .{});
            _ = try txn.get(main_db, "parent");
            _ = try txn.get(main_db, "keep");
            if (cycle % 2 == 0) {
                _ = try txn.get(main_db, "child");
            } else {
                _ = txn.get(main_db, "child") catch |err| switch (err) {
                    lmdb.Error.NotFound => {},
                    else => return err,
                };
            }
            ops += 3;
        }
    }

    return .{ .name = "nested_reopen", .ops = ops, .ns = nanotime() - start };
}

fn tempPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const ts = nanotime();
    const path = std.fmt.bufPrint(buf, "/tmp/antfly-lmdb-bench-{s}-{d}\x00", .{ label, ts }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(path.ptr)))) catch unreachable;
    return @ptrCast(path.ptr);
}

fn cleanupTempDir(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
