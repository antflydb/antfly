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
const derived_log = @import("derived_log");
const platform_time = derived_log.platform_time;

const Config = struct {
    samples: usize = 1,
    threads: usize = 4,
    appends_per_thread: usize = 64,
    payload_size: usize = 256,
    artificial_sync_delay_ns: u64 = 0,
    group_commit_window_ns: u64 = std.time.ns_per_ms,
    group_commit_max_requests: usize = 64,
    no_sync: bool = false,
    commit_backend: @FieldType(derived_log.OpenOptions, "commit_backend") = .sync,
};

const Barrier = struct {
    mutex: std.atomic.Mutex = .unlocked,
    waiting: usize = 0,
    open: bool = false,

    fn wait(self: *@This(), total: usize) void {
        var registered = false;
        while (true) {
            lockAtomic(&self.mutex);
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
    log: *derived_log.DerivedLog,
    barrier: *Barrier,
    payload: []const u8,
    appends: usize,
    err: ?anyerror = null,

    fn run(self: *@This(), total_threads: usize) void {
        self.barrier.wait(total_threads);
        var i: usize = 0;
        while (i < self.appends) : (i += 1) {
            _ = self.log.appendOpaque(self.payload) catch |err| {
                self.err = err;
                return;
            };
        }
    }
};

const RunResult = struct {
    elapsed_ns: u64,
    stats: derived_log.FullStats,
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
    elapsed_ns: NsStats = .{},
    publish_ns: NsStats = .{},
    elapsed_samples: std.ArrayListUnmanaged(u64) = .empty,
    publish_samples: std.ArrayListUnmanaged(u64) = .empty,
    grouped_commits: u64 = 0,
    physical_commits: u64 = 0,

    fn deinit(self: *Summary, alloc: std.mem.Allocator) void {
        self.elapsed_samples.deinit(alloc);
        self.publish_samples.deinit(alloc);
        self.* = undefined;
    }
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const cfg = try parseArgs(gpa, init.minimal.args);
    const total_ops = cfg.threads * cfg.appends_per_thread;
    var plain_summary = Summary{};
    var grouped_summary = Summary{};
    defer plain_summary.deinit(gpa);
    defer grouped_summary.deinit(gpa);

    var sample_idx: usize = 0;
    while (sample_idx < cfg.samples) : (sample_idx += 1) {
        const plain = try runCase(gpa, cfg, false);
        const grouped = try runCase(gpa, cfg, true);

        printResult("plain", cfg, total_ops, plain);
        printResult("grouped", cfg, total_ops, grouped);
        try accumulateSummary(gpa, &plain_summary, plain);
        try accumulateSummary(gpa, &grouped_summary, grouped);
    }

    if (cfg.samples > 1) {
        printSummary("plain", cfg, total_ops, plain_summary);
        printSummary("grouped", cfg, total_ops, grouped_summary);
    }
}

fn parseArgs(alloc: std.mem.Allocator, proc_args: std.process.Args) !Config {
    var cfg = Config{};
    var args = try std.process.Args.Iterator.initAllocator(proc_args, alloc);
    defer args.deinit();
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--threads")) {
            cfg.threads = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--appends")) {
            cfg.appends_per_thread = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--payload")) {
            cfg.payload_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--sync-delay-us")) {
            const delay_us = try parseNextU64(&args, arg);
            cfg.artificial_sync_delay_ns = delay_us * std.time.ns_per_us;
        } else if (std.mem.eql(u8, arg, "--window-ms")) {
            const window_ms = try parseNextU64(&args, arg);
            cfg.group_commit_window_ns = window_ms * std.time.ns_per_ms;
        } else if (std.mem.eql(u8, arg, "--max-requests")) {
            cfg.group_commit_max_requests = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--no-sync")) {
            cfg.no_sync = true;
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

fn parseNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const value = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return std.fmt.parseUnsigned(u64, value, 10);
}

fn runCase(alloc: std.mem.Allocator, cfg: Config, grouped: bool) !RunResult {
    var path_buf: [256]u8 = undefined;
    const path = benchTmpPath(&path_buf, if (grouped) "grouped" else "plain");
    cleanupBenchDirAt(path);

    var log = try derived_log.DerivedLog.open(path, .{
        .no_sync = cfg.no_sync,
        .artificial_sync_delay_ns = cfg.artificial_sync_delay_ns,
        .group_commit_window_ns = if (grouped) cfg.group_commit_window_ns else 0,
        .group_commit_max_requests = cfg.group_commit_max_requests,
        .commit_backend = cfg.commit_backend,
    });
    defer log.close();
    defer cleanupBenchDirAt(path);

    const payload = try alloc.alloc(u8, cfg.payload_size);
    defer alloc.free(payload);
    @memset(payload, 'x');

    var barrier = Barrier{};
    const workers = try alloc.alloc(Worker, cfg.threads);
    defer alloc.free(workers);
    const threads = try alloc.alloc(std.Thread, cfg.threads);
    defer alloc.free(threads);

    for (workers, 0..) |*worker, idx| {
        worker.* = .{
            .log = &log,
            .barrier = &barrier,
            .payload = payload,
            .appends = cfg.appends_per_thread,
        };
        threads[idx] = try std.Thread.spawn(.{}, Worker.run, .{ worker, cfg.threads });
    }

    const started = nowNs();
    for (threads) |thread| thread.join();
    const elapsed_ns = elapsedSince(started);

    for (workers) |worker| {
        if (worker.err) |err| return err;
    }

    return .{
        .elapsed_ns = elapsed_ns,
        .stats = log.fullStatsSnapshot(),
    };
}

fn printResult(label: []const u8, cfg: Config, total_ops: usize, result: RunResult) void {
    const elapsed_s = @as(f64, @floatFromInt(result.elapsed_ns)) / @as(f64, std.time.ns_per_s);
    const ops_per_s = @as(f64, @floatFromInt(total_ops)) / elapsed_s;
    std.debug.print(
        "{s}: commit_backend={s} threads={d} appends/thread={d} payload={d}B total_ops={d} elapsed={d:.3}ms ops/s={d:.1} commits={d} grouped={d} max_req/commit={d}",
        .{
            label,
            @tagName(cfg.commit_backend),
            cfg.threads,
            cfg.appends_per_thread,
            cfg.payload_size,
            total_ops,
            @as(f64, @floatFromInt(result.elapsed_ns)) / @as(f64, std.time.ns_per_ms),
            ops_per_s,
            result.stats.wal.physical_commits,
            result.stats.wal.grouped_commits,
            result.stats.wal.max_requests_per_commit,
        },
    );
    if (result.stats.commit) |commit| {
        const avg_publish_ms = if (commit.publish_calls == 0) 0 else (@as(f64, @floatFromInt(commit.total_publish_ns)) / @as(f64, @floatFromInt(commit.publish_calls))) /
            @as(f64, std.time.ns_per_ms);
        std.debug.print(" avg_publish={d:.3}ms page_write_ns={d} data_sync_ns={d} meta_sync_ns={d}", .{
            avg_publish_ms,
            commit.total_page_write_ns,
            commit.total_data_sync_ns,
            commit.total_meta_sync_ns,
        });
        std.debug.print(" selected(sync={d},worker={d},async={d})", .{
            commit.selected_sync_calls,
            commit.selected_worker_thread_calls,
            commit.selected_async_io_calls,
        });
    }
    std.debug.print("\n", .{});
}

fn accumulateSummary(alloc: std.mem.Allocator, summary: *Summary, result: RunResult) !void {
    summary.elapsed_ns.add(result.elapsed_ns);
    try summary.elapsed_samples.append(alloc, result.elapsed_ns);
    if (result.stats.commit) |commit| {
        if (commit.publish_calls > 0) {
            const publish_ns: u64 = @intCast(commit.total_publish_ns / commit.publish_calls);
            summary.publish_ns.add(publish_ns);
            try summary.publish_samples.append(alloc, publish_ns);
        }
    }
    summary.grouped_commits += result.stats.wal.grouped_commits;
    summary.physical_commits += result.stats.wal.physical_commits;
}

fn printSummary(label: []const u8, cfg: Config, total_ops: usize, summary: Summary) void {
    const elapsed_sorted = summary.elapsed_samples;
    std.sort.insertion(u64, elapsed_sorted.items, {}, lessThanU64);
    const avg_elapsed_ns = summary.elapsed_ns.avg();
    const elapsed_s = @as(f64, @floatFromInt(avg_elapsed_ns)) / @as(f64, std.time.ns_per_s);
    const ops_per_s = @as(f64, @floatFromInt(total_ops)) / elapsed_s;
    const avg_grouped = @as(f64, @floatFromInt(summary.grouped_commits)) / @as(f64, @floatFromInt(summary.elapsed_ns.count));
    const avg_commits = @as(f64, @floatFromInt(summary.physical_commits)) / @as(f64, @floatFromInt(summary.elapsed_ns.count));
    const median_elapsed_ns = percentileSorted(elapsed_sorted.items, 50);
    const p95_elapsed_ns = percentileSorted(elapsed_sorted.items, 95);
    std.debug.print(
        "{s} summary: commit_backend={s} samples={d} avg_elapsed={d:.3}ms median_elapsed={d:.3}ms p95_elapsed={d:.3}ms avg_ops/s={d:.1} min_elapsed={d:.3}ms max_elapsed={d:.3}ms avg_commits={d:.1} avg_grouped={d:.1}",
        .{
            label,
            @tagName(cfg.commit_backend),
            summary.elapsed_ns.count,
            @as(f64, @floatFromInt(avg_elapsed_ns)) / @as(f64, std.time.ns_per_ms),
            @as(f64, @floatFromInt(median_elapsed_ns)) / @as(f64, std.time.ns_per_ms),
            @as(f64, @floatFromInt(p95_elapsed_ns)) / @as(f64, std.time.ns_per_ms),
            ops_per_s,
            @as(f64, @floatFromInt(summary.elapsed_ns.min)) / @as(f64, std.time.ns_per_ms),
            @as(f64, @floatFromInt(summary.elapsed_ns.max)) / @as(f64, std.time.ns_per_ms),
            avg_commits,
            avg_grouped,
        },
    );
    if (summary.publish_ns.count > 0) {
        const publish_sorted = summary.publish_samples;
        std.sort.insertion(u64, publish_sorted.items, {}, lessThanU64);
        std.debug.print(" avg_publish={d:.3}ms median_publish={d:.3}ms p95_publish={d:.3}ms", .{
            @as(f64, @floatFromInt(summary.publish_ns.avg())) / @as(f64, std.time.ns_per_ms),
            @as(f64, @floatFromInt(percentileSorted(publish_sorted.items, 50))) / @as(f64, std.time.ns_per_ms),
            @as(f64, @floatFromInt(percentileSorted(publish_sorted.items, 95))) / @as(f64, std.time.ns_per_ms),
        });
    }
    std.debug.print("\n", .{});
}

fn percentileSorted(sorted: []const u64, pct: usize) u64 {
    if (sorted.len == 0) return 0;
    const idx = @min(sorted.len - 1, ((sorted.len - 1) * pct) / 100);
    return sorted[idx];
}

fn lessThanU64(_: void, lhs: u64, rhs: u64) bool {
    return lhs < rhs;
}

fn benchTmpPath(buf: []u8, suffix: []const u8) [*:0]const u8 {
    const base = "/tmp/antfly-derived-log-bench-";
    const ts = nowNs();
    const slice = std.fmt.bufPrint(buf, "{s}{s}-{d}\x00", .{ base, suffix, ts }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupBenchDirAt(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

fn nowNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(started: u64) u64 {
    return nowNs() - started;
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.Thread.yield() catch {};
}
