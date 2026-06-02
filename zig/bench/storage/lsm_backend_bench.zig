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
const antfly = @import("antfly_zig");

const backend_erased = antfly.storage_backend_erased;
const backend_types = antfly.storage_backend;
const bloom = antfly.bloom;
const platform_time = antfly.platform_time;

const Allocator = std.mem.Allocator;
const ReadStats = antfly.lsm_backend.Backend.ReadStats;
const CacheStats = antfly.lsm_backend.cache.Stats;

const bench_namespace: backend_types.Namespace = .{ .name = "docs" };

const Config = struct {
    samples: usize = 3,
    keys: usize = 20_000,
    value_size: usize = 128,
    hit_repeats: usize = 5,
    miss_repeats: usize = 5,
    short_scan_len: usize = 64,
    short_scan_repeats: usize = 16,
    full_scan_repeats: usize = 5,
    reopen_repeats: usize = 5,
    mixed_repeats: usize = 3,
    mixed_write_stride: usize = 16,
    concurrent_read_threads: usize = 8,
    concurrent_read_keys: usize = 256,
    concurrent_read_repeats: usize = 4,
    flush_threshold: usize = 512,
    compact_threshold_runs: usize = 16,
    level_target_runs_base: usize = 4,
    level_target_runs_multiplier: usize = 4,
    level_target_bytes_base: usize = 128 * 1024,
    level_target_bytes_multiplier: usize = 8,
    bloom_bits_per_key: usize = 10,
    bloom_min_bits: usize = 64,
    lsm_io_runtime: antfly.lsm_backend.IoRuntime = .threaded,
    cache_mode: CacheSelection = .both,
    storage_mode: StorageSelection = .host,
    cache_bytes: usize = antfly.lsm_backend.DefaultCacheSizeBytes,
};

const CacheSelection = enum {
    off,
    on,
    both,
};

const StorageSelection = enum {
    host,
    memory,
    both,
};

const KeySet = struct {
    keys: [][]u8,

    fn deinit(self: *const KeySet, allocator: Allocator) void {
        for (self.keys) |key| allocator.free(key);
        allocator.free(self.keys);
    }
};

const StorageCounters = struct {
    read_file: u64 = 0,
    read_range: u64 = 0,
    read_trailer: u64 = 0,
    file_size: u64 = 0,

    fn delta(after: StorageCounters, before: StorageCounters) StorageCounters {
        return .{
            .read_file = after.read_file - before.read_file,
            .read_range = after.read_range - before.read_range,
            .read_trailer = after.read_trailer - before.read_trailer,
            .file_size = after.file_size - before.file_size,
        };
    }
};

const Snapshot = struct {
    read: ReadStats,
    storage: StorageCounters,
    cache: CacheStats,
};

const latency_bucket_count = 48;

const LatencyStats = struct {
    ops: u64 = 0,
    total_ns: u64 = 0,
    max_ns: u64 = 0,
    buckets: [latency_bucket_count]u64 = [_]u64{0} ** latency_bucket_count,

    fn record(self: *LatencyStats, ns: u64) void {
        self.ops += 1;
        self.total_ns +|= ns;
        self.max_ns = @max(self.max_ns, ns);
        self.buckets[latencyBucket(ns)] += 1;
    }

    fn add(self: *LatencyStats, other: LatencyStats) void {
        self.ops +|= other.ops;
        self.total_ns +|= other.total_ns;
        self.max_ns = @max(self.max_ns, other.max_ns);
        for (&self.buckets, other.buckets) |*dst, src| dst.* +|= src;
    }

    fn percentileNs(self: LatencyStats, pct: u64) u64 {
        if (self.ops == 0) return 0;
        const target = @max(@as(u64, 1), (self.ops * pct + 99) / 100);
        var seen: u64 = 0;
        for (self.buckets, 0..) |count, idx| {
            seen += count;
            if (seen >= target) return bucketUpperBoundNs(idx);
        }
        return self.max_ns;
    }
};

const Barrier = struct {
    waiting: std.atomic.Value(usize) = .init(0),
    open: std.atomic.Value(bool) = .init(false),

    fn wait(self: *@This(), total: usize) void {
        const previous = self.waiting.fetchAdd(1, .acq_rel);
        if (previous + 1 == total) self.open.store(true, .release);
        while (!self.open.load(.acquire)) std.Thread.yield() catch {};
    }
};

const ConcurrentReadWorker = struct {
    store: *backend_erased.Store,
    barrier: *Barrier,
    keys: []const []u8,
    repeats: usize,
    stats: LatencyStats = .{},
    err: ?anyerror = null,

    fn run(self: *@This(), total_threads: usize) void {
        self.barrier.wait(total_threads);
        var txn = self.store.beginRead() catch |err| {
            self.err = err;
            return;
        };
        defer txn.abort();

        for (0..self.repeats) |_| {
            for (self.keys) |key| {
                const start = nanotime();
                const value = txn.get(key) catch |err| {
                    self.err = err;
                    return;
                };
                self.stats.record(nanotime() - start);
                std.mem.doNotOptimizeAway(value.len);
            }
        }
    }
};

const StorageHarness = struct {
    const CountingHost = struct {
        backing: *antfly.lsm_backend.MemoryStorage,
        counters: StorageCounters = .{},

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.read_file += 1;
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.read_range += 1;
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.file_size += 1;
            return self.backing.storage().fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.read_trailer += 1;
            return self.backing.storage().readFileTrailerAlloc(allocator, path, len);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    const host_vtable: antfly.lsm_backend.Storage.VTable = .{
        .create_dir_path = CountingHost.createDirPath,
        .read_file_alloc = CountingHost.readFileAlloc,
        .read_file_range_alloc = CountingHost.readFileRangeAlloc,
        .file_size = CountingHost.fileSize,
        .read_file_trailer_alloc = CountingHost.readFileTrailerAlloc,
        .write_file_absolute = CountingHost.writeFileAbsolute,
        .rename_absolute = CountingHost.renameAbsolute,
        .delete_file_absolute = CountingHost.deleteFileAbsolute,
        .delete_tree = CountingHost.deleteTree,
        .now_ns = CountingHost.nowNs,
    };

    allocator: Allocator,
    mode: StorageSelection,
    backing: *antfly.lsm_backend.MemoryStorage,
    host_ctx: ?*CountingHost = null,

    fn init(allocator: Allocator, mode: StorageSelection) !StorageHarness {
        const backing = try allocator.create(antfly.lsm_backend.MemoryStorage);
        errdefer allocator.destroy(backing);
        backing.* = antfly.lsm_backend.MemoryStorage.init(allocator);
        errdefer backing.deinit();

        var harness = StorageHarness{
            .allocator = allocator,
            .mode = mode,
            .backing = backing,
        };
        if (mode == .host) {
            const host_ctx = try allocator.create(CountingHost);
            errdefer allocator.destroy(host_ctx);
            host_ctx.* = .{ .backing = backing };
            harness.host_ctx = host_ctx;
        }
        return harness;
    }

    fn deinit(self: *StorageHarness) void {
        if (self.host_ctx) |host_ctx| self.allocator.destroy(host_ctx);
        self.backing.deinit();
        self.allocator.destroy(self.backing);
        self.* = undefined;
    }

    fn storage(self: *StorageHarness) antfly.lsm_backend.Storage {
        return switch (self.mode) {
            .host => antfly.lsm_backend.HostStorage.init(self.host_ctx.?, &host_vtable).storage(),
            .memory => self.backing.storage(),
            .both => unreachable,
        };
    }

    fn snapshotCounters(self: *const StorageHarness) StorageCounters {
        if (self.host_ctx) |host_ctx| return host_ctx.counters;
        return .{};
    }
};

const Scenario = struct {
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_kind: StorageSelection,
    cache_enabled: bool,
    label: []u8,
    root_dir: []u8,
    storage_harness: StorageHarness,
    cache_ptr: ?*antfly.lsm_backend.Cache = null,
    backend: antfly.lsm_backend.Backend,
    runtime: ?backend_erased.Store = null,
    namespace_runtime: ?backend_erased.NamespaceStore = null,

    fn init(
        allocator: Allocator,
        cfg: Config,
        sample_index: usize,
        storage_kind: StorageSelection,
        cache_enabled: bool,
    ) !Scenario {
        var storage_harness = try StorageHarness.init(allocator, storage_kind);
        errdefer storage_harness.deinit();

        const cache_ptr = if (cache_enabled) blk: {
            const cache = try allocator.create(antfly.lsm_backend.Cache);
            errdefer allocator.destroy(cache);
            cache.* = antfly.lsm_backend.Cache.init(allocator, cfg.cache_bytes);
            break :blk cache;
        } else null;
        errdefer if (cache_ptr) |cache| {
            cache.deinit();
            allocator.destroy(cache);
        };

        const storage_label = switch (storage_kind) {
            .host => "host",
            .memory => "memory",
            .both => unreachable,
        };
        const cache_label = if (cache_enabled) "cache" else "nocache";
        const label = try std.fmt.allocPrint(allocator, "{s}_{s}", .{ storage_label, cache_label });
        errdefer allocator.free(label);
        const root_dir = try std.fmt.allocPrint(allocator, "/lsm-backend-bench-{s}-{d}", .{ label, sample_index });
        errdefer allocator.free(root_dir);

        const backend = try antfly.lsm_backend.Backend.open(allocator, root_dir, .{
            .backend = .{ .create_if_missing = true },
            .storage = storage_harness.storage(),
            .cache = cache_ptr,
            .flush_threshold = cfg.flush_threshold,
            .compact_threshold_runs = cfg.compact_threshold_runs,
            .level_target_runs_base = cfg.level_target_runs_base,
            .level_target_runs_multiplier = cfg.level_target_runs_multiplier,
            .level_target_bytes_base = cfg.level_target_bytes_base,
            .level_target_bytes_multiplier = cfg.level_target_bytes_multiplier,
            .bloom = bloomConfig(cfg),
            .io_runtime = cfg.lsm_io_runtime,
        });
        errdefer {
            var cleanup = backend;
            cleanup.close();
        }

        const scenario = Scenario{
            .allocator = allocator,
            .cfg = cfg,
            .sample_index = sample_index,
            .storage_kind = storage_kind,
            .cache_enabled = cache_enabled,
            .label = label,
            .root_dir = root_dir,
            .storage_harness = storage_harness,
            .cache_ptr = cache_ptr,
            .backend = backend,
        };
        return scenario;
    }

    fn deinit(self: *Scenario) void {
        self.closeRuntime();
        self.backend.close();
        if (self.cache_ptr) |cache| {
            cache.deinit();
            self.allocator.destroy(cache);
        }
        self.storage_harness.deinit();
        self.allocator.free(self.root_dir);
        self.allocator.free(self.label);
        self.* = undefined;
    }

    fn closeRuntime(self: *Scenario) void {
        if (self.runtime) |*runtime| runtime.deinit();
        if (self.namespace_runtime) |*runtime| runtime.deinit();
        self.runtime = null;
        self.namespace_runtime = null;
    }

    fn initRuntime(self: *Scenario) !void {
        self.runtime = try self.backend.runtimeStore(self.allocator, bench_namespace);
        errdefer self.closeRuntime();
        self.namespace_runtime = try self.backend.runtimeNamespaceStore(self.allocator);
    }

    fn reopen(self: *Scenario) !void {
        self.closeRuntime();
        self.backend.close();
        self.backend = try antfly.lsm_backend.Backend.open(self.allocator, self.root_dir, .{
            .backend = .{ .create_if_missing = true },
            .storage = self.storage_harness.storage(),
            .cache = self.cache_ptr,
            .flush_threshold = self.cfg.flush_threshold,
            .compact_threshold_runs = self.cfg.compact_threshold_runs,
            .level_target_runs_base = self.cfg.level_target_runs_base,
            .level_target_runs_multiplier = self.cfg.level_target_runs_multiplier,
            .level_target_bytes_base = self.cfg.level_target_bytes_base,
            .level_target_bytes_multiplier = self.cfg.level_target_bytes_multiplier,
            .bloom = bloomConfig(self.cfg),
            .io_runtime = self.cfg.lsm_io_runtime,
        });
        try self.initRuntime();
    }

    fn runtimeStore(self: *Scenario) *backend_erased.Store {
        return &self.runtime.?;
    }

    fn namespaceStore(self: *Scenario) *backend_erased.NamespaceStore {
        return &self.namespace_runtime.?;
    }

    fn snapshot(self: *const Scenario) Snapshot {
        return .{
            .read = self.backend.snapshotReadStats(),
            .storage = self.storage_harness.snapshotCounters(),
            .cache = self.backend.snapshotCacheStats() orelse .{},
        };
    }
};

fn nanotime() u64 {
    return platform_time.monotonicNs();
}

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(alloc, init.minimal.args);

    const keys = try makeKeys(alloc, "doc", cfg.keys);
    defer keys.deinit(alloc);
    const hit_keys = try shuffledKeys(alloc, keys.keys);
    defer alloc.free(hit_keys);
    const misses = try makeKeys(alloc, "miss", cfg.keys);
    defer misses.deinit(alloc);
    const miss_keys = try shuffledKeys(alloc, misses.keys);
    defer alloc.free(miss_keys);

    const value = try alloc.alloc(u8, cfg.value_size);
    defer alloc.free(value);
    @memset(value, 'x');
    const update_value = try alloc.alloc(u8, cfg.value_size);
    defer alloc.free(update_value);
    @memset(update_value, 'y');

    const short_scan_start_index = if (cfg.keys == 0) 0 else @min(cfg.keys / 2, cfg.keys - 1);
    const short_scan_key = if (cfg.keys == 0) "" else keys.keys[short_scan_start_index];
    const short_scan_expected = if (cfg.keys == 0) 0 else @min(cfg.short_scan_len, cfg.keys - short_scan_start_index);
    const reopen_probe_key = if (cfg.keys == 0) "" else keys.keys[cfg.keys / 2];
    const reopen_miss_key = if (cfg.keys == 0) "miss:00000000" else misses.keys[cfg.keys / 2];

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;

    try out.print(
        "lsm backend bench samples={d} keys={d} value_size={d} hit_repeats={d} miss_repeats={d} short_scan_len={d} short_scan_repeats={d} full_scan_repeats={d} reopen_repeats={d} mixed_repeats={d} concurrent_read_threads={d} concurrent_read_keys={d} concurrent_read_repeats={d} storage={s} cache={s}\n",
        .{
            cfg.samples,
            cfg.keys,
            cfg.value_size,
            cfg.hit_repeats,
            cfg.miss_repeats,
            cfg.short_scan_len,
            cfg.short_scan_repeats,
            cfg.full_scan_repeats,
            cfg.reopen_repeats,
            cfg.mixed_repeats,
            cfg.concurrent_read_threads,
            cfg.concurrent_read_keys,
            cfg.concurrent_read_repeats,
            @tagName(cfg.storage_mode),
            @tagName(cfg.cache_mode),
        },
    );
    try stdout_writer.flush();

    const storage_modes: []const StorageSelection = switch (cfg.storage_mode) {
        .host => &[_]StorageSelection{.host},
        .memory => &[_]StorageSelection{.memory},
        .both => &[_]StorageSelection{ .host, .memory },
    };
    const cache_modes: []const bool = switch (cfg.cache_mode) {
        .off => &[_]bool{false},
        .on => &[_]bool{true},
        .both => &[_]bool{ false, true },
    };

    for (storage_modes) |storage_mode| {
        for (cache_modes) |cache_enabled| {
            for (0..cfg.samples) |sample_index| {
                var scenario = try Scenario.init(alloc, cfg, sample_index, storage_mode, cache_enabled);
                defer scenario.deinit();
                try scenario.initRuntime();

                {
                    const before = scenario.snapshot();
                    const start = nanotime();
                    try benchLoadSorted(scenario.namespaceStore(), keys.keys, value);
                    const elapsed = nanotime() - start;
                    const after = scenario.snapshot();
                    try printResult(out, &scenario, "load_sorted", keys.keys.len, elapsed, before, after, null, null);
                    try stdout_writer.flush();
                }

                if (cfg.concurrent_read_threads > 0 and cfg.concurrent_read_keys > 0 and cfg.concurrent_read_repeats > 0) {
                    const concurrent_keys = hit_keys[0..@min(hit_keys.len, cfg.concurrent_read_keys)];
                    const before = scenario.snapshot();
                    const start = nanotime();
                    const latency = try benchConcurrentReadHits(
                        alloc,
                        scenario.runtimeStore(),
                        concurrent_keys,
                        cfg.concurrent_read_threads,
                        cfg.concurrent_read_repeats,
                    );
                    const elapsed = nanotime() - start;
                    const after = scenario.snapshot();
                    try printResult(out, &scenario, "concurrent_cold_read_hits", @intCast(latency.ops), elapsed, before, after, null, latency);
                    try stdout_writer.flush();
                }

                {
                    const before = scenario.snapshot();
                    const start = nanotime();
                    try benchReadHits(scenario.runtimeStore(), hit_keys, cfg.hit_repeats);
                    const elapsed = nanotime() - start;
                    const after = scenario.snapshot();
                    try printResult(out, &scenario, "warm_read_hits", hit_keys.len * cfg.hit_repeats, elapsed, before, after, null, null);
                    try stdout_writer.flush();
                }

                {
                    const before = scenario.snapshot();
                    const start = nanotime();
                    try benchReadMisses(scenario.runtimeStore(), miss_keys, cfg.miss_repeats);
                    const elapsed = nanotime() - start;
                    const after = scenario.snapshot();
                    try printResult(out, &scenario, "warm_read_misses", miss_keys.len * cfg.miss_repeats, elapsed, before, after, null, null);
                    try stdout_writer.flush();
                }

                {
                    const before = scenario.snapshot();
                    const start = nanotime();
                    try benchShortScan(scenario.runtimeStore(), short_scan_key, short_scan_expected, cfg.short_scan_repeats);
                    const elapsed = nanotime() - start;
                    const after = scenario.snapshot();
                    try printResult(out, &scenario, "warm_scan_short", short_scan_expected * cfg.short_scan_repeats, elapsed, before, after, null, null);
                    try stdout_writer.flush();
                }

                {
                    const before = scenario.snapshot();
                    const start = nanotime();
                    try benchFullScan(scenario.runtimeStore(), keys.keys.len, cfg.full_scan_repeats);
                    const elapsed = nanotime() - start;
                    const after = scenario.snapshot();
                    try printResult(out, &scenario, "warm_scan_full", keys.keys.len * cfg.full_scan_repeats, elapsed, before, after, null, null);
                    try stdout_writer.flush();
                }

                {
                    const before = scenario.snapshot();
                    const start = nanotime();
                    const reopen_read = try benchReopenOpenOnly(&scenario, cfg.reopen_repeats);
                    const elapsed = nanotime() - start;
                    const after = scenario.snapshot();
                    try printResult(out, &scenario, "reopen_open_only", cfg.reopen_repeats, elapsed, before, after, reopen_read, null);
                    try stdout_writer.flush();
                }

                {
                    const before = scenario.snapshot();
                    const start = nanotime();
                    const reopen_read = try benchReopenReadHit(&scenario, reopen_probe_key, cfg.reopen_repeats);
                    const elapsed = nanotime() - start;
                    const after = scenario.snapshot();
                    try printResult(out, &scenario, "reopen_read_hit", cfg.reopen_repeats, elapsed, before, after, reopen_read, null);
                    try stdout_writer.flush();
                }

                {
                    const before = scenario.snapshot();
                    const start = nanotime();
                    const reopen_read = try benchReopenReadMiss(&scenario, reopen_miss_key, cfg.reopen_repeats);
                    const elapsed = nanotime() - start;
                    const after = scenario.snapshot();
                    try printResult(out, &scenario, "reopen_read_miss", cfg.reopen_repeats, elapsed, before, after, reopen_read, null);
                    try stdout_writer.flush();
                }

                {
                    const before = scenario.snapshot();
                    const start = nanotime();
                    const reopen_read = try benchReopenShortScan(&scenario, short_scan_key, short_scan_expected, cfg.reopen_repeats);
                    const elapsed = nanotime() - start;
                    const after = scenario.snapshot();
                    try printResult(out, &scenario, "reopen_scan_short", short_scan_expected * cfg.reopen_repeats, elapsed, before, after, reopen_read, null);
                    try stdout_writer.flush();
                }

                const writes_per_round = std.math.divCeil(usize, keys.keys.len, @max(cfg.mixed_write_stride, 1)) catch unreachable;
                {
                    const before = scenario.snapshot();
                    const start = nanotime();
                    try benchMixedReadWrite(scenario.runtimeStore(), keys.keys, update_value, cfg.mixed_repeats, cfg.mixed_write_stride);
                    const elapsed = nanotime() - start;
                    const after = scenario.snapshot();
                    try printResult(out, &scenario, "mixed_read_write", cfg.mixed_repeats * (keys.keys.len + writes_per_round), elapsed, before, after, null, null);
                    try stdout_writer.flush();
                }
            }
        }
    }
    try stdout_writer.flush();
}

fn benchLoadSorted(store: *backend_erased.NamespaceStore, keys: []const []u8, value: []const u8) !void {
    var txn = try store.beginWrite();
    errdefer txn.abort();
    const can_append = store.capabilities().ordered_append_puts;
    for (keys) |key| {
        if (can_append) {
            try txn.appendPut(bench_namespace, key, value);
        } else {
            try txn.put(bench_namespace, key, value);
        }
    }
    try txn.commit();
}

fn benchReadHits(store: *backend_erased.Store, keys: []const []u8, repeats: usize) !void {
    var txn = try store.beginRead();
    defer txn.abort();
    for (0..repeats) |_| {
        for (keys) |key| {
            const value = try txn.get(key);
            std.mem.doNotOptimizeAway(value.len);
        }
    }
}

fn benchConcurrentReadHits(
    allocator: Allocator,
    store: *backend_erased.Store,
    keys: []const []u8,
    threads_count: usize,
    repeats: usize,
) !LatencyStats {
    if (keys.len == 0 or threads_count == 0 or repeats == 0) return .{};
    var barrier = Barrier{};
    const workers = try allocator.alloc(ConcurrentReadWorker, threads_count);
    defer allocator.free(workers);
    var threads = try allocator.alloc(std.Thread, threads_count);
    defer allocator.free(threads);

    var started: usize = 0;
    errdefer {
        for (threads[0..started]) |thread| thread.join();
    }

    for (workers, 0..) |*worker, i| {
        worker.* = .{
            .store = store,
            .barrier = &barrier,
            .keys = keys,
            .repeats = repeats,
        };
        threads[i] = try std.Thread.spawn(.{}, ConcurrentReadWorker.run, .{ worker, threads_count });
        started += 1;
    }

    var total: LatencyStats = .{};
    for (threads[0..started]) |thread| thread.join();
    started = 0;
    for (workers) |worker| {
        if (worker.err) |err| return err;
        total.add(worker.stats);
    }
    return total;
}

fn benchReadMisses(store: *backend_erased.Store, keys: []const []u8, repeats: usize) !void {
    var txn = try store.beginRead();
    defer txn.abort();
    for (0..repeats) |_| {
        for (keys) |key| {
            _ = txn.get(key) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
        }
    }
}

fn benchShortScan(store: *backend_erased.Store, start_key: []const u8, expected_count: usize, repeats: usize) !void {
    var txn = try store.beginRead();
    defer txn.abort();
    for (0..repeats) |_| {
        var cursor = try txn.openCursor();
        var entry = try cursor.seekAtOrAfter(start_key);
        var seen: usize = 0;
        while (entry) |current| : (entry = try cursor.next()) {
            seen += 1;
            std.mem.doNotOptimizeAway(current.key.len);
            if (seen >= expected_count) break;
        }
        cursor.close();
        if (seen != expected_count) return error.InvalidCursorCount;
    }
}

fn benchFullScan(store: *backend_erased.Store, expected_count: usize, repeats: usize) !void {
    var txn = try store.beginRead();
    defer txn.abort();
    for (0..repeats) |_| {
        var cursor = try txn.openCursor();
        var entry = try cursor.first();
        var seen: usize = 0;
        while (entry) |current| : (entry = try cursor.next()) {
            seen += 1;
            std.mem.doNotOptimizeAway(current.key.len);
        }
        cursor.close();
        if (seen != expected_count) return error.InvalidCursorCount;
    }
}

fn benchMixedReadWrite(
    store: *backend_erased.Store,
    keys: []const []u8,
    update_value: []const u8,
    repeats: usize,
    write_stride: usize,
) !void {
    const stride = @max(write_stride, 1);
    for (0..repeats) |_| {
        var read_txn = try store.beginRead();
        for (keys) |key| {
            const value = try read_txn.get(key);
            std.mem.doNotOptimizeAway(value.len);
        }
        read_txn.abort();

        var write_txn = try store.beginWrite();
        errdefer write_txn.abort();
        var i: usize = 0;
        while (i < keys.len) : (i += stride) {
            try write_txn.put(keys[i], update_value);
        }
        try write_txn.commit();
    }
}

fn benchReopenOpenOnly(scenario: *Scenario, repeats: usize) !ReadStats {
    for (0..repeats) |_| try scenario.reopen();
    return .{};
}

fn benchReopenReadHit(scenario: *Scenario, key: []const u8, repeats: usize) !ReadStats {
    var total: ReadStats = .{};
    for (0..repeats) |_| {
        try scenario.reopen();
        var txn = try scenario.runtimeStore().beginRead();
        errdefer txn.abort();
        const value = try txn.get(key);
        std.mem.doNotOptimizeAway(value.len);
        txn.abort();
        addReadStats(&total, scenario.backend.snapshotReadStats());
    }
    return total;
}

fn benchReopenReadMiss(scenario: *Scenario, key: []const u8, repeats: usize) !ReadStats {
    var total: ReadStats = .{};
    for (0..repeats) |_| {
        try scenario.reopen();
        var txn = try scenario.runtimeStore().beginRead();
        errdefer txn.abort();
        _ = txn.get(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        txn.abort();
        addReadStats(&total, scenario.backend.snapshotReadStats());
    }
    return total;
}

fn benchReopenShortScan(scenario: *Scenario, start_key: []const u8, expected_count: usize, repeats: usize) !ReadStats {
    var total: ReadStats = .{};
    for (0..repeats) |_| {
        try scenario.reopen();
        var txn = try scenario.runtimeStore().beginRead();
        errdefer txn.abort();
        var cursor = try txn.openCursor();
        var entry = try cursor.seekAtOrAfter(start_key);
        var seen: usize = 0;
        while (entry) |current| : (entry = try cursor.next()) {
            seen += 1;
            std.mem.doNotOptimizeAway(current.key.len);
            if (seen >= expected_count) break;
        }
        cursor.close();
        txn.abort();
        if (seen != expected_count) return error.InvalidCursorCount;
        addReadStats(&total, scenario.backend.snapshotReadStats());
    }
    return total;
}

fn printResult(
    writer: anytype,
    scenario: *const Scenario,
    workload: []const u8,
    ops: usize,
    ns: u64,
    before: Snapshot,
    after: Snapshot,
    read_override: ?ReadStats,
    latency_override: ?LatencyStats,
) !void {
    const secs = @as(f64, @floatFromInt(@max(ns, 1))) / 1e9;
    const ops_per_sec = @as(f64, @floatFromInt(ops)) / secs;
    const ns_per_op = @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(@max(ops, 1)));
    const read_delta = read_override orelse diffReadStats(after.read, before.read);
    const storage_delta = StorageCounters.delta(after.storage, before.storage);
    const cache_delta = diffCacheStats(after.cache, before.cache);
    const latency = latency_override orelse LatencyStats{};

    try writer.print(
        "{{\"scenario\":\"{s}\",\"storage\":\"{s}\",\"cache\":\"{s}\",\"sample\":{d},\"workload\":\"{s}\",\"ops\":{d},\"ns\":{d},\"ops_per_sec\":{d:.2},\"ns_per_op\":{d:.2}",
        .{
            scenario.label,
            @tagName(scenario.storage_kind),
            if (scenario.cache_enabled) "on" else "off",
            scenario.sample_index,
            workload,
            ops,
            ns,
            ops_per_sec,
            ns_per_op,
        },
    );
    try writer.print(
        ",\"latency_p50_ns\":{d},\"latency_p95_ns\":{d},\"latency_p99_ns\":{d},\"latency_max_ns\":{d},\"storage_read_file\":{d},\"storage_read_range\":{d},\"storage_read_trailer\":{d},\"storage_file_size\":{d}",
        .{
            latency.percentileNs(50),
            latency.percentileNs(95),
            latency.percentileNs(99),
            latency.max_ns,
            storage_delta.read_file,
            storage_delta.read_range,
            storage_delta.read_trailer,
            storage_delta.file_size,
        },
    );
    try writer.print(
        ",\"read_point_gets\":{d},\"read_run_probes\":{d},\"read_bloom_negatives\":{d},\"read_mutable_hits\":{d},\"read_l0_hits\":{d},\"read_level_hits\":{d},\"read_cursor_block_loads\":{d},\"read_cursor_block_reuses\":{d},\"read_cursor_value_borrows\":{d},\"read_cursor_value_copies\":{d},\"read_point_value_borrows\":{d},\"read_point_value_copies\":{d}",
        .{
            read_delta.point_gets,
            read_delta.run_probes,
            read_delta.bloom_negatives,
            read_delta.mutable_hits,
            read_delta.l0_hits,
            read_delta.level_hits,
            read_delta.cursor_block_loads,
            read_delta.cursor_block_reuses,
            read_delta.cursor_value_borrows,
            read_delta.cursor_value_copies,
            read_delta.point_value_borrows,
            read_delta.point_value_copies,
        },
    );
    try writer.print(
        ",\"read_table_entry_parses\":{d},\"read_table_entry_parse_ns\":{d},\"read_table_index_loads\":{d},\"read_table_index_decodes\":{d},\"read_table_block_loads\":{d},\"read_table_block_bytes\":{d},\"read_table_block_load_ns\":{d},\"read_shared_block_cache_hits\":{d},\"read_shared_block_cache_misses\":{d},\"read_local_block_cache_hits\":{d},\"read_local_block_cache_misses\":{d}",
        .{
            read_delta.table_entry_parses,
            read_delta.table_entry_parse_ns,
            read_delta.table_index_loads,
            read_delta.table_index_decodes,
            read_delta.table_block_loads,
            read_delta.table_block_bytes,
            read_delta.table_block_load_ns,
            read_delta.shared_block_cache_hits,
            read_delta.shared_block_cache_misses,
            read_delta.local_block_cache_hits,
            read_delta.local_block_cache_misses,
        },
    );
    try writer.print(
        ",\"cache_raw_hits\":{d},\"cache_raw_misses\":{d},\"cache_index_hits\":{d},\"cache_index_misses\":{d},\"cache_block_hits\":{d},\"cache_block_misses\":{d},\"cache_block_waits\":{d},\"cache_used_bytes_after\":{d},\"cache_entries_after\":{d}}}\n",
        .{
            cache_delta.run_table_raw.hits,
            cache_delta.run_table_raw.misses,
            cache_delta.run_table_index.hits,
            cache_delta.run_table_index.misses,
            cache_delta.run_table_block.hits,
            cache_delta.run_table_block.misses,
            cache_delta.run_table_block.waits,
            after.cache.used_bytes,
            after.cache.entry_count,
        },
    );
}

fn diffReadStats(after: ReadStats, before: ReadStats) ReadStats {
    return .{
        .point_gets = saturatingSub(after.point_gets, before.point_gets),
        .get_many_sorted_calls = saturatingSub(after.get_many_sorted_calls, before.get_many_sorted_calls),
        .get_many_sorted_keys = saturatingSub(after.get_many_sorted_keys, before.get_many_sorted_keys),
        .mutable_hits = saturatingSub(after.mutable_hits, before.mutable_hits),
        .l0_hits = saturatingSub(after.l0_hits, before.l0_hits),
        .level_hits = saturatingSub(after.level_hits, before.level_hits),
        .run_probes = saturatingSub(after.run_probes, before.run_probes),
        .bloom_negatives = saturatingSub(after.bloom_negatives, before.bloom_negatives),
        .table_entry_parses = saturatingSub(after.table_entry_parses, before.table_entry_parses),
        .table_entry_parse_ns = saturatingSub(after.table_entry_parse_ns, before.table_entry_parse_ns),
        .table_index_loads = saturatingSub(after.table_index_loads, before.table_index_loads),
        .table_index_decodes = saturatingSub(after.table_index_decodes, before.table_index_decodes),
        .table_block_loads = saturatingSub(after.table_block_loads, before.table_block_loads),
        .table_block_bytes = saturatingSub(after.table_block_bytes, before.table_block_bytes),
        .table_block_load_ns = saturatingSub(after.table_block_load_ns, before.table_block_load_ns),
        .shared_block_cache_hits = saturatingSub(after.shared_block_cache_hits, before.shared_block_cache_hits),
        .shared_block_cache_misses = saturatingSub(after.shared_block_cache_misses, before.shared_block_cache_misses),
        .local_block_cache_hits = saturatingSub(after.local_block_cache_hits, before.local_block_cache_hits),
        .local_block_cache_misses = saturatingSub(after.local_block_cache_misses, before.local_block_cache_misses),
        .cursor_block_loads = saturatingSub(after.cursor_block_loads, before.cursor_block_loads),
        .cursor_block_reuses = saturatingSub(after.cursor_block_reuses, before.cursor_block_reuses),
        .cursor_value_borrows = saturatingSub(after.cursor_value_borrows, before.cursor_value_borrows),
        .cursor_value_copies = saturatingSub(after.cursor_value_copies, before.cursor_value_copies),
        .point_value_borrows = saturatingSub(after.point_value_borrows, before.point_value_borrows),
        .point_value_copies = saturatingSub(after.point_value_copies, before.point_value_copies),
    };
}

fn addReadStats(total: *ReadStats, add: ReadStats) void {
    total.point_gets += add.point_gets;
    total.get_many_sorted_calls += add.get_many_sorted_calls;
    total.get_many_sorted_keys += add.get_many_sorted_keys;
    total.mutable_hits += add.mutable_hits;
    total.l0_hits += add.l0_hits;
    total.level_hits += add.level_hits;
    total.run_probes += add.run_probes;
    total.bloom_negatives += add.bloom_negatives;
    total.table_entry_parses += add.table_entry_parses;
    total.table_entry_parse_ns += add.table_entry_parse_ns;
    total.table_index_loads += add.table_index_loads;
    total.table_index_decodes += add.table_index_decodes;
    total.table_block_loads += add.table_block_loads;
    total.table_block_bytes += add.table_block_bytes;
    total.table_block_load_ns += add.table_block_load_ns;
    total.shared_block_cache_hits += add.shared_block_cache_hits;
    total.shared_block_cache_misses += add.shared_block_cache_misses;
    total.local_block_cache_hits += add.local_block_cache_hits;
    total.local_block_cache_misses += add.local_block_cache_misses;
    total.cursor_block_loads += add.cursor_block_loads;
    total.cursor_block_reuses += add.cursor_block_reuses;
    total.cursor_value_borrows += add.cursor_value_borrows;
    total.cursor_value_copies += add.cursor_value_copies;
    total.point_value_borrows += add.point_value_borrows;
    total.point_value_copies += add.point_value_copies;
}

fn diffCacheStats(after: CacheStats, before: CacheStats) CacheStats {
    return .{
        .used_bytes = after.used_bytes - before.used_bytes,
        .entry_count = after.entry_count - before.entry_count,
        .run_state = diffKindStats(after.run_state, before.run_state),
        .run_table_raw = diffKindStats(after.run_table_raw, before.run_table_raw),
        .run_table_index = diffKindStats(after.run_table_index, before.run_table_index),
        .run_table_block = diffKindStats(after.run_table_block, before.run_table_block),
    };
}

fn diffKindStats(
    after: antfly.lsm_backend.cache.KindStats,
    before: antfly.lsm_backend.cache.KindStats,
) antfly.lsm_backend.cache.KindStats {
    return .{
        .hits = after.hits - before.hits,
        .misses = after.misses - before.misses,
        .inserts = after.inserts - before.inserts,
        .evictions = after.evictions - before.evictions,
        .invalidations = after.invalidations - before.invalidations,
        .waits = after.waits - before.waits,
    };
}

fn saturatingSub(after: u64, before: u64) u64 {
    return if (after >= before) after - before else after;
}

fn latencyBucket(ns: u64) usize {
    if (ns == 0) return 0;
    const bits = 64 - @clz(ns);
    return @min(@as(usize, @intCast(bits)), latency_bucket_count - 1);
}

fn bucketUpperBoundNs(bucket: usize) u64 {
    if (bucket == 0) return 0;
    const shift: u6 = @intCast(@min(bucket, 63));
    return @as(u64, 1) << shift;
}

fn bloomConfig(cfg: Config) bloom.Config {
    return .{
        .bits_per_key = cfg.bloom_bits_per_key,
        .min_bits = cfg.bloom_min_bits,
    };
}

fn parseArgs(alloc: Allocator, proc_args: std.process.Args) !Config {
    var cfg = Config{};
    var args = try std.process.Args.Iterator.initAllocator(proc_args, alloc);
    defer args.deinit();
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--keys")) {
            cfg.keys = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--value-size")) {
            cfg.value_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--hit-repeats")) {
            cfg.hit_repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--miss-repeats")) {
            cfg.miss_repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--short-scan-len")) {
            cfg.short_scan_len = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--short-scan-repeats")) {
            cfg.short_scan_repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--full-scan-repeats")) {
            cfg.full_scan_repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--reopen-repeats")) {
            cfg.reopen_repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--mixed-repeats")) {
            cfg.mixed_repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--mixed-write-stride")) {
            cfg.mixed_write_stride = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--concurrent-read-threads")) {
            cfg.concurrent_read_threads = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--concurrent-read-keys")) {
            cfg.concurrent_read_keys = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--concurrent-read-repeats")) {
            cfg.concurrent_read_repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--flush-threshold")) {
            cfg.flush_threshold = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--compact-threshold-runs")) {
            cfg.compact_threshold_runs = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--level-target-runs-base")) {
            cfg.level_target_runs_base = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--level-target-runs-multiplier")) {
            cfg.level_target_runs_multiplier = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--level-target-bytes-base")) {
            cfg.level_target_bytes_base = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--level-target-bytes-multiplier")) {
            cfg.level_target_bytes_multiplier = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--bloom-bits-per-key")) {
            cfg.bloom_bits_per_key = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--bloom-min-bits")) {
            cfg.bloom_min_bits = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--cache-bytes")) {
            cfg.cache_bytes = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--cache")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.cache_mode = std.meta.stringToEnum(CacheSelection, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--storage")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.storage_mode = std.meta.stringToEnum(StorageSelection, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--lsm-io")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.lsm_io_runtime = std.meta.stringToEnum(antfly.lsm_backend.IoRuntime, value) orelse return error.InvalidArgument;
        } else {
            return error.InvalidArgument;
        }
    }

    if (cfg.keys == 0) return error.InvalidArgument;
    if (cfg.short_scan_len == 0) return error.InvalidArgument;
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const value = args.next() orelse return error.InvalidArgument;
    return std.fmt.parseInt(usize, value, 10) catch {
        std.debug.print("invalid value for {s}: {s}\n", .{ flag, value });
        return error.InvalidArgument;
    };
}

fn makeKeys(allocator: Allocator, prefix: []const u8, count: usize) !KeySet {
    const keys = try allocator.alloc([]u8, count);
    errdefer allocator.free(keys);
    for (keys, 0..) |*slot, i| {
        slot.* = try std.fmt.allocPrint(allocator, "{s}:{d:0>8}", .{ prefix, i });
    }
    return .{ .keys = keys };
}

fn shuffledKeys(allocator: Allocator, keys: []const []u8) ![][]u8 {
    var shuffled = try allocator.alloc([]u8, keys.len);
    for (keys, 0..) |key, i| shuffled[i] = @constCast(key);

    var prng = std.Random.DefaultPrng.init(@as(u64, 0x51a7cafe));
    const random = prng.random();
    var i: usize = shuffled.len;
    while (i > 1) {
        i -= 1;
        const j = random.uintLessThan(usize, i + 1);
        std.mem.swap([]u8, &shuffled[i], &shuffled[j]);
    }
    return shuffled;
}
