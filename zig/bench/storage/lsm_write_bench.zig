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

const backend_types = antfly.storage_backend;
const bloom = antfly.bloom;
const platform_time = antfly.platform_time;

const Allocator = std.mem.Allocator;
const BatchMode = backend_types.BatchMode;
const CompactionStats = antfly.lsm_backend.Backend.CompactionStats;
const WriteStats = antfly.lsm_backend.Backend.WriteStats;

const bench_namespace: backend_types.Namespace = .{ .name = "docs" };

const Config = struct {
    samples: usize = 3,
    keys: usize = 20_000,
    hot_keys: usize = 1_000,
    overwrite_rounds: usize = 20,
    hot_maintenance_steps: usize = 8,
    value_size: usize = 128,
    value_pattern: ValuePattern = .repeat,
    batch_size: usize = 1_000,
    update_stride: usize = 8,
    delete_stride: usize = 16,
    flush_threshold: usize = 512,
    flush_threshold_bytes: u64 = 64 * 1024 * 1024,
    bulk_ingest_flush_threshold_multiplier: usize = 8,
    compact_threshold_runs: usize = 4,
    l0_soft_limit_runs: usize = 0,
    l0_hard_limit_runs: usize = 0,
    l0_soft_limit_bytes: u64 = 0,
    l0_hard_limit_bytes: u64 = 0,
    level_target_runs_base: usize = 4,
    level_target_runs_multiplier: usize = 4,
    level_target_bytes_base: usize = 128 * 1024,
    level_target_bytes_multiplier: usize = 8,
    max_run_file_bytes: usize = 512 * 1024 * 1024,
    max_compaction_input_bytes: u64 = 0,
    max_compaction_input_allow_oversized_single_job: bool = true,
    background_io_budget_bytes: u64 = 0,
    background_io_allow_oversized_single_job: bool = true,
    bloom_bits_per_key: usize = 10,
    bloom_min_bits: usize = 64,
    lsm_io_runtime: antfly.lsm_backend.IoRuntime = .threaded,
    wal_sync_on_commit: bool = false,
    readers: usize = 0,
    storage_mode: StorageSelection = .host,
    mode: ModeSelection = .both,
    workload_set: WorkloadSet = .all,
};

const StorageSelection = enum {
    host,
    native,
    memory,
    both,
};

const ModeSelection = enum {
    default,
    bulk_ingest,
    both,
};

const ValuePattern = enum {
    repeat,
    deterministic,
    keyed,
};

const WorkloadSet = enum {
    all,
    hot_overwrite,
    l0_pressure,
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
    write_file: u64 = 0,
    write_bytes: u64 = 0,
    manifest_write_file: u64 = 0,
    manifest_write_bytes: u64 = 0,
    rename: u64 = 0,
    delete_file: u64 = 0,
    delete_tree: u64 = 0,

    fn delta(after: StorageCounters, before: StorageCounters) StorageCounters {
        return .{
            .read_file = after.read_file - before.read_file,
            .read_range = after.read_range - before.read_range,
            .read_trailer = after.read_trailer - before.read_trailer,
            .file_size = after.file_size - before.file_size,
            .write_file = after.write_file - before.write_file,
            .write_bytes = after.write_bytes - before.write_bytes,
            .manifest_write_file = after.manifest_write_file - before.manifest_write_file,
            .manifest_write_bytes = after.manifest_write_bytes - before.manifest_write_bytes,
            .rename = after.rename - before.rename,
            .delete_file = after.delete_file - before.delete_file,
            .delete_tree = after.delete_tree - before.delete_tree,
        };
    }
};

const RunSummary = struct {
    count: usize = 0,
    l0_count: usize = 0,
    max_level: u32 = 0,
    bytes: u64 = 0,
    entries: u64 = 0,
};

const Snapshot = struct {
    storage: StorageCounters,
    compaction: CompactionStats,
    write: WriteStats,
    maintenance: antfly.lsm_backend.Backend.MaintenanceStats,
    read: ReadStats,
    runs: RunSummary,
    obsolete_paths: usize,
    mutable_entries: usize,
};

const read_latency_bucket_count = 48;

const ReadStats = struct {
    ops: u64 = 0,
    misses: u64 = 0,
    errors: u64 = 0,
    total_ns: u64 = 0,
    max_ns: u64 = 0,
    buckets: [read_latency_bucket_count]u64 = [_]u64{0} ** read_latency_bucket_count,

    fn record(self: *ReadStats, ns: u64, result: ReadResult) void {
        self.ops += 1;
        self.total_ns +|= ns;
        self.max_ns = @max(self.max_ns, ns);
        self.buckets[latencyBucket(ns)] += 1;
        switch (result) {
            .hit => {},
            .miss => self.misses += 1,
            .err => self.errors += 1,
        }
    }

    fn add(self: *ReadStats, other: ReadStats) void {
        self.ops +|= other.ops;
        self.misses +|= other.misses;
        self.errors +|= other.errors;
        self.total_ns +|= other.total_ns;
        self.max_ns = @max(self.max_ns, other.max_ns);
        for (&self.buckets, other.buckets) |*dst, src| dst.* +|= src;
    }

    fn delta(after: ReadStats, before: ReadStats) ReadStats {
        var out: ReadStats = .{
            .ops = after.ops - before.ops,
            .misses = after.misses - before.misses,
            .errors = after.errors - before.errors,
            .total_ns = after.total_ns - before.total_ns,
            .max_ns = after.max_ns,
        };
        for (&out.buckets, after.buckets, before.buckets) |*dst, a, b| dst.* = a - b;
        return out;
    }

    fn avgNs(self: ReadStats) u64 {
        if (self.ops == 0) return 0;
        return self.total_ns / self.ops;
    }

    fn percentileNs(self: ReadStats, pct: u64) u64 {
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

const ReadResult = enum {
    hit,
    miss,
    err,
};

const StorageHarness = struct {
    const CountingStorage = struct {
        backing: antfly.lsm_backend.Storage,
        counters: StorageCounters = .{},

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.read_file += 1;
            return self.backing.readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.read_range += 1;
            return self.backing.readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.file_size += 1;
            return self.backing.fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.read_trailer += 1;
            return self.backing.readFileTrailerAlloc(allocator, path, len);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.write_file += 1;
            self.counters.write_bytes += contents.len;
            if (isManifestPath(path)) {
                self.counters.manifest_write_file += 1;
                self.counters.manifest_write_bytes += contents.len;
            }
            return self.backing.writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.rename += 1;
            return self.backing.renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.delete_file += 1;
            return self.backing.deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.delete_tree += 1;
            return self.backing.deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.nowNs();
        }
    };

    const host_vtable: antfly.lsm_backend.Storage.VTable = .{
        .create_dir_path = CountingStorage.createDirPath,
        .read_file_alloc = CountingStorage.readFileAlloc,
        .read_file_range_alloc = CountingStorage.readFileRangeAlloc,
        .file_size = CountingStorage.fileSize,
        .read_file_trailer_alloc = CountingStorage.readFileTrailerAlloc,
        .write_file_absolute = CountingStorage.writeFileAbsolute,
        .rename_absolute = CountingStorage.renameAbsolute,
        .delete_file_absolute = CountingStorage.deleteFileAbsolute,
        .delete_tree = CountingStorage.deleteTree,
        .now_ns = CountingStorage.nowNs,
    };

    allocator: Allocator,
    mode: StorageSelection,
    memory_backing: ?*antfly.lsm_backend.MemoryStorage = null,
    native_backing: ?*antfly.lsm_backend.storage_io.NativeStorage = null,
    counting_ctx: ?*CountingStorage = null,

    fn init(allocator: Allocator, mode: StorageSelection) !StorageHarness {
        var harness = StorageHarness{
            .allocator = allocator,
            .mode = mode,
        };

        switch (mode) {
            .host, .memory => {
                const backing = try allocator.create(antfly.lsm_backend.MemoryStorage);
                errdefer allocator.destroy(backing);
                backing.* = antfly.lsm_backend.MemoryStorage.init(allocator);
                errdefer backing.deinit();
                harness.memory_backing = backing;

                if (mode == .host) {
                    const counting_ctx = try allocator.create(CountingStorage);
                    errdefer allocator.destroy(counting_ctx);
                    counting_ctx.* = .{ .backing = backing.storage() };
                    harness.counting_ctx = counting_ctx;
                }
            },
            .native => {
                const backing = try allocator.create(antfly.lsm_backend.storage_io.NativeStorage);
                errdefer allocator.destroy(backing);
                backing.* = try antfly.lsm_backend.storage_io.NativeStorage.init(allocator, .threaded);
                errdefer backing.deinit();
                harness.native_backing = backing;

                const counting_ctx = try allocator.create(CountingStorage);
                errdefer allocator.destroy(counting_ctx);
                counting_ctx.* = .{ .backing = backing.storage() };
                harness.counting_ctx = counting_ctx;
            },
            .both => unreachable,
        }
        return harness;
    }

    fn deinit(self: *StorageHarness) void {
        if (self.counting_ctx) |counting_ctx| self.allocator.destroy(counting_ctx);
        if (self.native_backing) |backing| {
            backing.deinit();
            self.allocator.destroy(backing);
        }
        if (self.memory_backing) |backing| {
            backing.deinit();
            self.allocator.destroy(backing);
        }
        self.* = undefined;
    }

    fn storage(self: *StorageHarness) antfly.lsm_backend.Storage {
        return switch (self.mode) {
            .host, .native => antfly.lsm_backend.HostStorage.init(self.counting_ctx.?, &host_vtable).storage(),
            .memory => self.memory_backing.?.storage(),
            .both => unreachable,
        };
    }

    fn snapshotCounters(self: *const StorageHarness) StorageCounters {
        if (self.counting_ctx) |counting_ctx| return counting_ctx.counters;
        return .{};
    }
};

const Scenario = struct {
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_kind: StorageSelection,
    batch_mode: BatchMode,
    label: []u8,
    root_dir: []u8,
    storage_harness: StorageHarness,
    backend: antfly.lsm_backend.Backend,
    read_stats: ReadStats = .{},
    last_finalize_ns: u64 = 0,

    fn init(
        allocator: Allocator,
        cfg: Config,
        sample_index: usize,
        storage_kind: StorageSelection,
        batch_mode: BatchMode,
        workload_label: []const u8,
    ) !Scenario {
        var storage_harness = try StorageHarness.init(allocator, storage_kind);
        errdefer storage_harness.deinit();

        const label = try std.fmt.allocPrint(
            allocator,
            "{s}_{s}_{s}",
            .{ @tagName(storage_kind), @tagName(batch_mode), workload_label },
        );
        errdefer allocator.free(label);

        const root_dir = try std.fmt.allocPrint(
            allocator,
            "{s}/lsm-write-bench-{s}-{d}",
            .{
                if (storage_kind == .native) "/tmp" else "",
                label,
                sample_index,
            },
        );
        errdefer allocator.free(root_dir);

        const backend = try antfly.lsm_backend.Backend.open(allocator, root_dir, .{
            .backend = .{ .create_if_missing = true },
            .storage = storage_harness.storage(),
            .flush_threshold = cfg.flush_threshold,
            .flush_threshold_bytes = cfg.flush_threshold_bytes,
            .bulk_ingest_flush_threshold_multiplier = cfg.bulk_ingest_flush_threshold_multiplier,
            .compact_threshold_runs = cfg.compact_threshold_runs,
            .l0_soft_limit_runs = cfg.l0_soft_limit_runs,
            .l0_hard_limit_runs = cfg.l0_hard_limit_runs,
            .l0_soft_limit_bytes = cfg.l0_soft_limit_bytes,
            .l0_hard_limit_bytes = cfg.l0_hard_limit_bytes,
            .level_target_runs_base = cfg.level_target_runs_base,
            .level_target_runs_multiplier = cfg.level_target_runs_multiplier,
            .level_target_bytes_base = cfg.level_target_bytes_base,
            .level_target_bytes_multiplier = cfg.level_target_bytes_multiplier,
            .max_run_file_bytes = cfg.max_run_file_bytes,
            .max_compaction_input_bytes = cfg.max_compaction_input_bytes,
            .max_compaction_input_allow_oversized_single_job = cfg.max_compaction_input_allow_oversized_single_job,
            .background_io_budget_bytes = cfg.background_io_budget_bytes,
            .background_io_allow_oversized_single_job = cfg.background_io_allow_oversized_single_job,
            .bloom = bloomConfig(cfg),
            .io_runtime = cfg.lsm_io_runtime,
            .wal_sync_on_commit = cfg.wal_sync_on_commit,
        });
        errdefer {
            var cleanup = backend;
            cleanup.close();
        }

        return .{
            .allocator = allocator,
            .cfg = cfg,
            .sample_index = sample_index,
            .storage_kind = storage_kind,
            .batch_mode = batch_mode,
            .label = label,
            .root_dir = root_dir,
            .storage_harness = storage_harness,
            .backend = backend,
            .read_stats = .{},
            .last_finalize_ns = 0,
        };
    }

    fn deinit(self: *Scenario) void {
        self.backend.close();
        self.storage_harness.storage().deleteTree(self.root_dir) catch {};
        self.storage_harness.deinit();
        self.allocator.free(self.root_dir);
        self.allocator.free(self.label);
        self.* = undefined;
    }

    fn snapshot(self: *const Scenario) Snapshot {
        return .{
            .storage = self.storage_harness.snapshotCounters(),
            .compaction = self.backend.compaction_stats,
            .write = self.backend.snapshotWriteStats(),
            .maintenance = self.backend.snapshotMaintenanceStats(),
            .read = self.read_stats,
            .runs = summarizeRuns(&self.backend),
            .obsolete_paths = self.backend.obsolete_paths.items.len,
            .mutable_entries = self.backend.mutable.entries.items.len,
        };
    }

    fn finalizeWrites(self: *Scenario, session_active: bool) !void {
        const start = nanotime();
        if (session_active) {
            try self.backend.finishBulkIngestSessionWithOptions(.{ .compact = false });
        } else {
            try self.backend.finalizeDeferredStorageWork();
        }
        self.last_finalize_ns +|= nanotime() - start;
    }
};

fn nanotime() u64 {
    return platform_time.monotonicNs();
}

fn latencyBucket(ns: u64) usize {
    if (ns == 0) return 0;
    const bit = 63 - @clz(ns);
    return @min(@as(usize, @intCast(bit)), read_latency_bucket_count - 1);
}

fn bucketUpperBoundNs(idx: usize) u64 {
    if (idx >= 63) return std.math.maxInt(u64);
    return (@as(u64, 1) << @intCast(idx + 1)) - 1;
}

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(alloc, init.minimal.args);

    const keys = try makeKeys(alloc, "doc", cfg.keys);
    defer keys.deinit(alloc);
    const random_keys = try shuffledKeys(alloc, keys.keys);
    defer alloc.free(random_keys);

    const value = try alloc.alloc(u8, cfg.value_size);
    defer alloc.free(value);
    const update_value = try alloc.alloc(u8, cfg.value_size);
    defer alloc.free(update_value);
    fillValue(value, cfg.value_pattern, 0x1234_5678);
    fillValue(update_value, cfg.value_pattern, 0x9abc_def0);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;

    try out.print(
        "lsm write bench samples={d} keys={d} hot_keys={d} overwrite_rounds={d} value_size={d} value_pattern={s} batch_size={d} update_stride={d} delete_stride={d} flush_threshold={d} flush_threshold_bytes={d} compact_threshold_runs={d} l0_soft_limit_runs={d} l0_hard_limit_runs={d} l0_soft_limit_bytes={d} l0_hard_limit_bytes={d} max_run_file_bytes={d} max_compaction_input_bytes={d} max_compaction_input_allow_oversized_single_job={} background_io_budget_bytes={d} background_io_allow_oversized_single_job={} wal_sync_on_commit={} readers={d} storage={s} mode={s} workload_set={s}\n",
        .{
            cfg.samples,
            cfg.keys,
            cfg.hot_keys,
            cfg.overwrite_rounds,
            cfg.value_size,
            @tagName(cfg.value_pattern),
            cfg.batch_size,
            cfg.update_stride,
            cfg.delete_stride,
            cfg.flush_threshold,
            cfg.flush_threshold_bytes,
            cfg.compact_threshold_runs,
            cfg.l0_soft_limit_runs,
            cfg.l0_hard_limit_runs,
            cfg.l0_soft_limit_bytes,
            cfg.l0_hard_limit_bytes,
            cfg.max_run_file_bytes,
            cfg.max_compaction_input_bytes,
            cfg.max_compaction_input_allow_oversized_single_job,
            cfg.background_io_budget_bytes,
            cfg.background_io_allow_oversized_single_job,
            cfg.wal_sync_on_commit,
            cfg.readers,
            @tagName(cfg.storage_mode),
            @tagName(cfg.mode),
            @tagName(cfg.workload_set),
        },
    );
    try stdout_writer.flush();

    const storage_modes: []const StorageSelection = switch (cfg.storage_mode) {
        .host => &[_]StorageSelection{.host},
        .native => &[_]StorageSelection{.native},
        .memory => &[_]StorageSelection{.memory},
        .both => &[_]StorageSelection{ .host, .native, .memory },
    };
    const batch_modes: []const BatchMode = switch (cfg.mode) {
        .default => &[_]BatchMode{.default},
        .bulk_ingest => &[_]BatchMode{.bulk_ingest},
        .both => &[_]BatchMode{ .default, .bulk_ingest },
    };

    for (storage_modes) |storage_mode| {
        for (batch_modes) |batch_mode| {
            for (0..cfg.samples) |sample_index| {
                if (cfg.workload_set == .all) {
                    var scenario = try Scenario.init(alloc, cfg, sample_index, storage_mode, batch_mode, "sorted");
                    defer scenario.deinit();

                    try runTimed(out, &stdout_writer, &scenario, "load_sorted", keys.keys.len, struct {
                        fn run(ctx: *Scenario, local_keys: []const []u8, local_value: []const u8) !void {
                            try benchLoad(ctx, local_keys, local_value, true);
                        }
                    }.run, keys.keys, value);

                    const update_ops = stridedCount(keys.keys.len, cfg.update_stride);
                    try runTimed(out, &stdout_writer, &scenario, "overwrite_strided", update_ops, struct {
                        fn run(ctx: *Scenario, local_keys: []const []u8, local_value: []const u8) !void {
                            try benchOverwrite(ctx, local_keys, local_value);
                        }
                    }.run, keys.keys, update_value);

                    const delete_ops = stridedCount(keys.keys.len, cfg.delete_stride);
                    try runTimed(out, &stdout_writer, &scenario, "delete_strided", delete_ops, struct {
                        fn run(ctx: *Scenario, local_keys: []const []u8, _: []const u8) !void {
                            try benchDelete(ctx, local_keys);
                        }
                    }.run, keys.keys, value);
                }

                if (cfg.workload_set == .all) {
                    var scenario = try Scenario.init(alloc, cfg, sample_index, storage_mode, batch_mode, "random");
                    defer scenario.deinit();

                    try runTimed(out, &stdout_writer, &scenario, "load_random", random_keys.len, struct {
                        fn run(ctx: *Scenario, local_keys: []const []u8, local_value: []const u8) !void {
                            try benchLoad(ctx, local_keys, local_value, false);
                        }
                    }.run, random_keys, value);
                }

                if (cfg.workload_set == .l0_pressure) {
                    var scenario = try Scenario.init(alloc, cfg, sample_index, storage_mode, batch_mode, "l0_pressure");
                    defer scenario.deinit();

                    try runTimed(out, &stdout_writer, &scenario, "load_l0_runs", keys.keys.len, struct {
                        fn run(ctx: *Scenario, local_keys: []const []u8, local_value: []const u8) !void {
                            try benchL0PressureLoad(ctx, local_keys, local_value);
                        }
                    }.run, keys.keys, value);

                    if (cfg.hot_maintenance_steps > 0) {
                        try runTimed(out, &stdout_writer, &scenario, "maintenance_l0", cfg.hot_maintenance_steps, struct {
                            fn run(ctx: *Scenario, _: []const []u8, _: []const u8) !void {
                                try benchMaintenance(ctx);
                            }
                        }.run, keys.keys, value);
                    }
                    continue;
                }

                {
                    const hot_len = @min(cfg.hot_keys, keys.keys.len);
                    var scenario = try Scenario.init(alloc, cfg, sample_index, storage_mode, batch_mode, "hot_overwrite");
                    defer scenario.deinit();

                    try runTimed(out, &stdout_writer, &scenario, "load_base", keys.keys.len, struct {
                        fn run(ctx: *Scenario, local_keys: []const []u8, local_value: []const u8) !void {
                            try benchLoad(ctx, local_keys, local_value, true);
                        }
                    }.run, keys.keys, value);

                    const overwrite_ops = hot_len * cfg.overwrite_rounds;
                    try runTimed(out, &stdout_writer, &scenario, "overwrite_hotset", overwrite_ops, struct {
                        fn run(ctx: *Scenario, local_keys: []const []u8, local_value: []const u8) !void {
                            try benchHotOverwrite(ctx, local_keys, local_value);
                        }
                    }.run, keys.keys[0..hot_len], update_value);

                    if (cfg.hot_maintenance_steps > 0) {
                        try runTimed(out, &stdout_writer, &scenario, "maintenance_hotset", cfg.hot_maintenance_steps, struct {
                            fn run(ctx: *Scenario, _: []const []u8, _: []const u8) !void {
                                try benchMaintenance(ctx);
                            }
                        }.run, keys.keys[0..hot_len], value);
                    }
                }
            }
        }
    }

    try stdout_writer.flush();
}

fn runTimed(
    writer: anytype,
    stdout_writer: anytype,
    scenario: *Scenario,
    workload: []const u8,
    ops: usize,
    comptime run: fn (*Scenario, []const []u8, []const u8) anyerror!void,
    keys: []const []u8,
    value: []const u8,
) !void {
    scenario.last_finalize_ns = 0;
    const before = scenario.snapshot();
    const start = nanotime();
    try run(scenario, keys, value);
    const elapsed = nanotime() - start;
    const after = scenario.snapshot();
    try printResult(writer, scenario, workload, ops, elapsed, before, after);
    try stdout_writer.flush();
}

fn benchLoad(scenario: *Scenario, keys: []const []u8, value: []const u8, sorted: bool) !void {
    if (sorted and scenario.batch_mode == .bulk_ingest and scenario.cfg.value_pattern != .keyed) {
        try benchSortedRunIngest(scenario, keys, value);
        return;
    }

    const session_active = scenario.batch_mode == .bulk_ingest;
    if (session_active) {
        try scenario.backend.beginBulkIngestSession();
        errdefer scenario.backend.abortBulkIngestSession();
    }

    const scratch = try scenario.allocator.alloc(u8, if (scenario.cfg.value_pattern == .keyed) scenario.cfg.value_size else 0);
    defer scenario.allocator.free(scratch);

    var start: usize = 0;
    var op_index: usize = 0;
    while (start < keys.len) {
        const end = @min(start + scenario.cfg.batch_size, keys.len);
        var txn = try scenario.backend.beginBatchWithOptions(.{ .mode = scenario.batch_mode });
        errdefer txn.abort();
        for (keys[start..end]) |key| {
            const write_value = valueForWrite(scenario.cfg, value, scratch, key, op_index, 0x1234_5678);
            if (sorted) {
                try txn.appendPut(bench_namespace, key, write_value);
            } else {
                try txn.put(bench_namespace, key, write_value);
            }
            op_index += 1;
        }
        try txn.commit();
        start = end;
    }
    try scenario.finalizeWrites(session_active);
}

fn benchSortedRunIngest(scenario: *Scenario, keys: []const []u8, value: []const u8) !void {
    var entries = try scenario.allocator.alloc(antfly.lsm_backend.TableEntry, keys.len);
    defer scenario.allocator.free(entries);

    for (keys, 0..) |key, i| {
        entries[i] = .{
            .namespace_name = bench_namespace.name,
            .key = key,
            .value = value,
            .tombstone = false,
        };
    }
    try scenario.backend.ingestSortedTableEntries(entries);
}

fn benchL0PressureLoad(scenario: *Scenario, keys: []const []u8, value: []const u8) !void {
    const scratch = try scenario.allocator.alloc(u8, if (scenario.cfg.value_pattern == .keyed) scenario.cfg.value_size else 0);
    defer scenario.allocator.free(scratch);

    var start: usize = 0;
    var op_index: usize = 0;
    while (start < keys.len) {
        const end = @min(start + scenario.cfg.batch_size, keys.len);
        var txn = try scenario.backend.beginBatchWithOptions(.{ .mode = scenario.batch_mode });
        errdefer txn.abort();
        for (keys[start..end]) |key| {
            const write_value = valueForWrite(scenario.cfg, value, scratch, key, op_index, 0x1357_9bdf);
            try txn.put(bench_namespace, key, write_value);
            op_index += 1;
        }
        try txn.commit();
        try scenario.backend.flushBufferedWritesWithOptions(.{ .compact = false, .flush = true });
        start = end;
    }
}

fn benchOverwrite(scenario: *Scenario, keys: []const []u8, value: []const u8) !void {
    const session_active = scenario.batch_mode == .bulk_ingest;
    if (session_active) {
        try scenario.backend.beginBulkIngestSession();
        errdefer scenario.backend.abortBulkIngestSession();
    }

    const scratch = try scenario.allocator.alloc(u8, if (scenario.cfg.value_pattern == .keyed) scenario.cfg.value_size else 0);
    defer scenario.allocator.free(scratch);

    const stride = @max(scenario.cfg.update_stride, 1);
    var start: usize = 0;
    var op_index: usize = 0;
    while (start < keys.len) {
        var txn = try scenario.backend.beginBatchWithOptions(.{ .mode = scenario.batch_mode });
        errdefer txn.abort();
        var i = start;
        var written: usize = 0;
        while (i < keys.len and written < scenario.cfg.batch_size) : (i += stride) {
            const write_value = valueForWrite(scenario.cfg, value, scratch, keys[i], op_index, 0x9abc_def0);
            try txn.put(bench_namespace, keys[i], write_value);
            written += 1;
            op_index += 1;
        }
        try txn.commit();
        start = i;
    }
    try scenario.finalizeWrites(session_active);
}

fn benchHotOverwrite(scenario: *Scenario, keys: []const []u8, value: []const u8) !void {
    if (keys.len == 0 or scenario.cfg.overwrite_rounds == 0) return;

    if (scenario.cfg.readers > 0) {
        try benchHotOverwriteWithReaders(scenario, keys, value);
        return;
    }

    const session_active = scenario.batch_mode == .bulk_ingest;
    if (session_active) {
        try scenario.backend.beginBulkIngestSession();
        errdefer scenario.backend.abortBulkIngestSession();
    }

    const scratch = try scenario.allocator.alloc(u8, if (scenario.cfg.value_pattern == .keyed) scenario.cfg.value_size else 0);
    defer scenario.allocator.free(scratch);

    var key_index: usize = 0;
    var op_index: usize = 0;
    var remaining_ops = keys.len * scenario.cfg.overwrite_rounds;
    while (remaining_ops > 0) {
        var txn = try scenario.backend.beginBatchWithOptions(.{ .mode = scenario.batch_mode });
        errdefer txn.abort();
        var written: usize = 0;
        while (written < scenario.cfg.batch_size and remaining_ops > 0) : (written += 1) {
            const write_value = valueForWrite(scenario.cfg, value, scratch, keys[key_index], op_index, 0x9abc_def0);
            try txn.put(bench_namespace, keys[key_index], write_value);
            key_index += 1;
            if (key_index == keys.len) {
                key_index = 0;
            }
            op_index += 1;
            remaining_ops -= 1;
        }
        try txn.commit();
    }

    try scenario.finalizeWrites(session_active);
}

const ReadWorker = struct {
    scenario: *Scenario,
    keys: []const []u8,
    stop: *std.atomic.Value(bool),
    worker_index: usize,
    stats: ReadStats = .{},

    fn run(self: *@This()) void {
        if (self.keys.len == 0) return;
        var index = self.worker_index % self.keys.len;
        const stride = @max(@as(usize, 1), self.worker_index * 2 + 1);
        while (!self.stop.load(.acquire)) {
            const start = nanotime();
            var result: ReadResult = .hit;
            var txn = self.scenario.backend.beginRead() catch {
                self.stats.record(nanotime() - start, .err);
                index = (index + stride) % self.keys.len;
                continue;
            };
            _ = txn.get(bench_namespace, self.keys[index]) catch |err| switch (err) {
                error.NotFound => {
                    result = .miss;
                },
                else => {
                    result = .err;
                },
            };
            txn.abort();
            self.stats.record(nanotime() - start, result);
            index = (index + stride) % self.keys.len;
        }
    }
};

fn benchHotOverwriteWithReaders(scenario: *Scenario, keys: []const []u8, value: []const u8) !void {
    var stop = std.atomic.Value(bool).init(false);
    const workers = try scenario.allocator.alloc(ReadWorker, scenario.cfg.readers);
    defer scenario.allocator.free(workers);
    var threads = try scenario.allocator.alloc(std.Thread, scenario.cfg.readers);
    defer scenario.allocator.free(threads);

    var started: usize = 0;
    errdefer {
        stop.store(true, .release);
        for (threads[0..started]) |thread| thread.join();
    }

    for (workers, 0..) |*worker, i| {
        worker.* = .{
            .scenario = scenario,
            .keys = keys,
            .stop = &stop,
            .worker_index = i,
        };
        threads[i] = try std.Thread.spawn(.{}, ReadWorker.run, .{worker});
        started += 1;
    }

    try benchHotOverwriteNoReaders(scenario, keys, value);
    stop.store(true, .release);
    for (threads[0..started]) |thread| thread.join();
    started = 0;

    for (workers) |worker| scenario.read_stats.add(worker.stats);
}

fn benchHotOverwriteNoReaders(scenario: *Scenario, keys: []const []u8, value: []const u8) !void {
    const session_active = scenario.batch_mode == .bulk_ingest;
    if (session_active) {
        try scenario.backend.beginBulkIngestSession();
        errdefer scenario.backend.abortBulkIngestSession();
    }

    const scratch = try scenario.allocator.alloc(u8, if (scenario.cfg.value_pattern == .keyed) scenario.cfg.value_size else 0);
    defer scenario.allocator.free(scratch);

    var key_index: usize = 0;
    var op_index: usize = 0;
    var remaining_ops = keys.len * scenario.cfg.overwrite_rounds;
    while (remaining_ops > 0) {
        var txn = try scenario.backend.beginBatchWithOptions(.{ .mode = scenario.batch_mode });
        errdefer txn.abort();
        var written: usize = 0;
        while (written < scenario.cfg.batch_size and remaining_ops > 0) : (written += 1) {
            const write_value = valueForWrite(scenario.cfg, value, scratch, keys[key_index], op_index, 0x9abc_def0);
            try txn.put(bench_namespace, keys[key_index], write_value);
            key_index += 1;
            if (key_index == keys.len) {
                key_index = 0;
            }
            op_index += 1;
            remaining_ops -= 1;
        }
        try txn.commit();
    }

    try scenario.finalizeWrites(session_active);
}

fn benchMaintenance(scenario: *Scenario) !void {
    var steps: usize = 0;
    while (steps < scenario.cfg.hot_maintenance_steps) : (steps += 1) {
        if (!try scenario.backend.runMaintenanceStep()) break;
    }
    try scenario.finalizeWrites(false);
}

fn benchDelete(scenario: *Scenario, keys: []const []u8) !void {
    const session_active = scenario.batch_mode == .bulk_ingest;
    if (session_active) {
        try scenario.backend.beginBulkIngestSession();
        errdefer scenario.backend.abortBulkIngestSession();
    }

    const stride = @max(scenario.cfg.delete_stride, 1);
    var start: usize = 0;
    while (start < keys.len) {
        var txn = try scenario.backend.beginBatchWithOptions(.{ .mode = scenario.batch_mode });
        errdefer txn.abort();
        var i = start;
        var written: usize = 0;
        while (i < keys.len and written < scenario.cfg.batch_size) : (i += stride) {
            try txn.delete(bench_namespace, keys[i]);
            written += 1;
        }
        try txn.commit();
        start = i;
    }
    try scenario.finalizeWrites(session_active);
}

fn printResult(
    writer: anytype,
    scenario: *const Scenario,
    workload: []const u8,
    ops: usize,
    ns: u64,
    before: Snapshot,
    after: Snapshot,
) !void {
    const secs = @as(f64, @floatFromInt(@max(ns, 1))) / 1e9;
    const ops_per_sec = @as(f64, @floatFromInt(ops)) / secs;
    const ns_per_op = @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(@max(ops, 1)));
    const storage_delta = StorageCounters.delta(after.storage, before.storage);
    const compaction_delta = diffCompactionStats(after.compaction, before.compaction);
    const write_delta = diffWriteStats(after.write, before.write);
    const read_delta = ReadStats.delta(after.read, before.read);
    const finalize_ns = @min(scenario.last_finalize_ns, ns);
    const writer_ns = ns - finalize_ns;
    const effective_l0_soft_limit_runs = effectiveL0SoftLimitRuns(scenario.cfg);
    const effective_l0_hard_limit_runs = effectiveL0HardLimitRuns(scenario.cfg);

    try writer.print(
        "{{\"scenario\":\"{s}\",\"storage\":\"{s}\",\"mode\":\"{s}\",\"sample\":{d},\"workload\":\"{s}\",\"ops\":{d},\"logical_value_write_bytes\":{d},\"ns\":{d},\"writer_ns\":{d},\"finalize_ns\":{d},\"ops_per_sec\":{d:.2},\"ns_per_op\":{d:.2},\"config_compact_threshold_runs\":{d},\"config_l0_soft_limit_runs\":{d},\"config_l0_hard_limit_runs\":{d},\"config_effective_l0_soft_limit_runs\":{d},\"config_effective_l0_hard_limit_runs\":{d},\"config_l0_soft_limit_bytes\":{d},\"config_l0_hard_limit_bytes\":{d},\"config_level_target_runs_base\":{d},\"config_level_target_runs_multiplier\":{d},\"config_level_target_bytes_base\":{d},\"config_level_target_bytes_multiplier\":{d},\"config_max_run_file_bytes\":{d},\"config_max_compaction_input_bytes\":{d},\"config_max_compaction_input_allow_oversized_single_job\":{},\"config_background_io_budget_bytes\":{d},\"config_background_io_allow_oversized_single_job\":{}",
        .{
            scenario.label,
            @tagName(scenario.storage_kind),
            @tagName(scenario.batch_mode),
            scenario.sample_index,
            workload,
            ops,
            logicalValueWriteBytes(workload, ops, scenario.cfg.value_size),
            ns,
            writer_ns,
            finalize_ns,
            ops_per_sec,
            ns_per_op,
            scenario.cfg.compact_threshold_runs,
            scenario.cfg.l0_soft_limit_runs,
            scenario.cfg.l0_hard_limit_runs,
            effective_l0_soft_limit_runs,
            effective_l0_hard_limit_runs,
            scenario.cfg.l0_soft_limit_bytes,
            scenario.cfg.l0_hard_limit_bytes,
            scenario.cfg.level_target_runs_base,
            scenario.cfg.level_target_runs_multiplier,
            scenario.cfg.level_target_bytes_base,
            scenario.cfg.level_target_bytes_multiplier,
            scenario.cfg.max_run_file_bytes,
            scenario.cfg.max_compaction_input_bytes,
            scenario.cfg.max_compaction_input_allow_oversized_single_job,
            scenario.cfg.background_io_budget_bytes,
            scenario.cfg.background_io_allow_oversized_single_job,
        },
    );
    try writer.print(
        ",\"storage_write_file\":{d},\"storage_write_bytes\":{d},\"storage_manifest_write_file\":{d},\"storage_manifest_write_bytes\":{d},\"storage_rename\":{d},\"storage_delete_file\":{d},\"storage_delete_tree\":{d},\"storage_read_file\":{d},\"storage_read_range\":{d},\"storage_read_trailer\":{d},\"storage_file_size\":{d}",
        .{
            storage_delta.write_file,
            storage_delta.write_bytes,
            storage_delta.manifest_write_file,
            storage_delta.manifest_write_bytes,
            storage_delta.rename,
            storage_delta.delete_file,
            storage_delta.delete_tree,
            storage_delta.read_file,
            storage_delta.read_range,
            storage_delta.read_trailer,
            storage_delta.file_size,
        },
    );
    try writer.print(
        ",\"lsm_flushes\":{d},\"lsm_flush_input_entries\":{d},\"lsm_flush_output_runs\":{d},\"lsm_flush_output_bytes\":{d},\"lsm_flush_ns\":{d},\"lsm_table_file_writes\":{d},\"lsm_table_file_bytes\":{d},\"lsm_table_file_logical_entry_bytes\":{d},\"lsm_table_file_physical_entry_bytes\":{d},\"lsm_table_file_raw_blocks\":{d},\"lsm_table_file_compressed_blocks\":{d},\"lsm_table_file_compression_codec_mask\":{d},\"lsm_sorted_ingest_runs\":{d},\"lsm_sorted_ingest_bytes\":{d},\"lsm_sorted_ingest_ns\":{d},\"lsm_manifest_writes\":{d},\"lsm_manifest_bytes\":{d},\"lsm_manifest_ns\":{d},\"lsm_write_pressure_compactions\":{d},\"lsm_write_pressure_ns\":{d},\"lsm_wal_pressure_flushes\":{d},\"lsm_wal_pressure_ns\":{d},\"lsm_wal_append_records\":{d},\"lsm_wal_append_entries\":{d},\"lsm_wal_append_bytes\":{d},\"lsm_wal_append_ns\":{d},\"lsm_wal_sync_records\":{d},\"lsm_wal_sync_ns\":{d},\"lsm_wal_resets\":{d},\"lsm_wal_reset_ns\":{d}",
        .{
            write_delta.flushes,
            write_delta.flush_input_entries,
            write_delta.flush_output_runs,
            write_delta.flush_output_bytes,
            write_delta.flush_ns,
            write_delta.table_file_writes,
            write_delta.table_file_bytes,
            write_delta.table_file_logical_entry_bytes,
            write_delta.table_file_physical_entry_bytes,
            write_delta.table_file_raw_blocks,
            write_delta.table_file_compressed_blocks,
            write_delta.table_file_compression_codec_mask,
            write_delta.sorted_ingest_runs,
            write_delta.sorted_ingest_bytes,
            write_delta.sorted_ingest_ns,
            write_delta.manifest_writes,
            write_delta.manifest_bytes,
            write_delta.manifest_ns,
            write_delta.write_pressure_compactions,
            write_delta.write_pressure_ns,
            write_delta.wal_pressure_flushes,
            write_delta.wal_pressure_ns,
            write_delta.wal_append_records,
            write_delta.wal_append_entries,
            write_delta.wal_append_bytes,
            write_delta.wal_append_ns,
            write_delta.wal_sync_records,
            write_delta.wal_sync_ns,
            write_delta.wal_resets,
            write_delta.wal_reset_ns,
        },
    );
    try writer.print(
        ",\"read_ops\":{d},\"read_misses\":{d},\"read_errors\":{d},\"read_avg_ns\":{d},\"read_p50_ns\":{d},\"read_p95_ns\":{d},\"read_p99_ns\":{d},\"read_max_ns\":{d}",
        .{
            read_delta.ops,
            read_delta.misses,
            read_delta.errors,
            read_delta.avgNs(),
            read_delta.percentileNs(50),
            read_delta.percentileNs(95),
            read_delta.percentileNs(99),
            read_delta.max_ns,
        },
    );
    try writer.print(
        ",\"compactions\":{d},\"compaction_input_runs\":{d},\"compaction_input_bytes\":{d},\"compaction_output_bytes\":{d},\"compaction_ns\":{d},\"runs_after\":{d},\"l0_runs_after\":{d},\"overlapping_l0_runs_after\":{d},\"compactable_l0_runs_after\":{d},\"l0_bytes_after\":{d},\"level_overflow_runs_after\":{d},\"level_overflow_bytes_after\":{d},\"max_level_after\":{d},\"run_bytes_after\":{d},\"run_entries_after\":{d},\"obsolete_paths_after\":{d},\"mutable_entries_after\":{d},\"mutable_bytes_after\":{d},\"immutable_entries_after\":{d},\"immutable_bytes_after\":{d}",
        .{
            compaction_delta.compactions,
            compaction_delta.input_runs,
            compaction_delta.input_bytes,
            compaction_delta.output_bytes,
            write_delta.compaction_ns,
            after.runs.count,
            after.runs.l0_count,
            after.maintenance.overlapping_l0_runs,
            after.maintenance.compactable_l0_runs,
            after.maintenance.l0_bytes,
            after.maintenance.level_overflow_runs,
            after.maintenance.level_overflow_bytes,
            after.runs.max_level,
            after.runs.bytes,
            after.runs.entries,
            after.obsolete_paths,
            after.mutable_entries,
            after.maintenance.mutable_bytes,
            after.maintenance.immutable_entries,
            after.maintenance.immutable_bytes,
        },
    );
    try writer.print(
        ",\"wal_retained_segments_after\":{d},\"wal_retained_bytes_after\":{d},\"wal_checkpoint_oldest_retained_segment_after\":{d},\"wal_checkpoint_covered_through_segment_after\":{d},\"wal_checkpoint_current_segment_after\":{d},\"wal_checkpoint_lag_segments_after\":{d},\"wal_replay_retained_segments_after\":{d},\"wal_replay_retained_bytes_after\":{d},\"wal_replay_current_segment_after\":{d},\"compaction_scheduler_grants_after\":{d},\"compaction_scheduler_denied_capacity_after\":{d},\"compaction_scheduler_denied_resource_pressure_after\":{d},\"compaction_scheduler_remembered_pending_after\":{d},\"compaction_scheduler_remembered_candidates_after\":{d},\"compaction_scheduler_remembered_retries_after\":{d},\"compaction_scheduler_remembered_hits_after\":{d},\"compaction_scheduler_remembered_stale_after\":{d},\"compaction_scheduler_conflict_denials_after\":{d}",
        .{
            after.maintenance.wal_retained_segments,
            after.maintenance.wal_retained_bytes,
            after.maintenance.wal_checkpoint_oldest_retained_segment,
            after.maintenance.wal_checkpoint_covered_through_segment,
            after.maintenance.wal_checkpoint_current_segment,
            after.maintenance.wal_checkpoint_lag_segments,
            after.maintenance.wal_replay_retained_segments,
            after.maintenance.wal_replay_retained_bytes,
            after.maintenance.wal_replay_current_segment,
            after.maintenance.compaction_scheduler_grants,
            after.maintenance.compaction_scheduler_denied_capacity,
            after.maintenance.compaction_scheduler_denied_resource_pressure,
            after.maintenance.compaction_scheduler_remembered_pending,
            after.maintenance.compaction_scheduler_remembered_candidates,
            after.maintenance.compaction_scheduler_remembered_retries,
            after.maintenance.compaction_scheduler_remembered_hits,
            after.maintenance.compaction_scheduler_remembered_stale,
            after.maintenance.compaction_scheduler_conflict_denials,
        },
    );
    try writer.print(
        ",\"background_io_budget_bytes_after\":{d},\"background_io_reserved_bytes_after\":{d},\"background_io_denied_jobs_after\":{d},\"background_io_oversized_jobs_after\":{d}}}\n",
        .{
            after.maintenance.background_io_budget_bytes,
            after.maintenance.background_io_reserved_bytes,
            after.maintenance.background_io_denied_jobs,
            after.maintenance.background_io_oversized_jobs,
        },
    );
}

fn logicalValueWriteBytes(workload: []const u8, ops: usize, value_size: usize) usize {
    if (std.mem.eql(u8, workload, "load_sorted") or
        std.mem.eql(u8, workload, "load_random") or
        std.mem.eql(u8, workload, "load_base") or
        std.mem.eql(u8, workload, "load_l0_runs") or
        std.mem.eql(u8, workload, "overwrite_strided") or
        std.mem.eql(u8, workload, "overwrite_hotset"))
    {
        return ops * value_size;
    }
    return 0;
}

fn effectiveL0SoftLimitRuns(cfg: Config) usize {
    if (cfg.l0_soft_limit_runs != 0) return cfg.l0_soft_limit_runs;
    return cfg.compact_threshold_runs;
}

fn effectiveL0HardLimitRuns(cfg: Config) usize {
    if (cfg.l0_hard_limit_runs != 0) return cfg.l0_hard_limit_runs;
    const soft = effectiveL0SoftLimitRuns(cfg);
    return std.math.mul(usize, @max(@as(usize, 1), soft), 2) catch std.math.maxInt(usize);
}

fn summarizeRuns(backend: *const antfly.lsm_backend.Backend) RunSummary {
    var summary: RunSummary = .{};
    summary.count = backend.runs.items.len;
    for (backend.runs.items) |run| {
        if (run.level == 0) summary.l0_count += 1;
        summary.max_level = @max(summary.max_level, run.level);
        summary.bytes += run.size_bytes;
        summary.entries += run.entry_count;
    }
    return summary;
}

fn diffCompactionStats(after: CompactionStats, before: CompactionStats) CompactionStats {
    return .{
        .compactions = after.compactions - before.compactions,
        .input_runs = after.input_runs - before.input_runs,
        .input_bytes = after.input_bytes - before.input_bytes,
        .output_bytes = after.output_bytes - before.output_bytes,
    };
}

fn diffWriteStats(after: WriteStats, before: WriteStats) WriteStats {
    return .{
        .flushes = after.flushes - before.flushes,
        .flush_input_entries = after.flush_input_entries - before.flush_input_entries,
        .flush_output_runs = after.flush_output_runs - before.flush_output_runs,
        .flush_output_bytes = after.flush_output_bytes - before.flush_output_bytes,
        .flush_ns = after.flush_ns - before.flush_ns,
        .table_file_writes = after.table_file_writes - before.table_file_writes,
        .table_file_bytes = after.table_file_bytes - before.table_file_bytes,
        .table_file_logical_entry_bytes = after.table_file_logical_entry_bytes - before.table_file_logical_entry_bytes,
        .table_file_physical_entry_bytes = after.table_file_physical_entry_bytes - before.table_file_physical_entry_bytes,
        .table_file_raw_blocks = after.table_file_raw_blocks - before.table_file_raw_blocks,
        .table_file_compressed_blocks = after.table_file_compressed_blocks - before.table_file_compressed_blocks,
        .table_file_compression_codec_mask = if (after.table_file_writes > before.table_file_writes)
            after.table_file_compression_codec_mask
        else
            0,
        .sorted_ingest_runs = after.sorted_ingest_runs - before.sorted_ingest_runs,
        .sorted_ingest_bytes = after.sorted_ingest_bytes - before.sorted_ingest_bytes,
        .sorted_ingest_ns = after.sorted_ingest_ns - before.sorted_ingest_ns,
        .compaction_ns = after.compaction_ns - before.compaction_ns,
        .manifest_writes = after.manifest_writes - before.manifest_writes,
        .manifest_bytes = after.manifest_bytes - before.manifest_bytes,
        .manifest_ns = after.manifest_ns - before.manifest_ns,
        .write_pressure_compactions = after.write_pressure_compactions - before.write_pressure_compactions,
        .write_pressure_ns = after.write_pressure_ns - before.write_pressure_ns,
        .wal_pressure_flushes = after.wal_pressure_flushes - before.wal_pressure_flushes,
        .wal_pressure_ns = after.wal_pressure_ns - before.wal_pressure_ns,
        .wal_append_records = after.wal_append_records - before.wal_append_records,
        .wal_append_entries = after.wal_append_entries - before.wal_append_entries,
        .wal_append_bytes = after.wal_append_bytes - before.wal_append_bytes,
        .wal_append_ns = after.wal_append_ns - before.wal_append_ns,
        .wal_sync_records = after.wal_sync_records - before.wal_sync_records,
        .wal_sync_ns = after.wal_sync_ns - before.wal_sync_ns,
        .wal_resets = after.wal_resets - before.wal_resets,
        .wal_reset_ns = after.wal_reset_ns - before.wal_reset_ns,
    };
}

fn stridedCount(len: usize, stride_value: usize) usize {
    const stride = @max(stride_value, 1);
    return std.math.divCeil(usize, len, stride) catch unreachable;
}

fn isManifestPath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, "manifest.bin") or std.mem.indexOf(u8, path, "manifest.bin.") != null;
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
        } else if (std.mem.eql(u8, arg, "--hot-keys")) {
            cfg.hot_keys = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--overwrite-rounds")) {
            cfg.overwrite_rounds = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--hot-maintenance-steps")) {
            cfg.hot_maintenance_steps = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--value-size")) {
            cfg.value_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--value-pattern")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.value_pattern = std.meta.stringToEnum(ValuePattern, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--update-stride")) {
            cfg.update_stride = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--delete-stride")) {
            cfg.delete_stride = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--flush-threshold")) {
            cfg.flush_threshold = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--flush-threshold-bytes")) {
            cfg.flush_threshold_bytes = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--bulk-ingest-flush-threshold-multiplier")) {
            cfg.bulk_ingest_flush_threshold_multiplier = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--compact-threshold-runs")) {
            cfg.compact_threshold_runs = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--l0-soft-limit-runs")) {
            cfg.l0_soft_limit_runs = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--l0-hard-limit-runs")) {
            cfg.l0_hard_limit_runs = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--l0-soft-limit-bytes")) {
            cfg.l0_soft_limit_bytes = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--l0-hard-limit-bytes")) {
            cfg.l0_hard_limit_bytes = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--level-target-runs-base")) {
            cfg.level_target_runs_base = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--level-target-runs-multiplier")) {
            cfg.level_target_runs_multiplier = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--level-target-bytes-base")) {
            cfg.level_target_bytes_base = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--level-target-bytes-multiplier")) {
            cfg.level_target_bytes_multiplier = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-run-file-bytes")) {
            cfg.max_run_file_bytes = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-compaction-input-bytes")) {
            cfg.max_compaction_input_bytes = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--strict-max-compaction-input-bytes")) {
            cfg.max_compaction_input_allow_oversized_single_job = false;
        } else if (std.mem.eql(u8, arg, "--background-io-budget-bytes")) {
            cfg.background_io_budget_bytes = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--background-io-disallow-oversized-single-job")) {
            cfg.background_io_allow_oversized_single_job = false;
        } else if (std.mem.eql(u8, arg, "--bloom-bits-per-key")) {
            cfg.bloom_bits_per_key = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--bloom-min-bits")) {
            cfg.bloom_min_bits = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--storage")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.storage_mode = std.meta.stringToEnum(StorageSelection, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--mode")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.mode = std.meta.stringToEnum(ModeSelection, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--lsm-io")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.lsm_io_runtime = std.meta.stringToEnum(antfly.lsm_backend.IoRuntime, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--wal-sync-on-commit")) {
            cfg.wal_sync_on_commit = true;
        } else if (std.mem.eql(u8, arg, "--readers")) {
            cfg.readers = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--workload-set")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.workload_set = std.meta.stringToEnum(WorkloadSet, value) orelse return error.InvalidArgument;
        } else {
            return error.InvalidArgument;
        }
    }

    if (cfg.keys == 0) return error.InvalidArgument;
    if (cfg.hot_keys == 0) return error.InvalidArgument;
    if (cfg.overwrite_rounds == 0) return error.InvalidArgument;
    if (cfg.batch_size == 0) return error.InvalidArgument;
    if (cfg.value_size == 0) return error.InvalidArgument;
    return cfg;
}

fn fillValue(value: []u8, pattern: ValuePattern, seed: u64) void {
    switch (pattern) {
        .repeat => @memset(value, @intCast(seed & 0xff)),
        .deterministic, .keyed => fillDeterministicValue(value, seed),
    }
}

fn valueForWrite(
    cfg: Config,
    fallback: []const u8,
    scratch: []u8,
    key: []const u8,
    op_index: usize,
    seed: u64,
) []const u8 {
    if (cfg.value_pattern != .keyed) return fallback;
    var keyed_seed = std.hash.Wyhash.hash(seed, key);
    keyed_seed +%= @as(u64, @intCast(op_index)) *% 0x9e37_79b9_7f4a_7c15;
    fillDeterministicValue(scratch, keyed_seed);
    return scratch;
}

fn fillDeterministicValue(value: []u8, seed: u64) void {
    var state = seed | 1;
    for (value) |*byte| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        byte.* = @intCast(state & 0xff);
    }
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const value = args.next() orelse return error.InvalidArgument;
    return std.fmt.parseInt(usize, value, 10) catch {
        std.debug.print("invalid value for {s}: {s}\n", .{ flag, value });
        return error.InvalidArgument;
    };
}

fn parseNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const value = args.next() orelse return error.InvalidArgument;
    return std.fmt.parseInt(u64, value, 10) catch {
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
