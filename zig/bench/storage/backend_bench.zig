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

const Config = struct {
    backend: BackendSelection = .all,
    samples: usize = 1,
    keys: usize = 20_000,
    value_size: usize = 128,
    hit_repeats: usize = 3,
    miss_repeats: usize = 3,
    scan_repeats: usize = 5,
    mixed_repeats: usize = 3,
    mixed_write_stride: usize = 16,
    flush_threshold: usize = 512,
    compact_threshold_runs: usize = 16,
    level_target_runs_base: usize = 4,
    level_target_runs_multiplier: usize = 4,
    level_target_bytes_base: usize = 128 * 1024,
    level_target_bytes_multiplier: usize = 8,
    fragmented_flush_threshold: usize = 64,
    fragmented_compact_threshold_runs: usize = 1024,
    compacting_flush_threshold: usize = 64,
    compacting_compact_threshold_runs: usize = 4,
    bloom_bits_per_key: usize = 10,
    bloom_min_bits: usize = 64,
    lsm_io_runtime: antfly.lsm_backend.IoRuntime = .threaded,
};

const BackendSelection = enum {
    all,
    lmdb,
    lsm,
    lsm_memory,
};

const Result = struct {
    backend: []const u8,
    workload: []const u8,
    ops: usize,
    ns: u64,
};

const KeySet = struct {
    keys: [][]u8,

    fn deinit(self: *const KeySet, allocator: std.mem.Allocator) void {
        for (self.keys) |key| allocator.free(key);
        allocator.free(self.keys);
    }
};

const OpenedStore = union(enum) {
    lmdb: struct {
        runtime: ?backend_erased.Store = null,
        namespace_runtime: ?backend_erased.NamespaceStore = null,
        backend: antfly.lmdb_backend.Backend,
        path: []u8,
    },
    lsm: struct {
        runtime: ?backend_erased.Store = null,
        namespace_runtime: ?backend_erased.NamespaceStore = null,
        backend: antfly.lsm_backend.Backend,
        path: []u8,
    },
    lsm_memory: struct {
        runtime: ?backend_erased.Store = null,
        namespace_runtime: ?backend_erased.NamespaceStore = null,
        backend: antfly.lsm_backend.Backend,
    },

    fn runtime(self: *OpenedStore) *backend_erased.Store {
        return switch (self.*) {
            .lmdb => |*opened| &opened.runtime.?,
            .lsm => |*opened| &opened.runtime.?,
            .lsm_memory => |*opened| &opened.runtime.?,
        };
    }

    fn namespaceRuntime(self: *OpenedStore) *backend_erased.NamespaceStore {
        return switch (self.*) {
            .lmdb => |*opened| &opened.namespace_runtime.?,
            .lsm => |*opened| &opened.namespace_runtime.?,
            .lsm_memory => |*opened| &opened.namespace_runtime.?,
        };
    }

    fn label(self: *const OpenedStore) []const u8 {
        return switch (self.*) {
            .lmdb => "lmdb",
            .lsm => "lsm",
            .lsm_memory => "lsm_memory",
        };
    }

    fn initRuntime(self: *OpenedStore, allocator: std.mem.Allocator) !void {
        switch (self.*) {
            .lmdb => |*opened| {
                if (opened.runtime == null) {
                    opened.runtime = try opened.backend.runtimeStore(allocator, bench_namespace);
                }
                if (opened.namespace_runtime == null) {
                    opened.namespace_runtime = try opened.backend.runtimeNamespaceStore(allocator);
                }
            },
            .lsm => |*opened| {
                if (opened.runtime == null) {
                    opened.runtime = try opened.backend.runtimeStore(allocator, bench_namespace);
                }
                if (opened.namespace_runtime == null) {
                    opened.namespace_runtime = try opened.backend.runtimeNamespaceStore(allocator);
                }
            },
            .lsm_memory => |*opened| {
                if (opened.runtime == null) {
                    opened.runtime = try opened.backend.runtimeStore(allocator, bench_namespace);
                }
                if (opened.namespace_runtime == null) {
                    opened.namespace_runtime = try opened.backend.runtimeNamespaceStore(allocator);
                }
            },
        }
    }

    fn deinit(self: *OpenedStore, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .lmdb => |*opened| {
                if (opened.runtime) |*store_runtime| store_runtime.deinit();
                if (opened.namespace_runtime) |*ns_runtime| ns_runtime.deinit();
                opened.backend.close();
                cleanupPath(opened.path);
                allocator.free(opened.path);
            },
            .lsm => |*opened| {
                if (opened.runtime) |*store_runtime| store_runtime.deinit();
                if (opened.namespace_runtime) |*ns_runtime| ns_runtime.deinit();
                opened.backend.close();
                cleanupPath(opened.path);
                allocator.free(opened.path);
            },
            .lsm_memory => |*opened| {
                if (opened.runtime) |*store_runtime| store_runtime.deinit();
                if (opened.namespace_runtime) |*ns_runtime| ns_runtime.deinit();
                opened.backend.close();
            },
        }
        self.* = undefined;
    }

    fn reopen(self: *OpenedStore, allocator: std.mem.Allocator, cfg: Config) !bool {
        if (!try self.reopenBackend(allocator, cfg)) return false;
        try self.initRuntime(allocator);
        return true;
    }

    fn reopenBackend(self: *OpenedStore, allocator: std.mem.Allocator, cfg: Config) !bool {
        try self.closeForReopen();
        return try self.openAfterClose(allocator, cfg);
    }

    fn closeForReopen(self: *OpenedStore) !void {
        switch (self.*) {
            .lmdb => |*opened| {
                if (opened.runtime) |*store_runtime| store_runtime.deinit();
                if (opened.namespace_runtime) |*ns_runtime| ns_runtime.deinit();
                opened.runtime = null;
                opened.namespace_runtime = null;
                opened.backend.close();
            },
            .lsm => |*opened| {
                if (opened.runtime) |*store_runtime| store_runtime.deinit();
                if (opened.namespace_runtime) |*ns_runtime| ns_runtime.deinit();
                opened.runtime = null;
                opened.namespace_runtime = null;
                opened.backend.close();
            },
            .lsm_memory => |*opened| {
                if (opened.runtime) |*store_runtime| store_runtime.deinit();
                if (opened.namespace_runtime) |*ns_runtime| ns_runtime.deinit();
                opened.runtime = null;
                opened.namespace_runtime = null;
                opened.backend.close();
            },
        }
    }

    fn openAfterClose(self: *OpenedStore, allocator: std.mem.Allocator, cfg: Config) !bool {
        switch (self.*) {
            .lmdb => |*opened| {
                const path_z = try allocator.dupeZ(u8, opened.path);
                defer allocator.free(path_z);
                opened.backend = try antfly.lmdb_backend.Backend.open(allocator, path_z.ptr, .{
                    .backend = .{ .create_if_missing = true },
                    .env = .{ .map_size = benchLmdbMapSize(cfg) },
                });
                return true;
            },
            .lsm => |*opened| {
                opened.backend = try antfly.lsm_backend.Backend.open(allocator, opened.path, .{
                    .backend = .{ .create_if_missing = true },
                    .flush_threshold = cfg.flush_threshold,
                    .compact_threshold_runs = cfg.compact_threshold_runs,
                    .level_target_runs_base = cfg.level_target_runs_base,
                    .level_target_runs_multiplier = cfg.level_target_runs_multiplier,
                    .level_target_bytes_base = cfg.level_target_bytes_base,
                    .level_target_bytes_multiplier = cfg.level_target_bytes_multiplier,
                    .bloom = bloomConfig(cfg),
                    .io_runtime = cfg.lsm_io_runtime,
                });
                return true;
            },
            .lsm_memory => return false,
        }
    }
};

const bench_namespace: backend_types.Namespace = .{};

fn nanotime() u64 {
    return platform_time.monotonicNs();
}

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(alloc, init.minimal.args);

    const keys = try makeKeys(alloc, "doc", cfg.keys);
    defer keys.deinit(alloc);
    const random_keys = try shuffledKeys(alloc, keys.keys);
    defer alloc.free(random_keys);
    const misses = try makeKeys(alloc, "miss", cfg.keys);
    defer misses.deinit(alloc);
    const value = try alloc.alloc(u8, cfg.value_size);
    defer alloc.free(value);
    @memset(value, 'x');
    const update_value = try alloc.alloc(u8, cfg.value_size);
    defer alloc.free(update_value);
    @memset(update_value, 'y');

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;

    try out.print(
        "backend bench samples={d} keys={d} value_size={d} hit_repeats={d} miss_repeats={d} scan_repeats={d} mixed_repeats={d} mixed_write_stride={d}\n",
        .{
            cfg.samples,
            cfg.keys,
            cfg.value_size,
            cfg.hit_repeats,
            cfg.miss_repeats,
            cfg.scan_repeats,
            cfg.mixed_repeats,
            cfg.mixed_write_stride,
        },
    );
    try stdout_writer.flush();

    const selections: []const BackendSelection = switch (cfg.backend) {
        .all => &[_]BackendSelection{ .lmdb, .lsm, .lsm_memory },
        else => &[_]BackendSelection{cfg.backend},
    };

    for (selections) |selection| {
        var sample_index: usize = 0;
        while (sample_index < cfg.samples) : (sample_index += 1) {
            var opened = try openBackend(alloc, selection, cfg);
            defer opened.deinit(alloc);
            try opened.initRuntime(alloc);

            try printResult(out, try benchLoad(opened.namespaceRuntime(), opened.label(), keys.keys, value));
            try stdout_writer.flush();
            try printLevelOccupancy(out, &opened, "levels_after_load_sorted");
            try stdout_writer.flush();
            var random_opened = try openBackend(alloc, selection, cfg);
            defer random_opened.deinit(alloc);
            try random_opened.initRuntime(alloc);
            try printResult(out, try benchLoadRandom(random_opened.namespaceRuntime(), random_opened.label(), random_keys, value));
            try stdout_writer.flush();
            try printResult(out, try benchReadHits(opened.runtime(), opened.label(), keys.keys, cfg.hit_repeats));
            try stdout_writer.flush();
            try printResult(out, try benchReadMisses(opened.runtime(), opened.label(), misses.keys, cfg.miss_repeats));
            try stdout_writer.flush();
            try printResult(out, try benchScan(opened.runtime(), opened.label(), cfg.keys, cfg.scan_repeats));
            try stdout_writer.flush();
            try printResult(out, try benchMixedReadWrite(
                opened.runtime(),
                opened.label(),
                keys.keys,
                update_value,
                cfg.mixed_repeats,
                cfg.mixed_write_stride,
            ));
            try stdout_writer.flush();
            if (try benchReopenBackendOnly(&opened, alloc, cfg)) |result| {
                try printResult(out, result);
                try stdout_writer.flush();
            }
            if (try benchReopenRuntimeAttachOnly(&opened, alloc)) |result| {
                try printResult(out, result);
                try stdout_writer.flush();
            }
            if (try benchReopenOpenOnly(&opened, alloc, cfg)) |result| {
                try printResult(out, result);
                try stdout_writer.flush();
            }
            if (try benchReopen(&opened, alloc, cfg, keys.keys[0])) |result| {
                try printResult(out, result);
                try stdout_writer.flush();
            }

            var fragmented_opened = try openBackend(alloc, selection, fragmentedConfig(cfg));
            defer fragmented_opened.deinit(alloc);
            try fragmented_opened.initRuntime(alloc);
            try printResult(out, try benchLoadFragmented(
                fragmented_opened.namespaceRuntime(),
                fragmented_opened.label(),
                keys.keys,
                value,
                cfg.fragmented_flush_threshold,
            ));
            try stdout_writer.flush();
            try printLevelOccupancy(out, &fragmented_opened, "levels_after_load_fragmented");
            try stdout_writer.flush();
            try printCompactionStats(out, &fragmented_opened, "compaction_stats_after_load_fragmented");
            try stdout_writer.flush();
            try printResult(out, try benchReadMissesFragmented(
                fragmented_opened.runtime(),
                fragmented_opened.label(),
                misses.keys,
                cfg.miss_repeats,
            ));
            try stdout_writer.flush();
            if (try benchReopenFragmentedBackendOnly(&fragmented_opened, alloc, fragmentedConfig(cfg))) |result| {
                try printResult(out, result);
                try stdout_writer.flush();
            }
            if (try benchReopenFragmentedRuntimeAttachOnly(&fragmented_opened, alloc)) |result| {
                try printResult(out, result);
                try stdout_writer.flush();
            }
            if (try benchReopenFragmentedOpenOnly(&fragmented_opened, alloc, fragmentedConfig(cfg))) |result| {
                try printResult(out, result);
                try stdout_writer.flush();
            }
            if (try benchReopenFragmented(&fragmented_opened, alloc, fragmentedConfig(cfg), keys.keys[0])) |result| {
                try printResult(out, result);
                try stdout_writer.flush();
            }

            var compacting_opened = try openBackend(alloc, selection, compactingConfig(cfg));
            defer compacting_opened.deinit(alloc);
            try compacting_opened.initRuntime(alloc);
            try printResult(out, try benchLoadCompacting(
                compacting_opened.namespaceRuntime(),
                compacting_opened.label(),
                random_keys,
                value,
                cfg.compacting_flush_threshold,
            ));
            try stdout_writer.flush();
            try printLevelOccupancy(out, &compacting_opened, "levels_after_load_compacting");
            try stdout_writer.flush();
            try printCompactionStats(out, &compacting_opened, "compaction_stats_after_load_compacting");
            try stdout_writer.flush();
        }
    }
    try stdout_writer.flush();
}

fn openBackend(allocator: std.mem.Allocator, selection: BackendSelection, cfg: Config) !OpenedStore {
    switch (selection) {
        .lmdb => {
            const path = try tmpPath(allocator, "backend-bench-lmdb");
            errdefer allocator.free(path);
            const path_z = try allocator.dupeZ(u8, path);
            defer allocator.free(path_z);
            const backend = try antfly.lmdb_backend.Backend.open(allocator, path_z.ptr, .{
                .backend = .{ .create_if_missing = true },
                .env = .{ .map_size = benchLmdbMapSize(cfg) },
            });
            errdefer {
                var backend_to_close = backend;
                backend_to_close.close();
                cleanupPath(path);
            }
            const opened: OpenedStore = .{
                .lmdb = .{
                    .backend = backend,
                    .path = path,
                },
            };
            return opened;
        },
        .lsm => {
            const path = try tmpPath(allocator, "backend-bench-lsm");
            const backend = try antfly.lsm_backend.Backend.open(allocator, path, .{
                .backend = .{ .create_if_missing = true },
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
                var backend_to_close = backend;
                backend_to_close.close();
                cleanupPath(path);
                allocator.free(path);
            }
            const opened: OpenedStore = .{
                .lsm = .{
                    .backend = backend,
                    .path = path,
                },
            };
            return opened;
        },
        .lsm_memory => {
            var backend = antfly.lsm_backend.Backend.init(allocator, .{
                .flush_threshold = cfg.flush_threshold,
                .compact_threshold_runs = cfg.compact_threshold_runs,
                .level_target_runs_base = cfg.level_target_runs_base,
                .level_target_runs_multiplier = cfg.level_target_runs_multiplier,
                .level_target_bytes_base = cfg.level_target_bytes_base,
                .level_target_bytes_multiplier = cfg.level_target_bytes_multiplier,
                .bloom = bloomConfig(cfg),
                .io_runtime = cfg.lsm_io_runtime,
            });
            errdefer backend.close();
            const opened: OpenedStore = .{
                .lsm_memory = .{
                    .backend = backend,
                },
            };
            return opened;
        },
        .all => unreachable,
    }
}

fn benchLoad(
    store: *backend_erased.NamespaceStore,
    backend_label: []const u8,
    keys: []const []u8,
    value: []const u8,
) !Result {
    var txn = try store.beginWrite();
    const start = nanotime();
    const can_append = store.capabilities().ordered_append_puts;
    for (keys) |key| {
        const write_result = if (can_append)
            txn.appendPut(bench_namespace, key, value)
        else
            txn.put(bench_namespace, key, value);
        write_result catch |err| {
            txn.abort();
            return err;
        };
    }
    try txn.commit();
    return .{
        .backend = backend_label,
        .workload = "load_sorted",
        .ops = keys.len,
        .ns = nanotime() - start,
    };
}

fn benchLoadRandom(
    store: *backend_erased.NamespaceStore,
    backend_label: []const u8,
    keys: []const []u8,
    value: []const u8,
) !Result {
    var txn = try store.beginWrite();
    const start = nanotime();
    for (keys) |key| {
        txn.put(bench_namespace, key, value) catch |err| {
            txn.abort();
            return err;
        };
    }
    try txn.commit();
    return .{
        .backend = backend_label,
        .workload = "load_random",
        .ops = keys.len,
        .ns = nanotime() - start,
    };
}

fn benchLoadFragmented(
    store: *backend_erased.NamespaceStore,
    backend_label: []const u8,
    keys: []const []u8,
    value: []const u8,
    batch_size: usize,
) !Result {
    const chunk_size = @max(batch_size, 1);
    const can_append = store.capabilities().ordered_append_puts;
    const start = nanotime();
    var start_index: usize = 0;
    while (start_index < keys.len) {
        const end_index = @min(start_index + chunk_size, keys.len);
        var txn = try store.beginWrite();
        for (keys[start_index..end_index]) |key| {
            const write_result = if (can_append)
                txn.appendPut(bench_namespace, key, value)
            else
                txn.put(bench_namespace, key, value);
            write_result catch |err| {
                txn.abort();
                return err;
            };
        }
        try txn.commit();
        start_index = end_index;
    }
    return .{
        .backend = backend_label,
        .workload = "load_fragmented",
        .ops = keys.len,
        .ns = nanotime() - start,
    };
}

fn benchLoadCompacting(
    store: *backend_erased.NamespaceStore,
    backend_label: []const u8,
    keys: []const []u8,
    value: []const u8,
    batch_size: usize,
) !Result {
    const start = nanotime();
    const chunk_size = @max(batch_size, 1);
    var start_index: usize = 0;
    while (start_index < keys.len) {
        const end_index = @min(start_index + chunk_size, keys.len);
        var txn = try store.beginWrite();
        for (keys[start_index..end_index]) |key| {
            txn.put(bench_namespace, key, value) catch |err| {
                txn.abort();
                return err;
            };
        }
        try txn.commit();
        start_index = end_index;
    }
    return .{
        .backend = backend_label,
        .workload = "load_compacting",
        .ops = keys.len,
        .ns = nanotime() - start,
    };
}

fn benchReadHits(
    store: *backend_erased.Store,
    backend_label: []const u8,
    keys: []const []u8,
    repeats: usize,
) !Result {
    var txn = try store.beginRead();
    defer txn.abort();

    const start = nanotime();
    for (0..repeats) |_| {
        for (keys) |key| {
            const value = try txn.get(key);
            std.mem.doNotOptimizeAway(value.len);
        }
    }
    return .{
        .backend = backend_label,
        .workload = "read_hits",
        .ops = keys.len * repeats,
        .ns = nanotime() - start,
    };
}

fn benchReadMisses(
    store: *backend_erased.Store,
    backend_label: []const u8,
    keys: []const []u8,
    repeats: usize,
) !Result {
    var txn = try store.beginRead();
    defer txn.abort();

    const start = nanotime();
    for (0..repeats) |_| {
        for (keys) |key| {
            _ = txn.get(key) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
        }
    }
    return .{
        .backend = backend_label,
        .workload = "read_misses",
        .ops = keys.len * repeats,
        .ns = nanotime() - start,
    };
}

fn benchReadMissesFragmented(
    store: *backend_erased.Store,
    backend_label: []const u8,
    keys: []const []u8,
    repeats: usize,
) !Result {
    const result = try benchReadMisses(store, backend_label, keys, repeats);
    return .{
        .backend = result.backend,
        .workload = "read_misses_fragmented",
        .ops = result.ops,
        .ns = result.ns,
    };
}

fn benchScan(
    store: *backend_erased.Store,
    backend_label: []const u8,
    key_count: usize,
    repeats: usize,
) !Result {
    var txn = try store.beginRead();
    defer txn.abort();

    const start = nanotime();
    for (0..repeats) |_| {
        var cursor = try txn.openCursor();
        var seen: usize = 0;
        var entry = try cursor.first();
        while (entry) |current| : (entry = try cursor.next()) {
            seen += 1;
            std.mem.doNotOptimizeAway(current.key.len);
        }
        cursor.close();
        if (seen != key_count) return error.InvalidCursorCount;
    }
    return .{
        .backend = backend_label,
        .workload = "scan_full",
        .ops = key_count * repeats,
        .ns = nanotime() - start,
    };
}

fn benchMixedReadWrite(
    store: *backend_erased.Store,
    backend_label: []const u8,
    keys: []const []u8,
    update_value: []const u8,
    repeats: usize,
    write_stride: usize,
) !Result {
    const stride = @max(write_stride, 1);
    const start = nanotime();
    for (0..repeats) |_| {
        var read_txn = try store.beginRead();
        for (keys) |key| {
            const value = try read_txn.get(key);
            std.mem.doNotOptimizeAway(value.len);
        }
        read_txn.abort();

        var write_txn = try store.beginWrite();
        var i: usize = 0;
        while (i < keys.len) : (i += stride) {
            try write_txn.put(keys[i], update_value);
        }
        try write_txn.commit();
    }
    const writes_per_round = std.math.divCeil(usize, keys.len, stride) catch unreachable;
    return .{
        .backend = backend_label,
        .workload = "mixed_read_write",
        .ops = repeats * (keys.len + writes_per_round),
        .ns = nanotime() - start,
    };
}

fn benchReopen(
    opened: *OpenedStore,
    allocator: std.mem.Allocator,
    cfg: Config,
    probe_key: []const u8,
) !?Result {
    const start = nanotime();
    if (!try opened.reopen(allocator, cfg)) return null;
    var txn = try opened.runtime().beginRead();
    defer txn.abort();
    const value = try txn.get(probe_key);
    std.mem.doNotOptimizeAway(value.len);
    return .{
        .backend = opened.label(),
        .workload = "reopen_snapshot_read",
        .ops = 1,
        .ns = nanotime() - start,
    };
}

fn benchReopenOpenOnly(
    opened: *OpenedStore,
    allocator: std.mem.Allocator,
    cfg: Config,
) !?Result {
    const start = nanotime();
    if (!try opened.reopen(allocator, cfg)) return null;
    return .{
        .backend = opened.label(),
        .workload = "reopen_open_only",
        .ops = 1,
        .ns = nanotime() - start,
    };
}

fn benchReopenBackendOnly(
    opened: *OpenedStore,
    allocator: std.mem.Allocator,
    cfg: Config,
) !?Result {
    const start = nanotime();
    if (!try opened.reopenBackend(allocator, cfg)) return null;
    return .{
        .backend = opened.label(),
        .workload = "reopen_backend_only",
        .ops = 1,
        .ns = nanotime() - start,
    };
}

fn benchReopenRuntimeAttachOnly(
    opened: *OpenedStore,
    allocator: std.mem.Allocator,
) !?Result {
    switch (opened.*) {
        .lsm_memory => return null,
        else => {},
    }
    const start = nanotime();
    try opened.initRuntime(allocator);
    return .{
        .backend = opened.label(),
        .workload = "reopen_runtime_attach",
        .ops = 1,
        .ns = nanotime() - start,
    };
}

fn benchReopenFragmented(
    opened: *OpenedStore,
    allocator: std.mem.Allocator,
    cfg: Config,
    probe_key: []const u8,
) !?Result {
    const start = nanotime();
    if (!try opened.reopen(allocator, cfg)) return null;
    var txn = try opened.runtime().beginRead();
    defer txn.abort();
    const value = try txn.get(probe_key);
    std.mem.doNotOptimizeAway(value.len);
    return .{
        .backend = opened.label(),
        .workload = "reopen_fragmented_snapshot_read",
        .ops = 1,
        .ns = nanotime() - start,
    };
}

fn benchReopenFragmentedOpenOnly(
    opened: *OpenedStore,
    allocator: std.mem.Allocator,
    cfg: Config,
) !?Result {
    const start = nanotime();
    if (!try opened.reopen(allocator, cfg)) return null;
    return .{
        .backend = opened.label(),
        .workload = "reopen_fragmented_open_only",
        .ops = 1,
        .ns = nanotime() - start,
    };
}

fn benchReopenFragmentedBackendOnly(
    opened: *OpenedStore,
    allocator: std.mem.Allocator,
    cfg: Config,
) !?Result {
    const start = nanotime();
    if (!try opened.reopenBackend(allocator, cfg)) return null;
    return .{
        .backend = opened.label(),
        .workload = "reopen_fragmented_backend_only",
        .ops = 1,
        .ns = nanotime() - start,
    };
}

fn benchReopenFragmentedRuntimeAttachOnly(
    opened: *OpenedStore,
    allocator: std.mem.Allocator,
) !?Result {
    switch (opened.*) {
        .lsm_memory => return null,
        else => {},
    }
    const start = nanotime();
    try opened.initRuntime(allocator);
    return .{
        .backend = opened.label(),
        .workload = "reopen_fragmented_runtime_attach",
        .ops = 1,
        .ns = nanotime() - start,
    };
}

fn printResult(writer: anytype, result: Result) !void {
    const secs = @as(f64, @floatFromInt(result.ns)) / 1e9;
    const ops_per_sec = @as(f64, @floatFromInt(result.ops)) / secs;
    try writer.print(
        "{{\"backend\":\"{s}\",\"workload\":\"{s}\",\"ops\":{d},\"ns\":{d},\"ops_per_sec\":{d:.2}}}\n",
        .{ result.backend, result.workload, result.ops, result.ns, ops_per_sec },
    );
}

fn printLevelOccupancy(writer: anytype, opened: *OpenedStore, workload: []const u8) !void {
    switch (opened.*) {
        .lmdb => return,
        .lsm => |*store| try printBackendLevelOccupancy(writer, store.backend.runs.items, "lsm", workload),
        .lsm_memory => |*store| try printBackendLevelOccupancy(writer, store.backend.runs.items, "lsm_memory", workload),
    }
}

fn printCompactionStats(writer: anytype, opened: *OpenedStore, workload: []const u8) !void {
    switch (opened.*) {
        .lmdb => return,
        .lsm => |*store| try printBackendCompactionStats(writer, "lsm", workload, store.backend.runs.items, store.backend.compaction_stats),
        .lsm_memory => |*store| try printBackendCompactionStats(writer, "lsm_memory", workload, store.backend.runs.items, store.backend.compaction_stats),
    }
}

fn printBackendLevelOccupancy(writer: anytype, runs: anytype, backend_label: []const u8, workload: []const u8) !void {
    if (runs.len == 0) return;
    var i: usize = 0;
    while (i < runs.len) {
        const level = runs[i].level;
        var run_count: usize = 0;
        var entry_count: u64 = 0;
        var size_bytes: u64 = 0;
        while (i < runs.len and runs[i].level == level) : (i += 1) {
            run_count += 1;
            entry_count += runs[i].entry_count;
            size_bytes += runs[i].size_bytes;
        }
        try writer.print(
            "{{\"backend\":\"{s}\",\"workload\":\"{s}\",\"level\":{d},\"runs\":{d},\"entries\":{d},\"bytes\":{d}}}\n",
            .{ backend_label, workload, level, run_count, entry_count, size_bytes },
        );
    }
}

fn printBackendCompactionStats(
    writer: anytype,
    backend_label: []const u8,
    workload: []const u8,
    runs: anytype,
    stats: antfly.lsm_backend.Backend.CompactionStats,
) !void {
    var resident_bytes: u64 = 0;
    for (runs) |run| resident_bytes += run.size_bytes;
    const rewrite_over_resident = if (resident_bytes == 0)
        0.0
    else
        @as(f64, @floatFromInt(stats.input_bytes)) / @as(f64, @floatFromInt(resident_bytes));
    try writer.print(
        "{{\"backend\":\"{s}\",\"workload\":\"{s}\",\"compactions\":{d},\"input_runs\":{d},\"input_bytes\":{d},\"output_bytes\":{d},\"resident_bytes\":{d},\"rewrite_over_resident\":{d:.2}}}\n",
        .{ backend_label, workload, stats.compactions, stats.input_runs, stats.input_bytes, stats.output_bytes, resident_bytes, rewrite_over_resident },
    );
}

fn parseArgs(alloc: std.mem.Allocator, proc_args: std.process.Args) !Config {
    var cfg = Config{};
    var args = try std.process.Args.Iterator.initAllocator(proc_args, alloc);
    defer args.deinit();
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--backend")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.backend = std.meta.stringToEnum(BackendSelection, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--keys")) {
            cfg.keys = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--value-size")) {
            cfg.value_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--hit-repeats")) {
            cfg.hit_repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--miss-repeats")) {
            cfg.miss_repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--scan-repeats")) {
            cfg.scan_repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--mixed-repeats")) {
            cfg.mixed_repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--mixed-write-stride")) {
            cfg.mixed_write_stride = try parseNextUsize(&args, arg);
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
        } else if (std.mem.eql(u8, arg, "--fragmented-flush-threshold")) {
            cfg.fragmented_flush_threshold = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--fragmented-compact-threshold-runs")) {
            cfg.fragmented_compact_threshold_runs = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--compacting-flush-threshold")) {
            cfg.compacting_flush_threshold = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--compacting-compact-threshold-runs")) {
            cfg.compacting_compact_threshold_runs = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--bloom-bits-per-key")) {
            cfg.bloom_bits_per_key = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--bloom-min-bits")) {
            cfg.bloom_min_bits = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--lsm-io")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.lsm_io_runtime = std.meta.stringToEnum(antfly.lsm_backend.IoRuntime, value) orelse return error.InvalidArgument;
        } else {
            return error.InvalidArgument;
        }
    }
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const value = args.next() orelse return error.InvalidArgument;
    return std.fmt.parseInt(usize, value, 10) catch {
        std.debug.print("invalid value for {s}: {s}\n", .{ flag, value });
        return error.InvalidArgument;
    };
}

fn makeKeys(allocator: std.mem.Allocator, prefix: []const u8, count: usize) !KeySet {
    const keys = try allocator.alloc([]u8, count);
    errdefer allocator.free(keys);
    for (keys, 0..) |*slot, i| {
        slot.* = try std.fmt.allocPrint(allocator, "{s}:{d:0>8}", .{ prefix, i });
    }
    return .{ .keys = keys };
}

fn shuffledKeys(allocator: std.mem.Allocator, keys: []const []u8) ![][]u8 {
    var shuffled = try allocator.alloc([]u8, keys.len);
    for (keys, 0..) |key, i| shuffled[i] = @constCast(key);

    var prng = std.Random.DefaultPrng.init(@as(u64, 0xdecafbad));
    const random = prng.random();
    var i: usize = shuffled.len;
    while (i > 1) {
        i -= 1;
        const j = random.uintLessThan(usize, i + 1);
        std.mem.swap([]u8, &shuffled[i], &shuffled[j]);
    }
    return shuffled;
}

fn benchLmdbMapSize(cfg: Config) usize {
    const estimated_payload = cfg.keys * (cfg.value_size + 256);
    const estimated_working_set = estimated_payload * 8;
    return @max(128 * 1024 * 1024, estimated_working_set);
}

fn fragmentedConfig(cfg: Config) Config {
    var out = cfg;
    out.flush_threshold = cfg.fragmented_flush_threshold;
    out.compact_threshold_runs = cfg.fragmented_compact_threshold_runs;
    return out;
}

fn compactingConfig(cfg: Config) Config {
    var out = cfg;
    out.flush_threshold = cfg.compacting_flush_threshold;
    out.compact_threshold_runs = cfg.compacting_compact_threshold_runs;
    return out;
}

fn bloomConfig(cfg: Config) bloom.Config {
    return .{
        .bits_per_key = cfg.bloom_bits_per_key,
        .min_bits = cfg.bloom_min_bits,
    };
}

fn tmpPath(allocator: std.mem.Allocator, label: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, "/tmp/antfly-{s}-{d}", .{ label, nanotime() });
}

fn cleanupPath(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
}
