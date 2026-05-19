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

const Allocator = std.mem.Allocator;
const hbc = antfly.hbc;
const lsm_backend = antfly.lsm_backend;
const lsm_manifest = antfly.storage_lsm.manifest;
const lsm_repository = antfly.lsm_backend.repository;
const lsm_table_file = antfly.storage_lsm.table_file;
const platform_time = antfly.platform_time;
const vec = antfly.vector;

const Config = struct {
    samples: usize = 3,
    vectors: usize = 10_000,
    dims: usize = 128,
    batch_size: usize = 1_000,
    seed: u64 = 42,
    leaf_size: u32 = 128,
    branching_factor: u32 = 128,
    storage_mode: StorageSelection = .host,
    split_algo: vec.ClustAlgorithm = .kmeans,
    bulk_build_algo: hbc.BulkBuildAlgo = .hilbert_seeded,
    kmeans_backend: hbc.HBCConfig.KmeansBackend = .auto,
    kmeans_update_strategy: hbc.HBCConfig.KmeansUpdateStrategy = .auto,
    use_quantization: bool = true,
    use_random_ortho_trans: bool = false,
};

const StorageSelection = enum {
    host,
    native,
    memory,
    both,
};

const StorageCounters = struct {
    read_file: u64 = 0,
    read_bytes: u64 = 0,
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
            .read_bytes = after.read_bytes - before.read_bytes,
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

const NamespaceActiveStats = struct {
    all_entries: u64 = 0,
    all_value_bytes: u64 = 0,
    latest_entries: u64 = 0,
    latest_value_bytes: u64 = 0,
};

const ActiveTableStats = struct {
    active_runs: u64 = 0,
    active_run_bytes: u64 = 0,
    obsolete_paths: u64 = 0,
    hbc_quant: NamespaceActiveStats = .{},
    hbc_vecs: NamespaceActiveStats = .{},
    hbc_nodes: NamespaceActiveStats = .{},
    hbc_meta: NamespaceActiveStats = .{},
    latest_keys: u64 = 0,

    fn quantVersionsPerKeyBps(self: ActiveTableStats) u64 {
        if (self.hbc_quant.latest_entries == 0) return 0;
        return (self.hbc_quant.all_entries * 10_000) / self.hbc_quant.latest_entries;
    }
};

const NamespaceTag = enum {
    other,
    hbc_quant,
    hbc_vecs,
    hbc_nodes,
    hbc_meta,
};

const LatestEntry = struct {
    tag: NamespaceTag,
    value_len: u64,
};

const StorageHarness = struct {
    const CountingStorage = struct {
        backing: lsm_backend.Storage,
        counters: StorageCounters = .{},

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.read_file += 1;
            const bytes = try self.backing.readFileAlloc(allocator, path, max_bytes);
            self.counters.read_bytes += bytes.len;
            return bytes;
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const bytes = try self.backing.readFileRangeAlloc(allocator, path, offset, len);
            self.counters.read_bytes += bytes.len;
            return bytes;
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const bytes = try self.backing.readFileTrailerAlloc(allocator, path, len);
            self.counters.read_bytes += bytes.len;
            return bytes;
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

    const counting_vtable: lsm_backend.Storage.VTable = .{
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
    memory_backing: ?*lsm_backend.MemoryStorage = null,
    native_backing: ?*lsm_backend.storage_io.NativeStorage = null,
    counting_ctx: ?*CountingStorage = null,

    fn init(allocator: Allocator, mode: StorageSelection) !StorageHarness {
        var harness = StorageHarness{ .allocator = allocator, .mode = mode };
        switch (mode) {
            .host, .memory => {
                const backing = try allocator.create(lsm_backend.MemoryStorage);
                errdefer allocator.destroy(backing);
                backing.* = lsm_backend.MemoryStorage.init(allocator);
                errdefer backing.deinit();
                harness.memory_backing = backing;
                if (mode == .host) {
                    const ctx = try allocator.create(CountingStorage);
                    errdefer allocator.destroy(ctx);
                    ctx.* = .{ .backing = backing.storage() };
                    harness.counting_ctx = ctx;
                }
            },
            .native => {
                const backing = try allocator.create(lsm_backend.storage_io.NativeStorage);
                errdefer allocator.destroy(backing);
                backing.* = try lsm_backend.storage_io.NativeStorage.init(allocator, .threaded);
                errdefer backing.deinit();
                harness.native_backing = backing;

                const ctx = try allocator.create(CountingStorage);
                errdefer allocator.destroy(ctx);
                ctx.* = .{ .backing = backing.storage() };
                harness.counting_ctx = ctx;
            },
            .both => unreachable,
        }
        return harness;
    }

    fn deinit(self: *StorageHarness) void {
        if (self.counting_ctx) |ctx| self.allocator.destroy(ctx);
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

    fn storage(self: *StorageHarness) lsm_backend.Storage {
        return switch (self.mode) {
            .host, .native => lsm_backend.HostStorage.init(self.counting_ctx.?, &counting_vtable).storage(),
            .memory => self.memory_backing.?.storage(),
            .both => unreachable,
        };
    }

    fn snapshotCounters(self: *const StorageHarness) StorageCounters {
        if (self.counting_ctx) |ctx| return ctx.counters;
        return .{};
    }
};

const Scenario = struct {
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_kind: StorageSelection,
    workload: []const u8,
    root_dir: [:0]u8,
    storage_harness: StorageHarness,
    index: hbc.HBCIndex,

    fn init(
        allocator: Allocator,
        cfg: Config,
        sample_index: usize,
        storage_kind: StorageSelection,
        workload: []const u8,
    ) !Scenario {
        var storage_harness = try StorageHarness.init(allocator, storage_kind);
        errdefer storage_harness.deinit();

        const root_dir = try allocPrintZ(allocator, "{s}/hbc-write-bench-{s}-{s}-{d}", .{
            if (storage_kind == .native) "/tmp" else "",
            @tagName(storage_kind),
            workload,
            sample_index,
        });
        errdefer allocator.free(root_dir);

        var index = try hbc.HBCIndex.openWithLsmStorage(allocator, root_dir, hbcConfig(cfg), storage_harness.storage());
        errdefer index.close();

        return .{
            .allocator = allocator,
            .cfg = cfg,
            .sample_index = sample_index,
            .storage_kind = storage_kind,
            .workload = workload,
            .root_dir = root_dir,
            .storage_harness = storage_harness,
            .index = index,
        };
    }

    fn deinit(self: *Scenario) void {
        self.index.close();
        self.storage_harness.storage().deleteTree(self.root_dir) catch {};
        self.storage_harness.deinit();
        self.allocator.free(self.root_dir);
        self.* = undefined;
    }
};

fn nanotime() u64 {
    return platform_time.monotonicNs();
}

fn allocPrintZ(allocator: Allocator, comptime fmt: []const u8, args: anytype) ![:0]u8 {
    const raw = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(raw);

    const out = try allocator.allocSentinel(u8, raw.len, 0);
    @memcpy(out[0..raw.len], raw);
    return out;
}

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.c_allocator;
    const cfg = try parseArgs(allocator, init.minimal.args);

    const dataset = try makeDataset(allocator, cfg);
    defer allocator.free(dataset);
    const items = try makeItems(allocator, cfg, dataset);
    defer freeItems(allocator, items);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;

    try out.print(
        "hbc write bench samples={d} vectors={d} dims={d} batch_size={d} leaf_size={d} branching_factor={d} storage={s} kmeans_backend={s} kmeans_update_strategy={s}\n",
        .{ cfg.samples, cfg.vectors, cfg.dims, cfg.batch_size, cfg.leaf_size, cfg.branching_factor, @tagName(cfg.storage_mode), @tagName(cfg.kmeans_backend), @tagName(cfg.kmeans_update_strategy) },
    );
    try stdout_writer.flush();

    const storage_modes: []const StorageSelection = switch (cfg.storage_mode) {
        .host => &[_]StorageSelection{.host},
        .native => &[_]StorageSelection{.native},
        .memory => &[_]StorageSelection{.memory},
        .both => &[_]StorageSelection{ .host, .native, .memory },
    };

    for (storage_modes) |storage_mode| {
        for (0..cfg.samples) |sample_index| {
            try benchBulkBuild(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, items);
            try benchBulkBuildWithOptions(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, items, "bulk_build_external_vectors_empty", .{
                .skip_vector_store = true,
            });
            try benchOnlineBatches(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, items, "online_batches_default_empty", .{});
            try benchOnlineBatches(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, items, "online_batches_assume_absent_empty", .{ .assume_absent_ids = true });
            try benchOnlineBatches(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, items, "online_batches_coalesced_empty", .{
                .assume_absent_ids = true,
                .coalesce_leaf_writes = true,
            });
            try benchOnlineBatches(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, items, "online_batches_coalesced_defer_quantized_empty", .{
                .assume_absent_ids = true,
                .coalesce_leaf_writes = true,
                .defer_quantized_rebuild = true,
            });
            try benchOnlineBatches(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, items, "online_batches_dense_external_vectors_empty", .{
                .assume_absent_ids = true,
                .coalesce_leaf_writes = true,
                .defer_quantized_rebuild = true,
                .skip_vector_store = true,
                .bulk_ingest = true,
            });
            try benchBatchApplyOnWarmIndex(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, items, "batch_apply_dense_external_vectors_warm", .{
                .assume_absent_ids = true,
                .coalesce_leaf_writes = true,
                .defer_quantized_rebuild = true,
                .skip_vector_store = true,
                .bulk_ingest = true,
            });
            try benchOnlineBatchesPerBatchSession(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, items, "online_batches_dense_external_vectors_per_batch_session_empty", .{
                .assume_absent_ids = true,
                .coalesce_leaf_writes = true,
                .defer_quantized_rebuild = true,
                .skip_vector_store = true,
                .bulk_ingest = true,
            });
        }
    }
    try stdout_writer.flush();
}

fn benchBulkBuild(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    items: []const hbc.BatchInsertItem,
) !void {
    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, "bulk_build_empty");
    defer scenario.deinit();

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    const start = nanotime();
    try scenario.index.bulkBuildWithMetadata(items);
    const elapsed = nanotime() - start;
    const after_storage = scenario.storage_harness.snapshotCounters();
    try printResult(writer, &scenario, items.len, elapsed, before_storage, after_storage, scenario.index.getWriteProfile(), .{});
    try stdout_writer.flush();
}

fn benchBulkBuildWithOptions(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    items: []const hbc.BatchInsertItem,
    workload: []const u8,
    options: hbc.BulkBuildOptions,
) !void {
    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, workload);
    defer scenario.deinit();

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    const start = nanotime();
    try scenario.index.bulkBuildWithMetadataOptions(items, options);
    const elapsed = nanotime() - start;
    const after_storage = scenario.storage_harness.snapshotCounters();
    try printResult(writer, &scenario, items.len, elapsed, before_storage, after_storage, scenario.index.getWriteProfile(), .{});
    try stdout_writer.flush();
}

fn benchOnlineBatches(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    items: []const hbc.BatchInsertItem,
    workload: []const u8,
    options: hbc.BatchInsertOptions,
) !void {
    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, workload);
    defer scenario.deinit();

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    const start = nanotime();
    const session_active = options.bulk_ingest;
    if (session_active) {
        try scenario.index.beginBulkIngestSession();
        errdefer scenario.index.abortBulkIngestSession();
    }
    var offset: usize = 0;
    while (offset < items.len) {
        const end = @min(offset + cfg.batch_size, items.len);
        try scenario.index.batchInsertWithMetadataOptions(items[offset..end], options);
        offset = end;
    }
    if (session_active) {
        try scenario.index.finishBulkIngestSessionWithOptions(.{ .compact = false });
    }
    const elapsed = nanotime() - start;
    const after_storage = scenario.storage_harness.snapshotCounters();
    try printResult(writer, &scenario, items.len, elapsed, before_storage, after_storage, scenario.index.getWriteProfile(), .{});
    try stdout_writer.flush();
}

fn benchOnlineBatchesPerBatchSession(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    items: []const hbc.BatchInsertItem,
    workload: []const u8,
    options: hbc.BatchInsertOptions,
) !void {
    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, workload);
    defer scenario.deinit();

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    const start = nanotime();
    var offset: usize = 0;
    while (offset < items.len) {
        const end = @min(offset + cfg.batch_size, items.len);
        try scenario.index.beginBulkIngestSession();
        var session_open = true;
        errdefer if (session_open) scenario.index.abortBulkIngestSession();
        try scenario.index.batchInsertWithMetadataOptions(items[offset..end], options);
        try scenario.index.finishBulkIngestSessionWithOptions(.{ .compact = false });
        session_open = false;
        offset = end;
    }
    const elapsed = nanotime() - start;
    const after_storage = scenario.storage_harness.snapshotCounters();
    try printResult(writer, &scenario, items.len, elapsed, before_storage, after_storage, scenario.index.getWriteProfile(), .{});
    try stdout_writer.flush();
}

fn benchBatchApplyOnWarmIndex(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    items: []const hbc.BatchInsertItem,
    workload: []const u8,
    options: hbc.BatchInsertOptions,
) !void {
    if (items.len < 2) return;

    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, workload);
    defer scenario.deinit();

    const seed_count = @max(@divFloor(items.len, 2), @as(usize, 1));
    try scenario.index.bulkBuildWithMetadataOptions(items[0..seed_count], .{
        .skip_vector_store = options.skip_vector_store,
    });

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    var write_elapsed: u64 = 0;
    var finish_elapsed: u64 = 0;
    const session_active = options.bulk_ingest;
    if (session_active) {
        try scenario.index.beginBulkIngestSession();
        errdefer scenario.index.abortBulkIngestSession();
    }
    var offset: usize = seed_count;
    const write_start = nanotime();
    while (offset < items.len) {
        const end = @min(offset + cfg.batch_size, items.len);
        try scenario.index.batchApplyOptions(items[offset..end], &.{}, options);
        offset = end;
    }
    write_elapsed = nanotime() - write_start;
    if (session_active) {
        const finish_start = nanotime();
        try scenario.index.finishBulkIngestSessionWithOptions(.{ .compact = false });
        finish_elapsed = nanotime() - finish_start;
    }
    const elapsed = write_elapsed + finish_elapsed;
    const after_storage = scenario.storage_harness.snapshotCounters();
    try printResult(writer, &scenario, items.len - seed_count, elapsed, before_storage, after_storage, scenario.index.getWriteProfile(), .{
        .write_ns = write_elapsed,
        .finish_ns = finish_elapsed,
    });
    try stdout_writer.flush();
}

const ResultExtra = struct {
    write_ns: ?u64 = null,
    finish_ns: ?u64 = null,
};

fn printResult(
    writer: anytype,
    scenario: *Scenario,
    vectors: usize,
    ns: u64,
    before_storage: StorageCounters,
    after_storage: StorageCounters,
    profile: hbc.WriteProfile,
    extra: ResultExtra,
) !void {
    const storage_delta = StorageCounters.delta(after_storage, before_storage);
    const maintenance = scenario.index.snapshotLsmMaintenanceStats();
    const active_table_stats = try analyzeActiveTables(scenario);
    const ns_per_vector = @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(@max(vectors, 1)));
    try writer.print(
        "{{\"scenario\":\"{s}_{s}\",\"storage\":\"{s}\",\"sample\":{d},\"workload\":\"{s}\",\"vectors\":{d},\"dims\":{d},\"ns\":{d},\"ns_per_vector\":{d:.2}",
        .{
            @tagName(scenario.storage_kind),
            scenario.workload,
            @tagName(scenario.storage_kind),
            scenario.sample_index,
            scenario.workload,
            vectors,
            scenario.cfg.dims,
            ns,
            ns_per_vector,
        },
    );
    if (extra.write_ns) |write_ns| {
        try writer.print(",\"write_ns\":{d},\"write_ns_per_vector\":{d:.2}", .{
            write_ns,
            @as(f64, @floatFromInt(write_ns)) / @as(f64, @floatFromInt(@max(vectors, 1))),
        });
    }
    if (extra.finish_ns) |finish_ns| {
        try writer.print(",\"finish_ns\":{d},\"finish_ns_per_vector\":{d:.2}", .{
            finish_ns,
            @as(f64, @floatFromInt(finish_ns)) / @as(f64, @floatFromInt(@max(vectors, 1))),
        });
    }
    try writer.print(
        ",\"active_count_after\":{d},\"node_count_after\":{d},\"storage_write_file\":{d},\"storage_write_bytes\":{d},\"storage_manifest_write_file\":{d},\"storage_manifest_write_bytes\":{d},\"storage_rename\":{d},\"storage_delete_file\":{d},\"storage_delete_tree\":{d},\"storage_read_file\":{d},\"storage_read_bytes\":{d}",
        .{
            scenario.index.metadata.active_count,
            scenario.index.metadata.node_count,
            storage_delta.write_file,
            storage_delta.write_bytes,
            storage_delta.manifest_write_file,
            storage_delta.manifest_write_bytes,
            storage_delta.rename,
            storage_delta.delete_file,
            storage_delta.delete_tree,
            storage_delta.read_file,
            storage_delta.read_bytes,
        },
    );
    try writer.print(
        ",\"insert_calls\":{d},\"save_node_calls\":{d},\"split_leaf_calls\":{d},\"split_internal_calls\":{d},\"bulk_build_store_ns\":{d},\"bulk_build_tree_ns\":{d},\"kmeans_assignment_calls\":{d},\"kmeans_assignment_cpu_calls\":{d},\"kmeans_assignment_metal_calls\":{d},\"kmeans_assignment_points_total\":{d},\"kmeans_assignment_ns\":{d},\"kmeans_assignment_cpu_ns\":{d},\"kmeans_assignment_metal_ns\":{d},\"kmeans_update_calls\":{d},\"kmeans_update_cpu_calls\":{d},\"kmeans_update_metal_calls\":{d},\"kmeans_update_ns\":{d},\"kmeans_update_cpu_ns\":{d},\"kmeans_update_metal_ns\":{d},\"insert_transform_ns\":{d},\"insert_store_vector_ns\":{d},\"insert_find_leaf_ns\":{d},\"insert_mutate_leaf_ns\":{d}",
        .{
            profile.insert_calls,
            profile.save_node_calls,
            profile.split_leaf_calls,
            profile.split_internal_calls,
            profile.bulk_build_store_ns,
            profile.bulk_build_tree_ns,
            profile.kmeans_assignment_calls,
            profile.kmeans_assignment_cpu_calls,
            profile.kmeans_assignment_metal_calls,
            profile.kmeans_assignment_points_total,
            profile.kmeans_assignment_ns,
            profile.kmeans_assignment_cpu_ns,
            profile.kmeans_assignment_metal_ns,
            profile.kmeans_update_calls,
            profile.kmeans_update_cpu_calls,
            profile.kmeans_update_metal_calls,
            profile.kmeans_update_ns,
            profile.kmeans_update_cpu_ns,
            profile.kmeans_update_metal_ns,
            profile.insert_transform_ns,
            profile.insert_store_vector_ns,
            profile.insert_find_leaf_ns,
            profile.insert_mutate_leaf_ns,
        },
    );
    try writer.print(
        ",\"insert_flush_metadata_ns\":{d},\"insert_commit_ns\":{d},\"save_node_ns\":{d},\"save_split_range_ns\":{d},\"update_parent_ns\":{d},\"refresh_quantized_ns\":{d},\"quantized_vector_load_ns\":{d},\"quantized_compute_ns\":{d},\"quantized_store_ns\":{d},\"quantized_encode_ns\":{d},\"quantized_put_ns\":{d},\"split_leaf_vector_load_ns\":{d},\"split_leaf_partition_ns\":{d},\"split_leaf_finalize_ns\":{d}",
        .{
            profile.insert_flush_metadata_ns,
            profile.insert_commit_ns,
            profile.save_node_ns,
            profile.save_split_range_ns,
            profile.update_parent_ns,
            profile.refresh_quantized_ns,
            profile.quantized_vector_load_ns,
            profile.quantized_compute_ns,
            profile.quantized_store_ns,
            profile.quantized_encode_ns,
            profile.quantized_put_ns,
            profile.split_leaf_vector_load_ns,
            profile.split_leaf_partition_ns,
            profile.split_leaf_finalize_ns,
        },
    );
    try writer.print(
        ",\"grouped_leaf_groups\":{d},\"grouped_items\":{d},\"grouped_fallback_items\":{d},\"grouped_split_candidates\":{d},\"grouped_recursive_splits\":{d},\"grouped_leaf_range_writes\":{d},\"grouped_ancestor_range_refreshes\":{d},\"grouped_ancestor_range_nodes\":{d},\"grouped_node_body_writes\":{d},\"grouped_vec_leaf_writes\":{d}",
        .{
            profile.grouped_leaf_groups,
            profile.grouped_items,
            profile.grouped_fallback_items,
            profile.grouped_split_candidates,
            profile.grouped_recursive_splits,
            profile.grouped_leaf_range_writes,
            profile.grouped_ancestor_range_refreshes,
            profile.grouped_ancestor_range_nodes,
            profile.grouped_node_body_writes,
            profile.grouped_vec_leaf_writes,
        },
    );
    try writer.print(
        ",\"ns_nodes_put_calls\":{d},\"ns_nodes_append_calls\":{d},\"ns_nodes_delete_calls\":{d},\"ns_nodes_key_bytes\":{d},\"ns_nodes_value_bytes\":{d},\"ns_meta_put_calls\":{d},\"ns_meta_append_calls\":{d},\"ns_meta_delete_calls\":{d},\"ns_meta_key_bytes\":{d},\"ns_meta_value_bytes\":{d}",
        .{
            profile.ns_nodes_put_calls,
            profile.ns_nodes_append_calls,
            profile.ns_nodes_delete_calls,
            profile.ns_nodes_key_bytes,
            profile.ns_nodes_value_bytes,
            profile.ns_meta_put_calls,
            profile.ns_meta_append_calls,
            profile.ns_meta_delete_calls,
            profile.ns_meta_key_bytes,
            profile.ns_meta_value_bytes,
        },
    );
    try writer.print(
        ",\"ns_quant_put_calls\":{d},\"ns_quant_append_calls\":{d},\"ns_quant_delete_calls\":{d},\"ns_quant_key_bytes\":{d},\"ns_quant_value_bytes\":{d},\"ns_vecs_put_calls\":{d},\"ns_vecs_append_calls\":{d},\"ns_vecs_delete_calls\":{d},\"ns_vecs_key_bytes\":{d},\"ns_vecs_value_bytes\":{d},\"range_put_calls\":{d},\"range_delete_calls\":{d},\"range_key_bytes\":{d},\"range_value_bytes\":{d}",
        .{
            profile.ns_quant_put_calls,
            profile.ns_quant_append_calls,
            profile.ns_quant_delete_calls,
            profile.ns_quant_key_bytes,
            profile.ns_quant_value_bytes,
            profile.ns_vecs_put_calls,
            profile.ns_vecs_append_calls,
            profile.ns_vecs_delete_calls,
            profile.ns_vecs_key_bytes,
            profile.ns_vecs_value_bytes,
            profile.range_put_calls,
            profile.range_delete_calls,
            profile.range_key_bytes,
            profile.range_value_bytes,
        },
    );
    if (maintenance) |stats| {
        try writer.print(
            ",\"lsm_total_runs\":{d},\"lsm_total_run_bytes\":{d},\"lsm_total_run_logical_entry_bytes\":{d},\"lsm_total_run_physical_entry_bytes\":{d},\"lsm_total_run_compressed_blocks\":{d},\"lsm_total_run_raw_blocks\":{d},\"lsm_total_run_compression_codec_mask\":{d},\"lsm_l0_runs\":{d},\"lsm_l0_bytes\":{d},\"lsm_overlapping_l0_runs\":{d},\"lsm_lower_level_runs\":{d},\"lsm_lower_level_bytes\":{d},\"lsm_max_level\":{d},\"lsm_obsolete_paths\":{d},\"lsm_compaction_scheduler_grants\":{d},\"lsm_compaction_scheduler_denied_capacity\":{d},\"lsm_compaction_scheduler_denied_resource_pressure\":{d},\"lsm_compaction_scheduler_remembered_pending\":{d},\"lsm_compaction_scheduler_remembered_candidates\":{d},\"lsm_compaction_scheduler_remembered_retries\":{d},\"lsm_compaction_scheduler_remembered_hits\":{d},\"lsm_compaction_scheduler_remembered_stale\":{d},\"lsm_compaction_scheduler_conflict_denials\":{d}",
            .{
                stats.total_runs,
                stats.total_run_bytes,
                stats.total_run_logical_entry_bytes,
                stats.total_run_physical_entry_bytes,
                stats.total_run_compressed_blocks,
                stats.total_run_raw_blocks,
                stats.total_run_compression_codec_mask,
                stats.l0_runs,
                stats.l0_bytes,
                stats.overlapping_l0_runs,
                stats.lower_level_runs,
                stats.lower_level_bytes,
                stats.max_level,
                stats.obsolete_paths,
                stats.compaction_scheduler_grants,
                stats.compaction_scheduler_denied_capacity,
                stats.compaction_scheduler_denied_resource_pressure,
                stats.compaction_scheduler_remembered_pending,
                stats.compaction_scheduler_remembered_candidates,
                stats.compaction_scheduler_remembered_retries,
                stats.compaction_scheduler_remembered_hits,
                stats.compaction_scheduler_remembered_stale,
                stats.compaction_scheduler_conflict_denials,
            },
        );
    }
    if (active_table_stats) |stats| {
        try writer.print(
            ",\"active_table_runs\":{d},\"active_table_run_bytes\":{d},\"active_table_obsolete_paths\":{d},\"active_hbc_quant_entries\":{d},\"active_hbc_quant_value_bytes\":{d},\"latest_hbc_quant_entries\":{d},\"latest_hbc_quant_value_bytes\":{d},\"hbc_quant_versions_per_key_bps\":{d},\"active_hbc_vecs_entries\":{d},\"active_hbc_vecs_value_bytes\":{d},\"latest_hbc_vecs_entries\":{d},\"latest_hbc_vecs_value_bytes\":{d},\"active_hbc_nodes_entries\":{d},\"active_hbc_nodes_value_bytes\":{d},\"latest_hbc_nodes_entries\":{d},\"latest_hbc_nodes_value_bytes\":{d},\"active_hbc_meta_entries\":{d},\"active_hbc_meta_value_bytes\":{d},\"latest_hbc_meta_entries\":{d},\"latest_hbc_meta_value_bytes\":{d},\"latest_lsm_keys\":{d}",
            .{
                stats.active_runs,
                stats.active_run_bytes,
                stats.obsolete_paths,
                stats.hbc_quant.all_entries,
                stats.hbc_quant.all_value_bytes,
                stats.hbc_quant.latest_entries,
                stats.hbc_quant.latest_value_bytes,
                stats.quantVersionsPerKeyBps(),
                stats.hbc_vecs.all_entries,
                stats.hbc_vecs.all_value_bytes,
                stats.hbc_vecs.latest_entries,
                stats.hbc_vecs.latest_value_bytes,
                stats.hbc_nodes.all_entries,
                stats.hbc_nodes.all_value_bytes,
                stats.hbc_nodes.latest_entries,
                stats.hbc_nodes.latest_value_bytes,
                stats.hbc_meta.all_entries,
                stats.hbc_meta.all_value_bytes,
                stats.hbc_meta.latest_entries,
                stats.hbc_meta.latest_value_bytes,
                stats.latest_keys,
            },
        );
    }
    try writer.print("}}\n", .{});
}

fn analyzeActiveTables(scenario: *Scenario) !?ActiveTableStats {
    if (scenario.storage_kind == .memory) return null;

    const allocator = scenario.allocator;
    const storage = scenario.storage_harness.storage();
    const manifest_path = try std.fmt.allocPrint(allocator, "{s}/manifest.bin", .{scenario.root_dir});
    defer allocator.free(manifest_path);

    const manifest_bytes = storage.readFileAlloc(allocator, manifest_path, std.math.maxInt(usize)) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer allocator.free(manifest_bytes);

    var manifest = try lsm_manifest.decodeAlloc(allocator, manifest_bytes);
    defer manifest.deinit(allocator);

    var stats = ActiveTableStats{
        .active_runs = @intCast(manifest.runs.len),
        .obsolete_paths = @intCast(manifest.obsolete_paths.len),
    };
    for (manifest.runs) |run| stats.active_run_bytes += run.size_bytes;

    var latest = std.StringHashMap(LatestEntry).init(allocator);
    defer {
        var key_it = latest.keyIterator();
        while (key_it.next()) |key| allocator.free(key.*);
        latest.deinit();
    }

    for (manifest.runs) |run| {
        const path = try resolveRunPathAlloc(allocator, scenario.root_dir, run.path);
        defer allocator.free(path);
        try analyzeRunTable(allocator, storage, path, &stats, &latest);
    }

    var value_it = latest.valueIterator();
    while (value_it.next()) |entry| {
        stats.latest_keys += 1;
        const ns_stats = statsForTag(&stats, entry.tag) orelse continue;
        ns_stats.latest_entries += 1;
        ns_stats.latest_value_bytes += entry.value_len;
    }

    return stats;
}

fn analyzeRunTable(
    allocator: Allocator,
    storage: lsm_backend.Storage,
    path: []const u8,
    stats: *ActiveTableStats,
    latest: *std.StringHashMap(LatestEntry),
) !void {
    var index = try lsm_repository.loadRunTableIndexAllocWithStorage(storage, allocator, path);
    defer index.deinit(allocator);

    if (index.blockCount() == 0) return;
    for (index.blocks, 0..) |block, block_index| {
        const window = index.blockWindow(block_index);
        const physical_offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
        const payload = try storage.readFileRangeAlloc(allocator, path, physical_offset, window.physicalLen());
        defer allocator.free(payload);

        const decoded = try lsm_table_file.decodeBlockPayloadAlloc(allocator, window.compression, payload, window.len);
        defer allocator.free(decoded);

        const end = block.first_entry_index + block.entry_count;
        for (block.first_entry_index..end) |entry_index| {
            const local_offset = index.entry_offsets[entry_index] - block.relative_offset;
            const entry = try lsm_table_file.parseEntryAt(decoded, local_offset);
            try recordActiveEntry(allocator, stats, latest, entry);
        }
    }
}

fn recordActiveEntry(
    allocator: Allocator,
    stats: *ActiveTableStats,
    latest: *std.StringHashMap(LatestEntry),
    entry: lsm_table_file.Entry,
) !void {
    const tag = namespaceTag(entry.namespace_name);
    if (statsForTag(stats, tag)) |ns_stats| {
        ns_stats.all_entries += 1;
        ns_stats.all_value_bytes += entry.value.len;
    }

    const key = try compositeEntryKeyAlloc(allocator, entry.namespace_name, entry.key);
    errdefer allocator.free(key);
    const result = try latest.getOrPut(key);
    if (result.found_existing) {
        allocator.free(key);
        return;
    }
    result.value_ptr.* = .{
        .tag = tag,
        .value_len = @intCast(entry.value.len),
    };
}

fn statsForTag(stats: *ActiveTableStats, tag: NamespaceTag) ?*NamespaceActiveStats {
    return switch (tag) {
        .hbc_quant => &stats.hbc_quant,
        .hbc_vecs => &stats.hbc_vecs,
        .hbc_nodes => &stats.hbc_nodes,
        .hbc_meta => &stats.hbc_meta,
        .other => null,
    };
}

fn namespaceTag(namespace_name: ?[]const u8) NamespaceTag {
    const name = namespace_name orelse return .other;
    if (std.mem.eql(u8, name, "hbc_quant")) return .hbc_quant;
    if (std.mem.eql(u8, name, "hbc_vecs")) return .hbc_vecs;
    if (std.mem.eql(u8, name, "hbc_nodes")) return .hbc_nodes;
    if (std.mem.eql(u8, name, "hbc_meta")) return .hbc_meta;
    return .other;
}

fn compositeEntryKeyAlloc(allocator: Allocator, namespace_name: ?[]const u8, key: []const u8) ![]u8 {
    const namespace = namespace_name orelse "";
    const out = try allocator.alloc(u8, @sizeOf(u32) + namespace.len + key.len);
    std.mem.writeInt(u32, out[0..4], @intCast(namespace.len), .little);
    @memcpy(out[4..][0..namespace.len], namespace);
    @memcpy(out[4 + namespace.len ..], key);
    return out;
}

fn resolveRunPathAlloc(allocator: Allocator, root_dir: []const u8, run_path: []const u8) ![]u8 {
    if (std.fs.path.isAbsolute(run_path)) return try allocator.dupe(u8, run_path);
    return try std.fs.path.join(allocator, &.{ root_dir, run_path });
}

fn hbcConfig(cfg: Config) hbc.HBCConfig {
    return .{
        .storage_backend = .lsm,
        .dims = @intCast(cfg.dims),
        .metric = .cosine,
        .split_algo = cfg.split_algo,
        .branching_factor = cfg.branching_factor,
        .leaf_size = cfg.leaf_size,
        .search_width = cfg.branching_factor,
        .epsilon = 7,
        .use_quantization = cfg.use_quantization,
        .rerank_policy = .boundary,
        .quantizer_seed = cfg.seed,
        .use_random_ortho_trans = cfg.use_random_ortho_trans,
        .bulk_build_algo = cfg.bulk_build_algo,
        .kmeans_backend = cfg.kmeans_backend,
        .kmeans_update_strategy = cfg.kmeans_update_strategy,
        .max_cached_nodes = 100_000,
        .max_cached_vectors = 100_000,
    };
}

fn makeDataset(allocator: Allocator, cfg: Config) ![]f32 {
    var rng = std.Random.DefaultPrng.init(cfg.seed);
    const random = rng.random();
    const data = try allocator.alloc(f32, cfg.vectors * cfg.dims);
    for (0..cfg.vectors) |row| {
        const cluster = row % 16;
        const base = @as(f32, @floatFromInt(cluster)) * 0.10;
        for (0..cfg.dims) |dim| {
            data[row * cfg.dims + dim] = base + random.float(f32) * 0.01;
        }
        _ = vec.normalize(data[row * cfg.dims ..][0..cfg.dims]);
    }
    return data;
}

fn makeItems(allocator: Allocator, cfg: Config, dataset: []const f32) ![]hbc.BatchInsertItem {
    const items = try allocator.alloc(hbc.BatchInsertItem, cfg.vectors);
    errdefer allocator.free(items);
    var initialized: usize = 0;
    errdefer for (items[0..initialized]) |item| allocator.free(item.metadata);

    for (items, 0..) |*item, i| {
        const metadata = try std.fmt.allocPrint(allocator, "doc:{d:0>8}", .{i});
        item.* = .{
            .vector_id = @intCast(i + 1),
            .vector = dataset[i * cfg.dims ..][0..cfg.dims],
            .metadata = metadata,
        };
        initialized += 1;
    }
    return items;
}

fn freeItems(allocator: Allocator, items: []hbc.BatchInsertItem) void {
    for (items) |item| allocator.free(item.metadata);
    allocator.free(items);
}

fn parseArgs(allocator: Allocator, proc_args: std.process.Args) !Config {
    _ = allocator;
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(proc_args);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--vectors")) {
            cfg.vectors = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--dims")) {
            cfg.dims = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--leaf-size")) {
            cfg.leaf_size = @intCast(try parseNextUsize(&args, arg));
        } else if (std.mem.eql(u8, arg, "--branching-factor")) {
            cfg.branching_factor = @intCast(try parseNextUsize(&args, arg));
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--storage")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.storage_mode = std.meta.stringToEnum(StorageSelection, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--split-hilbert")) {
            cfg.split_algo = .hilbert;
        } else if (std.mem.eql(u8, arg, "--bulk-build-recursive")) {
            cfg.bulk_build_algo = .recursive;
        } else if (std.mem.eql(u8, arg, "--bulk-build-doc-key-seeded")) {
            cfg.bulk_build_algo = .doc_key_seeded;
        } else if (std.mem.eql(u8, arg, "--bulk-build-kmeans")) {
            cfg.bulk_build_algo = .kmeans;
        } else if (std.mem.eql(u8, arg, "--kmeans-backend")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.kmeans_backend = std.meta.stringToEnum(hbc.HBCConfig.KmeansBackend, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--kmeans-update-strategy")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.kmeans_update_strategy = std.meta.stringToEnum(hbc.HBCConfig.KmeansUpdateStrategy, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--no-quantization")) {
            cfg.use_quantization = false;
        } else if (std.mem.eql(u8, arg, "--random-ortho")) {
            cfg.use_random_ortho_trans = true;
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.samples == 0 or cfg.vectors == 0 or cfg.dims == 0 or cfg.batch_size == 0) return error.InvalidArgument;
    return cfg;
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

fn isManifestPath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, "manifest.bin") or std.mem.indexOf(u8, path, "manifest.bin.") != null;
}
