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
    inspect_root: ?[]const u8 = null,
    inspect_maintenance_steps: usize = 0,
    samples: usize = 3,
    vectors: usize = 10_000,
    dims: usize = 128,
    batch_size: usize = 1_000,
    seed: u64 = 42,
    leaf_size: u32 = 128,
    branching_factor: u32 = 128,
    search_width: u32 = 0,
    storage_mode: StorageSelection = .host,
    split_algo: vec.ClustAlgorithm = .kmeans,
    bulk_build_algo: hbc.BulkBuildAlgo = .hilbert_seeded,
    kmeans_backend: hbc.HBCConfig.KmeansBackend = .auto,
    kmeans_update_strategy: hbc.HBCConfig.KmeansUpdateStrategy = .auto,
    use_quantization: bool = true,
    use_random_ortho_trans: bool = false,
    centroid_directory_mode: hbc.HBCConfig.CentroidDirectoryMode = .hbc,
    posting_storage_mode: hbc.HBCConfig.PostingStorageMode = .packed_hbc,
    posting_base_member_block_size: usize = 32,
    flat_centroid_block_size: usize = 128,
    flat_centroid_probe_count: usize = 0,
    flat_centroid_block_probe_count: usize = 0,
    max_posting_overlay_cache_bytes: usize = 8 * 1024 * 1024,
    max_posting_overlay_cache_entry_bytes: usize = 0,
    workload_filter: ?[]const u8 = null,
    lazy_posting_maintenance: bool = false,
    auto_posting_maintenance_max_postings: usize = 0,
    auto_posting_maintenance_fold_delta_tails: bool = true,
    auto_posting_maintenance_min_delta_records_to_fold: usize = 64,
    auto_posting_maintenance_min_tombstone_records_to_fold: usize = 16,
    auto_posting_maintenance_min_delta_to_base_ratio_bps: u32 = 2500,
    auto_posting_maintenance_max_delta_tail_postings: usize = std.math.maxInt(usize),
    auto_posting_maintenance_min_dirty_postings: usize = 0,
    auto_posting_maintenance_max_dirty_version_age: u64 = 0,
    auto_posting_maintenance_min_delta_records_to_run: usize = 0,
    auto_posting_maintenance_min_tombstone_records_to_run: usize = 0,
    auto_posting_maintenance_min_delta_to_base_ratio_bps_to_run: u32 = 0,
    auto_posting_maintenance_min_centroid_version_lag: u64 = 0,
    auto_posting_maintenance_min_payload_version_lag: u64 = 0,
    auto_posting_maintenance_max_layout_changes: usize = 0,
    auto_posting_maintenance_split_full_postings: bool = false,
    auto_posting_maintenance_min_overfull_postings_to_run: usize = 0,
    auto_posting_maintenance_min_postings_at_capacity_to_run: usize = 0,
    auto_posting_maintenance_max_boundary_reassignments: usize = 0,
    auto_posting_maintenance_allow_overfull_reassignment: bool = false,
    auto_posting_maintenance_max_overfull_reassignment_postings: usize = 0,
    auto_posting_maintenance_max_over_capacity_reassignment_members: usize = 0,
    auto_posting_maintenance_boundary_reassignment_min_improvement: f32 = 0.0,
    repair_postings_after_write: bool = false,
    repair_postings_before_bulk_finish: bool = false,
    repair_fold_delta_tails: bool = true,
    repair_min_delta_records_to_fold: usize = 64,
    repair_min_tombstone_records_to_fold: usize = 16,
    repair_min_delta_to_base_ratio_bps: u32 = 0,
    repair_max_delta_tail_postings: usize = std.math.maxInt(usize),
    repair_dirty_reassignments: usize = 0,
    repair_rebalance_layout: bool = false,
    repair_split_full_postings: bool = false,
    repair_max_layout_changes: usize = std.math.maxInt(usize),
    repair_dirty_reassignment_allow_overfull: bool = false,
    repair_dirty_reassignment_max_overfull_postings: usize = 0,
    repair_dirty_reassignment_max_over_capacity_members: usize = 0,
    repair_dirty_reassignment_min_improvement: f32 = 0.0,
    defer_leaf_splits_to_posting_maintenance: bool = false,
    bulk_ingest_finish_compact: bool = false,
    bulk_ingest_finish_max_deferred_l0_runs: ?usize = null,
    bulk_ingest_finish_max_foreground_compaction_steps: usize = 0,
    coalesce_overwrite_leaf_writes: bool = true,
    skip_vector_store: bool = false,
    overwrite_hot_keys: usize = 0,
    overwrite_rounds: usize = 1,
    post_write_queries: usize = 0,
    post_write_query_rounds: usize = 1,
    post_write_k: usize = 10,
    post_write_recall_mode: PostWriteRecallMode = .exact,
    post_write_truth_cache_path: ?[]const u8 = null,
    post_write_truth_cache_only: bool = false,
    dataset_mode: DatasetMode = .materialized,
};

const DatasetMode = enum {
    materialized,
    procedural,
};

const PostWriteRecallMode = enum {
    exact,
    self_hit,
};

const StorageSelection = enum {
    host,
    native,
    memory,
    both,
};

const StorageCounters = struct {
    read_file: u64 = 0,
    read_range: u64 = 0,
    read_trailer: u64 = 0,
    file_size: u64 = 0,
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
            .read_range = after.read_range - before.read_range,
            .read_trailer = after.read_trailer - before.read_trailer,
            .file_size = after.file_size - before.file_size,
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
    obsolete_file_bytes: u64 = 0,
    obsolete_due_paths: u64 = 0,
    obsolete_due_file_bytes: u64 = 0,
    obsolete_future_paths: u64 = 0,
    manifest_bytes: u64 = 0,
    active_bloom_filter_bytes: u64 = 0,
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
            self.counters.read_range += 1;
            const bytes = try self.backing.readFileRangeAlloc(allocator, path, offset, len);
            self.counters.read_bytes += bytes.len;
            return bytes;
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.file_size += 1;
            return self.backing.fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.read_trailer += 1;
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

const ExternalVectorDataset = struct {
    data: []const f32,
    dims: usize,
    seed: u64,
    mode: DatasetMode = .materialized,
    override_slots: []const usize = &.{},
    override_vectors: []const f32 = &.{},

    fn copyVector(self: *const ExternalVectorDataset, vector_id: u64, dst: []f32) !void {
        if (vector_id == 0) return error.NotFound;
        const row: usize = @intCast(vector_id - 1);
        if (dst.len < self.dims) return error.BufferTooSmall;
        if (self.overrideSlot(row)) |slot| {
            const offset = std.math.mul(usize, slot, self.dims) catch return error.NotFound;
            if (offset + self.dims > self.override_vectors.len) return error.NotFound;
            @memcpy(dst[0..self.dims], self.override_vectors[offset..][0..self.dims]);
            return;
        }
        switch (self.mode) {
            .materialized => {
                const offset = std.math.mul(usize, row, self.dims) catch return error.NotFound;
                if (offset + self.dims > self.data.len) return error.NotFound;
                @memcpy(dst[0..self.dims], self.data[offset..][0..self.dims]);
            },
            .procedural => writeProceduralDatasetVector(self.seed, row, dst[0..self.dims]),
        }
    }

    fn overrideSlot(self: *const ExternalVectorDataset, row: usize) ?usize {
        if (row >= self.override_slots.len) return null;
        const slot = self.override_slots[row];
        if (slot == std.math.maxInt(usize)) return null;
        return slot;
    }

    fn hasOverride(self: *const ExternalVectorDataset, row: usize) bool {
        return self.overrideSlot(row) != null;
    }

    fn loadBatchScratch(
        raw_ctx: *anyopaque,
        vector_ids: []const u64,
        metadata: []const ?[]const u8,
        vector_views: [][]const f32,
        batch_scratch: []f32,
        dims: usize,
    ) anyerror!void {
        _ = metadata;
        const self: *ExternalVectorDataset = @ptrCast(@alignCast(raw_ctx));
        if (dims != self.dims) return error.InvalidArgument;
        if (batch_scratch.len < vector_ids.len * dims) return error.BufferTooSmall;
        for (vector_ids, 0..) |vector_id, i| {
            const out = batch_scratch[i * dims ..][0..dims];
            try self.copyVector(vector_id, out);
            vector_views[i] = out;
        }
    }

    fn loadScratch(
        raw_ctx: *anyopaque,
        vector_id: u64,
        metadata: []const u8,
        scratch: []f32,
    ) anyerror![]const f32 {
        _ = metadata;
        const self: *ExternalVectorDataset = @ptrCast(@alignCast(raw_ctx));
        if (scratch.len < self.dims) return error.BufferTooSmall;
        try self.copyVector(vector_id, scratch[0..self.dims]);
        return scratch[0..self.dims];
    }

    fn loadTransformedMatrix(
        raw_ctx: *anyopaque,
        vector_ids: []const u64,
        metadata: []const ?[]const u8,
        matrix_positions: []const usize,
        matrix: []f32,
        scratch: []f32,
        dims: usize,
        index: *hbc.HBCIndex,
        transform: hbc.HBCIndex.ExternalVectorTransformFn,
    ) anyerror!void {
        _ = metadata;
        const self: *ExternalVectorDataset = @ptrCast(@alignCast(raw_ctx));
        if (dims != self.dims) return error.InvalidArgument;
        if (vector_ids.len != matrix_positions.len) return error.InvalidArgument;
        if (scratch.len < dims) return error.BufferTooSmall;
        for (vector_ids, matrix_positions) |vector_id, matrix_position| {
            try self.copyVector(vector_id, scratch[0..dims]);
            const out = matrix[matrix_position * dims ..][0..dims];
            _ = transform(index, scratch[0..dims], out);
        }
    }
};

const ProceduralBatch = struct {
    allocator: Allocator,
    vectors: []f32,
    items: []hbc.BatchInsertItem,

    fn init(allocator: Allocator, cfg: Config) !ProceduralBatch {
        return .{
            .allocator = allocator,
            .vectors = try allocator.alloc(f32, cfg.batch_size * cfg.dims),
            .items = try allocator.alloc(hbc.BatchInsertItem, cfg.batch_size),
        };
    }

    fn deinit(self: *ProceduralBatch) void {
        self.allocator.free(self.items);
        self.allocator.free(self.vectors);
        self.* = undefined;
    }

    fn fill(self: *ProceduralBatch, cfg: Config, start_row: usize, end_row: usize) ![]hbc.BatchInsertItem {
        if (end_row < start_row or end_row - start_row > self.items.len) return error.InvalidArgument;
        const count = end_row - start_row;
        for (0..count) |i| {
            const row = start_row + i;
            const vector = self.vectors[i * cfg.dims ..][0..cfg.dims];
            writeProceduralDatasetVector(cfg.seed, row, vector);
            self.items[i] = .{
                .vector_id = @intCast(row + 1),
                .vector = vector,
                .metadata = "",
            };
        }
        return self.items[0..count];
    }
};

fn buildProceduralWarmIndex(scenario: *Scenario) !void {
    if (scenario.cfg.skip_vector_store) {
        try scenario.index.bulkBuildExternalSequentialWithMetadataOptions(scenario.cfg.vectors, .{
            .skip_vector_store = true,
        });
        return;
    }

    try scenario.index.beginBulkIngestSession();
    var session_open = true;
    errdefer if (session_open) scenario.index.abortBulkIngestSession();

    var batch = try ProceduralBatch.init(scenario.allocator, scenario.cfg);
    defer batch.deinit();
    var offset: usize = 0;
    while (offset < scenario.cfg.vectors) {
        const end = @min(offset + scenario.cfg.batch_size, scenario.cfg.vectors);
        const batch_items = try batch.fill(scenario.cfg, offset, end);
        try scenario.index.batchInsertWithMetadataOptions(batch_items, .{
            .assume_absent_ids = true,
            .coalesce_leaf_writes = true,
            .defer_quantized_rebuild = true,
            .skip_vector_store = scenario.cfg.skip_vector_store,
            .bulk_ingest = true,
        });
        offset = end;
    }
    try scenario.index.finishBulkIngestSessionWithOptions(.{
        .compact = scenario.cfg.bulk_ingest_finish_compact,
        .max_deferred_l0_runs = scenario.cfg.bulk_ingest_finish_max_deferred_l0_runs,
        .max_foreground_compaction_steps = scenario.cfg.bulk_ingest_finish_max_foreground_compaction_steps,
    });
    session_open = false;
}

fn benchBulkBuildProceduralExternalVectorsSequential(
    io: std.Io,
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
) !void {
    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, "bulk_build_external_vectors_sequential_empty", &.{});
    defer scenario.deinit();

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    const start = nanotime();
    try scenario.index.bulkBuildExternalSequentialWithMetadataOptions(cfg.vectors, .{
        .skip_vector_store = true,
    });
    const elapsed = nanotime() - start;
    const foreground_profile = scenario.index.getWriteProfile();
    const pre_repair_query = try benchPostWriteQueriesGenerated(io, &scenario, cfg.vectors, null);
    clearSearchScratchCacheIfAvailable(&scenario.index);
    const repair_before_bulk_finish_ns = try repairPostingsBeforeBulkFinish(&scenario);
    const repair_after_write_ns = if (repair_before_bulk_finish_ns == null)
        try repairPostingsAfterWrite(&scenario)
    else
        null;
    const final_profile = scenario.index.getWriteProfile();
    const repair_profile = writeProfileDelta(final_profile, foreground_profile);
    const after_storage = scenario.storage_harness.snapshotCounters();
    const post_write_query = try benchPostWriteQueriesGenerated(io, &scenario, cfg.vectors, null);
    try printResult(writer, &scenario, cfg.vectors, elapsed, before_storage, after_storage, final_profile, .{
        .foreground_profile = foreground_profile,
        .repair_profile = repair_profile,
        .posting_repair_before_bulk_finish_ns = repair_before_bulk_finish_ns,
        .posting_repair_after_write_ns = repair_after_write_ns,
        .pre_repair_query = pre_repair_query,
        .post_write_query = post_write_query,
    });
    try stdout_writer.flush();
}

const Scenario = struct {
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_kind: StorageSelection,
    workload: []const u8,
    root_dir: [:0]u8,
    storage_harness: StorageHarness,
    external_vector_dataset: *ExternalVectorDataset,
    index: hbc.HBCIndex,
    posting_repair_before_bulk_finish_result: ?hbc.PostingMaintenanceResult = null,
    posting_repair_after_write_result: ?hbc.PostingMaintenanceResult = null,

    fn init(
        allocator: Allocator,
        cfg: Config,
        sample_index: usize,
        storage_kind: StorageSelection,
        workload: []const u8,
        dataset: []const f32,
    ) !Scenario {
        var storage_harness = try StorageHarness.init(allocator, storage_kind);
        errdefer storage_harness.deinit();

        const root_dir = try allocPrintZ(allocator, "{s}/hbc-write-bench-{s}-{s}-{s}-{d}", .{
            if (storage_kind == .native) "/tmp" else "",
            @tagName(storage_kind),
            @tagName(cfg.posting_storage_mode),
            workload,
            sample_index,
        });
        errdefer allocator.free(root_dir);

        var index = try hbc.HBCIndex.openWithLsmStorage(allocator, root_dir, hbcConfig(cfg), storage_harness.storage());
        errdefer index.close();

        const external_vector_dataset = try allocator.create(ExternalVectorDataset);
        errdefer allocator.destroy(external_vector_dataset);
        external_vector_dataset.* = .{
            .data = dataset,
            .dims = cfg.dims,
            .seed = cfg.seed,
            .mode = cfg.dataset_mode,
        };
        index.setExternalVectorScratchLoader(external_vector_dataset, ExternalVectorDataset.loadScratch);
        index.setExternalVectorBatchScratchLoader(external_vector_dataset, ExternalVectorDataset.loadBatchScratch);
        index.setExternalVectorBatchTransformedMatrixLoader(external_vector_dataset, ExternalVectorDataset.loadTransformedMatrix);
        index.setExternalVectorMetadataRequired(false);

        return .{
            .allocator = allocator,
            .cfg = cfg,
            .sample_index = sample_index,
            .storage_kind = storage_kind,
            .workload = workload,
            .root_dir = root_dir,
            .storage_harness = storage_harness,
            .external_vector_dataset = external_vector_dataset,
            .index = index,
        };
    }

    fn deinit(self: *Scenario) void {
        self.index.close();
        self.storage_harness.storage().deleteTree(self.root_dir) catch {};
        self.storage_harness.deinit();
        self.allocator.destroy(self.external_vector_dataset);
        self.allocator.free(self.root_dir);
        self.* = undefined;
    }
};

const QueryProfileTotals = struct {
    total_ns: u64 = 0,
    setup_ns: u64 = 0,
    runtime_txn_ns: u64 = 0,
    scratch_acquire_ns: u64 = 0,
    search_scratch_allocations: u64 = 0,
    search_scratch_allocation_bytes: u64 = 0,
    search_scratch_retained_bytes: u64 = 0,
    upper_tree_pin_ns: u64 = 0,
    root_load_ns: u64 = 0,
    node_cache_miss_ns: u64 = 0,
    node_cache_misses: u64 = 0,
    quantized_cache_miss_ns: u64 = 0,
    quantized_cache_misses: u64 = 0,
    quantized_internal_cache_miss_ns: u64 = 0,
    quantized_internal_cache_misses: u64 = 0,
    quantized_leaf_cache_miss_ns: u64 = 0,
    quantized_leaf_cache_misses: u64 = 0,
    child_expand_ns: u64 = 0,
    leaf_score_ns: u64 = 0,
    posting_overlay_ns: u64 = 0,
    posting_overlay_calls: u64 = 0,
    posting_overlay_base_members: u64 = 0,
    posting_base_decode_ns: u64 = 0,
    posting_base_decode_members: u64 = 0,
    posting_delta_replay_ns: u64 = 0,
    posting_delta_replay_records: u64 = 0,
    posting_overlay_delta_records: u64 = 0,
    posting_overlay_delta_scan_skips: u64 = 0,
    posting_overlay_materialized_members: u64 = 0,
    posting_overlay_fallbacks: u64 = 0,
    posting_overlay_cache_hits: u64 = 0,
    posting_overlay_cache_misses: u64 = 0,
    posting_overlay_cache_evictions: u64 = 0,
    posting_overlay_cache_admission_skips: u64 = 0,
    posting_overlay_cache_member_bytes: u64 = 0,
    rerank_ns: u64 = 0,
    rerank_vector_load_ns: u64 = 0,
    rerank_metadata_ns: u64 = 0,
    nodes_visited: u64 = 0,
    leaves_explored: u64 = 0,
    centroid_directory_blocks_scanned: u64 = 0,
    centroid_directory_blocks_selected: u64 = 0,
    centroid_directory_block_probe_limit: u64 = 0,
    centroid_directory_block_probe_count: u64 = 0,
    centroid_directory_block_centroids_scored: u64 = 0,
    centroid_directory_block_centroid_estimates: u64 = 0,
    centroid_directory_posting_centroids_scored: u64 = 0,
    centroid_directory_posting_centroid_estimates: u64 = 0,
    approx_vectors_scored: u64 = 0,
    exact_vectors_scored: u64 = 0,
    reranked_vectors: u64 = 0,
    approx_candidate_count: u64 = 0,
    rerank_candidate_count: u64 = 0,
    result_count: u64 = 0,

    fn add(self: *QueryProfileTotals, profiled: *const hbc.ProfiledSearchResults) void {
        const p = profiled.profile;
        self.total_ns += p.total_ns;
        self.setup_ns += p.setup_ns;
        self.runtime_txn_ns += p.runtime_txn_ns;
        self.scratch_acquire_ns += p.scratch_acquire_ns;
        self.search_scratch_allocations += p.search_scratch_allocations;
        self.search_scratch_allocation_bytes += p.search_scratch_allocation_bytes;
        self.search_scratch_retained_bytes = @max(self.search_scratch_retained_bytes, p.search_scratch_retained_bytes);
        self.upper_tree_pin_ns += p.upper_tree_pin_ns;
        self.root_load_ns += p.root_load_ns;
        self.node_cache_miss_ns += p.node_cache_miss_ns;
        self.node_cache_misses += p.node_cache_misses;
        self.quantized_cache_miss_ns += p.quantized_cache_miss_ns;
        self.quantized_cache_misses += p.quantized_cache_misses;
        self.quantized_internal_cache_miss_ns += p.quantized_internal_cache_miss_ns;
        self.quantized_internal_cache_misses += p.quantized_internal_cache_misses;
        self.quantized_leaf_cache_miss_ns += p.quantized_leaf_cache_miss_ns;
        self.quantized_leaf_cache_misses += p.quantized_leaf_cache_misses;
        self.child_expand_ns += p.child_expand_ns;
        self.leaf_score_ns += p.leaf_score_ns;
        self.posting_overlay_ns += p.posting_overlay_ns;
        self.posting_overlay_calls += p.posting_overlay_calls;
        self.posting_overlay_base_members += p.posting_overlay_base_members;
        self.posting_base_decode_ns += p.posting_base_decode_ns;
        self.posting_base_decode_members += p.posting_base_decode_members;
        self.posting_delta_replay_ns += p.posting_delta_replay_ns;
        self.posting_delta_replay_records += p.posting_delta_replay_records;
        self.posting_overlay_delta_records += p.posting_overlay_delta_records;
        self.posting_overlay_delta_scan_skips += p.posting_overlay_delta_scan_skips;
        self.posting_overlay_materialized_members += p.posting_overlay_materialized_members;
        self.posting_overlay_fallbacks += p.posting_overlay_fallbacks;
        self.posting_overlay_cache_hits += p.posting_overlay_cache_hits;
        self.posting_overlay_cache_misses += p.posting_overlay_cache_misses;
        self.posting_overlay_cache_evictions += p.posting_overlay_cache_evictions;
        self.posting_overlay_cache_admission_skips += p.posting_overlay_cache_admission_skips;
        self.posting_overlay_cache_member_bytes = @max(self.posting_overlay_cache_member_bytes, p.posting_overlay_cache_member_bytes);
        self.rerank_ns += p.rerank_ns;
        self.rerank_vector_load_ns += p.rerank_vector_load_ns;
        self.rerank_metadata_ns += p.rerank_metadata_ns;
        self.nodes_visited += p.nodes_visited;
        self.leaves_explored += p.leaves_explored;
        self.centroid_directory_blocks_scanned += p.centroid_directory_blocks_scanned;
        self.centroid_directory_blocks_selected += p.centroid_directory_blocks_selected;
        self.centroid_directory_block_probe_limit += p.centroid_directory_block_probe_limit;
        self.centroid_directory_block_probe_count += p.centroid_directory_block_probe_count;
        self.centroid_directory_block_centroids_scored += p.centroid_directory_block_centroids_scored;
        self.centroid_directory_block_centroid_estimates += p.centroid_directory_block_centroid_estimates;
        self.centroid_directory_posting_centroids_scored += p.centroid_directory_posting_centroids_scored;
        self.centroid_directory_posting_centroid_estimates += p.centroid_directory_posting_centroid_estimates;
        self.approx_vectors_scored += p.approx_vectors_scored;
        self.exact_vectors_scored += p.exact_vectors_scored;
        self.reranked_vectors += p.reranked_vectors;
        self.approx_candidate_count += p.approx_candidate_count;
        self.rerank_candidate_count += p.rerank_candidate_count;
        self.result_count += profiled.results.items.items.len;
    }
};

const PostWriteQueryResult = struct {
    queries: usize = 0,
    rounds: usize = 0,
    exact_truth_build_ns: u64 = 0,
    exact_truth_cache_hit: bool = false,
    ns: u64 = 0,
    query_p50_ns: u64 = 0,
    query_p95_ns: u64 = 0,
    query_p99_ns: u64 = 0,
    recall_hits: u64 = 0,
    recall_total: u64 = 0,
    storage: StorageCounters = .{},
    totals: QueryProfileTotals = .{},
    warm_queries: usize = 0,
    warm_ns: u64 = 0,
    warm_query_p50_ns: u64 = 0,
    warm_query_p95_ns: u64 = 0,
    warm_query_p99_ns: u64 = 0,
    warm_recall_hits: u64 = 0,
    warm_recall_total: u64 = 0,
    warm_storage: StorageCounters = .{},
    warm_totals: QueryProfileTotals = .{},
};

fn nanotime() u64 {
    return platform_time.monotonicNs();
}

const LatencySummary = struct {
    p50_ns: u64 = 0,
    p95_ns: u64 = 0,
    p99_ns: u64 = 0,
};

fn latencyLessThan(_: void, a: u64, b: u64) bool {
    return a < b;
}

fn latencyPercentileNs(sorted_latencies: []const u64, percentile: u32) u64 {
    if (sorted_latencies.len == 0) return 0;
    const idx = (@as(u64, @intCast(sorted_latencies.len - 1)) * percentile + 50) / 100;
    return sorted_latencies[@intCast(idx)];
}

fn summarizeLatencies(latencies: []u64) LatencySummary {
    if (latencies.len == 0) return .{};
    std.mem.sort(u64, latencies, {}, latencyLessThan);
    return .{
        .p50_ns = latencyPercentileNs(latencies, 50),
        .p95_ns = latencyPercentileNs(latencies, 95),
        .p99_ns = latencyPercentileNs(latencies, 99),
    };
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
    defer if (cfg.inspect_root) |root| allocator.free(root);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;

    if (cfg.inspect_root) |root| {
        if (cfg.inspect_maintenance_steps > 0) {
            try runRootMaintenance(allocator, root, cfg.inspect_maintenance_steps);
        }
        try inspectRoot(out, allocator, root);
        try stdout_writer.flush();
        return;
    }

    const dataset: []f32 = if (cfg.dataset_mode == .materialized)
        try makeDataset(allocator, cfg)
    else
        &.{};
    defer if (cfg.dataset_mode == .materialized) allocator.free(dataset);
    const items: []hbc.BatchInsertItem = if (cfg.dataset_mode == .materialized)
        try makeItems(allocator, cfg, dataset)
    else
        &.{};
    defer if (cfg.dataset_mode == .materialized) freeItems(allocator, items);

    try out.print(
        "hbc write bench samples={d} vectors={d} dims={d} batch_size={d} leaf_size={d} branching_factor={d} search_width={d} storage={s} kmeans_backend={s} kmeans_update_strategy={s} centroid_directory={s} posting_storage={s} flat_centroid_block_size={d} flat_centroid_probe_count={d} flat_centroid_block_probe_count={d} max_posting_overlay_cache_bytes={d} max_posting_overlay_cache_entry_bytes={d} dataset_mode={s} lazy_posting_maintenance={any}",
        .{ cfg.samples, cfg.vectors, cfg.dims, cfg.batch_size, cfg.leaf_size, cfg.branching_factor, effectiveSearchWidth(cfg), @tagName(cfg.storage_mode), @tagName(cfg.kmeans_backend), @tagName(cfg.kmeans_update_strategy), @tagName(cfg.centroid_directory_mode), @tagName(cfg.posting_storage_mode), cfg.flat_centroid_block_size, cfg.flat_centroid_probe_count, cfg.flat_centroid_block_probe_count, cfg.max_posting_overlay_cache_bytes, cfg.max_posting_overlay_cache_entry_bytes, @tagName(cfg.dataset_mode), cfg.lazy_posting_maintenance },
    );
    try out.print(
        " auto_posting_maintenance_max_postings={d} auto_posting_maintenance_fold_delta_tails={any} auto_posting_maintenance_min_delta_records_to_fold={d} auto_posting_maintenance_min_tombstone_records_to_fold={d} auto_posting_maintenance_min_delta_to_base_ratio_bps={d} auto_posting_maintenance_max_delta_tail_postings={d} auto_posting_maintenance_min_dirty_postings={d} auto_posting_maintenance_max_dirty_version_age={d} auto_posting_maintenance_min_delta_records_to_run={d} auto_posting_maintenance_min_tombstone_records_to_run={d} auto_posting_maintenance_min_delta_to_base_ratio_bps_to_run={d} auto_posting_maintenance_min_centroid_version_lag={d} auto_posting_maintenance_min_payload_version_lag={d} auto_posting_maintenance_max_layout_changes={d} auto_posting_maintenance_split_full_postings={any} auto_posting_maintenance_min_overfull_postings_to_run={d} auto_posting_maintenance_min_postings_at_capacity_to_run={d} auto_posting_maintenance_max_boundary_reassignments={d} auto_posting_maintenance_allow_overfull_reassignment={any} auto_posting_maintenance_max_overfull_reassignment_postings={d} auto_posting_maintenance_max_over_capacity_reassignment_members={d} auto_posting_maintenance_boundary_reassignment_min_improvement={d:.4}",
        .{ cfg.auto_posting_maintenance_max_postings, cfg.auto_posting_maintenance_fold_delta_tails, cfg.auto_posting_maintenance_min_delta_records_to_fold, cfg.auto_posting_maintenance_min_tombstone_records_to_fold, cfg.auto_posting_maintenance_min_delta_to_base_ratio_bps, cfg.auto_posting_maintenance_max_delta_tail_postings, cfg.auto_posting_maintenance_min_dirty_postings, cfg.auto_posting_maintenance_max_dirty_version_age, cfg.auto_posting_maintenance_min_delta_records_to_run, cfg.auto_posting_maintenance_min_tombstone_records_to_run, cfg.auto_posting_maintenance_min_delta_to_base_ratio_bps_to_run, cfg.auto_posting_maintenance_min_centroid_version_lag, cfg.auto_posting_maintenance_min_payload_version_lag, cfg.auto_posting_maintenance_max_layout_changes, cfg.auto_posting_maintenance_split_full_postings, cfg.auto_posting_maintenance_min_overfull_postings_to_run, cfg.auto_posting_maintenance_min_postings_at_capacity_to_run, cfg.auto_posting_maintenance_max_boundary_reassignments, cfg.auto_posting_maintenance_allow_overfull_reassignment, cfg.auto_posting_maintenance_max_overfull_reassignment_postings, cfg.auto_posting_maintenance_max_over_capacity_reassignment_members, cfg.auto_posting_maintenance_boundary_reassignment_min_improvement },
    );
    try out.print(
        " repair_postings_after_write={any} repair_postings_before_bulk_finish={any} repair_fold_delta_tails={any} repair_min_delta_records_to_fold={d} repair_min_tombstone_records_to_fold={d} repair_min_delta_to_base_ratio_bps={d} repair_max_delta_tail_postings={d} repair_dirty_reassignments={d} repair_rebalance_layout={any} repair_split_full_postings={any} repair_max_layout_changes={d} repair_dirty_reassignment_allow_overfull={any} repair_dirty_reassignment_max_overfull_postings={d} repair_dirty_reassignment_max_over_capacity_members={d} repair_dirty_reassignment_min_improvement={d:.4} defer_leaf_splits_to_posting_maintenance={any} bulk_ingest_finish_compact={any} bulk_ingest_finish_max_deferred_l0_runs={d} bulk_ingest_finish_max_foreground_compaction_steps={d} coalesce_overwrite_leaf_writes={any} skip_vector_store={any} overwrite_hot_keys={d} overwrite_rounds={d} post_write_queries={d} post_write_query_rounds={d} post_write_k={d} post_write_recall_mode={s} post_write_truth_cache_path={s}\n",
        .{ cfg.repair_postings_after_write, cfg.repair_postings_before_bulk_finish, cfg.repair_fold_delta_tails, cfg.repair_min_delta_records_to_fold, cfg.repair_min_tombstone_records_to_fold, cfg.repair_min_delta_to_base_ratio_bps, cfg.repair_max_delta_tail_postings, cfg.repair_dirty_reassignments, cfg.repair_rebalance_layout, cfg.repair_split_full_postings, cfg.repair_max_layout_changes, cfg.repair_dirty_reassignment_allow_overfull, cfg.repair_dirty_reassignment_max_overfull_postings, cfg.repair_dirty_reassignment_max_over_capacity_members, cfg.repair_dirty_reassignment_min_improvement, cfg.defer_leaf_splits_to_posting_maintenance, cfg.bulk_ingest_finish_compact, cfg.bulk_ingest_finish_max_deferred_l0_runs orelse 0, cfg.bulk_ingest_finish_max_foreground_compaction_steps, cfg.coalesce_overwrite_leaf_writes, cfg.skip_vector_store, cfg.overwrite_hot_keys, cfg.overwrite_rounds, cfg.post_write_queries, cfg.post_write_query_rounds, cfg.post_write_k, @tagName(cfg.post_write_recall_mode), cfg.post_write_truth_cache_path orelse "" },
    );
    try stdout_writer.flush();

    if (cfg.post_write_truth_cache_only) {
        try buildPostWriteTruthCacheOnly(init.io, out, allocator, cfg);
        try stdout_writer.flush();
        return;
    }

    const storage_modes: []const StorageSelection = switch (cfg.storage_mode) {
        .host => &[_]StorageSelection{.host},
        .native => &[_]StorageSelection{.native},
        .memory => &[_]StorageSelection{.memory},
        .both => &[_]StorageSelection{ .host, .native, .memory },
    };

    for (storage_modes) |storage_mode| {
        for (0..cfg.samples) |sample_index| {
            if (cfg.dataset_mode == .procedural) {
                if (shouldRunWorkload(cfg, "bulk_build_external_vectors_sequential_empty"))
                    try benchBulkBuildProceduralExternalVectorsSequential(init.io, out, &stdout_writer, allocator, cfg, sample_index, storage_mode);
                if (shouldRunWorkload(cfg, "online_batches_dense_external_vectors_empty"))
                    try benchOnlineBatchesProceduralExternalVectors(init.io, out, &stdout_writer, allocator, cfg, sample_index, storage_mode, "online_batches_dense_external_vectors_empty", .{
                        .assume_absent_ids = true,
                        .coalesce_leaf_writes = true,
                        .defer_quantized_rebuild = true,
                        .skip_vector_store = true,
                        .bulk_ingest = true,
                    });
                if (shouldRunWorkload(cfg, "online_batches_dense_external_vectors_per_batch_session_empty"))
                    try benchOnlineBatchesProceduralExternalVectorsPerBatchSession(init.io, out, &stdout_writer, allocator, cfg, sample_index, storage_mode, "online_batches_dense_external_vectors_per_batch_session_empty", .{
                        .assume_absent_ids = true,
                        .coalesce_leaf_writes = true,
                        .defer_quantized_rebuild = true,
                        .skip_vector_store = true,
                        .bulk_ingest = true,
                    });
                if (shouldRunWorkload(cfg, "overwrite_hot_vectors_warm"))
                    try benchOverwriteHotVectorsProceduralOnWarmIndex(init.io, out, &stdout_writer, allocator, cfg, sample_index, storage_mode);
                if (shouldRunWorkload(cfg, "overwrite_random_vectors_warm"))
                    try benchOverwriteRandomVectorsProceduralOnWarmIndex(init.io, out, &stdout_writer, allocator, cfg, sample_index, storage_mode);
                if (shouldRunWorkload(cfg, "overwrite_semantic_drift_vectors_warm"))
                    try benchOverwriteSemanticDriftVectorsProceduralOnWarmIndex(init.io, out, &stdout_writer, allocator, cfg, sample_index, storage_mode);
                if (shouldRunWorkload(cfg, "append_streaming_warm"))
                    try benchAppendStreamingProceduralOnWarmIndex(init.io, out, &stdout_writer, allocator, cfg, sample_index, storage_mode);
                if (shouldRunWorkload(cfg, "mixed_insert_delete_update_warm"))
                    try benchMixedMutationsProceduralOnWarmIndex(init.io, out, &stdout_writer, allocator, cfg, sample_index, storage_mode);
                continue;
            }
            if (shouldRunWorkload(cfg, "bulk_build_empty"))
                try benchBulkBuild(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items);
            if (shouldRunWorkload(cfg, "bulk_build_external_vectors_empty"))
                try benchBulkBuildWithOptions(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items, "bulk_build_external_vectors_empty", .{
                    .skip_vector_store = true,
                });
            if (shouldRunWorkload(cfg, "online_batches_default_empty"))
                try benchOnlineBatches(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items, "online_batches_default_empty", .{});
            if (shouldRunWorkload(cfg, "online_batches_assume_absent_empty"))
                try benchOnlineBatches(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items, "online_batches_assume_absent_empty", .{ .assume_absent_ids = true });
            if (shouldRunWorkload(cfg, "online_batches_coalesced_empty"))
                try benchOnlineBatches(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items, "online_batches_coalesced_empty", .{
                    .assume_absent_ids = true,
                    .coalesce_leaf_writes = true,
                });
            if (shouldRunWorkload(cfg, "online_batches_coalesced_defer_quantized_empty"))
                try benchOnlineBatches(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items, "online_batches_coalesced_defer_quantized_empty", .{
                    .assume_absent_ids = true,
                    .coalesce_leaf_writes = true,
                    .defer_quantized_rebuild = true,
                });
            if (shouldRunWorkload(cfg, "online_batches_dense_external_vectors_empty"))
                try benchOnlineBatches(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items, "online_batches_dense_external_vectors_empty", .{
                    .assume_absent_ids = true,
                    .coalesce_leaf_writes = true,
                    .defer_quantized_rebuild = true,
                    .skip_vector_store = true,
                    .bulk_ingest = true,
                });
            if (shouldRunWorkload(cfg, "batch_apply_dense_external_vectors_warm"))
                try benchBatchApplyOnWarmIndex(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items, "batch_apply_dense_external_vectors_warm", .{
                    .assume_absent_ids = true,
                    .coalesce_leaf_writes = true,
                    .defer_quantized_rebuild = true,
                    .defer_leaf_splits_to_posting_maintenance = cfg.defer_leaf_splits_to_posting_maintenance,
                    .skip_vector_store = true,
                    .bulk_ingest = true,
                });
            if (shouldRunWorkload(cfg, "overwrite_same_leaf_vectors_warm"))
                try benchOverwriteSameLeafVectorsOnWarmIndex(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items);
            if (shouldRunWorkload(cfg, "overwrite_hot_vectors_warm"))
                try benchOverwriteHotVectorsOnWarmIndex(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items);
            if (shouldRunWorkload(cfg, "overwrite_random_vectors_warm"))
                try benchOverwriteRandomVectorsOnWarmIndex(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items);
            if (shouldRunWorkload(cfg, "overwrite_semantic_drift_vectors_warm"))
                try benchOverwriteSemanticDriftVectorsOnWarmIndex(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items);
            if (shouldRunWorkload(cfg, "append_streaming_warm"))
                try benchAppendStreamingOnWarmIndex(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items);
            if (shouldRunWorkload(cfg, "mixed_insert_delete_update_warm"))
                try benchMixedMutationsOnWarmIndex(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items);
            if (shouldRunWorkload(cfg, "online_batches_dense_external_vectors_per_batch_session_empty"))
                try benchOnlineBatchesPerBatchSession(out, &stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items, "online_batches_dense_external_vectors_per_batch_session_empty", .{
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

fn shouldRunWorkload(cfg: Config, workload: []const u8) bool {
    const filter = cfg.workload_filter orelse return true;
    return std.mem.eql(u8, filter, workload);
}

fn buildPostWriteTruthCacheOnly(
    io: std.Io,
    writer: anytype,
    allocator: Allocator,
    cfg: Config,
) !void {
    if (cfg.dataset_mode != .procedural) return error.InvalidArgument;
    if (cfg.post_write_recall_mode != .exact) return error.InvalidArgument;
    if (cfg.post_write_queries == 0) return error.InvalidArgument;
    if (cfg.post_write_truth_cache_path == null) return error.InvalidArgument;
    const workload = cfg.workload_filter orelse return error.InvalidArgument;

    var external_vector_dataset = ExternalVectorDataset{
        .data = &.{},
        .dims = cfg.dims,
        .seed = cfg.seed,
        .mode = cfg.dataset_mode,
    };
    var vector_count = cfg.vectors;
    var active_rows: ?[]const bool = null;
    var maybe_mutations: ?ProceduralMutationSet = null;
    defer if (maybe_mutations) |*mutations| mutations.deinit();

    if (std.mem.eql(u8, workload, "bulk_build_external_vectors_sequential_empty") or
        std.mem.eql(u8, workload, "online_batches_dense_external_vectors_empty") or
        std.mem.eql(u8, workload, "online_batches_dense_external_vectors_per_batch_session_empty"))
    {
        vector_count = cfg.vectors;
    } else {
        const mutations = if (std.mem.eql(u8, workload, "overwrite_hot_vectors_warm"))
            try makeHotProceduralOverwriteSet(allocator, cfg)
        else if (std.mem.eql(u8, workload, "overwrite_random_vectors_warm"))
            try makeRandomProceduralOverwriteSet(allocator, cfg)
        else if (std.mem.eql(u8, workload, "overwrite_semantic_drift_vectors_warm"))
            try makeSemanticDriftProceduralOverwriteSet(allocator, cfg)
        else if (std.mem.eql(u8, workload, "append_streaming_warm"))
            try makeAppendStreamingProceduralSet(allocator, cfg)
        else if (std.mem.eql(u8, workload, "mixed_insert_delete_update_warm"))
            try makeMixedProceduralMutationSet(allocator, cfg)
        else
            return error.InvalidArgument;
        vector_count = mutations.total_rows;
        active_rows = mutations.active_rows;
        external_vector_dataset.override_slots = mutations.override_slots;
        external_vector_dataset.override_vectors = mutations.override_vectors;
        maybe_mutations = mutations;
    }

    const query = try allocator.alloc(f32, cfg.dims);
    defer allocator.free(query);
    const candidate = try allocator.alloc(f32, cfg.dims);
    defer allocator.free(candidate);

    const result = try loadOrBuildGeneratedTruthCache(
        io,
        allocator,
        cfg,
        .cosine,
        &external_vector_dataset,
        vector_count,
        active_rows,
        query,
        candidate,
    );
    var cache = result.cache;
    defer cache.deinit(allocator);

    try writer.print(
        "hbc write bench post_write_truth_cache_only path={s} workload={s} vectors={d} live_rows={d} queries={d} k={d} cache_hit={any} elapsed_ns={d} rows={d} truth_count={d}\n",
        .{
            cfg.post_write_truth_cache_path.?,
            workload,
            vector_count,
            liveRowCount(vector_count, active_rows),
            cfg.post_write_queries,
            cfg.post_write_k,
            result.cache_hit,
            result.ns,
            cache.rows.len,
            cache.truth_count,
        },
    );
}

fn benchBulkBuild(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    dataset: []const f32,
    items: []const hbc.BatchInsertItem,
) !void {
    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, "bulk_build_empty", dataset);
    defer scenario.deinit();

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    const start = nanotime();
    try scenario.index.bulkBuildWithMetadataOptions(items, .{
        .skip_vector_store = cfg.skip_vector_store,
    });
    const elapsed = nanotime() - start;
    const repair_ns = try repairPostingsAfterWrite(&scenario);
    const after_storage = scenario.storage_harness.snapshotCounters();
    try printResult(writer, &scenario, items.len, elapsed, before_storage, after_storage, scenario.index.getWriteProfile(), .{ .posting_repair_after_write_ns = repair_ns });
    try stdout_writer.flush();
}

fn benchBulkBuildWithOptions(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    dataset: []const f32,
    items: []const hbc.BatchInsertItem,
    workload: []const u8,
    options: hbc.BulkBuildOptions,
) !void {
    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, workload, dataset);
    defer scenario.deinit();

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    const start = nanotime();
    try scenario.index.bulkBuildWithMetadataOptions(items, options);
    const elapsed = nanotime() - start;
    const repair_ns = try repairPostingsAfterWrite(&scenario);
    const after_storage = scenario.storage_harness.snapshotCounters();
    try printResult(writer, &scenario, items.len, elapsed, before_storage, after_storage, scenario.index.getWriteProfile(), .{ .posting_repair_after_write_ns = repair_ns });
    try stdout_writer.flush();
}

fn benchOnlineBatches(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    dataset: []const f32,
    items: []const hbc.BatchInsertItem,
    workload: []const u8,
    options: hbc.BatchInsertOptions,
) !void {
    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, workload, dataset);
    defer scenario.deinit();

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    const start = nanotime();
    const session_active = options.bulk_ingest;
    var session_open = false;
    if (session_active) {
        try scenario.index.beginBulkIngestSession();
        session_open = true;
        errdefer if (session_open) scenario.index.abortBulkIngestSession();
    }
    var offset: usize = 0;
    while (offset < items.len) {
        const end = @min(offset + cfg.batch_size, items.len);
        try scenario.index.batchInsertWithMetadataOptions(items[offset..end], options);
        offset = end;
    }
    if (session_active) {
        try scenario.index.finishBulkIngestSessionWithOptions(.{
            .compact = cfg.bulk_ingest_finish_compact,
            .max_deferred_l0_runs = cfg.bulk_ingest_finish_max_deferred_l0_runs,
            .max_foreground_compaction_steps = cfg.bulk_ingest_finish_max_foreground_compaction_steps,
        });
        session_open = false;
    }
    const elapsed = nanotime() - start;
    const repair_ns = try repairPostingsAfterWrite(&scenario);
    const after_storage = scenario.storage_harness.snapshotCounters();
    try printResult(writer, &scenario, items.len, elapsed, before_storage, after_storage, scenario.index.getWriteProfile(), .{ .posting_repair_after_write_ns = repair_ns });
    try stdout_writer.flush();
}

fn benchOnlineBatchesPerBatchSession(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    dataset: []const f32,
    items: []const hbc.BatchInsertItem,
    workload: []const u8,
    options: hbc.BatchInsertOptions,
) !void {
    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, workload, dataset);
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
        try scenario.index.finishBulkIngestSessionWithOptions(.{
            .compact = cfg.bulk_ingest_finish_compact,
            .max_deferred_l0_runs = cfg.bulk_ingest_finish_max_deferred_l0_runs,
            .max_foreground_compaction_steps = cfg.bulk_ingest_finish_max_foreground_compaction_steps,
        });
        session_open = false;
        offset = end;
    }
    const elapsed = nanotime() - start;
    const repair_ns = try repairPostingsAfterWrite(&scenario);
    const after_storage = scenario.storage_harness.snapshotCounters();
    try printResult(writer, &scenario, items.len, elapsed, before_storage, after_storage, scenario.index.getWriteProfile(), .{ .posting_repair_after_write_ns = repair_ns });
    try stdout_writer.flush();
}

fn benchOnlineBatchesProceduralExternalVectors(
    io: std.Io,
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    workload: []const u8,
    options: hbc.BatchInsertOptions,
) !void {
    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, workload, &.{});
    defer scenario.deinit();

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    const start = nanotime();
    const session_active = options.bulk_ingest;
    var session_open = false;
    if (session_active) {
        try scenario.index.beginBulkIngestSession();
        session_open = true;
        errdefer if (session_open) scenario.index.abortBulkIngestSession();
    }

    var batch = try ProceduralBatch.init(allocator, cfg);
    defer batch.deinit();
    var offset: usize = 0;
    while (offset < cfg.vectors) {
        const end = @min(offset + cfg.batch_size, cfg.vectors);
        const batch_items = try batch.fill(cfg, offset, end);
        try scenario.index.batchInsertWithMetadataOptions(batch_items, options);
        offset = end;
    }
    const before_repair_profile = scenario.index.getWriteProfile();
    const repair_before_bulk_finish_ns = try repairPostingsBeforeBulkFinish(&scenario);
    const repair_profile = if (repair_before_bulk_finish_ns != null)
        writeProfileDelta(scenario.index.getWriteProfile(), before_repair_profile)
    else
        null;
    if (session_active) {
        try scenario.index.finishBulkIngestSessionWithOptions(.{
            .compact = cfg.bulk_ingest_finish_compact,
            .max_deferred_l0_runs = cfg.bulk_ingest_finish_max_deferred_l0_runs,
            .max_foreground_compaction_steps = cfg.bulk_ingest_finish_max_foreground_compaction_steps,
        });
        session_open = false;
    }

    const elapsed = nanotime() - start;
    const repair_ns = try repairPostingsAfterWrite(&scenario);
    const after_storage = scenario.storage_harness.snapshotCounters();
    const post_write_query = try benchPostWriteQueriesGenerated(io, &scenario, cfg.vectors, null);
    try printResult(writer, &scenario, cfg.vectors, elapsed, before_storage, after_storage, scenario.index.getWriteProfile(), .{
        .posting_repair_before_bulk_finish_ns = repair_before_bulk_finish_ns,
        .posting_repair_after_write_ns = repair_ns,
        .foreground_profile = before_repair_profile,
        .repair_profile = repair_profile,
        .post_write_query = post_write_query,
    });
    try stdout_writer.flush();
}

fn benchOnlineBatchesProceduralExternalVectorsPerBatchSession(
    io: std.Io,
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    workload: []const u8,
    options: hbc.BatchInsertOptions,
) !void {
    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, workload, &.{});
    defer scenario.deinit();

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    const start = nanotime();
    var batch = try ProceduralBatch.init(allocator, cfg);
    defer batch.deinit();
    var offset: usize = 0;
    while (offset < cfg.vectors) {
        const end = @min(offset + cfg.batch_size, cfg.vectors);
        const batch_items = try batch.fill(cfg, offset, end);
        try scenario.index.beginBulkIngestSession();
        var session_open = true;
        errdefer if (session_open) scenario.index.abortBulkIngestSession();
        try scenario.index.batchInsertWithMetadataOptions(batch_items, options);
        try scenario.index.finishBulkIngestSessionWithOptions(.{
            .compact = cfg.bulk_ingest_finish_compact,
            .max_deferred_l0_runs = cfg.bulk_ingest_finish_max_deferred_l0_runs,
            .max_foreground_compaction_steps = cfg.bulk_ingest_finish_max_foreground_compaction_steps,
        });
        session_open = false;
        offset = end;
    }
    const elapsed = nanotime() - start;
    const before_repair_profile = scenario.index.getWriteProfile();
    const repair_before_bulk_finish_ns = try repairPostingsBeforeBulkFinish(&scenario);
    const repair_profile = if (repair_before_bulk_finish_ns != null)
        writeProfileDelta(scenario.index.getWriteProfile(), before_repair_profile)
    else
        null;
    const repair_ns = try repairPostingsAfterWrite(&scenario);
    const after_storage = scenario.storage_harness.snapshotCounters();
    const post_write_query = try benchPostWriteQueriesGenerated(io, &scenario, cfg.vectors, null);
    try printResult(writer, &scenario, cfg.vectors, elapsed, before_storage, after_storage, scenario.index.getWriteProfile(), .{
        .posting_repair_before_bulk_finish_ns = repair_before_bulk_finish_ns,
        .posting_repair_after_write_ns = repair_ns,
        .foreground_profile = before_repair_profile,
        .repair_profile = repair_profile,
        .post_write_query = post_write_query,
    });
    try stdout_writer.flush();
}

fn benchBatchApplyOnWarmIndex(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    dataset: []const f32,
    items: []const hbc.BatchInsertItem,
    workload: []const u8,
    options: hbc.BatchInsertOptions,
) !void {
    if (items.len < 2) return;

    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, workload, dataset);
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
    var foreground_profile: hbc.WriteProfile = .{};
    var repair_profile: hbc.WriteProfile = .{};
    var repair_ns: ?u64 = null;
    if (session_active) {
        const write_phase_profile = scenario.index.getWriteProfile();
        if (cfg.repair_postings_before_bulk_finish) {
            repair_ns = try repairPostingsAfterWrite(&scenario);
        }
        const finish_start = nanotime();
        try scenario.index.finishBulkIngestSessionWithOptions(.{
            .compact = cfg.bulk_ingest_finish_compact,
            .max_deferred_l0_runs = cfg.bulk_ingest_finish_max_deferred_l0_runs,
            .max_foreground_compaction_steps = cfg.bulk_ingest_finish_max_foreground_compaction_steps,
        });
        finish_elapsed = nanotime() - finish_start;
        const after_finish_profile = scenario.index.getWriteProfile();
        if (cfg.repair_postings_before_bulk_finish) {
            foreground_profile = write_phase_profile;
            repair_profile = writeProfileDelta(after_finish_profile, write_phase_profile);
        } else {
            foreground_profile = after_finish_profile;
        }
    } else {
        foreground_profile = scenario.index.getWriteProfile();
    }
    const elapsed = write_elapsed + finish_elapsed;
    if (!(session_active and cfg.repair_postings_before_bulk_finish)) {
        repair_ns = try repairPostingsAfterWrite(&scenario);
        const final_profile_after_repair = scenario.index.getWriteProfile();
        repair_profile = writeProfileDelta(final_profile_after_repair, foreground_profile);
    }
    const final_profile = scenario.index.getWriteProfile();
    const after_storage = scenario.storage_harness.snapshotCounters();
    const post_write_query = try benchPostWriteQueries(&scenario, dataset);
    try printResult(writer, &scenario, items.len - seed_count, elapsed, before_storage, after_storage, final_profile, .{
        .write_ns = write_elapsed,
        .finish_ns = finish_elapsed,
        .foreground_profile = foreground_profile,
        .repair_profile = repair_profile,
        .posting_repair_after_write_ns = repair_ns,
        .post_write_query = post_write_query,
    });
    try stdout_writer.flush();
}

fn benchOverwriteSameLeafVectorsOnWarmIndex(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    dataset: []const f32,
    items: []const hbc.BatchInsertItem,
) !void {
    return try benchOverwriteVectorsOnWarmIndex(writer, stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items, "overwrite_same_leaf_vectors_warm", makeSameLeafOverwriteItems);
}

fn benchOverwriteHotVectorsOnWarmIndex(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    dataset: []const f32,
    items: []const hbc.BatchInsertItem,
) !void {
    return try benchOverwriteVectorsOnWarmIndex(writer, stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items, "overwrite_hot_vectors_warm", makeHotOverwriteItems);
}

fn benchOverwriteRandomVectorsOnWarmIndex(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    dataset: []const f32,
    items: []const hbc.BatchInsertItem,
) !void {
    return try benchOverwriteVectorsOnWarmIndex(writer, stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items, "overwrite_random_vectors_warm", makeRandomOverwriteItems);
}

fn benchOverwriteSemanticDriftVectorsOnWarmIndex(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    dataset: []const f32,
    items: []const hbc.BatchInsertItem,
) !void {
    return try benchOverwriteVectorsOnWarmIndex(writer, stdout_writer, allocator, cfg, sample_index, storage_mode, dataset, items, "overwrite_semantic_drift_vectors_warm", makeSemanticDriftOverwriteItems);
}

const ProceduralMutationSet = struct {
    allocator: Allocator,
    writes: []hbc.BatchInsertItem = &.{},
    deletes: []u64 = &.{},
    active_rows: []bool = &.{},
    override_slots: []usize = &.{},
    override_vectors: []f32 = &.{},
    total_rows: usize = 0,
    initialized_writes: usize = 0,

    fn init(allocator: Allocator, cfg: Config, total_rows: usize, write_count: usize, delete_count: usize) !ProceduralMutationSet {
        var set = ProceduralMutationSet{
            .allocator = allocator,
            .writes = try allocator.alloc(hbc.BatchInsertItem, write_count),
            .deletes = try allocator.alloc(u64, delete_count),
            .active_rows = try allocator.alloc(bool, total_rows),
            .override_slots = try allocator.alloc(usize, total_rows),
            .override_vectors = try allocator.alloc(f32, write_count * cfg.dims),
            .total_rows = total_rows,
        };
        @memset(set.active_rows[0..@min(cfg.vectors, total_rows)], true);
        if (total_rows > cfg.vectors) @memset(set.active_rows[cfg.vectors..], false);
        @memset(set.override_slots, std.math.maxInt(usize));
        errdefer set.deinit();
        return set;
    }

    fn putWrite(self: *ProceduralMutationSet, cfg: Config, slot: usize, row: usize, metadata: []const u8) ![]f32 {
        if (slot >= self.writes.len or row >= self.total_rows) return error.InvalidArgument;
        const vector = self.override_vectors[slot * cfg.dims ..][0..cfg.dims];
        self.writes[slot] = .{
            .vector_id = @intCast(row + 1),
            .vector = vector,
            .metadata = try self.allocator.dupe(u8, metadata),
        };
        self.initialized_writes = @max(self.initialized_writes, slot + 1);
        self.override_slots[row] = slot;
        self.active_rows[row] = true;
        return vector;
    }

    fn deinit(self: *ProceduralMutationSet) void {
        for (self.writes[0..self.initialized_writes]) |item| self.allocator.free(item.metadata);
        self.allocator.free(self.writes);
        self.allocator.free(self.deletes);
        self.allocator.free(self.active_rows);
        self.allocator.free(self.override_slots);
        self.allocator.free(self.override_vectors);
        self.* = undefined;
    }
};

fn benchOverwriteHotVectorsProceduralOnWarmIndex(
    io: std.Io,
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
) !void {
    return try benchProceduralMutationsOnWarmIndex(io, writer, stdout_writer, allocator, cfg, sample_index, storage_mode, "overwrite_hot_vectors_warm", makeHotProceduralOverwriteSet);
}

fn benchOverwriteRandomVectorsProceduralOnWarmIndex(
    io: std.Io,
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
) !void {
    return try benchProceduralMutationsOnWarmIndex(io, writer, stdout_writer, allocator, cfg, sample_index, storage_mode, "overwrite_random_vectors_warm", makeRandomProceduralOverwriteSet);
}

fn benchOverwriteSemanticDriftVectorsProceduralOnWarmIndex(
    io: std.Io,
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
) !void {
    return try benchProceduralMutationsOnWarmIndex(io, writer, stdout_writer, allocator, cfg, sample_index, storage_mode, "overwrite_semantic_drift_vectors_warm", makeSemanticDriftProceduralOverwriteSet);
}

fn benchAppendStreamingProceduralOnWarmIndex(
    io: std.Io,
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
) !void {
    return try benchProceduralMutationsOnWarmIndex(io, writer, stdout_writer, allocator, cfg, sample_index, storage_mode, "append_streaming_warm", makeAppendStreamingProceduralSet);
}

fn benchMixedMutationsProceduralOnWarmIndex(
    io: std.Io,
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
) !void {
    return try benchProceduralMutationsOnWarmIndex(io, writer, stdout_writer, allocator, cfg, sample_index, storage_mode, "mixed_insert_delete_update_warm", makeMixedProceduralMutationSet);
}

fn benchProceduralMutationsOnWarmIndex(
    io: std.Io,
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    workload: []const u8,
    makeMutations: fn (Allocator, Config) anyerror!ProceduralMutationSet,
) !void {
    if (cfg.vectors < 2 or cfg.overwrite_rounds == 0) return;

    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, workload, &.{});
    defer scenario.deinit();

    try buildProceduralWarmIndex(&scenario);
    try repairWarmIndexPostingSetupDebt(&scenario);

    var mutations = try makeMutations(allocator, cfg);
    defer mutations.deinit();

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    const start = nanotime();
    var write_offset: usize = 0;
    var delete_offset: usize = 0;
    while (write_offset < mutations.writes.len or delete_offset < mutations.deletes.len) {
        const write_end = @min(write_offset + cfg.batch_size, mutations.writes.len);
        const delete_end = @min(delete_offset + cfg.batch_size, mutations.deletes.len);
        try scenario.index.batchApplyOptions(mutations.writes[write_offset..write_end], mutations.deletes[delete_offset..delete_end], .{
            .coalesce_leaf_writes = cfg.coalesce_overwrite_leaf_writes,
            .defer_leaf_splits_to_posting_maintenance = cfg.defer_leaf_splits_to_posting_maintenance,
            .skip_vector_store = cfg.skip_vector_store,
        });
        write_offset = write_end;
        delete_offset = delete_end;
    }
    const elapsed = nanotime() - start;
    const foreground_profile = scenario.index.getWriteProfile();
    scenario.external_vector_dataset.override_slots = mutations.override_slots;
    scenario.external_vector_dataset.override_vectors = mutations.override_vectors;
    const pre_repair_query = try benchPostWriteQueriesGenerated(io, &scenario, mutations.total_rows, mutations.active_rows);
    clearSearchScratchCacheIfAvailable(&scenario.index);
    const repair_ns = try repairPostingsAfterWrite(&scenario);
    const final_profile = scenario.index.getWriteProfile();
    const repair_profile = writeProfileDelta(final_profile, foreground_profile);
    const after_storage = scenario.storage_harness.snapshotCounters();
    const post_write_query = try benchPostWriteQueriesGenerated(io, &scenario, mutations.total_rows, mutations.active_rows);
    try printResult(writer, &scenario, mutations.writes.len + mutations.deletes.len, elapsed, before_storage, after_storage, final_profile, .{
        .foreground_profile = foreground_profile,
        .repair_profile = repair_profile,
        .posting_repair_after_write_ns = repair_ns,
        .pre_repair_query = pre_repair_query,
        .post_write_query = post_write_query,
    });
    try stdout_writer.flush();
}

fn benchAppendStreamingOnWarmIndex(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    dataset: []const f32,
    items: []const hbc.BatchInsertItem,
) !void {
    if (items.len < 2 or cfg.overwrite_rounds == 0) return;

    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, "append_streaming_warm", dataset);
    defer scenario.deinit();

    try scenario.index.bulkBuildWithMetadata(items);

    const appends_per_round = if (cfg.overwrite_hot_keys == 0)
        @min(cfg.batch_size, cfg.vectors)
    else
        cfg.overwrite_hot_keys;
    if (appends_per_round == 0) return error.InvalidArgument;
    const append_count = appends_per_round * cfg.overwrite_rounds;
    const total_rows = cfg.vectors + append_count;

    const post_write_dataset = try allocator.alloc(f32, total_rows * cfg.dims);
    defer allocator.free(post_write_dataset);
    @memcpy(post_write_dataset[0 .. cfg.vectors * cfg.dims], dataset);

    const writes = try allocator.alloc(hbc.BatchInsertItem, append_count);
    errdefer allocator.free(writes);
    var initialized: usize = 0;
    errdefer for (writes[0..initialized]) |item| allocator.free(item.metadata);

    for (writes, 0..) |*item, i| {
        const row = cfg.vectors + i;
        const dst = post_write_dataset[row * cfg.dims ..][0..cfg.dims];
        writeSyntheticAppendVector(cfg, i, dst);
        item.* = .{
            .vector_id = @intCast(row + 1),
            .vector = dst,
            .metadata = try allocator.dupe(u8, ""),
        };
        initialized += 1;
    }

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    scenario.external_vector_dataset.data = post_write_dataset;
    const start = nanotime();
    var offset: usize = 0;
    while (offset < writes.len) {
        const end = @min(offset + cfg.batch_size, writes.len);
        try scenario.index.batchApplyOptions(writes[offset..end], &.{}, .{
            .assume_absent_ids = true,
            .coalesce_leaf_writes = true,
            .defer_quantized_rebuild = true,
            .defer_leaf_splits_to_posting_maintenance = cfg.defer_leaf_splits_to_posting_maintenance,
            .skip_vector_store = cfg.skip_vector_store,
        });
        offset = end;
    }
    const elapsed = nanotime() - start;
    const foreground_profile = scenario.index.getWriteProfile();
    const pre_repair_query = try benchPostWriteQueries(&scenario, post_write_dataset);
    clearSearchScratchCacheIfAvailable(&scenario.index);
    const repair_ns = try repairPostingsAfterWrite(&scenario);
    const final_profile = scenario.index.getWriteProfile();
    const repair_profile = writeProfileDelta(final_profile, foreground_profile);
    const after_storage = scenario.storage_harness.snapshotCounters();
    const post_write_query = try benchPostWriteQueries(&scenario, post_write_dataset);
    try printResult(writer, &scenario, writes.len, elapsed, before_storage, after_storage, final_profile, .{
        .foreground_profile = foreground_profile,
        .repair_profile = repair_profile,
        .posting_repair_after_write_ns = repair_ns,
        .pre_repair_query = pre_repair_query,
        .post_write_query = post_write_query,
    });
    try stdout_writer.flush();

    freeItems(allocator, writes);
}

fn benchOverwriteVectorsOnWarmIndex(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    dataset: []const f32,
    items: []const hbc.BatchInsertItem,
    workload: []const u8,
    makeOverwriteItems: fn (Allocator, Config, []const f32) anyerror![]hbc.BatchInsertItem,
) !void {
    if (items.len < 2) return;

    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, workload, dataset);
    defer scenario.deinit();

    try scenario.index.bulkBuildWithMetadataOptions(items, .{
        .skip_vector_store = cfg.skip_vector_store,
    });
    if (std.mem.eql(u8, workload, "overwrite_same_leaf_vectors_warm")) {
        try repairWarmIndexPostingSetupDebt(&scenario);
    }

    const overwrite_items = try makeOverwriteItems(allocator, cfg, dataset);
    defer freeItems(allocator, overwrite_items);
    const post_write_dataset = try allocator.dupe(f32, dataset);
    defer allocator.free(post_write_dataset);
    applyOverwriteItemsToDataset(cfg, post_write_dataset, overwrite_items);

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    const start = nanotime();
    var offset: usize = 0;
    while (offset < overwrite_items.len) {
        const end = @min(offset + cfg.batch_size, overwrite_items.len);
        try scenario.index.batchApplyOptions(overwrite_items[offset..end], &.{}, .{
            .coalesce_leaf_writes = cfg.coalesce_overwrite_leaf_writes,
            .defer_leaf_splits_to_posting_maintenance = cfg.defer_leaf_splits_to_posting_maintenance,
            .skip_vector_store = cfg.skip_vector_store,
        });
        offset = end;
    }
    const elapsed = nanotime() - start;
    const foreground_profile = scenario.index.getWriteProfile();
    scenario.external_vector_dataset.data = post_write_dataset;
    const pre_repair_query = try benchPostWriteQueries(&scenario, post_write_dataset);
    clearSearchScratchCacheIfAvailable(&scenario.index);
    const repair_ns = try repairPostingsAfterWrite(&scenario);
    const final_profile = scenario.index.getWriteProfile();
    const repair_profile = writeProfileDelta(final_profile, foreground_profile);
    const after_storage = scenario.storage_harness.snapshotCounters();
    const post_write_query = try benchPostWriteQueries(&scenario, post_write_dataset);
    try printResult(writer, &scenario, overwrite_items.len, elapsed, before_storage, after_storage, final_profile, .{
        .foreground_profile = foreground_profile,
        .repair_profile = repair_profile,
        .posting_repair_after_write_ns = repair_ns,
        .pre_repair_query = pre_repair_query,
        .post_write_query = post_write_query,
    });
    try stdout_writer.flush();
}

fn benchMixedMutationsOnWarmIndex(
    writer: anytype,
    stdout_writer: anytype,
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_mode: StorageSelection,
    dataset: []const f32,
    items: []const hbc.BatchInsertItem,
) !void {
    if (items.len < 4 or cfg.overwrite_rounds == 0) return;

    var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, "mixed_insert_delete_update_warm", dataset);
    defer scenario.deinit();

    try scenario.index.bulkBuildWithMetadataOptions(items, .{
        .skip_vector_store = cfg.skip_vector_store,
    });

    const mutations_per_round = if (cfg.overwrite_hot_keys == 0)
        @min(cfg.batch_size, cfg.vectors)
    else
        @min(cfg.overwrite_hot_keys, cfg.vectors);
    if (mutations_per_round == 0) return error.InvalidArgument;

    const mutation_count = mutations_per_round * cfg.overwrite_rounds;
    const delete_count = @max(@divFloor(mutation_count, 4), @as(usize, 1));
    const append_count = @max(@divFloor(mutation_count, 4), @as(usize, 1));
    const update_count = mutation_count - delete_count - append_count;
    const total_rows = cfg.vectors + append_count;

    const post_write_dataset = try allocator.alloc(f32, total_rows * cfg.dims);
    defer allocator.free(post_write_dataset);
    @memcpy(post_write_dataset[0 .. cfg.vectors * cfg.dims], dataset);

    const active_rows = try allocator.alloc(bool, total_rows);
    defer allocator.free(active_rows);
    @memset(active_rows[0..cfg.vectors], true);
    @memset(active_rows[cfg.vectors..], false);

    const deletes = try allocator.alloc(u64, delete_count);
    defer allocator.free(deletes);
    for (deletes, 0..) |*delete_id, i| {
        const row = (i * 7) % cfg.vectors;
        delete_id.* = @intCast(row + 1);
        active_rows[row] = false;
    }

    const writes = try allocator.alloc(hbc.BatchInsertItem, update_count + append_count);
    errdefer allocator.free(writes);
    var initialized: usize = 0;
    errdefer for (writes[0..initialized]) |item| allocator.free(item.metadata);

    for (0..update_count) |i| {
        var row = (i * 5 + 1) % cfg.vectors;
        while (!active_rows[row]) row = (row + 1) % cfg.vectors;
        const dst = post_write_dataset[row * cfg.dims ..][0..cfg.dims];
        writeSemanticDriftVector(cfg, dataset, row, i, dst);
        writes[initialized] = .{
            .vector_id = @intCast(row + 1),
            .vector = dst,
            .metadata = try allocator.dupe(u8, ""),
        };
        initialized += 1;
    }

    for (0..append_count) |i| {
        const row = cfg.vectors + i;
        const dst = post_write_dataset[row * cfg.dims ..][0..cfg.dims];
        writeSyntheticAppendVector(cfg, i, dst);
        active_rows[row] = true;
        writes[initialized] = .{
            .vector_id = @intCast(row + 1),
            .vector = dst,
            .metadata = try allocator.dupe(u8, ""),
        };
        initialized += 1;
    }

    const before_storage = scenario.storage_harness.snapshotCounters();
    scenario.index.resetWriteProfile();
    const start = nanotime();
    var write_offset: usize = 0;
    var delete_offset: usize = 0;
    while (write_offset < writes.len or delete_offset < deletes.len) {
        const write_end = @min(write_offset + cfg.batch_size, writes.len);
        const delete_end = @min(delete_offset + cfg.batch_size, deletes.len);
        try scenario.index.batchApplyOptions(writes[write_offset..write_end], deletes[delete_offset..delete_end], .{
            .coalesce_leaf_writes = cfg.coalesce_overwrite_leaf_writes,
            .defer_leaf_splits_to_posting_maintenance = cfg.defer_leaf_splits_to_posting_maintenance,
            .skip_vector_store = cfg.skip_vector_store,
        });
        write_offset = write_end;
        delete_offset = delete_end;
    }
    const elapsed = nanotime() - start;
    const foreground_profile = scenario.index.getWriteProfile();
    scenario.external_vector_dataset.data = post_write_dataset;
    const pre_repair_query = try benchPostWriteQueriesActive(&scenario, post_write_dataset, active_rows);
    clearSearchScratchCacheIfAvailable(&scenario.index);
    const repair_ns = try repairPostingsAfterWrite(&scenario);
    const final_profile = scenario.index.getWriteProfile();
    const repair_profile = writeProfileDelta(final_profile, foreground_profile);
    const after_storage = scenario.storage_harness.snapshotCounters();
    const post_write_query = try benchPostWriteQueriesActive(&scenario, post_write_dataset, active_rows);
    try printResult(writer, &scenario, writes.len + deletes.len, elapsed, before_storage, after_storage, final_profile, .{
        .foreground_profile = foreground_profile,
        .repair_profile = repair_profile,
        .posting_repair_after_write_ns = repair_ns,
        .pre_repair_query = pre_repair_query,
        .post_write_query = post_write_query,
    });
    try stdout_writer.flush();

    freeItems(allocator, writes);
}

const ResultExtra = struct {
    write_ns: ?u64 = null,
    finish_ns: ?u64 = null,
    posting_repair_before_bulk_finish_ns: ?u64 = null,
    posting_repair_after_write_ns: ?u64 = null,
    foreground_profile: ?hbc.WriteProfile = null,
    repair_profile: ?hbc.WriteProfile = null,
    pre_repair_query: ?PostWriteQueryResult = null,
    post_write_query: ?PostWriteQueryResult = null,
};

fn writeProfileDelta(after: hbc.WriteProfile, before: hbc.WriteProfile) hbc.WriteProfile {
    var delta: hbc.WriteProfile = .{};
    inline for (@typeInfo(hbc.WriteProfile).@"struct".fields) |field| {
        const after_value = @field(after, field.name);
        const before_value = @field(before, field.name);
        @field(delta, field.name) = if (after_value >= before_value) after_value - before_value else 0;
    }
    return delta;
}

fn clearSearchScratchCacheIfAvailable(index: anytype) void {
    if (comptime @hasDecl(@TypeOf(index.*), "clearSearchScratchCache")) {
        index.clearSearchScratchCache();
    }
}

fn repairPostingsAfterWrite(scenario: *Scenario) !?u64 {
    scenario.posting_repair_after_write_result = null;
    if (!scenario.cfg.repair_postings_after_write) return null;
    const start = nanotime();
    scenario.posting_repair_after_write_result = try repairPostingsWithConfiguredOptions(scenario);
    return nanotime() - start;
}

fn repairPostingsBeforeBulkFinish(scenario: *Scenario) !?u64 {
    scenario.posting_repair_before_bulk_finish_result = null;
    if (!scenario.cfg.repair_postings_before_bulk_finish) return null;
    const start = nanotime();
    scenario.posting_repair_before_bulk_finish_result = try repairPostingsWithConfiguredOptions(scenario);
    return nanotime() - start;
}

fn repairPostingsWithConfiguredOptions(scenario: *Scenario) !hbc.PostingMaintenanceResult {
    return try scenario.index.repairDirtyPostingsWithOptions(.{
        .fold_delta_tails = scenario.cfg.repair_fold_delta_tails,
        .min_delta_records_to_fold = scenario.cfg.repair_min_delta_records_to_fold,
        .min_tombstone_records_to_fold = scenario.cfg.repair_min_tombstone_records_to_fold,
        .min_delta_to_base_ratio_bps = scenario.cfg.repair_min_delta_to_base_ratio_bps,
        .max_delta_tail_postings = scenario.cfg.repair_max_delta_tail_postings,
        .max_boundary_reassignments = scenario.cfg.repair_dirty_reassignments,
        .reassign_dirty_postings = scenario.cfg.repair_dirty_reassignments > 0,
        .rebalance_layout = scenario.cfg.repair_rebalance_layout or scenario.cfg.repair_split_full_postings,
        .split_full_postings = scenario.cfg.repair_split_full_postings,
        .max_layout_changes = scenario.cfg.repair_max_layout_changes,
        .allow_overfull_reassignment = scenario.cfg.repair_dirty_reassignment_allow_overfull,
        .max_overfull_reassignment_postings = scenario.cfg.repair_dirty_reassignment_max_overfull_postings,
        .max_over_capacity_reassignment_members = scenario.cfg.repair_dirty_reassignment_max_over_capacity_members,
        .boundary_reassignment_min_improvement = scenario.cfg.repair_dirty_reassignment_min_improvement,
    });
}

fn repairWarmIndexPostingSetupDebt(scenario: *Scenario) !void {
    if (!scenario.cfg.lazy_posting_maintenance) return;
    _ = try scenario.index.repairDirtyPostingsWithOptions(.{
        .fold_delta_tails = true,
        .min_delta_records_to_fold = 0,
        .min_tombstone_records_to_fold = 0,
        .min_delta_to_base_ratio_bps = 0,
    });
}

fn benchPostWriteQueries(scenario: *Scenario, dataset: []const f32) !?PostWriteQueryResult {
    return try benchPostWriteQueriesActive(scenario, dataset, null);
}

fn benchPostWriteQueriesGenerated(io: std.Io, scenario: *Scenario, vector_count: usize, active_rows: ?[]const bool) !?PostWriteQueryResult {
    if (scenario.cfg.post_write_queries == 0) return null;
    var result = PostWriteQueryResult{
        .queries = scenario.cfg.post_write_queries * scenario.cfg.post_write_query_rounds,
        .rounds = scenario.cfg.post_write_query_rounds,
        .warm_queries = if (scenario.cfg.post_write_query_rounds > 1)
            scenario.cfg.post_write_queries * (scenario.cfg.post_write_query_rounds - 1)
        else
            0,
    };
    var query_latencies = try scenario.allocator.alloc(u64, result.queries);
    defer scenario.allocator.free(query_latencies);
    var measured_queries: usize = 0;
    var warm_latencies = try scenario.allocator.alloc(u64, result.warm_queries);
    defer scenario.allocator.free(warm_latencies);
    var measured_warm_queries: usize = 0;
    const query = try scenario.allocator.alloc(f32, scenario.cfg.dims);
    defer scenario.allocator.free(query);
    const candidate = try scenario.allocator.alloc(f32, scenario.cfg.dims);
    defer scenario.allocator.free(candidate);
    const exact_truth_result = if (scenario.cfg.post_write_recall_mode == .exact)
        try loadOrBuildGeneratedTruthCache(io, scenario.allocator, scenario.cfg, scenario.index.config.metric, scenario.external_vector_dataset, vector_count, active_rows, query, candidate)
    else
        ExactTruthCacheLoadResult{ .cache = .{}, .ns = 0, .cache_hit = false };
    var exact_truth_cache = exact_truth_result.cache;
    defer exact_truth_cache.deinit(scenario.allocator);
    result.exact_truth_build_ns = exact_truth_result.ns;
    result.exact_truth_cache_hit = exact_truth_result.cache_hit;

    const before_storage = scenario.storage_harness.snapshotCounters();
    const start = nanotime();
    var warm_before_storage: StorageCounters = .{};
    var warm_start: u64 = 0;
    for (0..scenario.cfg.post_write_query_rounds) |round| {
        if (round == 1) {
            warm_before_storage = scenario.storage_harness.snapshotCounters();
            warm_start = nanotime();
        }
        for (0..scenario.cfg.post_write_queries) |i| {
            const row = nthLiveRow(vector_count, active_rows, i) orelse break;
            try scenario.external_vector_dataset.copyVector(@intCast(row + 1), query);
            const query_start = nanotime();
            var profiled = try scenario.index.searchProfiledRequest(.{
                .query = query,
                .k = scenario.cfg.post_write_k,
                .load_metadata = false,
            });
            const query_ns = nanotime() - query_start;
            query_latencies[measured_queries] = query_ns;
            measured_queries += 1;
            if (round > 0) {
                warm_latencies[measured_warm_queries] = query_ns;
                measured_warm_queries += 1;
            }
            const recall = switch (scenario.cfg.post_write_recall_mode) {
                .exact => recallHitsFromTruth(exact_truth_cache.lookup(row) orelse &.{}, profiled.results.getHits()),
                .self_hit => selfHitRecall(row, profiled.results.getHits()),
            };
            result.recall_hits += recall.hits;
            result.recall_total += recall.total;
            if (round > 0) {
                result.warm_recall_hits += recall.hits;
                result.warm_recall_total += recall.total;
            }
            result.totals.add(&profiled);
            if (round > 0) result.warm_totals.add(&profiled);
            profiled.results.deinit();
        }
    }
    result.ns = nanotime() - start;
    const query_latency = summarizeLatencies(query_latencies[0..measured_queries]);
    result.query_p50_ns = query_latency.p50_ns;
    result.query_p95_ns = query_latency.p95_ns;
    result.query_p99_ns = query_latency.p99_ns;
    result.storage = StorageCounters.delta(scenario.storage_harness.snapshotCounters(), before_storage);
    if (result.warm_queries > 0) {
        result.warm_ns = nanotime() - warm_start;
        const warm_latency = summarizeLatencies(warm_latencies[0..measured_warm_queries]);
        result.warm_query_p50_ns = warm_latency.p50_ns;
        result.warm_query_p95_ns = warm_latency.p95_ns;
        result.warm_query_p99_ns = warm_latency.p99_ns;
        result.warm_storage = StorageCounters.delta(scenario.storage_harness.snapshotCounters(), warm_before_storage);
    }
    return result;
}

fn benchPostWriteQueriesActive(scenario: *Scenario, dataset: []const f32, active_rows: ?[]const bool) !?PostWriteQueryResult {
    if (scenario.cfg.post_write_queries == 0) return null;
    var result = PostWriteQueryResult{
        .queries = scenario.cfg.post_write_queries * scenario.cfg.post_write_query_rounds,
        .rounds = scenario.cfg.post_write_query_rounds,
        .warm_queries = if (scenario.cfg.post_write_query_rounds > 1)
            scenario.cfg.post_write_queries * (scenario.cfg.post_write_query_rounds - 1)
        else
            0,
    };
    var query_latencies = try scenario.allocator.alloc(u64, result.queries);
    defer scenario.allocator.free(query_latencies);
    var measured_queries: usize = 0;
    var warm_latencies = try scenario.allocator.alloc(u64, result.warm_queries);
    defer scenario.allocator.free(warm_latencies);
    var measured_warm_queries: usize = 0;
    var exact_truth_cache = ExactTruthCache{};
    if (scenario.cfg.post_write_recall_mode == .exact) {
        const truth_start = nanotime();
        exact_truth_cache = try buildMaterializedTruthCache(scenario, dataset, active_rows);
        result.exact_truth_build_ns = nanotime() - truth_start;
    }
    defer exact_truth_cache.deinit(scenario.allocator);
    const before_storage = scenario.storage_harness.snapshotCounters();
    const start = nanotime();
    var warm_before_storage: StorageCounters = .{};
    var warm_start: u64 = 0;
    for (0..scenario.cfg.post_write_query_rounds) |round| {
        if (round == 1) {
            warm_before_storage = scenario.storage_harness.snapshotCounters();
            warm_start = nanotime();
        }
        for (0..scenario.cfg.post_write_queries) |i| {
            const row = nthLiveRow(dataset.len / scenario.cfg.dims, active_rows, i) orelse break;
            const query = dataset[row * scenario.cfg.dims ..][0..scenario.cfg.dims];
            const query_start = nanotime();
            var profiled = try scenario.index.searchProfiledRequest(.{
                .query = query,
                .k = scenario.cfg.post_write_k,
                .load_metadata = false,
            });
            const query_ns = nanotime() - query_start;
            query_latencies[measured_queries] = query_ns;
            measured_queries += 1;
            if (round > 0) {
                warm_latencies[measured_warm_queries] = query_ns;
                measured_warm_queries += 1;
            }
            const recall = switch (scenario.cfg.post_write_recall_mode) {
                .exact => recallHitsFromTruth(exact_truth_cache.lookup(row) orelse &.{}, profiled.results.getHits()),
                .self_hit => selfHitRecall(row, profiled.results.getHits()),
            };
            result.recall_hits += recall.hits;
            result.recall_total += recall.total;
            if (round > 0) {
                result.warm_recall_hits += recall.hits;
                result.warm_recall_total += recall.total;
            }
            result.totals.add(&profiled);
            if (round > 0) result.warm_totals.add(&profiled);
            profiled.results.deinit();
        }
    }
    result.ns = nanotime() - start;
    const query_latency = summarizeLatencies(query_latencies[0..measured_queries]);
    result.query_p50_ns = query_latency.p50_ns;
    result.query_p95_ns = query_latency.p95_ns;
    result.query_p99_ns = query_latency.p99_ns;
    result.storage = StorageCounters.delta(scenario.storage_harness.snapshotCounters(), before_storage);
    if (result.warm_queries > 0) {
        result.warm_ns = nanotime() - warm_start;
        const warm_latency = summarizeLatencies(warm_latencies[0..measured_warm_queries]);
        result.warm_query_p50_ns = warm_latency.p50_ns;
        result.warm_query_p95_ns = warm_latency.p95_ns;
        result.warm_query_p99_ns = warm_latency.p99_ns;
        result.warm_storage = StorageCounters.delta(scenario.storage_harness.snapshotCounters(), warm_before_storage);
    }
    return result;
}

fn nthLiveRow(vector_count: usize, active_rows: ?[]const bool, ordinal: usize) ?usize {
    if (vector_count == 0) return null;
    if (active_rows == null) return ordinal % vector_count;
    const active = active_rows.?;
    var live_count: usize = 0;
    for (0..@min(vector_count, active.len)) |row| {
        if (active[row]) live_count += 1;
    }
    if (live_count == 0) return null;
    var target = ordinal % live_count;
    for (0..@min(vector_count, active.len)) |row| {
        if (!active[row]) continue;
        if (target == 0) return row;
        target -= 1;
    }
    return null;
}

fn effectiveSearchWidth(cfg: Config) u32 {
    return if (cfg.search_width != 0) cfg.search_width else cfg.branching_factor;
}

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
    const backlog = try scenario.index.postingBacklogStats();
    const active_table_stats = try analyzeActiveTables(scenario);
    const cache_stats = scenario.index.hbcCacheStats();
    const ns_per_vector = @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(@max(vectors, 1)));
    const vectors_per_second = if (ns == 0) 0 else (@as(f64, @floatFromInt(vectors)) * 1_000_000_000.0) / @as(f64, @floatFromInt(ns));
    try writer.print(
        "{{\"scenario\":\"{s}_{s}_{s}_{s}\",\"storage\":\"{s}\",\"centroid_directory\":\"{s}\",\"posting_storage\":\"{s}\",\"branching_factor\":{d},\"search_width\":{d},\"max_posting_overlay_cache_bytes\":{d},\"max_posting_overlay_cache_entry_bytes\":{d},\"repair_before_bulk_finish\":{},\"repair_fold_delta_tails\":{},\"repair_min_delta_records_to_fold\":{d},\"repair_min_tombstone_records_to_fold\":{d},\"repair_min_delta_to_base_ratio_bps\":{d},\"repair_max_delta_tail_postings\":{d},\"repair_dirty_reassignments\":{d},\"repair_split_full_postings\":{},\"repair_max_layout_changes\":{d},\"repair_dirty_reassignment_allow_overfull\":{},\"repair_dirty_reassignment_max_overfull_postings\":{d},\"repair_dirty_reassignment_max_over_capacity_members\":{d},\"repair_dirty_reassignment_min_improvement\":{d:.4},\"defer_leaf_splits_to_posting_maintenance\":{},\"sample\":{d},\"workload\":\"{s}\",\"vectors\":{d},\"dims\":{d},\"ns\":{d},\"ns_per_vector\":{d:.2},\"vectors_per_second\":{d:.2}",
        .{
            @tagName(scenario.storage_kind),
            @tagName(scenario.cfg.centroid_directory_mode),
            @tagName(scenario.cfg.posting_storage_mode),
            scenario.workload,
            @tagName(scenario.storage_kind),
            @tagName(scenario.cfg.centroid_directory_mode),
            @tagName(scenario.cfg.posting_storage_mode),
            scenario.cfg.branching_factor,
            effectiveSearchWidth(scenario.cfg),
            scenario.cfg.max_posting_overlay_cache_bytes,
            scenario.cfg.max_posting_overlay_cache_entry_bytes,
            scenario.cfg.repair_postings_before_bulk_finish,
            scenario.cfg.repair_fold_delta_tails,
            scenario.cfg.repair_min_delta_records_to_fold,
            scenario.cfg.repair_min_tombstone_records_to_fold,
            scenario.cfg.repair_min_delta_to_base_ratio_bps,
            scenario.cfg.repair_max_delta_tail_postings,
            scenario.cfg.repair_dirty_reassignments,
            scenario.cfg.repair_split_full_postings,
            scenario.cfg.repair_max_layout_changes,
            scenario.cfg.repair_dirty_reassignment_allow_overfull,
            scenario.cfg.repair_dirty_reassignment_max_overfull_postings,
            scenario.cfg.repair_dirty_reassignment_max_over_capacity_members,
            scenario.cfg.repair_dirty_reassignment_min_improvement,
            scenario.cfg.defer_leaf_splits_to_posting_maintenance,
            scenario.sample_index,
            scenario.workload,
            vectors,
            scenario.cfg.dims,
            ns,
            ns_per_vector,
            vectors_per_second,
        },
    );
    try writer.print(
        ",\"posting_base_member_block_size\":{d},\"flat_centroid_block_size\":{d},\"flat_centroid_probe_count\":{d},\"flat_centroid_block_probe_count\":{d}",
        .{
            scenario.cfg.posting_base_member_block_size,
            scenario.cfg.flat_centroid_block_size,
            scenario.cfg.flat_centroid_probe_count,
            scenario.cfg.flat_centroid_block_probe_count,
        },
    );
    try writer.print(",\"dataset_mode\":\"{s}\",\"post_write_recall_mode\":\"{s}\"", .{ @tagName(scenario.cfg.dataset_mode), @tagName(scenario.cfg.post_write_recall_mode) });
    try writer.print(
        ",\"repair_rebalance_layout\":{},\"auto_posting_maintenance_max_postings\":{d},\"auto_posting_maintenance_fold_delta_tails\":{},\"auto_posting_maintenance_min_delta_records_to_fold\":{d},\"auto_posting_maintenance_min_tombstone_records_to_fold\":{d},\"auto_posting_maintenance_min_delta_to_base_ratio_bps\":{d},\"auto_posting_maintenance_max_delta_tail_postings\":{d},\"auto_posting_maintenance_min_dirty_postings\":{d},\"auto_posting_maintenance_max_dirty_version_age\":{d},\"auto_posting_maintenance_min_delta_records_to_run\":{d},\"auto_posting_maintenance_min_tombstone_records_to_run\":{d},\"auto_posting_maintenance_min_delta_to_base_ratio_bps_to_run\":{d},\"auto_posting_maintenance_min_centroid_version_lag\":{d},\"auto_posting_maintenance_min_payload_version_lag\":{d},\"auto_posting_maintenance_max_layout_changes\":{d},\"auto_posting_maintenance_split_full_postings\":{},\"auto_posting_maintenance_min_overfull_postings_to_run\":{d},\"auto_posting_maintenance_min_postings_at_capacity_to_run\":{d},\"auto_posting_maintenance_max_boundary_reassignments\":{d},\"auto_posting_maintenance_allow_overfull_reassignment\":{},\"auto_posting_maintenance_max_overfull_reassignment_postings\":{d},\"auto_posting_maintenance_max_over_capacity_reassignment_members\":{d},\"auto_posting_maintenance_boundary_reassignment_min_improvement\":{d:.4},\"bulk_ingest_finish_compact\":{},\"bulk_ingest_finish_max_deferred_l0_runs\":{d},\"bulk_ingest_finish_max_foreground_compaction_steps\":{d},\"coalesce_overwrite_leaf_writes\":{},\"skip_vector_store\":{}",
        .{
            scenario.cfg.repair_rebalance_layout,
            scenario.cfg.auto_posting_maintenance_max_postings,
            scenario.cfg.auto_posting_maintenance_fold_delta_tails,
            scenario.cfg.auto_posting_maintenance_min_delta_records_to_fold,
            scenario.cfg.auto_posting_maintenance_min_tombstone_records_to_fold,
            scenario.cfg.auto_posting_maintenance_min_delta_to_base_ratio_bps,
            scenario.cfg.auto_posting_maintenance_max_delta_tail_postings,
            scenario.cfg.auto_posting_maintenance_min_dirty_postings,
            scenario.cfg.auto_posting_maintenance_max_dirty_version_age,
            scenario.cfg.auto_posting_maintenance_min_delta_records_to_run,
            scenario.cfg.auto_posting_maintenance_min_tombstone_records_to_run,
            scenario.cfg.auto_posting_maintenance_min_delta_to_base_ratio_bps_to_run,
            scenario.cfg.auto_posting_maintenance_min_centroid_version_lag,
            scenario.cfg.auto_posting_maintenance_min_payload_version_lag,
            scenario.cfg.auto_posting_maintenance_max_layout_changes,
            scenario.cfg.auto_posting_maintenance_split_full_postings,
            scenario.cfg.auto_posting_maintenance_min_overfull_postings_to_run,
            scenario.cfg.auto_posting_maintenance_min_postings_at_capacity_to_run,
            scenario.cfg.auto_posting_maintenance_max_boundary_reassignments,
            scenario.cfg.auto_posting_maintenance_allow_overfull_reassignment,
            scenario.cfg.auto_posting_maintenance_max_overfull_reassignment_postings,
            scenario.cfg.auto_posting_maintenance_max_over_capacity_reassignment_members,
            scenario.cfg.auto_posting_maintenance_boundary_reassignment_min_improvement,
            scenario.cfg.bulk_ingest_finish_compact,
            scenario.cfg.bulk_ingest_finish_max_deferred_l0_runs orelse 0,
            scenario.cfg.bulk_ingest_finish_max_foreground_compaction_steps,
            scenario.cfg.coalesce_overwrite_leaf_writes,
            scenario.cfg.skip_vector_store,
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
    if (extra.posting_repair_before_bulk_finish_ns) |repair_ns| {
        try writer.print(",\"posting_repair_before_bulk_finish_ns\":{d},\"posting_repair_before_bulk_finish_ns_per_vector\":{d:.2}", .{
            repair_ns,
            @as(f64, @floatFromInt(repair_ns)) / @as(f64, @floatFromInt(@max(vectors, 1))),
        });
        if (scenario.posting_repair_before_bulk_finish_result) |result| {
            try printPostingRepairResult(writer, "posting_repair_before_bulk_finish", result);
        }
    }
    if (extra.posting_repair_after_write_ns) |repair_ns| {
        try writer.print(",\"posting_repair_after_write_ns\":{d},\"posting_repair_after_write_ns_per_vector\":{d:.2}", .{
            repair_ns,
            @as(f64, @floatFromInt(repair_ns)) / @as(f64, @floatFromInt(@max(vectors, 1))),
        });
        if (scenario.posting_repair_after_write_result) |result| {
            try printPostingRepairResult(writer, "posting_repair_after_write", result);
        }
    }
    if (extra.foreground_profile) |foreground| {
        try writer.print(
            ",\"foreground_insert_calls\":{d},\"foreground_save_node_calls\":{d},\"foreground_split_leaf_calls\":{d},\"foreground_ns_nodes_put_calls\":{d},\"foreground_ns_nodes_append_calls\":{d},\"foreground_ns_nodes_value_bytes\":{d},\"foreground_ns_vecs_put_calls\":{d},\"foreground_posting_base_put_calls\":{d},\"foreground_centroid_directory_put_calls\":{d},\"foreground_assignment_map_put_calls\":{d},\"foreground_posting_delta_append_calls\":{d},\"foreground_posting_delta_records\":{d},\"foreground_posting_delta_value_bytes\":{d},\"foreground_posting_delta_fold_calls\":{d},\"foreground_posting_delta_fold_records\":{d},\"foreground_posting_delta_fold_deleted_tail_keys\":{d},\"foreground_posting_delta_fold_deleted_tail_value_bytes\":{d},\"foreground_posting_delta_fold_written_base_key_bytes\":{d},\"foreground_posting_delta_fold_written_base_value_bytes\":{d}",
            .{
                foreground.insert_calls,
                foreground.save_node_calls,
                foreground.split_leaf_calls,
                foreground.ns_nodes_put_calls,
                foreground.ns_nodes_append_calls,
                foreground.ns_nodes_value_bytes,
                foreground.ns_vecs_put_calls,
                foreground.posting_base_put_calls,
                foreground.centroid_directory_put_calls,
                foreground.assignment_map_put_calls,
                foreground.posting_delta_append_calls,
                foreground.posting_delta_records,
                foreground.posting_delta_value_bytes,
                foreground.posting_delta_fold_calls,
                foreground.posting_delta_fold_records,
                foreground.posting_delta_fold_deleted_tail_keys,
                foreground.posting_delta_fold_deleted_tail_value_bytes,
                foreground.posting_delta_fold_written_base_key_bytes,
                foreground.posting_delta_fold_written_base_value_bytes,
            },
        );
        try writer.print(
            ",\"foreground_batch_route_calls\":{d},\"foreground_batch_route_internal_nodes\":{d},\"foreground_batch_route_leaf_groups\":{d},\"foreground_batch_route_items\":{d},\"foreground_batch_route_quantized_nodes\":{d},\"foreground_batch_route_exact_child_scores\":{d},\"foreground_batch_route_fallback_nodes\":{d},\"foreground_range_put_calls\":{d},\"foreground_range_value_bytes\":{d},\"foreground_split_leaf_input_members_total\":{d},\"foreground_split_leaf_input_overflow_members_total\":{d},\"foreground_bulk_leaf_rebuild_calls\":{d},\"foreground_bulk_leaf_rebuild_members_total\":{d},\"foreground_bulk_leaf_rebuild_members_max\":{d}",
            .{
                foreground.batch_route_calls,
                foreground.batch_route_internal_nodes,
                foreground.batch_route_leaf_groups,
                foreground.batch_route_items,
                foreground.batch_route_quantized_nodes,
                foreground.batch_route_exact_child_scores,
                foreground.batch_route_fallback_nodes,
                foreground.range_put_calls,
                foreground.range_value_bytes,
                foreground.split_leaf_input_members_total,
                foreground.split_leaf_input_overflow_members_total,
                foreground.bulk_leaf_rebuild_calls,
                foreground.bulk_leaf_rebuild_members_total,
                foreground.bulk_leaf_rebuild_members_max,
            },
        );
    }
    if (extra.repair_profile) |repair| {
        try writer.print(
            ",\"repair_posting_maintenance_repaired_postings\":{d},\"repair_posting_maintenance_split_postings\":{d},\"repair_posting_maintenance_boundary_reassigned_vectors\":{d},\"repair_posting_maintenance_delta_fold_attempts\":{d},\"repair_posting_maintenance_delta_fold_skipped\":{d},\"repair_posting_maintenance_delta_fold_records\":{d},\"repair_posting_base_put_calls\":{d},\"repair_centroid_directory_put_calls\":{d},\"repair_posting_delta_fold_calls\":{d},\"repair_posting_delta_fold_records\":{d},\"repair_posting_delta_fold_deleted_tail_keys\":{d},\"repair_posting_delta_fold_deleted_tail_value_bytes\":{d},\"repair_posting_delta_fold_written_base_key_bytes\":{d},\"repair_posting_delta_fold_written_base_value_bytes\":{d},\"repair_ns_nodes_put_calls\":{d},\"repair_ns_nodes_append_calls\":{d},\"repair_ns_nodes_value_bytes\":{d}",
            .{
                repair.posting_maintenance_repaired_postings,
                repair.posting_maintenance_split_postings,
                repair.posting_maintenance_boundary_reassigned_vectors,
                repair.posting_maintenance_delta_fold_attempts,
                repair.posting_maintenance_delta_fold_skipped,
                repair.posting_maintenance_delta_fold_records,
                repair.posting_base_put_calls,
                repair.centroid_directory_put_calls,
                repair.posting_delta_fold_calls,
                repair.posting_delta_fold_records,
                repair.posting_delta_fold_deleted_tail_keys,
                repair.posting_delta_fold_deleted_tail_value_bytes,
                repair.posting_delta_fold_written_base_key_bytes,
                repair.posting_delta_fold_written_base_value_bytes,
                repair.ns_nodes_put_calls,
                repair.ns_nodes_append_calls,
                repair.ns_nodes_value_bytes,
            },
        );
    }
    if (extra.pre_repair_query) |query| {
        try printCompactQueryResult(writer, "pre_repair", query);
    }
    if (extra.post_write_query) |query| {
        const ns_per_query = @as(f64, @floatFromInt(query.ns)) / @as(f64, @floatFromInt(@max(query.queries, 1)));
        const queries_per_second = if (query.ns == 0) 0 else (@as(f64, @floatFromInt(query.queries)) * 1_000_000_000.0) / @as(f64, @floatFromInt(query.ns));
        const recall_at_k = if (query.recall_total == 0) 0 else @as(f64, @floatFromInt(query.recall_hits)) / @as(f64, @floatFromInt(query.recall_total));
        try writer.print(
            ",\"post_write_queries\":{d},\"post_write_query_rounds\":{d},\"post_write_query_k\":{d},\"post_write_exact_truth_build_ns\":{d},\"post_write_exact_truth_cache_hit\":{},\"post_write_query_ns\":{d},\"post_write_query_ns_per_query\":{d:.2},\"post_write_query_p50_ns\":{d},\"post_write_query_p95_ns\":{d},\"post_write_query_p99_ns\":{d},\"post_write_queries_per_second\":{d:.2},\"post_write_recall_hits\":{d},\"post_write_recall_total\":{d},\"post_write_recall_at_k\":{d:.4},\"post_write_storage_read_file\":{d},\"post_write_storage_read_range\":{d},\"post_write_storage_read_trailer\":{d},\"post_write_storage_file_size\":{d},\"post_write_storage_read_bytes\":{d}",
            .{
                query.queries,
                query.rounds,
                scenario.cfg.post_write_k,
                query.exact_truth_build_ns,
                query.exact_truth_cache_hit,
                query.ns,
                ns_per_query,
                query.query_p50_ns,
                query.query_p95_ns,
                query.query_p99_ns,
                queries_per_second,
                query.recall_hits,
                query.recall_total,
                recall_at_k,
                query.storage.read_file,
                query.storage.read_range,
                query.storage.read_trailer,
                query.storage.file_size,
                query.storage.read_bytes,
            },
        );
        try writer.print(
            ",\"post_write_profile_total_ns\":{d},\"post_write_profile_setup_ns\":{d},\"post_write_profile_runtime_txn_ns\":{d},\"post_write_profile_scratch_acquire_ns\":{d},\"post_write_profile_search_scratch_allocations\":{d},\"post_write_profile_search_scratch_allocation_bytes\":{d},\"post_write_profile_search_scratch_retained_bytes\":{d},\"post_write_profile_upper_tree_pin_ns\":{d},\"post_write_profile_root_load_ns\":{d},\"post_write_profile_node_cache_miss_ns\":{d},\"post_write_profile_node_cache_misses\":{d},\"post_write_profile_quantized_cache_miss_ns\":{d},\"post_write_profile_quantized_cache_misses\":{d},\"post_write_profile_quantized_internal_cache_miss_ns\":{d},\"post_write_profile_quantized_internal_cache_misses\":{d},\"post_write_profile_quantized_leaf_cache_miss_ns\":{d},\"post_write_profile_quantized_leaf_cache_misses\":{d}",
            .{
                query.totals.total_ns,
                query.totals.setup_ns,
                query.totals.runtime_txn_ns,
                query.totals.scratch_acquire_ns,
                query.totals.search_scratch_allocations,
                query.totals.search_scratch_allocation_bytes,
                query.totals.search_scratch_retained_bytes,
                query.totals.upper_tree_pin_ns,
                query.totals.root_load_ns,
                query.totals.node_cache_miss_ns,
                query.totals.node_cache_misses,
                query.totals.quantized_cache_miss_ns,
                query.totals.quantized_cache_misses,
                query.totals.quantized_internal_cache_miss_ns,
                query.totals.quantized_internal_cache_misses,
                query.totals.quantized_leaf_cache_miss_ns,
                query.totals.quantized_leaf_cache_misses,
            },
        );
        try writer.print(
            ",\"post_write_profile_child_expand_ns\":{d},\"post_write_profile_leaf_score_ns\":{d},\"post_write_profile_posting_overlay_ns\":{d},\"post_write_profile_posting_overlay_calls\":{d},\"post_write_profile_posting_overlay_base_members\":{d},\"post_write_profile_posting_base_decode_ns\":{d},\"post_write_profile_posting_base_decode_members\":{d},\"post_write_profile_posting_delta_replay_ns\":{d},\"post_write_profile_posting_delta_replay_records\":{d},\"post_write_profile_posting_overlay_delta_records\":{d},\"post_write_profile_posting_overlay_delta_scan_skips\":{d},\"post_write_profile_posting_overlay_materialized_members\":{d},\"post_write_profile_posting_overlay_fallbacks\":{d},\"post_write_profile_posting_overlay_cache_hits\":{d},\"post_write_profile_posting_overlay_cache_misses\":{d},\"post_write_profile_posting_overlay_cache_evictions\":{d},\"post_write_profile_posting_overlay_cache_admission_skips\":{d},\"post_write_profile_posting_overlay_cache_member_bytes\":{d},\"post_write_profile_rerank_ns\":{d},\"post_write_profile_rerank_vector_load_ns\":{d},\"post_write_profile_rerank_metadata_ns\":{d}",
            .{
                query.totals.child_expand_ns,
                query.totals.leaf_score_ns,
                query.totals.posting_overlay_ns,
                query.totals.posting_overlay_calls,
                query.totals.posting_overlay_base_members,
                query.totals.posting_base_decode_ns,
                query.totals.posting_base_decode_members,
                query.totals.posting_delta_replay_ns,
                query.totals.posting_delta_replay_records,
                query.totals.posting_overlay_delta_records,
                query.totals.posting_overlay_delta_scan_skips,
                query.totals.posting_overlay_materialized_members,
                query.totals.posting_overlay_fallbacks,
                query.totals.posting_overlay_cache_hits,
                query.totals.posting_overlay_cache_misses,
                query.totals.posting_overlay_cache_evictions,
                query.totals.posting_overlay_cache_admission_skips,
                query.totals.posting_overlay_cache_member_bytes,
                query.totals.rerank_ns,
                query.totals.rerank_vector_load_ns,
                query.totals.rerank_metadata_ns,
            },
        );
        try writer.print(
            ",\"post_write_nodes_visited\":{d},\"post_write_leaves_explored\":{d},\"post_write_centroid_directory_blocks_scanned\":{d},\"post_write_centroid_directory_blocks_selected\":{d},\"post_write_centroid_directory_block_probe_limit\":{d},\"post_write_centroid_directory_block_probe_count\":{d},\"post_write_centroid_directory_block_centroids_scored\":{d},\"post_write_centroid_directory_block_centroid_estimates\":{d},\"post_write_centroid_directory_posting_centroids_scored\":{d},\"post_write_centroid_directory_posting_centroid_estimates\":{d},\"post_write_approx_vectors_scored\":{d},\"post_write_exact_vectors_scored\":{d},\"post_write_reranked_vectors\":{d},\"post_write_approx_candidate_count\":{d},\"post_write_rerank_candidate_count\":{d},\"post_write_result_count\":{d},\"post_write_search_workspace_bytes\":{d}",
            .{
                query.totals.nodes_visited,
                query.totals.leaves_explored,
                query.totals.centroid_directory_blocks_scanned,
                query.totals.centroid_directory_blocks_selected,
                query.totals.centroid_directory_block_probe_limit,
                query.totals.centroid_directory_block_probe_count,
                query.totals.centroid_directory_block_centroids_scored,
                query.totals.centroid_directory_block_centroid_estimates,
                query.totals.centroid_directory_posting_centroids_scored,
                query.totals.centroid_directory_posting_centroid_estimates,
                query.totals.approx_vectors_scored,
                query.totals.exact_vectors_scored,
                query.totals.reranked_vectors,
                query.totals.approx_candidate_count,
                query.totals.rerank_candidate_count,
                query.totals.result_count,
                scenario.index.search_workspace_bytes_accounted,
            },
        );
        if (query.warm_queries > 0) {
            const warm_ns_per_query = @as(f64, @floatFromInt(query.warm_ns)) / @as(f64, @floatFromInt(@max(query.warm_queries, 1)));
            const warm_queries_per_second = if (query.warm_ns == 0) 0 else (@as(f64, @floatFromInt(query.warm_queries)) * 1_000_000_000.0) / @as(f64, @floatFromInt(query.warm_ns));
            const warm_recall_at_k = if (query.warm_recall_total == 0) 0 else @as(f64, @floatFromInt(query.warm_recall_hits)) / @as(f64, @floatFromInt(query.warm_recall_total));
            try writer.print(
                ",\"post_write_warm_queries\":{d},\"post_write_warm_query_ns\":{d},\"post_write_warm_query_ns_per_query\":{d:.2},\"post_write_warm_query_p50_ns\":{d},\"post_write_warm_query_p95_ns\":{d},\"post_write_warm_query_p99_ns\":{d},\"post_write_warm_queries_per_second\":{d:.2},\"post_write_warm_recall_hits\":{d},\"post_write_warm_recall_total\":{d},\"post_write_warm_recall_at_k\":{d:.4},\"post_write_warm_storage_read_file\":{d},\"post_write_warm_storage_read_range\":{d},\"post_write_warm_storage_read_trailer\":{d},\"post_write_warm_storage_file_size\":{d},\"post_write_warm_storage_read_bytes\":{d}",
                .{
                    query.warm_queries,
                    query.warm_ns,
                    warm_ns_per_query,
                    query.warm_query_p50_ns,
                    query.warm_query_p95_ns,
                    query.warm_query_p99_ns,
                    warm_queries_per_second,
                    query.warm_recall_hits,
                    query.warm_recall_total,
                    warm_recall_at_k,
                    query.warm_storage.read_file,
                    query.warm_storage.read_range,
                    query.warm_storage.read_trailer,
                    query.warm_storage.file_size,
                    query.warm_storage.read_bytes,
                },
            );
            try writer.print(
                ",\"post_write_warm_profile_total_ns\":{d},\"post_write_warm_profile_setup_ns\":{d},\"post_write_warm_profile_runtime_txn_ns\":{d},\"post_write_warm_profile_scratch_acquire_ns\":{d},\"post_write_warm_profile_search_scratch_allocations\":{d},\"post_write_warm_profile_search_scratch_allocation_bytes\":{d},\"post_write_warm_profile_search_scratch_retained_bytes\":{d},\"post_write_warm_profile_upper_tree_pin_ns\":{d},\"post_write_warm_profile_root_load_ns\":{d},\"post_write_warm_profile_node_cache_miss_ns\":{d},\"post_write_warm_profile_node_cache_misses\":{d},\"post_write_warm_profile_quantized_cache_miss_ns\":{d},\"post_write_warm_profile_quantized_cache_misses\":{d},\"post_write_warm_profile_quantized_internal_cache_miss_ns\":{d},\"post_write_warm_profile_quantized_internal_cache_misses\":{d},\"post_write_warm_profile_quantized_leaf_cache_miss_ns\":{d},\"post_write_warm_profile_quantized_leaf_cache_misses\":{d},\"post_write_warm_profile_child_expand_ns\":{d},\"post_write_warm_profile_leaf_score_ns\":{d},\"post_write_warm_profile_posting_overlay_ns\":{d},\"post_write_warm_profile_posting_overlay_calls\":{d},\"post_write_warm_profile_posting_overlay_base_members\":{d},\"post_write_warm_profile_posting_base_decode_ns\":{d},\"post_write_warm_profile_posting_base_decode_members\":{d},\"post_write_warm_profile_posting_delta_replay_ns\":{d},\"post_write_warm_profile_posting_delta_replay_records\":{d},\"post_write_warm_profile_posting_overlay_delta_records\":{d},\"post_write_warm_profile_posting_overlay_delta_scan_skips\":{d},\"post_write_warm_profile_posting_overlay_materialized_members\":{d},\"post_write_warm_profile_posting_overlay_fallbacks\":{d},\"post_write_warm_profile_posting_overlay_cache_hits\":{d},\"post_write_warm_profile_posting_overlay_cache_misses\":{d},\"post_write_warm_profile_posting_overlay_cache_evictions\":{d},\"post_write_warm_profile_posting_overlay_cache_admission_skips\":{d},\"post_write_warm_profile_posting_overlay_cache_member_bytes\":{d},\"post_write_warm_profile_rerank_ns\":{d},\"post_write_warm_profile_rerank_vector_load_ns\":{d}",
                .{
                    query.warm_totals.total_ns,
                    query.warm_totals.setup_ns,
                    query.warm_totals.runtime_txn_ns,
                    query.warm_totals.scratch_acquire_ns,
                    query.warm_totals.search_scratch_allocations,
                    query.warm_totals.search_scratch_allocation_bytes,
                    query.warm_totals.search_scratch_retained_bytes,
                    query.warm_totals.upper_tree_pin_ns,
                    query.warm_totals.root_load_ns,
                    query.warm_totals.node_cache_miss_ns,
                    query.warm_totals.node_cache_misses,
                    query.warm_totals.quantized_cache_miss_ns,
                    query.warm_totals.quantized_cache_misses,
                    query.warm_totals.quantized_internal_cache_miss_ns,
                    query.warm_totals.quantized_internal_cache_misses,
                    query.warm_totals.quantized_leaf_cache_miss_ns,
                    query.warm_totals.quantized_leaf_cache_misses,
                    query.warm_totals.child_expand_ns,
                    query.warm_totals.leaf_score_ns,
                    query.warm_totals.posting_overlay_ns,
                    query.warm_totals.posting_overlay_calls,
                    query.warm_totals.posting_overlay_base_members,
                    query.warm_totals.posting_base_decode_ns,
                    query.warm_totals.posting_base_decode_members,
                    query.warm_totals.posting_delta_replay_ns,
                    query.warm_totals.posting_delta_replay_records,
                    query.warm_totals.posting_overlay_delta_records,
                    query.warm_totals.posting_overlay_delta_scan_skips,
                    query.warm_totals.posting_overlay_materialized_members,
                    query.warm_totals.posting_overlay_fallbacks,
                    query.warm_totals.posting_overlay_cache_hits,
                    query.warm_totals.posting_overlay_cache_misses,
                    query.warm_totals.posting_overlay_cache_evictions,
                    query.warm_totals.posting_overlay_cache_admission_skips,
                    query.warm_totals.posting_overlay_cache_member_bytes,
                    query.warm_totals.rerank_ns,
                    query.warm_totals.rerank_vector_load_ns,
                },
            );
            try writer.print(
                ",\"post_write_warm_nodes_visited\":{d},\"post_write_warm_leaves_explored\":{d},\"post_write_warm_centroid_directory_blocks_scanned\":{d},\"post_write_warm_centroid_directory_blocks_selected\":{d},\"post_write_warm_centroid_directory_block_probe_limit\":{d},\"post_write_warm_centroid_directory_block_probe_count\":{d},\"post_write_warm_centroid_directory_block_centroids_scored\":{d},\"post_write_warm_centroid_directory_block_centroid_estimates\":{d},\"post_write_warm_centroid_directory_posting_centroids_scored\":{d},\"post_write_warm_centroid_directory_posting_centroid_estimates\":{d},\"post_write_warm_approx_vectors_scored\":{d},\"post_write_warm_exact_vectors_scored\":{d},\"post_write_warm_reranked_vectors\":{d},\"post_write_warm_approx_candidate_count\":{d},\"post_write_warm_rerank_candidate_count\":{d},\"post_write_warm_result_count\":{d}",
                .{
                    query.warm_totals.nodes_visited,
                    query.warm_totals.leaves_explored,
                    query.warm_totals.centroid_directory_blocks_scanned,
                    query.warm_totals.centroid_directory_blocks_selected,
                    query.warm_totals.centroid_directory_block_probe_limit,
                    query.warm_totals.centroid_directory_block_probe_count,
                    query.warm_totals.centroid_directory_block_centroids_scored,
                    query.warm_totals.centroid_directory_block_centroid_estimates,
                    query.warm_totals.centroid_directory_posting_centroids_scored,
                    query.warm_totals.centroid_directory_posting_centroid_estimates,
                    query.warm_totals.approx_vectors_scored,
                    query.warm_totals.exact_vectors_scored,
                    query.warm_totals.reranked_vectors,
                    query.warm_totals.approx_candidate_count,
                    query.warm_totals.rerank_candidate_count,
                    query.warm_totals.result_count,
                },
            );
        }
    }
    try writer.print(
        ",\"active_count_after\":{d},\"node_count_after\":{d},\"storage_write_file\":{d},\"storage_write_bytes\":{d},\"storage_manifest_write_file\":{d},\"storage_manifest_write_bytes\":{d},\"storage_rename\":{d},\"storage_delete_file\":{d},\"storage_delete_tree\":{d},\"storage_read_file\":{d},\"storage_read_range\":{d},\"storage_read_trailer\":{d},\"storage_file_size\":{d},\"storage_read_bytes\":{d}",
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
            storage_delta.read_range,
            storage_delta.read_trailer,
            storage_delta.file_size,
            storage_delta.read_bytes,
        },
    );
    try writer.print(
        ",\"hbc_cache_total_bytes\":{d},\"hbc_cache_node_bytes\":{d},\"hbc_cache_quantized_bytes\":{d},\"hbc_cache_vector_bytes\":{d},\"hbc_cache_metadata_bytes\":{d}",
        .{
            cache_stats.total_bytes,
            cache_stats.node.used_bytes,
            cache_stats.quantized.used_bytes,
            cache_stats.vector.used_bytes,
            cache_stats.metadata.used_bytes,
        },
    );
    try writer.print(
        ",\"insert_calls\":{d},\"save_node_calls\":{d},\"split_leaf_calls\":{d},\"split_internal_calls\":{d},\"existing_same_vector_coalesces\":{d},\"existing_same_leaf_updates\":{d},\"existing_vector_reroutes\":{d},\"bulk_build_store_ns\":{d},\"bulk_build_tree_ns\":{d},\"kmeans_assignment_calls\":{d},\"kmeans_assignment_cpu_calls\":{d},\"kmeans_assignment_metal_calls\":{d},\"kmeans_assignment_points_total\":{d},\"kmeans_assignment_ns\":{d},\"kmeans_assignment_cpu_ns\":{d},\"kmeans_assignment_metal_ns\":{d},\"kmeans_update_calls\":{d},\"kmeans_update_cpu_calls\":{d},\"kmeans_update_metal_calls\":{d},\"kmeans_update_ns\":{d},\"kmeans_update_cpu_ns\":{d},\"kmeans_update_metal_ns\":{d},\"insert_transform_ns\":{d},\"insert_store_vector_ns\":{d},\"insert_find_leaf_ns\":{d},\"insert_mutate_leaf_ns\":{d}",
        .{
            profile.insert_calls,
            profile.save_node_calls,
            profile.split_leaf_calls,
            profile.split_internal_calls,
            profile.existing_same_vector_coalesces,
            profile.existing_same_leaf_updates,
            profile.existing_vector_reroutes,
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
        ",\"split_leaf_input_members_total\":{d},\"split_leaf_input_overflow_members_total\":{d},\"bulk_leaf_rebuild_calls\":{d},\"bulk_leaf_rebuild_members_total\":{d},\"bulk_leaf_rebuild_members_max\":{d}",
        .{
            profile.split_leaf_input_members_total,
            profile.split_leaf_input_overflow_members_total,
            profile.bulk_leaf_rebuild_calls,
            profile.bulk_leaf_rebuild_members_total,
            profile.bulk_leaf_rebuild_members_max,
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
        ",\"posting_maintenance_scanned_nodes\":{d},\"posting_maintenance_scanned_postings\":{d},\"posting_maintenance_dirty_postings\":{d},\"posting_maintenance_repaired_postings\":{d},\"posting_maintenance_centroid_refreshed\":{d},\"posting_maintenance_payload_refreshed\":{d},\"posting_maintenance_ancestor_refresh_roots\":{d},\"posting_maintenance_split_postings\":{d},\"posting_maintenance_merged_postings\":{d},\"posting_maintenance_boundary_reassigned_vectors\":{d},\"posting_maintenance_boundary_reassignment_capacity_skips\":{d},\"posting_maintenance_boundary_reassignment_min_source_skips\":{d},\"posting_maintenance_boundary_reassignment_swap_moves\":{d},\"posting_maintenance_delta_fold_attempts\":{d},\"posting_maintenance_delta_fold_skipped\":{d},\"posting_maintenance_delta_fold_records\":{d},\"auto_posting_maintenance_runs\":{d},\"auto_posting_maintenance_observed_max_repaired_postings\":{d},\"auto_posting_maintenance_observed_max_layout_changes\":{d},\"auto_posting_maintenance_observed_max_split_postings\":{d},\"auto_posting_maintenance_observed_max_merged_postings\":{d},\"auto_posting_maintenance_observed_max_boundary_reassigned_vectors\":{d},\"auto_posting_maintenance_observed_max_delta_fold_records\":{d}",
        .{
            profile.posting_maintenance_scanned_nodes,
            profile.posting_maintenance_scanned_postings,
            profile.posting_maintenance_dirty_postings,
            profile.posting_maintenance_repaired_postings,
            profile.posting_maintenance_centroid_refreshed,
            profile.posting_maintenance_payload_refreshed,
            profile.posting_maintenance_ancestor_refresh_roots,
            profile.posting_maintenance_split_postings,
            profile.posting_maintenance_merged_postings,
            profile.posting_maintenance_boundary_reassigned_vectors,
            profile.posting_maintenance_boundary_reassignment_capacity_skips,
            profile.posting_maintenance_boundary_reassignment_min_source_skips,
            profile.posting_maintenance_boundary_reassignment_swap_moves,
            profile.posting_maintenance_delta_fold_attempts,
            profile.posting_maintenance_delta_fold_skipped,
            profile.posting_maintenance_delta_fold_records,
            profile.auto_posting_maintenance_runs,
            profile.auto_posting_maintenance_observed_max_repaired_postings,
            profile.auto_posting_maintenance_observed_max_layout_changes,
            profile.auto_posting_maintenance_observed_max_split_postings,
            profile.auto_posting_maintenance_observed_max_merged_postings,
            profile.auto_posting_maintenance_observed_max_boundary_reassigned_vectors,
            profile.auto_posting_maintenance_observed_max_delta_fold_records,
        },
    );
    try writer.print(
        ",\"posting_backlog_scanned_nodes\":{d},\"posting_backlog_scanned_postings\":{d},\"posting_backlog_dirty_postings\":{d},\"posting_backlog_centroid_dirty_postings\":{d},\"posting_backlog_payload_dirty_postings\":{d},\"posting_backlog_min_dirty_mutation_version\":{d},\"posting_backlog_max_dirty_version_age\":{d},\"posting_backlog_delta_tail_postings\":{d},\"posting_backlog_max_delta_tail_records\":{d},\"posting_backlog_max_tombstone_tail_records\":{d},\"posting_backlog_max_delta_to_base_ratio_bps\":{d},\"posting_backlog_overfull_postings\":{d},\"posting_backlog_postings_at_capacity\":{d},\"posting_backlog_max_over_capacity_members\":{d},\"posting_backlog_max_centroid_version_lag\":{d},\"posting_backlog_max_payload_version_lag\":{d},\"posting_backlog_max_mutation_version\":{d},\"posting_backlog_skipped_missing\":{d}",
        .{
            backlog.scanned_nodes,
            backlog.scanned_postings,
            backlog.dirty_postings,
            backlog.centroid_dirty_postings,
            backlog.payload_dirty_postings,
            backlog.min_dirty_mutation_version,
            backlog.max_dirty_version_age,
            backlog.delta_tail_postings,
            backlog.max_delta_tail_records,
            backlog.max_tombstone_tail_records,
            backlog.max_delta_to_base_ratio_bps,
            backlog.overfull_postings,
            backlog.postings_at_capacity,
            backlog.max_over_capacity_members,
            backlog.max_centroid_version_lag,
            backlog.max_payload_version_lag,
            backlog.max_mutation_version,
            backlog.skipped_missing,
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
    try writer.print(
        ",\"posting_base_put_calls\":{d},\"posting_base_key_bytes\":{d},\"posting_base_value_bytes\":{d},\"posting_base_blocks\":{d},\"centroid_directory_put_calls\":{d},\"centroid_directory_key_bytes\":{d},\"centroid_directory_value_bytes\":{d},\"assignment_map_put_calls\":{d},\"assignment_map_delete_calls\":{d},\"assignment_map_key_bytes\":{d},\"assignment_map_value_bytes\":{d},\"posting_delta_append_calls\":{d},\"posting_delta_records\":{d},\"posting_delta_key_bytes\":{d},\"posting_delta_value_bytes\":{d},\"posting_delta_fold_calls\":{d},\"posting_delta_fold_records\":{d},\"posting_delta_fold_base_members\":{d},\"posting_delta_fold_materialized_members\":{d},\"posting_delta_fold_deleted_tail_keys\":{d},\"posting_delta_fold_deleted_tail_key_bytes\":{d},\"posting_delta_fold_deleted_tail_value_bytes\":{d},\"posting_delta_fold_written_base_key_bytes\":{d},\"posting_delta_fold_written_base_value_bytes\":{d}",
        .{
            profile.posting_base_put_calls,
            profile.posting_base_key_bytes,
            profile.posting_base_value_bytes,
            profile.posting_base_blocks,
            profile.centroid_directory_put_calls,
            profile.centroid_directory_key_bytes,
            profile.centroid_directory_value_bytes,
            profile.assignment_map_put_calls,
            profile.assignment_map_delete_calls,
            profile.assignment_map_key_bytes,
            profile.assignment_map_value_bytes,
            profile.posting_delta_append_calls,
            profile.posting_delta_records,
            profile.posting_delta_key_bytes,
            profile.posting_delta_value_bytes,
            profile.posting_delta_fold_calls,
            profile.posting_delta_fold_records,
            profile.posting_delta_fold_base_members,
            profile.posting_delta_fold_materialized_members,
            profile.posting_delta_fold_deleted_tail_keys,
            profile.posting_delta_fold_deleted_tail_key_bytes,
            profile.posting_delta_fold_deleted_tail_value_bytes,
            profile.posting_delta_fold_written_base_key_bytes,
            profile.posting_delta_fold_written_base_value_bytes,
        },
    );
    if (maintenance) |stats| {
        try writer.print(
            ",\"lsm_total_runs\":{d},\"lsm_total_run_bytes\":{d},\"lsm_total_run_logical_entry_bytes\":{d},\"lsm_total_run_physical_entry_bytes\":{d},\"lsm_total_run_compressed_blocks\":{d},\"lsm_total_run_raw_blocks\":{d},\"lsm_total_run_compression_codec_mask\":{d},\"lsm_l0_runs\":{d},\"lsm_l0_bytes\":{d},\"lsm_overlapping_l0_runs\":{d},\"lsm_lower_level_runs\":{d},\"lsm_lower_level_bytes\":{d},\"lsm_max_level\":{d},\"lsm_obsolete_paths\":{d},\"lsm_compaction_scheduler_grants\":{d},\"lsm_compaction_scheduler_denied_capacity\":{d},\"lsm_compaction_scheduler_denied_resource_pressure\":{d},\"lsm_compaction_scheduler_oversized_grants\":{d},\"lsm_compaction_scheduler_oversized_skips\":{d},\"lsm_compaction_scheduler_remembered_pending\":{d},\"lsm_compaction_scheduler_remembered_candidates\":{d},\"lsm_compaction_scheduler_remembered_retries\":{d},\"lsm_compaction_scheduler_remembered_hits\":{d},\"lsm_compaction_scheduler_remembered_stale\":{d},\"lsm_compaction_scheduler_conflict_denials\":{d}",
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
                stats.compaction_scheduler_oversized_grants,
                stats.compaction_scheduler_oversized_skips,
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
            ",\"active_table_runs\":{d},\"active_table_run_bytes\":{d},\"active_table_obsolete_paths\":{d},\"active_table_obsolete_file_bytes\":{d},\"active_table_obsolete_due_paths\":{d},\"active_table_obsolete_due_file_bytes\":{d},\"active_table_obsolete_future_paths\":{d},\"active_table_manifest_bytes\":{d},\"active_table_bloom_filter_bytes\":{d},\"active_hbc_quant_entries\":{d},\"active_hbc_quant_value_bytes\":{d},\"latest_hbc_quant_entries\":{d},\"latest_hbc_quant_value_bytes\":{d},\"hbc_quant_versions_per_key_bps\":{d},\"active_hbc_vecs_entries\":{d},\"active_hbc_vecs_value_bytes\":{d},\"latest_hbc_vecs_entries\":{d},\"latest_hbc_vecs_value_bytes\":{d},\"active_hbc_nodes_entries\":{d},\"active_hbc_nodes_value_bytes\":{d},\"latest_hbc_nodes_entries\":{d},\"latest_hbc_nodes_value_bytes\":{d},\"active_hbc_meta_entries\":{d},\"active_hbc_meta_value_bytes\":{d},\"latest_hbc_meta_entries\":{d},\"latest_hbc_meta_value_bytes\":{d},\"latest_lsm_keys\":{d}",
            .{
                stats.active_runs,
                stats.active_run_bytes,
                stats.obsolete_paths,
                stats.obsolete_file_bytes,
                stats.obsolete_due_paths,
                stats.obsolete_due_file_bytes,
                stats.obsolete_future_paths,
                stats.manifest_bytes,
                stats.active_bloom_filter_bytes,
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

fn printPostingRepairResult(writer: anytype, comptime prefix: []const u8, result: hbc.PostingMaintenanceResult) !void {
    try writer.print(
        ",\"{s}_limit_reached\":{},\"{s}_remaining_dirty_postings\":{d},\"{s}_remaining_delta_tail_postings\":{d},\"{s}_remaining_overfull_postings\":{d},\"{s}_remaining_postings_at_capacity\":{d},\"{s}_remaining_max_over_capacity_members\":{d}",
        .{
            prefix,
            result.limit_reached,
            prefix,
            result.remaining_dirty_postings,
            prefix,
            result.remaining_delta_tail_postings,
            prefix,
            result.remaining_overfull_postings,
            prefix,
            result.remaining_postings_at_capacity,
            prefix,
            result.remaining_max_over_capacity_members,
        },
    );
}

fn printCompactQueryResult(writer: anytype, prefix: []const u8, query: PostWriteQueryResult) !void {
    const ns_per_query = @as(f64, @floatFromInt(query.ns)) / @as(f64, @floatFromInt(@max(query.queries, 1)));
    const queries_per_second = if (query.ns == 0) 0 else (@as(f64, @floatFromInt(query.queries)) * 1_000_000_000.0) / @as(f64, @floatFromInt(query.ns));
    const recall_at_k = if (query.recall_total == 0) 0 else @as(f64, @floatFromInt(query.recall_hits)) / @as(f64, @floatFromInt(query.recall_total));
    try writer.print(
        ",\"{s}_queries\":{d},\"{s}_exact_truth_build_ns\":{d},\"{s}_exact_truth_cache_hit\":{},\"{s}_query_ns_per_query\":{d:.2},\"{s}_query_p95_ns\":{d},\"{s}_queries_per_second\":{d:.2},\"{s}_recall_hits\":{d},\"{s}_recall_total\":{d},\"{s}_recall_at_k\":{d:.4},\"{s}_storage_read_bytes\":{d},\"{s}_profile_posting_delta_replay_ns\":{d},\"{s}_profile_posting_delta_replay_records\":{d},\"{s}_profile_posting_overlay_delta_records\":{d},\"{s}_profile_posting_overlay_cache_hits\":{d},\"{s}_profile_posting_overlay_cache_misses\":{d}",
        .{
            prefix,
            query.queries,
            prefix,
            query.exact_truth_build_ns,
            prefix,
            query.exact_truth_cache_hit,
            prefix,
            ns_per_query,
            prefix,
            query.query_p95_ns,
            prefix,
            queries_per_second,
            prefix,
            query.recall_hits,
            prefix,
            query.recall_total,
            prefix,
            recall_at_k,
            prefix,
            query.storage.read_bytes,
            prefix,
            query.totals.posting_delta_replay_ns,
            prefix,
            query.totals.posting_delta_replay_records,
            prefix,
            query.totals.posting_overlay_delta_records,
            prefix,
            query.totals.posting_overlay_cache_hits,
            prefix,
            query.totals.posting_overlay_cache_misses,
        },
    );
    if (query.warm_queries > 0) {
        const warm_ns_per_query = @as(f64, @floatFromInt(query.warm_ns)) / @as(f64, @floatFromInt(@max(query.warm_queries, 1)));
        const warm_queries_per_second = if (query.warm_ns == 0) 0 else (@as(f64, @floatFromInt(query.warm_queries)) * 1_000_000_000.0) / @as(f64, @floatFromInt(query.warm_ns));
        const warm_recall_at_k = if (query.warm_recall_total == 0) 0 else @as(f64, @floatFromInt(query.warm_recall_hits)) / @as(f64, @floatFromInt(query.warm_recall_total));
        try writer.print(
            ",\"{s}_warm_queries\":{d},\"{s}_warm_query_ns_per_query\":{d:.2},\"{s}_warm_query_p95_ns\":{d},\"{s}_warm_queries_per_second\":{d:.2},\"{s}_warm_recall_hits\":{d},\"{s}_warm_recall_total\":{d},\"{s}_warm_recall_at_k\":{d:.4},\"{s}_warm_storage_read_bytes\":{d},\"{s}_warm_profile_posting_delta_replay_ns\":{d},\"{s}_warm_profile_posting_delta_replay_records\":{d},\"{s}_warm_profile_posting_overlay_delta_records\":{d},\"{s}_warm_profile_posting_overlay_cache_hits\":{d},\"{s}_warm_profile_posting_overlay_cache_misses\":{d}",
            .{
                prefix,
                query.warm_queries,
                prefix,
                warm_ns_per_query,
                prefix,
                query.warm_query_p95_ns,
                prefix,
                warm_queries_per_second,
                prefix,
                query.warm_recall_hits,
                prefix,
                query.warm_recall_total,
                prefix,
                warm_recall_at_k,
                prefix,
                query.warm_storage.read_bytes,
                prefix,
                query.warm_totals.posting_delta_replay_ns,
                prefix,
                query.warm_totals.posting_delta_replay_records,
                prefix,
                query.warm_totals.posting_overlay_delta_records,
                prefix,
                query.warm_totals.posting_overlay_cache_hits,
                prefix,
                query.warm_totals.posting_overlay_cache_misses,
            },
        );
    }
}

fn analyzeActiveTables(scenario: *Scenario) !?ActiveTableStats {
    if (scenario.storage_kind == .memory) return null;
    return try analyzeActiveTablesRoot(scenario.allocator, scenario.storage_harness.storage(), scenario.root_dir);
}

fn analyzeActiveTablesRoot(
    allocator: Allocator,
    storage: lsm_backend.Storage,
    root_dir: []const u8,
) !?ActiveTableStats {
    const manifest_path = try std.fmt.allocPrint(allocator, "{s}/manifest.bin", .{root_dir});
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
        .manifest_bytes = @intCast(manifest_bytes.len),
    };
    for (manifest.runs) |run| {
        stats.active_run_bytes += run.size_bytes;
        const path = try resolveRunPathAlloc(allocator, root_dir, run.path);
        defer allocator.free(path);
        var index = try lsm_repository.loadRunTableIndexAllocWithStorage(storage, allocator, path);
        defer index.deinit(allocator);
        stats.active_bloom_filter_bytes += index.filter.bytes.len;
    }

    const now_ns = storage.nowNs();
    for (manifest.obsolete_paths) |obsolete| {
        const path = try resolveRunPathAlloc(allocator, root_dir, obsolete.path);
        defer allocator.free(path);
        const file_bytes = storage.fileSize(path) catch 0;
        stats.obsolete_file_bytes += file_bytes;
        if (obsolete.delete_after_ns <= now_ns) {
            stats.obsolete_due_paths += 1;
            stats.obsolete_due_file_bytes += file_bytes;
        } else {
            stats.obsolete_future_paths += 1;
        }
    }

    var latest = std.StringHashMap(LatestEntry).init(allocator);
    defer {
        var key_it = latest.keyIterator();
        while (key_it.next()) |key| allocator.free(key.*);
        latest.deinit();
    }

    for (manifest.runs) |run| {
        const path = try resolveRunPathAlloc(allocator, root_dir, run.path);
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

fn inspectRoot(writer: anytype, allocator: Allocator, root_dir: []const u8) !void {
    var native = try lsm_backend.storage_io.NativeStorage.init(allocator, .threaded);
    defer native.deinit();

    const maybe_stats = try analyzeActiveTablesRoot(allocator, native.storage(), root_dir);
    const stats = maybe_stats orelse return error.FileNotFound;
    try writer.print(
        "{{\"inspect_root\":\"{s}\",\"active_table_runs\":{d},\"active_table_run_bytes\":{d},\"active_table_obsolete_paths\":{d},\"active_table_obsolete_file_bytes\":{d},\"active_table_obsolete_due_paths\":{d},\"active_table_obsolete_due_file_bytes\":{d},\"active_table_obsolete_future_paths\":{d},\"active_table_manifest_bytes\":{d},\"active_table_bloom_filter_bytes\":{d},\"active_hbc_quant_entries\":{d},\"active_hbc_quant_value_bytes\":{d},\"latest_hbc_quant_entries\":{d},\"latest_hbc_quant_value_bytes\":{d},\"hbc_quant_versions_per_key_bps\":{d},\"active_hbc_vecs_entries\":{d},\"active_hbc_vecs_value_bytes\":{d},\"latest_hbc_vecs_entries\":{d},\"latest_hbc_vecs_value_bytes\":{d},\"active_hbc_nodes_entries\":{d},\"active_hbc_nodes_value_bytes\":{d},\"latest_hbc_nodes_entries\":{d},\"latest_hbc_nodes_value_bytes\":{d},\"active_hbc_meta_entries\":{d},\"active_hbc_meta_value_bytes\":{d},\"latest_hbc_meta_entries\":{d},\"latest_hbc_meta_value_bytes\":{d},\"latest_lsm_keys\":{d}}}\n",
        .{
            root_dir,
            stats.active_runs,
            stats.active_run_bytes,
            stats.obsolete_paths,
            stats.obsolete_file_bytes,
            stats.obsolete_due_paths,
            stats.obsolete_due_file_bytes,
            stats.obsolete_future_paths,
            stats.manifest_bytes,
            stats.active_bloom_filter_bytes,
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

fn runRootMaintenance(allocator: Allocator, root_dir: []const u8, max_steps: usize) !void {
    var native = try lsm_backend.storage_io.NativeStorage.init(allocator, .threaded);
    defer native.deinit();

    var backend = try lsm_backend.Backend.open(allocator, root_dir, .{
        .backend = .{ .create_if_missing = false },
        .storage = native.storage(),
    });
    defer {
        backend.options.backend.read_only = true;
        backend.close();
    }

    var steps: usize = 0;
    while (steps < max_steps) : (steps += 1) {
        if (!try backend.runMaintenanceStep()) break;
    }
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
        .search_width = effectiveSearchWidth(cfg),
        .epsilon = 7,
        .use_quantization = cfg.use_quantization,
        .rerank_policy = .boundary,
        .quantizer_seed = cfg.seed,
        .use_random_ortho_trans = cfg.use_random_ortho_trans,
        .bulk_build_algo = cfg.bulk_build_algo,
        .kmeans_backend = cfg.kmeans_backend,
        .kmeans_update_strategy = cfg.kmeans_update_strategy,
        .centroid_directory_mode = cfg.centroid_directory_mode,
        .posting_storage_mode = cfg.posting_storage_mode,
        .posting_base_member_block_size = cfg.posting_base_member_block_size,
        .flat_centroid_block_size = cfg.flat_centroid_block_size,
        .flat_centroid_probe_count = cfg.flat_centroid_probe_count,
        .flat_centroid_block_probe_count = cfg.flat_centroid_block_probe_count,
        .max_posting_overlay_cache_bytes = cfg.max_posting_overlay_cache_bytes,
        .max_posting_overlay_cache_entry_bytes = cfg.max_posting_overlay_cache_entry_bytes,
        .lazy_posting_maintenance = cfg.lazy_posting_maintenance,
        .auto_posting_maintenance_max_postings = cfg.auto_posting_maintenance_max_postings,
        .auto_posting_maintenance_fold_delta_tails = cfg.auto_posting_maintenance_fold_delta_tails,
        .auto_posting_maintenance_min_delta_records_to_fold = cfg.auto_posting_maintenance_min_delta_records_to_fold,
        .auto_posting_maintenance_min_tombstone_records_to_fold = cfg.auto_posting_maintenance_min_tombstone_records_to_fold,
        .auto_posting_maintenance_min_delta_to_base_ratio_bps = cfg.auto_posting_maintenance_min_delta_to_base_ratio_bps,
        .auto_posting_maintenance_max_delta_tail_postings = cfg.auto_posting_maintenance_max_delta_tail_postings,
        .auto_posting_maintenance_min_dirty_postings = cfg.auto_posting_maintenance_min_dirty_postings,
        .auto_posting_maintenance_max_dirty_version_age = cfg.auto_posting_maintenance_max_dirty_version_age,
        .auto_posting_maintenance_min_delta_records_to_run = cfg.auto_posting_maintenance_min_delta_records_to_run,
        .auto_posting_maintenance_min_tombstone_records_to_run = cfg.auto_posting_maintenance_min_tombstone_records_to_run,
        .auto_posting_maintenance_min_delta_to_base_ratio_bps_to_run = cfg.auto_posting_maintenance_min_delta_to_base_ratio_bps_to_run,
        .auto_posting_maintenance_min_centroid_version_lag = cfg.auto_posting_maintenance_min_centroid_version_lag,
        .auto_posting_maintenance_min_payload_version_lag = cfg.auto_posting_maintenance_min_payload_version_lag,
        .auto_posting_maintenance_max_layout_changes = cfg.auto_posting_maintenance_max_layout_changes,
        .auto_posting_maintenance_split_full_postings = cfg.auto_posting_maintenance_split_full_postings,
        .auto_posting_maintenance_min_overfull_postings_to_run = cfg.auto_posting_maintenance_min_overfull_postings_to_run,
        .auto_posting_maintenance_min_postings_at_capacity_to_run = cfg.auto_posting_maintenance_min_postings_at_capacity_to_run,
        .auto_posting_maintenance_max_boundary_reassignments = cfg.auto_posting_maintenance_max_boundary_reassignments,
        .auto_posting_maintenance_allow_overfull_reassignment = cfg.auto_posting_maintenance_allow_overfull_reassignment,
        .auto_posting_maintenance_max_overfull_reassignment_postings = cfg.auto_posting_maintenance_max_overfull_reassignment_postings,
        .auto_posting_maintenance_max_over_capacity_reassignment_members = cfg.auto_posting_maintenance_max_over_capacity_reassignment_members,
        .auto_posting_maintenance_boundary_reassignment_min_improvement = cfg.auto_posting_maintenance_boundary_reassignment_min_improvement,
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

fn writeProceduralDatasetVector(seed: u64, row: usize, dst: []f32) void {
    const cluster = row % 16;
    const base = @as(f32, @floatFromInt(cluster)) * 0.10;
    for (dst, 0..) |*value, dim| {
        const noise = proceduralUnitNoise(seed, row, dim);
        value.* = base + noise * 0.01;
    }
    _ = vec.normalize(dst);
}

fn proceduralCosineDistanceToQuery(seed: u64, row: usize, query: []const f32) f32 {
    return proceduralCosineDistanceToQueryWithNormSq(seed, row, query, vectorNormSqScalar(query));
}

fn proceduralCosineDistanceToQueryWithNormSq(seed: u64, row: usize, query: []const f32, query_norm_sq: f32) f32 {
    return proceduralCosineDistanceToQueryWithNorms(seed, row, query, query_norm_sq, proceduralCandidateNormSq(seed, row, query.len));
}

fn proceduralCosineDistanceToQueryWithNorms(seed: u64, row: usize, query: []const f32, query_norm_sq: f32, candidate_norm_sq: f32) f32 {
    const cluster = row % 16;
    const base = @as(f32, @floatFromInt(cluster)) * 0.10;
    var dot: f32 = 0;
    for (query, 0..) |query_component, dim| {
        const candidate_component = base + proceduralUnitNoise(seed, row, dim) * 0.01;
        dot += query_component * candidate_component;
    }
    if (query_norm_sq == 0 or candidate_norm_sq == 0) return 1.0;
    return 1.0 - dot / (@sqrt(query_norm_sq) * @sqrt(candidate_norm_sq));
}

fn proceduralCandidateNormSq(seed: u64, row: usize, dims: usize) f32 {
    const cluster = row % 16;
    const base = @as(f32, @floatFromInt(cluster)) * 0.10;
    var sum: f32 = 0;
    for (0..dims) |dim| {
        const candidate_component = base + proceduralUnitNoise(seed, row, dim) * 0.01;
        sum += candidate_component * candidate_component;
    }
    return sum;
}

fn vectorNormSqScalar(values: []const f32) f32 {
    var sum: f32 = 0;
    for (values) |value| sum += value * value;
    return sum;
}

fn proceduralUnitNoise(seed: u64, row: usize, dim: usize) f32 {
    var x = seed ^ (@as(u64, @intCast(row + 1)) *% 0x9e37_79b9_7f4a_7c15);
    x ^= @as(u64, @intCast(dim + 1)) *% 0xbf58_476d_1ce4_e5b9;
    x = (x ^ (x >> 30)) *% 0xbf58_476d_1ce4_e5b9;
    x = (x ^ (x >> 27)) *% 0x94d0_49bb_1331_11eb;
    x ^= x >> 31;
    const mantissa = @as(u32, @intCast(x >> 40));
    return @as(f32, @floatFromInt(mantissa)) / @as(f32, @floatFromInt(@as(u32, 1) << 24));
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

fn makeSameLeafOverwriteItems(allocator: Allocator, cfg: Config, dataset: []const f32) ![]hbc.BatchInsertItem {
    if (cfg.vectors < 1) return error.InvalidArgument;
    const keys_per_round = if (cfg.overwrite_hot_keys == 0)
        @min(cfg.batch_size, cfg.vectors)
    else
        @min(cfg.overwrite_hot_keys, cfg.vectors);
    if (keys_per_round == 0 or cfg.overwrite_rounds == 0) return error.InvalidArgument;

    const count = keys_per_round * cfg.overwrite_rounds;
    const items = try allocator.alloc(hbc.BatchInsertItem, count);
    errdefer allocator.free(items);
    var initialized: usize = 0;
    errdefer for (items[0..initialized]) |item| allocator.free(item.metadata);

    for (items, 0..) |*item, i| {
        const target_row = i % keys_per_round;
        item.* = .{
            .vector_id = @intCast(target_row + 1),
            .vector = dataset[target_row * cfg.dims ..][0..cfg.dims],
            .metadata = try std.fmt.allocPrint(allocator, "same-leaf-overwrite:{d}", .{i}),
        };
        initialized += 1;
    }
    return items;
}

fn makeHotOverwriteItems(allocator: Allocator, cfg: Config, dataset: []const f32) ![]hbc.BatchInsertItem {
    if (cfg.vectors < 2) return error.InvalidArgument;
    const hot_keys = if (cfg.overwrite_hot_keys == 0)
        @min(cfg.batch_size, cfg.vectors)
    else
        @min(cfg.overwrite_hot_keys, cfg.vectors);
    if (hot_keys == 0 or cfg.overwrite_rounds == 0) return error.InvalidArgument;

    const count = hot_keys * cfg.overwrite_rounds;
    const items = try allocator.alloc(hbc.BatchInsertItem, count);
    errdefer allocator.free(items);
    var initialized: usize = 0;
    errdefer for (items[0..initialized]) |item| allocator.free(item.metadata);

    for (items, 0..) |*item, i| {
        const vector_id: u64 = @intCast((i % hot_keys) + 1);
        var source_row = (hot_keys + i) % cfg.vectors;
        if (source_row == vector_id - 1) source_row = (source_row + 1) % cfg.vectors;
        const metadata = try allocator.dupe(u8, "");
        item.* = .{
            .vector_id = vector_id,
            .vector = dataset[source_row * cfg.dims ..][0..cfg.dims],
            .metadata = metadata,
        };
        initialized += 1;
    }
    return items;
}

fn makeRandomOverwriteItems(allocator: Allocator, cfg: Config, dataset: []const f32) ![]hbc.BatchInsertItem {
    if (cfg.vectors < 2) return error.InvalidArgument;
    const keys_per_round = if (cfg.overwrite_hot_keys == 0)
        @min(cfg.batch_size, cfg.vectors)
    else
        @min(cfg.overwrite_hot_keys, cfg.vectors);
    if (keys_per_round == 0 or cfg.overwrite_rounds == 0) return error.InvalidArgument;

    const count = keys_per_round * cfg.overwrite_rounds;
    const items = try allocator.alloc(hbc.BatchInsertItem, count);
    errdefer allocator.free(items);
    var initialized: usize = 0;
    errdefer for (items[0..initialized]) |item| allocator.free(item.metadata);

    for (items, 0..) |*item, i| {
        const round = i / keys_per_round;
        const slot = i % keys_per_round;
        const round_offset = @as(usize, @intCast((cfg.seed + @as(u64, @intCast(round)) *% 7919) % @as(u64, @intCast(cfg.vectors))));
        const target_row = (round_offset + slot * (cfg.vectors - 1)) % cfg.vectors;
        var source_row = (target_row + 1 + ((round * 17 + slot * 31) % (cfg.vectors - 1))) % cfg.vectors;
        if (source_row == target_row) source_row = (source_row + 1) % cfg.vectors;
        const metadata = try allocator.dupe(u8, "");
        item.* = .{
            .vector_id = @intCast(target_row + 1),
            .vector = dataset[source_row * cfg.dims ..][0..cfg.dims],
            .metadata = metadata,
        };
        initialized += 1;
    }
    return items;
}

fn makeSemanticDriftOverwriteItems(allocator: Allocator, cfg: Config, dataset: []const f32) ![]hbc.BatchInsertItem {
    if (cfg.vectors < 2) return error.InvalidArgument;
    const keys_per_round = if (cfg.overwrite_hot_keys == 0)
        @min(cfg.batch_size, cfg.vectors)
    else
        @min(cfg.overwrite_hot_keys, cfg.vectors);
    if (keys_per_round == 0 or cfg.overwrite_rounds == 0) return error.InvalidArgument;

    const count = keys_per_round * cfg.overwrite_rounds;
    const items = try allocator.alloc(hbc.BatchInsertItem, count);
    errdefer allocator.free(items);
    var initialized: usize = 0;
    errdefer for (items[0..initialized]) |item| allocator.free(item.metadata);

    for (items, 0..) |*item, i| {
        const target_row = i % keys_per_round;
        var source_row = (target_row + 16 + (i / keys_per_round)) % cfg.vectors;
        if (source_row == target_row) source_row = (source_row + 1) % cfg.vectors;
        item.* = .{
            .vector_id = @intCast(target_row + 1),
            .vector = dataset[source_row * cfg.dims ..][0..cfg.dims],
            .metadata = try allocator.dupe(u8, ""),
        };
        initialized += 1;
    }
    return items;
}

fn applyOverwriteItemsToDataset(cfg: Config, dataset: []f32, items: []const hbc.BatchInsertItem) void {
    for (items) |item| {
        if (item.vector_id == 0) continue;
        const row: usize = @intCast(item.vector_id - 1);
        if (row >= cfg.vectors) continue;
        @memcpy(dataset[row * cfg.dims ..][0..cfg.dims], item.vector);
    }
}

fn writeSemanticDriftVector(cfg: Config, dataset: []const f32, row: usize, step: usize, dst: []f32) void {
    const source = dataset[row * cfg.dims ..][0..cfg.dims];
    const neighbor = dataset[((row + 1 + (step % @max(cfg.vectors - 1, @as(usize, 1)))) % cfg.vectors) * cfg.dims ..][0..cfg.dims];
    const alpha = 0.08 + 0.02 * @as(f32, @floatFromInt(step % 5));
    for (dst, 0..) |*value, dim| {
        value.* = source[dim] * (1.0 - alpha) + neighbor[dim] * alpha;
    }
    _ = vec.normalize(dst);
}

fn writeSyntheticAppendVector(cfg: Config, ordinal: usize, dst: []f32) void {
    const cluster = ordinal % 16;
    const base = @as(f32, @floatFromInt(cluster)) * 0.10;
    for (dst, 0..) |*value, dim| {
        const n = ((ordinal + 1) * (dim + 3) * 1103515245 + cfg.seed + 12345) & 0xffff;
        value.* = base + @as(f32, @floatFromInt(n)) / 65535.0 * 0.01;
    }
    _ = vec.normalize(dst);
}

fn makeHotProceduralOverwriteSet(allocator: Allocator, cfg: Config) !ProceduralMutationSet {
    if (cfg.vectors < 2) return error.InvalidArgument;
    const hot_keys = if (cfg.overwrite_hot_keys == 0)
        @min(cfg.batch_size, cfg.vectors)
    else
        @min(cfg.overwrite_hot_keys, cfg.vectors);
    if (hot_keys == 0 or cfg.overwrite_rounds == 0) return error.InvalidArgument;

    const count = hot_keys * cfg.overwrite_rounds;
    var set = try ProceduralMutationSet.init(allocator, cfg, cfg.vectors, count, 0);
    errdefer set.deinit();
    for (0..count) |i| {
        const target_row = i % hot_keys;
        var source_row = (hot_keys + i) % cfg.vectors;
        if (source_row == target_row) source_row = (source_row + 1) % cfg.vectors;
        const vector = try set.putWrite(cfg, i, target_row, "");
        writeProceduralDatasetVector(cfg.seed, source_row, vector);
    }
    return set;
}

fn makeRandomProceduralOverwriteSet(allocator: Allocator, cfg: Config) !ProceduralMutationSet {
    if (cfg.vectors < 2) return error.InvalidArgument;
    const keys_per_round = if (cfg.overwrite_hot_keys == 0)
        @min(cfg.batch_size, cfg.vectors)
    else
        @min(cfg.overwrite_hot_keys, cfg.vectors);
    if (keys_per_round == 0 or cfg.overwrite_rounds == 0) return error.InvalidArgument;

    const count = keys_per_round * cfg.overwrite_rounds;
    var set = try ProceduralMutationSet.init(allocator, cfg, cfg.vectors, count, 0);
    errdefer set.deinit();
    for (0..count) |i| {
        const round = i / keys_per_round;
        const slot = i % keys_per_round;
        const round_offset = @as(usize, @intCast((cfg.seed + @as(u64, @intCast(round)) *% 7919) % @as(u64, @intCast(cfg.vectors))));
        const target_row = (round_offset + slot * (cfg.vectors - 1)) % cfg.vectors;
        var source_row = (target_row + 1 + ((round * 17 + slot * 31) % (cfg.vectors - 1))) % cfg.vectors;
        if (source_row == target_row) source_row = (source_row + 1) % cfg.vectors;
        const vector = try set.putWrite(cfg, i, target_row, "");
        writeProceduralDatasetVector(cfg.seed, source_row, vector);
    }
    return set;
}

fn makeSemanticDriftProceduralOverwriteSet(allocator: Allocator, cfg: Config) !ProceduralMutationSet {
    if (cfg.vectors < 2) return error.InvalidArgument;
    const keys_per_round = if (cfg.overwrite_hot_keys == 0)
        @min(cfg.batch_size, cfg.vectors)
    else
        @min(cfg.overwrite_hot_keys, cfg.vectors);
    if (keys_per_round == 0 or cfg.overwrite_rounds == 0) return error.InvalidArgument;

    const count = keys_per_round * cfg.overwrite_rounds;
    var set = try ProceduralMutationSet.init(allocator, cfg, cfg.vectors, count, 0);
    errdefer set.deinit();
    for (0..count) |i| {
        const target_row = i % keys_per_round;
        var source_row = (target_row + 16 + (i / keys_per_round)) % cfg.vectors;
        if (source_row == target_row) source_row = (source_row + 1) % cfg.vectors;
        const vector = try set.putWrite(cfg, i, target_row, "");
        writeProceduralDatasetVector(cfg.seed, source_row, vector);
    }
    return set;
}

fn makeAppendStreamingProceduralSet(allocator: Allocator, cfg: Config) !ProceduralMutationSet {
    if (cfg.vectors < 1 or cfg.overwrite_rounds == 0) return error.InvalidArgument;
    const appends_per_round = if (cfg.overwrite_hot_keys == 0)
        @min(cfg.batch_size, cfg.vectors)
    else
        cfg.overwrite_hot_keys;
    if (appends_per_round == 0) return error.InvalidArgument;

    const append_count = appends_per_round * cfg.overwrite_rounds;
    var set = try ProceduralMutationSet.init(allocator, cfg, cfg.vectors + append_count, append_count, 0);
    errdefer set.deinit();
    for (0..append_count) |i| {
        const row = cfg.vectors + i;
        const vector = try set.putWrite(cfg, i, row, "");
        writeSyntheticAppendVector(cfg, i, vector);
    }
    return set;
}

fn makeMixedProceduralMutationSet(allocator: Allocator, cfg: Config) !ProceduralMutationSet {
    if (cfg.vectors < 4 or cfg.overwrite_rounds == 0) return error.InvalidArgument;
    const mutations_per_round = if (cfg.overwrite_hot_keys == 0)
        @min(cfg.batch_size, cfg.vectors)
    else
        @min(cfg.overwrite_hot_keys, cfg.vectors);
    if (mutations_per_round == 0) return error.InvalidArgument;

    const mutation_count = mutations_per_round * cfg.overwrite_rounds;
    const delete_count = @max(@divFloor(mutation_count, 4), @as(usize, 1));
    const append_count = @max(@divFloor(mutation_count, 4), @as(usize, 1));
    const update_count = mutation_count - delete_count - append_count;
    var set = try ProceduralMutationSet.init(allocator, cfg, cfg.vectors + append_count, update_count + append_count, delete_count);
    errdefer set.deinit();

    for (0..delete_count) |i| {
        const row = (i * 7) % cfg.vectors;
        set.deletes[i] = @intCast(row + 1);
        set.active_rows[row] = false;
    }

    var scratch = try allocator.alloc(f32, cfg.dims * 2);
    defer allocator.free(scratch);
    for (0..update_count) |i| {
        var row = (i * 5 + 1) % cfg.vectors;
        while (!set.active_rows[row]) row = (row + 1) % cfg.vectors;
        const vector = try set.putWrite(cfg, i, row, "");
        writeProceduralDatasetVector(cfg.seed, row, scratch[0..cfg.dims]);
        writeProceduralDatasetVector(cfg.seed, (row + 1 + (i % @max(cfg.vectors - 1, @as(usize, 1)))) % cfg.vectors, scratch[cfg.dims..][0..cfg.dims]);
        const alpha = 0.08 + 0.02 * @as(f32, @floatFromInt(i % 5));
        for (vector, 0..) |*value, dim| {
            value.* = scratch[dim] * (1.0 - alpha) + scratch[cfg.dims + dim] * alpha;
        }
        _ = vec.normalize(vector);
    }

    for (0..append_count) |i| {
        const slot = update_count + i;
        const row = cfg.vectors + i;
        const vector = try set.putWrite(cfg, slot, row, "");
        writeSyntheticAppendVector(cfg, i, vector);
    }
    return set;
}

const RecallHits = struct {
    hits: u64 = 0,
    total: u64 = 0,
};

const ExactTruthCache = struct {
    rows: []usize = &.{},
    ids: []u64 = &.{},
    truth_count: usize = 0,

    fn deinit(self: *ExactTruthCache, allocator: Allocator) void {
        allocator.free(self.rows);
        allocator.free(self.ids);
        self.* = .{};
    }

    fn lookup(self: *const ExactTruthCache, row: usize) ?[]const u64 {
        for (self.rows, 0..) |cached_row, index| {
            if (cached_row == row) {
                const start = index * self.truth_count;
                return self.ids[start..][0..self.truth_count];
            }
        }
        return null;
    }
};

const ExactTruthCacheLoadResult = struct {
    cache: ExactTruthCache,
    ns: u64,
    cache_hit: bool,
};

const post_write_truth_cache_magic: [8]u8 = .{ 'H', 'B', 'C', 'W', 'T', 'R', '1', 0 };
const post_write_truth_cache_header_len = post_write_truth_cache_magic.len + 11 * @sizeOf(u64);

fn hashCacheU64(hasher: *std.hash.Wyhash, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hasher.update(&bytes);
}

fn hashCacheBool(hasher: *std.hash.Wyhash, value: bool) void {
    hasher.update(if (value) "\x01" else "\x00");
}

fn postWriteTruthDatasetHash(
    vector_count: usize,
    active_rows: ?[]const bool,
    external_vector_dataset: *const ExternalVectorDataset,
) u64 {
    var hasher = std.hash.Wyhash.init(0x5350_4652_4553_4857);
    hashCacheU64(&hasher, @intCast(vector_count));
    hashCacheU64(&hasher, @intCast(external_vector_dataset.dims));
    hashCacheU64(&hasher, @intCast(@intFromEnum(external_vector_dataset.mode)));
    if (active_rows) |active| {
        hashCacheU64(&hasher, @intCast(active.len));
        for (0..vector_count) |row| {
            hashCacheBool(&hasher, row < active.len and active[row]);
        }
    } else {
        hashCacheU64(&hasher, 0);
    }
    hashCacheU64(&hasher, @intCast(external_vector_dataset.override_slots.len));
    hashCacheU64(&hasher, @intCast(external_vector_dataset.override_vectors.len));
    for (0..vector_count) |row| {
        const slot = if (row < external_vector_dataset.override_slots.len)
            external_vector_dataset.override_slots[row]
        else
            std.math.maxInt(usize);
        hashCacheU64(&hasher, @intCast(slot));
    }
    const override_bytes = std.mem.sliceAsBytes(external_vector_dataset.override_vectors);
    hashCacheU64(&hasher, @intCast(override_bytes.len));
    hasher.update(override_bytes);
    return hasher.final();
}

fn buildGeneratedTruthCache(
    allocator: Allocator,
    cfg: Config,
    metric: vec.DistanceMetric,
    external_vector_dataset: *const ExternalVectorDataset,
    vector_count: usize,
    active_rows: ?[]const bool,
    query: []f32,
    candidate: []f32,
) !ExactTruthCache {
    const candidate_count = liveRowCount(vector_count, active_rows);
    const truth_count = @min(cfg.post_write_k, candidate_count);
    if (truth_count == 0) return .{};

    var query_count: usize = 0;
    while (query_count < cfg.post_write_queries) : (query_count += 1) {
        if (nthLiveRow(vector_count, active_rows, query_count) == null) break;
    }
    if (query_count == 0) return .{};

    const rows = try allocator.alloc(usize, query_count);
    errdefer allocator.free(rows);
    const ids = try allocator.alloc(u64, query_count * truth_count);
    errdefer allocator.free(ids);

    const candidate_norms = if (metric == .cosine)
        try buildGeneratedCandidateNorms(allocator, cfg.seed, external_vector_dataset, vector_count, cfg.dims, candidate)
    else
        null;
    defer if (candidate_norms) |norms| allocator.free(norms);

    for (rows, 0..) |*cached_row, i| {
        const row = nthLiveRow(vector_count, active_rows, i) orelse unreachable;
        cached_row.* = row;
        try external_vector_dataset.copyVector(@intCast(row + 1), query);
        const truth_ids = try exactTruthIdsGenerated(
            allocator,
            cfg.seed,
            external_vector_dataset,
            vector_count,
            cfg.dims,
            metric,
            query,
            candidate,
            cfg.post_write_k,
            active_rows,
            candidate_norms,
        );
        std.debug.assert(truth_ids.len == truth_count);
        @memcpy(ids[i * truth_count ..][0..truth_count], truth_ids);
        allocator.free(truth_ids);
    }
    return .{ .rows = rows, .ids = ids, .truth_count = truth_count };
}

fn buildGeneratedCandidateNorms(
    allocator: Allocator,
    seed: u64,
    external_vector_dataset: *const ExternalVectorDataset,
    vector_count: usize,
    dims: usize,
    candidate: []f32,
) ![]f32 {
    const norms = try allocator.alloc(f32, vector_count);
    errdefer allocator.free(norms);
    for (norms, 0..) |*norm, row| {
        if (external_vector_dataset.hasOverride(row)) {
            try external_vector_dataset.copyVector(@intCast(row + 1), candidate[0..dims]);
            norm.* = vectorNormSqScalar(candidate[0..dims]);
        } else {
            norm.* = proceduralCandidateNormSq(seed, row, dims);
        }
    }
    return norms;
}

fn buildMaterializedTruthCache(
    scenario: *Scenario,
    dataset: []const f32,
    active_rows: ?[]const bool,
) !ExactTruthCache {
    if (scenario.cfg.dims == 0) return .{};
    const vector_count = dataset.len / scenario.cfg.dims;
    const candidate_count = liveRowCount(vector_count, active_rows);
    const truth_count = @min(scenario.cfg.post_write_k, candidate_count);
    if (truth_count == 0) return .{};

    var query_count: usize = 0;
    while (query_count < scenario.cfg.post_write_queries) : (query_count += 1) {
        if (nthLiveRow(vector_count, active_rows, query_count) == null) break;
    }
    if (query_count == 0) return .{};

    const rows = try scenario.allocator.alloc(usize, query_count);
    errdefer scenario.allocator.free(rows);
    const ids = try scenario.allocator.alloc(u64, query_count * truth_count);
    errdefer scenario.allocator.free(ids);

    for (rows, 0..) |*cached_row, i| {
        const row = nthLiveRow(vector_count, active_rows, i) orelse unreachable;
        cached_row.* = row;
        const query = dataset[row * scenario.cfg.dims ..][0..scenario.cfg.dims];
        const truth_ids = try exactTruthIdsMaterialized(
            scenario.allocator,
            dataset,
            scenario.cfg.dims,
            scenario.index.config.metric,
            query,
            scenario.cfg.post_write_k,
            active_rows,
        );
        std.debug.assert(truth_ids.len == truth_count);
        @memcpy(ids[i * truth_count ..][0..truth_count], truth_ids);
        scenario.allocator.free(truth_ids);
    }
    return .{ .rows = rows, .ids = ids, .truth_count = truth_count };
}

fn loadOrBuildGeneratedTruthCache(
    io: std.Io,
    allocator: Allocator,
    cfg: Config,
    metric: vec.DistanceMetric,
    external_vector_dataset: *const ExternalVectorDataset,
    vector_count: usize,
    active_rows: ?[]const bool,
    query: []f32,
    candidate: []f32,
) !ExactTruthCacheLoadResult {
    const data_hash = postWriteTruthDatasetHash(vector_count, active_rows, external_vector_dataset);
    if (cfg.post_write_truth_cache_path) |path| {
        const load_start = nanotime();
        if (loadGeneratedTruthCacheFile(io, allocator, cfg, metric, vector_count, active_rows, data_hash, path)) |cache| {
            return .{
                .cache = cache,
                .ns = nanotime() - load_start,
                .cache_hit = true,
            };
        } else |_| {}
    }

    const build_start = nanotime();
    const cache = try buildGeneratedTruthCache(allocator, cfg, metric, external_vector_dataset, vector_count, active_rows, query, candidate);
    const build_ns = nanotime() - build_start;
    if (cfg.post_write_truth_cache_path) |path| {
        writeGeneratedTruthCacheFile(io, allocator, cfg, metric, vector_count, active_rows, data_hash, path, &cache) catch |err| {
            std.debug.print("hbc write bench post_write_truth_cache_write_failed path={s} err={s}\n", .{ path, @errorName(err) });
        };
    }
    return .{
        .cache = cache,
        .ns = build_ns,
        .cache_hit = false,
    };
}

fn loadGeneratedTruthCacheFile(
    io: std.Io,
    allocator: Allocator,
    cfg: Config,
    metric: vec.DistanceMetric,
    vector_count: usize,
    active_rows: ?[]const bool,
    data_hash: u64,
    path: []const u8,
) !ExactTruthCache {
    const expected_bytes = postWriteTruthCacheExpectedBytes(cfg, vector_count, active_rows);
    const bytes = try readFileAllocPath(io, allocator, path, expected_bytes);
    defer allocator.free(bytes);
    if (bytes.len != expected_bytes) return error.InvalidTruthCache;
    if (!std.mem.eql(u8, bytes[0..post_write_truth_cache_magic.len], &post_write_truth_cache_magic)) return error.InvalidTruthCache;
    var pos: usize = post_write_truth_cache_magic.len;
    const seed = readCacheU64(bytes, &pos);
    const vectors = readCacheU64(bytes, &pos);
    const dims = readCacheU64(bytes, &pos);
    const queries = readCacheU64(bytes, &pos);
    const k = readCacheU64(bytes, &pos);
    const dataset_mode = readCacheU64(bytes, &pos);
    const stored_metric = readCacheU64(bytes, &pos);
    const live_count = readCacheU64(bytes, &pos);
    const stored_data_hash = readCacheU64(bytes, &pos);
    const truth_count = readCacheU64(bytes, &pos);
    const row_count = readCacheU64(bytes, &pos);
    const expected_live_count = liveRowCount(vector_count, active_rows);
    const expected_row_count = postWriteTruthCacheRowCount(cfg, vector_count, active_rows);
    const expected_truth_count = @min(cfg.post_write_k, expected_live_count);
    if (seed != cfg.seed or
        vectors != vector_count or
        dims != cfg.dims or
        queries != cfg.post_write_queries or
        k != cfg.post_write_k or
        dataset_mode != @as(u64, @intCast(@intFromEnum(cfg.dataset_mode))) or
        stored_metric != @as(u64, @intCast(@intFromEnum(metric))) or
        live_count != expected_live_count or
        stored_data_hash != data_hash or
        truth_count != expected_truth_count or
        row_count != expected_row_count)
    {
        return error.InvalidTruthCache;
    }

    const rows = try allocator.alloc(usize, @intCast(row_count));
    errdefer allocator.free(rows);
    for (rows) |*row| {
        row.* = @intCast(readCacheU64(bytes, &pos));
    }
    const ids = try allocator.alloc(u64, @as(usize, @intCast(row_count)) * @as(usize, @intCast(truth_count)));
    errdefer allocator.free(ids);
    for (ids) |*id| {
        id.* = readCacheU64(bytes, &pos);
    }
    return .{
        .rows = rows,
        .ids = ids,
        .truth_count = @intCast(truth_count),
    };
}

fn writeGeneratedTruthCacheFile(
    io: std.Io,
    allocator: Allocator,
    cfg: Config,
    metric: vec.DistanceMetric,
    vector_count: usize,
    active_rows: ?[]const bool,
    data_hash: u64,
    path: []const u8,
    cache: *const ExactTruthCache,
) !void {
    const expected_bytes = postWriteTruthCacheExpectedBytes(cfg, vector_count, active_rows);
    var bytes = try allocator.alloc(u8, expected_bytes);
    defer allocator.free(bytes);
    @memcpy(bytes[0..post_write_truth_cache_magic.len], &post_write_truth_cache_magic);
    var pos: usize = post_write_truth_cache_magic.len;
    writeCacheU64(bytes, &pos, cfg.seed);
    writeCacheU64(bytes, &pos, @intCast(vector_count));
    writeCacheU64(bytes, &pos, @intCast(cfg.dims));
    writeCacheU64(bytes, &pos, @intCast(cfg.post_write_queries));
    writeCacheU64(bytes, &pos, @intCast(cfg.post_write_k));
    writeCacheU64(bytes, &pos, @intCast(@intFromEnum(cfg.dataset_mode)));
    writeCacheU64(bytes, &pos, @intCast(@intFromEnum(metric)));
    writeCacheU64(bytes, &pos, @intCast(liveRowCount(vector_count, active_rows)));
    writeCacheU64(bytes, &pos, data_hash);
    writeCacheU64(bytes, &pos, @intCast(cache.truth_count));
    writeCacheU64(bytes, &pos, @intCast(cache.rows.len));
    for (cache.rows) |row| {
        writeCacheU64(bytes, &pos, @intCast(row));
    }
    for (cache.ids) |id| {
        writeCacheU64(bytes, &pos, id);
    }
    std.debug.assert(pos == bytes.len);
    try writeFilePath(io, path, bytes);
}

fn postWriteTruthCacheRowCount(cfg: Config, vector_count: usize, active_rows: ?[]const bool) usize {
    var query_count: usize = 0;
    while (query_count < cfg.post_write_queries) : (query_count += 1) {
        if (nthLiveRow(vector_count, active_rows, query_count) == null) break;
    }
    return query_count;
}

fn postWriteTruthCacheExpectedBytes(cfg: Config, vector_count: usize, active_rows: ?[]const bool) usize {
    const live_count = liveRowCount(vector_count, active_rows);
    const truth_count = @min(cfg.post_write_k, live_count);
    const row_count = postWriteTruthCacheRowCount(cfg, vector_count, active_rows);
    return post_write_truth_cache_header_len + row_count * @sizeOf(u64) + row_count * truth_count * @sizeOf(u64);
}

fn readCacheU64(bytes: []const u8, pos: *usize) u64 {
    const value = std.mem.readInt(u64, bytes[pos.*..][0..8], .little);
    pos.* += 8;
    return value;
}

fn writeCacheU64(bytes: []u8, pos: *usize, value: u64) void {
    std.mem.writeInt(u64, bytes[pos.*..][0..8], value, .little);
    pos.* += 8;
}

fn readFileAllocPath(io: std.Io, allocator: Allocator, path: []const u8, expected_bytes: usize) ![]u8 {
    if (std.fs.path.isAbsolute(path)) {
        var file = try std.Io.Dir.openFileAbsolute(io, path, .{});
        defer file.close(io);
        var reader = file.reader(io, &.{});
        return try reader.interface.allocRemaining(allocator, .limited(expected_bytes + 1));
    }
    return try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(expected_bytes + 1));
}

fn writeFilePath(io: std.Io, path: []const u8, bytes: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| {
        if (!std.fs.path.isAbsolute(parent)) try std.Io.Dir.cwd().createDirPath(io, parent);
    }
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.createFileAbsolute(io, path, .{ .truncate = true })
    else
        try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
    defer file.close(io);
    var buf: [4096]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.writeAll(bytes);
    try writer.end();
    try file.sync(io);
}

fn exactRecallHits(
    allocator: Allocator,
    dataset: []const f32,
    dims: usize,
    metric: vec.DistanceMetric,
    query: []const f32,
    hits: anytype,
    top_k: usize,
    active_rows: ?[]const bool,
) !RecallHits {
    const truth_ids = try exactTruthIdsMaterialized(allocator, dataset, dims, metric, query, top_k, active_rows);
    defer allocator.free(truth_ids);
    return recallHitsFromTruth(truth_ids, hits);
}

fn exactTruthIdsMaterialized(
    allocator: Allocator,
    dataset: []const f32,
    dims: usize,
    metric: vec.DistanceMetric,
    query: []const f32,
    top_k: usize,
    active_rows: ?[]const bool,
) ![]u64 {
    if (dims == 0 or top_k == 0) return try allocator.alloc(u64, 0);
    const vector_count = dataset.len / dims;
    const candidate_count = liveRowCount(vector_count, active_rows);
    const truth_count = @min(top_k, candidate_count);
    if (truth_count == 0) return try allocator.alloc(u64, 0);

    const truth_ids = try allocator.alloc(u64, truth_count);
    errdefer allocator.free(truth_ids);
    const truth_distances = try allocator.alloc(f32, truth_count);
    defer allocator.free(truth_distances);
    @memset(truth_ids, std.math.maxInt(u64));
    @memset(truth_distances, std.math.inf(f32));

    for (0..vector_count) |row| {
        if (active_rows) |active| {
            if (row >= active.len or !active[row]) continue;
        }
        const vector_id: u64 = @intCast(row + 1);
        const distance = vec.distance(query, dataset[row * dims ..][0..dims], metric);
        var pos: usize = 0;
        while (pos < truth_count) : (pos += 1) {
            if (!betterTruthCandidate(distance, vector_id, truth_distances[pos], truth_ids[pos])) continue;
            var shift = truth_count - 1;
            while (shift > pos) : (shift -= 1) {
                truth_distances[shift] = truth_distances[shift - 1];
                truth_ids[shift] = truth_ids[shift - 1];
            }
            truth_distances[pos] = distance;
            truth_ids[pos] = vector_id;
            break;
        }
    }
    return truth_ids;
}

fn exactRecallHitsGenerated(
    allocator: Allocator,
    seed: u64,
    external_vector_dataset: ?*const ExternalVectorDataset,
    vector_count: usize,
    dims: usize,
    metric: vec.DistanceMetric,
    query: []const f32,
    candidate: []f32,
    hits: anytype,
    top_k: usize,
    active_rows: ?[]const bool,
) !RecallHits {
    const truth_ids = try exactTruthIdsGenerated(allocator, seed, external_vector_dataset, vector_count, dims, metric, query, candidate, top_k, active_rows, null);
    defer allocator.free(truth_ids);
    return recallHitsFromTruth(truth_ids, hits);
}

fn exactTruthIdsGenerated(
    allocator: Allocator,
    seed: u64,
    external_vector_dataset: ?*const ExternalVectorDataset,
    vector_count: usize,
    dims: usize,
    metric: vec.DistanceMetric,
    query: []const f32,
    candidate: []f32,
    top_k: usize,
    active_rows: ?[]const bool,
    candidate_norms: ?[]const f32,
) ![]u64 {
    if (dims == 0 or top_k == 0) return try allocator.alloc(u64, 0);
    const candidate_count = liveRowCount(vector_count, active_rows);
    const truth_count = @min(top_k, candidate_count);
    if (truth_count == 0) return try allocator.alloc(u64, 0);

    const truth_ids = try allocator.alloc(u64, truth_count);
    errdefer allocator.free(truth_ids);
    const truth_distances = try allocator.alloc(f32, truth_count);
    defer allocator.free(truth_distances);
    @memset(truth_ids, std.math.maxInt(u64));
    @memset(truth_distances, std.math.inf(f32));
    const procedural_query_norm_sq = if (metric == .cosine)
        vectorNormSqScalar(query)
    else
        0;

    for (0..vector_count) |row| {
        if (active_rows) |active| {
            if (row >= active.len or !active[row]) continue;
        }
        const vector_id: u64 = @intCast(row + 1);
        const can_use_procedural_fast_path = metric == .cosine and (external_vector_dataset == null or !external_vector_dataset.?.hasOverride(row));
        const distance = if (can_use_procedural_fast_path) blk: {
            const procedural_candidate_norm_sq = if (candidate_norms) |norms|
                norms[row]
            else
                proceduralCandidateNormSq(seed, row, dims);
            break :blk proceduralCosineDistanceToQueryWithNorms(seed, row, query, procedural_query_norm_sq, procedural_candidate_norm_sq);
        } else blk: {
            if (external_vector_dataset) |dataset| {
                try dataset.copyVector(vector_id, candidate[0..dims]);
            } else {
                writeProceduralDatasetVector(seed, row, candidate[0..dims]);
            }
            break :blk vec.distance(query, candidate[0..dims], metric);
        };
        var pos: usize = 0;
        while (pos < truth_count) : (pos += 1) {
            if (!betterTruthCandidate(distance, vector_id, truth_distances[pos], truth_ids[pos])) continue;
            var shift = truth_count - 1;
            while (shift > pos) : (shift -= 1) {
                truth_distances[shift] = truth_distances[shift - 1];
                truth_ids[shift] = truth_ids[shift - 1];
            }
            truth_distances[pos] = distance;
            truth_ids[pos] = vector_id;
            break;
        }
    }
    return truth_ids;
}

fn recallHitsFromTruth(truth_ids: []const u64, hits: anytype) RecallHits {
    var recall_hits: u64 = 0;
    for (truth_ids) |truth_id| {
        for (hits) |hit| {
            if (hit.vector_id == truth_id) {
                recall_hits += 1;
                break;
            }
        }
    }
    return .{
        .hits = recall_hits,
        .total = @intCast(truth_ids.len),
    };
}

fn selfHitRecall(row: usize, hits: anytype) RecallHits {
    const expected_id: u64 = @intCast(row + 1);
    for (hits) |hit| {
        if (hit.vector_id == expected_id) {
            return .{ .hits = 1, .total = 1 };
        }
    }
    return .{ .hits = 0, .total = 1 };
}

fn liveRowCount(vector_count: usize, active_rows: ?[]const bool) usize {
    if (active_rows == null) return vector_count;
    var count: usize = 0;
    for (0..@min(vector_count, active_rows.?.len)) |row| {
        if (active_rows.?[row]) count += 1;
    }
    return count;
}

fn betterTruthCandidate(candidate_distance: f32, candidate_id: u64, current_distance: f32, current_id: u64) bool {
    if (candidate_distance != current_distance) return candidate_distance < current_distance;
    return candidate_id < current_id;
}

fn freeItems(allocator: Allocator, items: []hbc.BatchInsertItem) void {
    for (items) |item| allocator.free(item.metadata);
    allocator.free(items);
}

fn parseArgs(allocator: Allocator, proc_args: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(proc_args);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--inspect-root")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.inspect_root = try allocator.dupe(u8, value);
        } else if (std.mem.eql(u8, arg, "--maintenance-steps")) {
            cfg.inspect_maintenance_steps = try parseNextUsize(&args, arg);
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
        } else if (std.mem.eql(u8, arg, "--search-width")) {
            cfg.search_width = @intCast(try parseNextUsize(&args, arg));
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
        } else if (std.mem.eql(u8, arg, "--centroid-directory")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.centroid_directory_mode = std.meta.stringToEnum(hbc.HBCConfig.CentroidDirectoryMode, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--posting-storage")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.posting_storage_mode = std.meta.stringToEnum(hbc.HBCConfig.PostingStorageMode, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--posting-base-member-block-size")) {
            cfg.posting_base_member_block_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--flat-centroid-block-size")) {
            cfg.flat_centroid_block_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--flat-centroid-probe-count")) {
            cfg.flat_centroid_probe_count = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--flat-centroid-block-probe-count")) {
            cfg.flat_centroid_block_probe_count = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-posting-overlay-cache-bytes")) {
            cfg.max_posting_overlay_cache_bytes = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-posting-overlay-cache-entry-bytes")) {
            cfg.max_posting_overlay_cache_entry_bytes = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--workload")) {
            cfg.workload_filter = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--lazy-posting-maintenance")) {
            cfg.lazy_posting_maintenance = true;
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-max-postings")) {
            cfg.auto_posting_maintenance_max_postings = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-no-fold-delta-tails")) {
            cfg.auto_posting_maintenance_fold_delta_tails = false;
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-min-delta-records-to-fold")) {
            cfg.auto_posting_maintenance_min_delta_records_to_fold = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-min-tombstone-records-to-fold")) {
            cfg.auto_posting_maintenance_min_tombstone_records_to_fold = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-min-delta-to-base-ratio-bps")) {
            cfg.auto_posting_maintenance_min_delta_to_base_ratio_bps = @intCast(try parseNextUsize(&args, arg));
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-max-delta-tail-postings")) {
            cfg.auto_posting_maintenance_max_delta_tail_postings = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-min-dirty-postings")) {
            cfg.auto_posting_maintenance_min_dirty_postings = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-max-dirty-version-age")) {
            cfg.auto_posting_maintenance_max_dirty_version_age = @intCast(try parseNextUsize(&args, arg));
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-min-delta-records-to-run")) {
            cfg.auto_posting_maintenance_min_delta_records_to_run = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-min-tombstone-records-to-run")) {
            cfg.auto_posting_maintenance_min_tombstone_records_to_run = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-min-delta-to-base-ratio-bps-to-run")) {
            cfg.auto_posting_maintenance_min_delta_to_base_ratio_bps_to_run = @intCast(try parseNextUsize(&args, arg));
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-min-centroid-version-lag")) {
            cfg.auto_posting_maintenance_min_centroid_version_lag = @intCast(try parseNextUsize(&args, arg));
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-min-payload-version-lag")) {
            cfg.auto_posting_maintenance_min_payload_version_lag = @intCast(try parseNextUsize(&args, arg));
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-max-layout-changes")) {
            cfg.auto_posting_maintenance_max_layout_changes = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-split-full-postings")) {
            cfg.auto_posting_maintenance_split_full_postings = true;
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-min-overfull-postings-to-run")) {
            cfg.auto_posting_maintenance_min_overfull_postings_to_run = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-min-postings-at-capacity-to-run")) {
            cfg.auto_posting_maintenance_min_postings_at_capacity_to_run = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-max-boundary-reassignments")) {
            cfg.auto_posting_maintenance_max_boundary_reassignments = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-allow-overfull-reassignment")) {
            cfg.auto_posting_maintenance_allow_overfull_reassignment = true;
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-max-overfull-reassignment-postings")) {
            cfg.auto_posting_maintenance_max_overfull_reassignment_postings = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-max-over-capacity-reassignment-members")) {
            cfg.auto_posting_maintenance_max_over_capacity_reassignment_members = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--auto-posting-maintenance-boundary-reassignment-min-improvement")) {
            cfg.auto_posting_maintenance_boundary_reassignment_min_improvement = try parseNextF32(&args, arg);
        } else if (std.mem.eql(u8, arg, "--repair-postings-after-write")) {
            cfg.repair_postings_after_write = true;
        } else if (std.mem.eql(u8, arg, "--repair-postings-before-bulk-finish")) {
            cfg.repair_postings_before_bulk_finish = true;
        } else if (std.mem.eql(u8, arg, "--repair-no-fold-delta-tails")) {
            cfg.repair_fold_delta_tails = false;
        } else if (std.mem.eql(u8, arg, "--repair-min-delta-records-to-fold")) {
            cfg.repair_min_delta_records_to_fold = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--repair-min-tombstone-records-to-fold")) {
            cfg.repair_min_tombstone_records_to_fold = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--repair-min-delta-to-base-ratio-bps")) {
            cfg.repair_min_delta_to_base_ratio_bps = @intCast(try parseNextUsize(&args, arg));
        } else if (std.mem.eql(u8, arg, "--repair-max-delta-tail-postings")) {
            cfg.repair_max_delta_tail_postings = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--repair-dirty-reassignments")) {
            cfg.repair_dirty_reassignments = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--repair-rebalance-layout")) {
            cfg.repair_rebalance_layout = true;
        } else if (std.mem.eql(u8, arg, "--repair-split-full-postings")) {
            cfg.repair_split_full_postings = true;
        } else if (std.mem.eql(u8, arg, "--repair-max-layout-changes")) {
            cfg.repair_max_layout_changes = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--repair-dirty-reassignment-allow-overfull")) {
            cfg.repair_dirty_reassignment_allow_overfull = true;
        } else if (std.mem.eql(u8, arg, "--repair-dirty-reassignment-max-overfull-postings")) {
            cfg.repair_dirty_reassignment_max_overfull_postings = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--repair-dirty-reassignment-max-over-capacity-members")) {
            cfg.repair_dirty_reassignment_max_over_capacity_members = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--repair-dirty-reassignment-min-improvement")) {
            cfg.repair_dirty_reassignment_min_improvement = try parseNextF32(&args, arg);
        } else if (std.mem.eql(u8, arg, "--defer-leaf-splits-to-posting-maintenance")) {
            cfg.defer_leaf_splits_to_posting_maintenance = true;
        } else if (std.mem.eql(u8, arg, "--bulk-ingest-finish-compact")) {
            cfg.bulk_ingest_finish_compact = true;
        } else if (std.mem.eql(u8, arg, "--bulk-ingest-finish-max-deferred-l0-runs")) {
            cfg.bulk_ingest_finish_max_deferred_l0_runs = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--bulk-ingest-finish-max-foreground-compaction-steps")) {
            cfg.bulk_ingest_finish_max_foreground_compaction_steps = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--no-coalesce-overwrite-leaf-writes")) {
            cfg.coalesce_overwrite_leaf_writes = false;
        } else if (std.mem.eql(u8, arg, "--skip-vector-store")) {
            cfg.skip_vector_store = true;
        } else if (std.mem.eql(u8, arg, "--overwrite-hot-keys")) {
            cfg.overwrite_hot_keys = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--overwrite-rounds")) {
            cfg.overwrite_rounds = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--post-write-queries")) {
            cfg.post_write_queries = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--post-write-query-rounds")) {
            cfg.post_write_query_rounds = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--post-write-k")) {
            cfg.post_write_k = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--post-write-recall-mode")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.post_write_recall_mode = std.meta.stringToEnum(PostWriteRecallMode, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--post-write-truth-cache-path")) {
            cfg.post_write_truth_cache_path = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--post-write-truth-cache-only")) {
            cfg.post_write_truth_cache_only = true;
        } else if (std.mem.eql(u8, arg, "--dataset-mode")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.dataset_mode = std.meta.stringToEnum(DatasetMode, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--no-quantization")) {
            cfg.use_quantization = false;
        } else if (std.mem.eql(u8, arg, "--random-ortho")) {
            cfg.use_random_ortho_trans = true;
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.samples == 0 or cfg.vectors == 0 or cfg.dims == 0 or cfg.batch_size == 0 or cfg.post_write_query_rounds == 0) return error.InvalidArgument;
    if (cfg.post_write_truth_cache_only and
        (cfg.dataset_mode != .procedural or
            cfg.post_write_recall_mode != .exact or
            cfg.post_write_queries == 0 or
            cfg.post_write_truth_cache_path == null))
    {
        return error.InvalidArgument;
    }
    if (cfg.dataset_mode == .procedural) {
        if (cfg.workload_filter == null) return error.InvalidArgument;
        const workload = cfg.workload_filter.?;
        if (!std.mem.eql(u8, workload, "bulk_build_external_vectors_sequential_empty") and
            !std.mem.eql(u8, workload, "online_batches_dense_external_vectors_empty") and
            !std.mem.eql(u8, workload, "online_batches_dense_external_vectors_per_batch_session_empty") and
            !std.mem.eql(u8, workload, "overwrite_hot_vectors_warm") and
            !std.mem.eql(u8, workload, "overwrite_random_vectors_warm") and
            !std.mem.eql(u8, workload, "overwrite_semantic_drift_vectors_warm") and
            !std.mem.eql(u8, workload, "append_streaming_warm") and
            !std.mem.eql(u8, workload, "mixed_insert_delete_update_warm"))
            return error.InvalidArgument;
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

fn parseNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const value = args.next() orelse return error.InvalidArgument;
    return std.fmt.parseInt(u64, value, 10) catch {
        std.debug.print("invalid value for {s}: {s}\n", .{ flag, value });
        return error.InvalidArgument;
    };
}

fn parseNextF32(args: *std.process.Args.Iterator, flag: []const u8) !f32 {
    const value = args.next() orelse return error.InvalidArgument;
    return std.fmt.parseFloat(f32, value) catch {
        std.debug.print("invalid value for {s}: {s}\n", .{ flag, value });
        return error.InvalidArgument;
    };
}

fn isManifestPath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, "manifest.bin") or std.mem.indexOf(u8, path, "manifest.bin.") != null;
}

test "procedural cosine exact recall distance matches materialized vector" {
    const seed: u64 = 42;
    var query: [32]f32 = undefined;
    var candidate: [32]f32 = undefined;
    writeProceduralDatasetVector(seed, 7, query[0..]);

    for ([_]usize{ 0, 7, 15, 16, 127 }) |row| {
        writeProceduralDatasetVector(seed, row, candidate[0..]);
        const materialized = vec.distance(query[0..], candidate[0..], .cosine);
        const generated = proceduralCosineDistanceToQuery(seed, row, query[0..]);
        const generated_cached_norm = proceduralCosineDistanceToQueryWithNorms(seed, row, query[0..], vectorNormSqScalar(query[0..]), proceduralCandidateNormSq(seed, row, query.len));
        try std.testing.expectApproxEqAbs(materialized, generated, 0.000001);
        try std.testing.expectApproxEqAbs(materialized, generated_cached_norm, 0.000001);
    }
}
