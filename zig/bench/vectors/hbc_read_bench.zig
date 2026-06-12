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
const platform_time = antfly.platform_time;
const vec = antfly.vector;

const Config = struct {
    samples: usize = 3,
    vectors: usize = 10_000,
    dims: usize = 128,
    queries: usize = 200,
    k: usize = 10,
    batch_size: usize = 1_000,
    seed: u64 = 42,
    leaf_size: u32 = 128,
    branching_factor: u32 = 128,
    search_width: u32 = 0,
    storage_mode: StorageSelection = .host,
    build_mode: BuildSelection = .both,
    split_algo: vec.ClustAlgorithm = .kmeans,
    bulk_build_algo: hbc.BulkBuildAlgo = .hilbert_seeded,
    kmeans_backend: hbc.HBCConfig.KmeansBackend = .auto,
    kmeans_update_strategy: hbc.HBCConfig.KmeansUpdateStrategy = .auto,
    use_quantization: bool = true,
    use_random_ortho_trans: bool = false,
    centroid_directory_mode: hbc.HBCConfig.CentroidDirectoryMode = .hbc,
    posting_storage_mode: hbc.HBCConfig.PostingStorageMode = .packed_hbc,
    posting_base_member_block_size: usize = 32,
    dataset_mode: DatasetMode = .materialized,
    flat_centroid_block_size: usize = 128,
    flat_centroid_probe_count: usize = 0,
    flat_centroid_block_probe_count: usize = 0,
    max_posting_overlay_cache_bytes: usize = 8 * 1024 * 1024,
    max_posting_overlay_cache_entry_bytes: usize = 0,
    exact_truth_cache_path: ?[]const u8 = null,
    reopen_before_query: bool = true,
    repair_postings_after_build: bool = false,
    prewarm_warm_queries: bool = true,
};

const StorageSelection = enum {
    host,
    native,
    memory,
    both,
};

const BuildSelection = enum {
    bulk_build,
    online_coalesced,
    both,
};

const DatasetMode = enum {
    materialized,
    procedural,
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

    fn copyVector(self: *const ExternalVectorDataset, vector_id: u64, dst: []f32) !void {
        if (vector_id == 0) return error.NotFound;
        const row: usize = @intCast(vector_id - 1);
        if (dst.len < self.dims) return error.BufferTooSmall;
        switch (self.mode) {
            .materialized => {
                const offset = std.math.mul(usize, row, self.dims) catch return error.NotFound;
                if (offset + self.dims > self.data.len) return error.NotFound;
                @memcpy(dst[0..self.dims], self.data[offset..][0..self.dims]);
            },
            .procedural => writeProceduralDatasetVector(self.seed, row, dst[0..self.dims]),
        }
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

const Scenario = struct {
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_kind: StorageSelection,
    build_kind: BuildSelection,
    root_dir: [:0]u8,
    storage_harness: StorageHarness,
    external_vector_dataset: *ExternalVectorDataset,
    index: hbc.HBCIndex,
    posting_repair_after_build_ns: u64 = 0,
    exact_truth_build_ns: u64 = 0,
    exact_truth_cache_hit: bool = false,

    fn init(
        allocator: Allocator,
        cfg: Config,
        sample_index: usize,
        storage_kind: StorageSelection,
        build_kind: BuildSelection,
    ) !Scenario {
        var storage_harness = try StorageHarness.init(allocator, storage_kind);
        errdefer storage_harness.deinit();

        const root_dir = try allocPrintZ(allocator, "{s}/hbc-read-bench-{s}-{s}-{d}", .{
            if (storage_kind == .native) "/tmp" else "",
            @tagName(storage_kind),
            @tagName(build_kind),
            sample_index,
        });
        errdefer allocator.free(root_dir);

        var index = try hbc.HBCIndex.openWithLsmStorage(allocator, root_dir, hbcConfig(cfg), storage_harness.storage());
        errdefer index.close();

        const external_vector_dataset = try allocator.create(ExternalVectorDataset);
        errdefer allocator.destroy(external_vector_dataset);
        external_vector_dataset.* = .{
            .data = &.{},
            .dims = cfg.dims,
            .seed = cfg.seed,
            .mode = cfg.dataset_mode,
        };
        configureExternalVectorLoader(&index, external_vector_dataset);

        return .{
            .allocator = allocator,
            .cfg = cfg,
            .sample_index = sample_index,
            .storage_kind = storage_kind,
            .build_kind = build_kind,
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

    fn reopen(self: *Scenario) !void {
        self.index.close();
        self.index = try hbc.HBCIndex.openWithLsmStorage(self.allocator, self.root_dir, hbcConfig(self.cfg), self.storage_harness.storage());
        configureExternalVectorLoader(&self.index, self.external_vector_dataset);
    }
};

fn configureExternalVectorLoader(index: *hbc.HBCIndex, dataset: *ExternalVectorDataset) void {
    index.setExternalVectorScratchLoader(dataset, ExternalVectorDataset.loadScratch);
    index.setExternalVectorBatchScratchLoader(dataset, ExternalVectorDataset.loadBatchScratch);
    index.setExternalVectorBatchTransformedMatrixLoader(dataset, ExternalVectorDataset.loadTransformedMatrix);
    index.setExternalVectorMetadataRequired(false);
}

const ProfileTotals = struct {
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

    fn add(self: *ProfileTotals, profiled: *const hbc.ProfiledSearchResults) void {
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

fn nanotime() u64 {
    return platform_time.monotonicNs();
}

const LatencySummary = struct {
    p50_ns: u64 = 0,
    p95_ns: u64 = 0,
    p99_ns: u64 = 0,
};

const RecallHits = struct {
    hits: u64 = 0,
    total: u64 = 0,
};

const ExactTruthCache = struct {
    ids: []u64 = &.{},
    truth_count: usize = 0,
    query_count: usize = 0,

    fn deinit(self: *ExactTruthCache, allocator: Allocator) void {
        allocator.free(self.ids);
        self.* = .{};
    }

    fn lookup(self: *const ExactTruthCache, query_index: usize) []const u64 {
        if (self.truth_count == 0 or query_index >= self.query_count) return &.{};
        const start = query_index * self.truth_count;
        return self.ids[start..][0..self.truth_count];
    }
};

const ExactTruthCacheLoadResult = struct {
    cache: ExactTruthCache,
    ns: u64,
    cache_hit: bool,
};

const exact_truth_cache_magic: [8]u8 = .{ 'H', 'B', 'C', 'R', 'D', 'T', 'R', '1' };
const exact_truth_cache_header_len = exact_truth_cache_magic.len + 7 * @sizeOf(u64);

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

fn betterTruthCandidate(candidate_distance: f32, candidate_id: u64, current_distance: f32, current_id: u64) bool {
    if (candidate_distance != current_distance) return candidate_distance < current_distance;
    return candidate_id < current_id;
}

fn buildExactTruthCache(
    allocator: Allocator,
    cfg: Config,
    dataset: []const f32,
    metric: vec.DistanceMetric,
    queries: []const f32,
) !ExactTruthCache {
    if (cfg.dims == 0 or cfg.k == 0 or cfg.queries == 0) return .{};
    const vector_count = switch (cfg.dataset_mode) {
        .materialized => dataset.len / cfg.dims,
        .procedural => cfg.vectors,
    };
    const truth_count = @min(cfg.k, vector_count);
    if (truth_count == 0) return .{};

    const ids = try allocator.alloc(u64, cfg.queries * truth_count);
    errdefer allocator.free(ids);
    const candidate_norms = if (cfg.dataset_mode == .procedural and metric == .cosine)
        try buildProceduralCandidateNorms(allocator, cfg.seed, cfg.vectors, cfg.dims)
    else
        null;
    defer if (candidate_norms) |norms| allocator.free(norms);

    for (0..cfg.queries) |query_index| {
        const query = queries[query_index * cfg.dims ..][0..cfg.dims];
        const truth_ids = try exactTruthIds(allocator, cfg, dataset, metric, query, candidate_norms);
        defer allocator.free(truth_ids);
        std.debug.assert(truth_ids.len == truth_count);
        @memcpy(ids[query_index * truth_count ..][0..truth_count], truth_ids);
    }
    return .{
        .ids = ids,
        .truth_count = truth_count,
        .query_count = cfg.queries,
    };
}

fn buildProceduralCandidateNorms(allocator: Allocator, seed: u64, vector_count: usize, dims: usize) ![]f32 {
    const norms = try allocator.alloc(f32, vector_count);
    errdefer allocator.free(norms);
    for (norms, 0..) |*norm, row| {
        norm.* = proceduralCandidateNormSq(seed, row, dims);
    }
    return norms;
}

fn loadOrBuildExactTruthCache(
    io: std.Io,
    allocator: Allocator,
    cfg: Config,
    dataset: []const f32,
    metric: vec.DistanceMetric,
    queries: []const f32,
) !ExactTruthCacheLoadResult {
    if (cfg.exact_truth_cache_path) |path| {
        const load_start = nanotime();
        if (loadExactTruthCacheFile(io, allocator, cfg, path)) |cache| {
            return .{
                .cache = cache,
                .ns = nanotime() - load_start,
                .cache_hit = true,
            };
        } else |_| {}
    }

    const build_start = nanotime();
    const cache = try buildExactTruthCache(allocator, cfg, dataset, metric, queries);
    const build_ns = nanotime() - build_start;
    if (cfg.exact_truth_cache_path) |path| {
        writeExactTruthCacheFile(io, allocator, cfg, path, &cache) catch |err| {
            std.debug.print("hbc read bench exact_truth_cache_write_failed path={s} err={s}\n", .{ path, @errorName(err) });
        };
    }
    return .{
        .cache = cache,
        .ns = build_ns,
        .cache_hit = false,
    };
}

fn loadExactTruthCacheFile(
    io: std.Io,
    allocator: Allocator,
    cfg: Config,
    path: []const u8,
) !ExactTruthCache {
    const bytes = try readFileAllocPath(io, allocator, path, exactTruthCacheExpectedBytes(cfg));
    defer allocator.free(bytes);
    if (bytes.len != exactTruthCacheExpectedBytes(cfg)) return error.InvalidTruthCache;
    if (!std.mem.eql(u8, bytes[0..exact_truth_cache_magic.len], &exact_truth_cache_magic)) return error.InvalidTruthCache;
    var pos: usize = exact_truth_cache_magic.len;
    const seed = readCacheU64(bytes, &pos);
    const vectors = readCacheU64(bytes, &pos);
    const dims = readCacheU64(bytes, &pos);
    const queries = readCacheU64(bytes, &pos);
    const k = readCacheU64(bytes, &pos);
    const dataset_mode = readCacheU64(bytes, &pos);
    const truth_count = readCacheU64(bytes, &pos);
    if (seed != cfg.seed or
        vectors != cfg.vectors or
        dims != cfg.dims or
        queries != cfg.queries or
        k != cfg.k or
        dataset_mode != @as(u64, @intCast(@intFromEnum(cfg.dataset_mode))) or
        truth_count != @min(cfg.k, cfg.vectors))
    {
        return error.InvalidTruthCache;
    }

    const ids = try allocator.alloc(u64, cfg.queries * @as(usize, @intCast(truth_count)));
    errdefer allocator.free(ids);
    for (ids) |*id| {
        id.* = readCacheU64(bytes, &pos);
    }
    return .{
        .ids = ids,
        .truth_count = @intCast(truth_count),
        .query_count = cfg.queries,
    };
}

fn writeExactTruthCacheFile(
    io: std.Io,
    allocator: Allocator,
    cfg: Config,
    path: []const u8,
    cache: *const ExactTruthCache,
) !void {
    const expected_bytes = exactTruthCacheExpectedBytes(cfg);
    var bytes = try allocator.alloc(u8, expected_bytes);
    defer allocator.free(bytes);
    @memcpy(bytes[0..exact_truth_cache_magic.len], &exact_truth_cache_magic);
    var pos: usize = exact_truth_cache_magic.len;
    writeCacheU64(bytes, &pos, cfg.seed);
    writeCacheU64(bytes, &pos, @intCast(cfg.vectors));
    writeCacheU64(bytes, &pos, @intCast(cfg.dims));
    writeCacheU64(bytes, &pos, @intCast(cfg.queries));
    writeCacheU64(bytes, &pos, @intCast(cfg.k));
    writeCacheU64(bytes, &pos, @intCast(@intFromEnum(cfg.dataset_mode)));
    writeCacheU64(bytes, &pos, @intCast(cache.truth_count));
    for (cache.ids) |id| {
        writeCacheU64(bytes, &pos, id);
    }
    std.debug.assert(pos == bytes.len);
    try writeFilePath(io, path, bytes);
}

fn exactTruthCacheExpectedBytes(cfg: Config) usize {
    const truth_count = @min(cfg.k, cfg.vectors);
    return exact_truth_cache_header_len + cfg.queries * truth_count * @sizeOf(u64);
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

fn exactTruthIds(
    allocator: Allocator,
    cfg: Config,
    dataset: []const f32,
    metric: vec.DistanceMetric,
    query: []const f32,
    candidate_norms: ?[]const f32,
) ![]u64 {
    const vector_count = switch (cfg.dataset_mode) {
        .materialized => dataset.len / cfg.dims,
        .procedural => cfg.vectors,
    };
    const truth_count = @min(cfg.k, vector_count);
    const truth_ids = try allocator.alloc(u64, truth_count);
    errdefer allocator.free(truth_ids);
    const truth_distances = try allocator.alloc(f32, truth_count);
    defer allocator.free(truth_distances);
    @memset(truth_ids, std.math.maxInt(u64));
    @memset(truth_distances, std.math.inf(f32));
    const procedural_query_norm_sq = if (cfg.dataset_mode == .procedural and metric == .cosine)
        vectorNormSqScalar(query)
    else
        0;

    for (0..vector_count) |row| {
        const vector_id: u64 = @intCast(row + 1);
        const distance = switch (cfg.dataset_mode) {
            .materialized => vec.distance(query, dataset[row * cfg.dims ..][0..cfg.dims], metric),
            .procedural => if (metric == .cosine) blk: {
                const procedural_candidate_norm_sq = if (candidate_norms) |norms|
                    norms[row]
                else
                    proceduralCandidateNormSq(cfg.seed, row, cfg.dims);
                break :blk proceduralCosineDistanceToQueryWithNorms(cfg.seed, row, query, procedural_query_norm_sq, procedural_candidate_norm_sq);
            } else return error.UnsupportedMetric,
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

fn allocPrintZ(allocator: Allocator, comptime fmt: []const u8, args: anytype) ![:0]u8 {
    const raw = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(raw);

    const out = try allocator.allocSentinel(u8, raw.len, 0);
    @memcpy(out[0..raw.len], raw);
    return out;
}

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);

    var empty_dataset: [0]f32 = .{};
    const dataset = switch (cfg.dataset_mode) {
        .materialized => try makeDataset(allocator, cfg),
        .procedural => empty_dataset[0..],
    };
    defer if (cfg.dataset_mode == .materialized) allocator.free(dataset);
    const queries = try makeQueries(allocator, cfg, dataset);
    defer allocator.free(queries);
    const items = switch (cfg.dataset_mode) {
        .materialized => try makeItems(allocator, cfg, dataset),
        .procedural => &[_]hbc.BatchInsertItem{},
    };
    defer if (cfg.dataset_mode == .materialized) freeItems(allocator, items);
    const exact_truth_result = try loadOrBuildExactTruthCache(init.io, allocator, cfg, dataset, .cosine, queries);
    var exact_truth_cache = exact_truth_result.cache;
    defer exact_truth_cache.deinit(allocator);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;

    try out.print(
        "hbc read bench samples={d} vectors={d} dims={d} queries={d} k={d} batch_size={d} leaf_size={d} branching_factor={d} search_width={d} storage={s} build={s} kmeans_backend={s} kmeans_update_strategy={s} centroid_directory={s} posting_storage={s} dataset_mode={s} exact_truth_cache_path={s} exact_truth_cache_hit={any} flat_centroid_block_size={d} flat_centroid_probe_count={d} flat_centroid_block_probe_count={d} max_posting_overlay_cache_bytes={d} max_posting_overlay_cache_entry_bytes={d} reopen_before_query={any} repair_postings_after_build={any} prewarm_warm_queries={any}\n",
        .{
            cfg.samples,
            cfg.vectors,
            cfg.dims,
            cfg.queries,
            cfg.k,
            cfg.batch_size,
            cfg.leaf_size,
            cfg.branching_factor,
            effectiveSearchWidth(cfg),
            @tagName(cfg.storage_mode),
            @tagName(cfg.build_mode),
            @tagName(cfg.kmeans_backend),
            @tagName(cfg.kmeans_update_strategy),
            @tagName(cfg.centroid_directory_mode),
            @tagName(cfg.posting_storage_mode),
            @tagName(cfg.dataset_mode),
            cfg.exact_truth_cache_path orelse "",
            exact_truth_result.cache_hit,
            cfg.flat_centroid_block_size,
            cfg.flat_centroid_probe_count,
            cfg.flat_centroid_block_probe_count,
            cfg.max_posting_overlay_cache_bytes,
            cfg.max_posting_overlay_cache_entry_bytes,
            cfg.reopen_before_query,
            cfg.repair_postings_after_build,
            cfg.prewarm_warm_queries,
        },
    );
    try stdout_writer.flush();

    const storage_modes: []const StorageSelection = switch (cfg.storage_mode) {
        .host => &[_]StorageSelection{.host},
        .native => &[_]StorageSelection{.native},
        .memory => &[_]StorageSelection{.memory},
        .both => &[_]StorageSelection{ .host, .native, .memory },
    };
    const build_modes: []const BuildSelection = switch (cfg.build_mode) {
        .bulk_build => &[_]BuildSelection{.bulk_build},
        .online_coalesced => &[_]BuildSelection{.online_coalesced},
        .both => &[_]BuildSelection{ .bulk_build, .online_coalesced },
    };

    for (storage_modes) |storage_mode| {
        for (build_modes) |build_mode| {
            for (0..cfg.samples) |sample_index| {
                var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, build_mode);
                defer scenario.deinit();
                scenario.exact_truth_build_ns = exact_truth_result.ns;
                scenario.exact_truth_cache_hit = exact_truth_result.cache_hit;
                scenario.external_vector_dataset.data = dataset;
                try buildIndex(&scenario, items);
                if (cfg.repair_postings_after_build) {
                    const repair_start = nanotime();
                    _ = try scenario.index.repairDirtyPostings();
                    scenario.posting_repair_after_build_ns = nanotime() - repair_start;
                }
                if (cfg.reopen_before_query) try scenario.reopen();

                try benchQueries(out, &stdout_writer, &scenario, "cold_first_query_no_metadata", queries, 1, .{
                    .query = queries[0..cfg.dims],
                    .k = cfg.k,
                    .load_metadata = false,
                }, &exact_truth_cache, true);
                if (cfg.prewarm_warm_queries) {
                    try prewarmQueries(&scenario, queries, cfg.queries, .{
                        .query = queries[0..cfg.dims],
                        .k = cfg.k,
                        .load_metadata = false,
                    });
                }
                try benchQueries(out, &stdout_writer, &scenario, "warm_query_no_metadata", queries, cfg.queries, .{
                    .query = queries[0..cfg.dims],
                    .k = cfg.k,
                    .load_metadata = false,
                }, &exact_truth_cache, true);
                try benchQueries(out, &stdout_writer, &scenario, "warm_query_metadata", queries, cfg.queries, .{
                    .query = queries[0..cfg.dims],
                    .k = cfg.k,
                    .load_metadata = true,
                }, &exact_truth_cache, true);
                try benchQueries(out, &stdout_writer, &scenario, "warm_query_filter_prefix", queries, cfg.queries, .{
                    .query = queries[0..cfg.dims],
                    .k = cfg.k,
                    .load_metadata = false,
                    .filter_prefix = "doc:0000",
                }, &exact_truth_cache, false);
            }
        }
    }
    try stdout_writer.flush();
}

fn prewarmQueries(
    scenario: *Scenario,
    queries: []const f32,
    query_count: usize,
    request_template: hbc.SearchRequest,
) !void {
    for (0..query_count) |i| {
        var req = request_template;
        req.query = queries[(i % scenario.cfg.queries) * scenario.cfg.dims ..][0..scenario.cfg.dims];
        var profiled = try scenario.index.searchProfiledRequest(req);
        profiled.results.deinit();
    }
}

fn buildIndex(scenario: *Scenario, items: []const hbc.BatchInsertItem) !void {
    if (scenario.cfg.dataset_mode == .procedural) {
        return buildProceduralIndex(scenario);
    }
    switch (scenario.build_kind) {
        .bulk_build => try scenario.index.bulkBuildWithMetadata(items),
        .online_coalesced => {
            var offset: usize = 0;
            while (offset < items.len) {
                const end = @min(offset + scenario.cfg.batch_size, items.len);
                try scenario.index.batchInsertWithMetadataOptions(items[offset..end], .{
                    .assume_absent_ids = true,
                    .coalesce_leaf_writes = true,
                });
                offset = end;
            }
        },
        .both => unreachable,
    }
}

fn buildProceduralIndex(scenario: *Scenario) !void {
    switch (scenario.build_kind) {
        .bulk_build => {
            try scenario.index.bulkBuildExternalSequentialWithMetadataOptions(scenario.cfg.vectors, .{
                .skip_vector_store = true,
            });
        },
        .online_coalesced => try insertProceduralBatches(scenario, .{
            .assume_absent_ids = true,
            .coalesce_leaf_writes = true,
            .skip_vector_store = true,
        }),
        .both => unreachable,
    }
}

fn insertProceduralBatches(scenario: *Scenario, options: hbc.BatchInsertOptions) !void {
    var batch = try ProceduralBatch.init(scenario.allocator, scenario.cfg);
    defer batch.deinit();
    var offset: usize = 0;
    while (offset < scenario.cfg.vectors) {
        const end = @min(offset + scenario.cfg.batch_size, scenario.cfg.vectors);
        const batch_items = try batch.fill(scenario.cfg, offset, end);
        try scenario.index.batchInsertWithMetadataOptions(batch_items, options);
        offset = end;
    }
}

fn benchQueries(
    writer: anytype,
    stdout_writer: anytype,
    scenario: *Scenario,
    workload: []const u8,
    queries: []const f32,
    query_count: usize,
    request_template: hbc.SearchRequest,
    exact_truth_cache: *const ExactTruthCache,
    measure_recall: bool,
) !void {
    const before_storage = scenario.storage_harness.snapshotCounters();
    var totals: ProfileTotals = .{};
    var query_latencies = try scenario.allocator.alloc(u64, query_count);
    defer scenario.allocator.free(query_latencies);
    var measured_queries: usize = 0;
    var recall_hits: u64 = 0;
    var recall_total: u64 = 0;
    const start = nanotime();
    for (0..query_count) |i| {
        var req = request_template;
        req.query = queries[(i % scenario.cfg.queries) * scenario.cfg.dims ..][0..scenario.cfg.dims];
        const query_start = nanotime();
        var profiled = try scenario.index.searchProfiledRequest(req);
        query_latencies[measured_queries] = nanotime() - query_start;
        measured_queries += 1;
        totals.add(&profiled);
        if (measure_recall) {
            const recall = recallHitsFromTruth(
                exact_truth_cache.lookup(i % scenario.cfg.queries),
                profiled.results.getHits(),
            );
            recall_hits += recall.hits;
            recall_total += recall.total;
        }
        profiled.results.deinit();
    }
    const elapsed = nanotime() - start;
    const latency = summarizeLatencies(query_latencies[0..measured_queries]);
    const after_storage = scenario.storage_harness.snapshotCounters();
    try printResult(writer, scenario, workload, query_count, elapsed, before_storage, after_storage, totals, latency, recall_hits, recall_total);
    try stdout_writer.flush();
}

fn effectiveSearchWidth(cfg: Config) u32 {
    return if (cfg.search_width != 0) cfg.search_width else cfg.branching_factor;
}

fn printResult(
    writer: anytype,
    scenario: *Scenario,
    workload: []const u8,
    queries: usize,
    ns: u64,
    before_storage: StorageCounters,
    after_storage: StorageCounters,
    totals: ProfileTotals,
    latency: LatencySummary,
    recall_hits: u64,
    recall_total: u64,
) !void {
    const storage_delta = StorageCounters.delta(after_storage, before_storage);
    const cache_stats = scenario.index.hbcCacheStats();
    const ns_per_query = @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(@max(queries, 1)));
    const queries_per_second = if (ns == 0) 0 else (@as(f64, @floatFromInt(queries)) * 1_000_000_000.0) / @as(f64, @floatFromInt(ns));
    const recall_at_k = if (recall_total == 0) 0 else @as(f64, @floatFromInt(recall_hits)) / @as(f64, @floatFromInt(recall_total));
    const repair_ns_per_vector = @as(f64, @floatFromInt(scenario.posting_repair_after_build_ns)) / @as(f64, @floatFromInt(@max(scenario.cfg.vectors, 1)));
    try writer.print(
        "{{\"scenario\":\"{s}_{s}_{s}_{s}\",\"storage\":\"{s}\",\"build\":\"{s}\",\"centroid_directory\":\"{s}\",\"posting_storage\":\"{s}\",\"posting_base_member_block_size\":{d},\"dataset_mode\":\"{s}\",\"skip_vector_store\":{},\"branching_factor\":{d},\"search_width\":{d},\"max_posting_overlay_cache_bytes\":{d},\"max_posting_overlay_cache_entry_bytes\":{d},\"sample\":{d},\"workload\":\"{s}\",\"vectors\":{d},\"dims\":{d},\"queries\":{d},\"k\":{d},\"ns\":{d},\"ns_per_query\":{d:.2},\"query_p50_ns\":{d},\"query_p95_ns\":{d},\"query_p99_ns\":{d},\"queries_per_second\":{d:.2},\"recall_hits\":{d},\"recall_total\":{d},\"recall_at_k\":{d:.4}",
        .{
            @tagName(scenario.storage_kind),
            @tagName(scenario.build_kind),
            @tagName(scenario.cfg.posting_storage_mode),
            workload,
            @tagName(scenario.storage_kind),
            @tagName(scenario.build_kind),
            @tagName(scenario.cfg.centroid_directory_mode),
            @tagName(scenario.cfg.posting_storage_mode),
            scenario.cfg.posting_base_member_block_size,
            @tagName(scenario.cfg.dataset_mode),
            scenario.cfg.dataset_mode == .procedural,
            scenario.cfg.branching_factor,
            effectiveSearchWidth(scenario.cfg),
            scenario.cfg.max_posting_overlay_cache_bytes,
            scenario.cfg.max_posting_overlay_cache_entry_bytes,
            scenario.sample_index,
            workload,
            scenario.cfg.vectors,
            scenario.cfg.dims,
            queries,
            scenario.cfg.k,
            ns,
            ns_per_query,
            latency.p50_ns,
            latency.p95_ns,
            latency.p99_ns,
            queries_per_second,
            recall_hits,
            recall_total,
            recall_at_k,
        },
    );
    try writer.print(
        ",\"posting_repair_after_build\":{},\"posting_repair_after_build_ns\":{d},\"posting_repair_after_build_ns_per_vector\":{d:.2},\"exact_truth_build_ns\":{d},\"exact_truth_cache_hit\":{}",
        .{
            scenario.cfg.repair_postings_after_build,
            scenario.posting_repair_after_build_ns,
            repair_ns_per_vector,
            scenario.exact_truth_build_ns,
            scenario.exact_truth_cache_hit,
        },
    );
    try writer.print(
        ",\"storage_read_file\":{d},\"storage_read_range\":{d},\"storage_read_trailer\":{d},\"storage_file_size\":{d},\"storage_read_bytes\":{d}",
        .{
            storage_delta.read_file,
            storage_delta.read_range,
            storage_delta.read_trailer,
            storage_delta.file_size,
            storage_delta.read_bytes,
        },
    );
    try writer.print(
        ",\"hbc_cache_total_bytes\":{d},\"hbc_cache_node_bytes\":{d},\"hbc_cache_quantized_bytes\":{d},\"hbc_cache_vector_bytes\":{d},\"hbc_cache_metadata_bytes\":{d},\"search_workspace_bytes\":{d}",
        .{
            cache_stats.total_bytes,
            cache_stats.node.used_bytes,
            cache_stats.quantized.used_bytes,
            cache_stats.vector.used_bytes,
            cache_stats.metadata.used_bytes,
            scenario.index.search_workspace_bytes_accounted,
        },
    );
    try writer.print(
        ",\"profile_total_ns\":{d},\"profile_setup_ns\":{d},\"profile_runtime_txn_ns\":{d},\"profile_scratch_acquire_ns\":{d},\"profile_search_scratch_allocations\":{d},\"profile_search_scratch_allocation_bytes\":{d},\"profile_search_scratch_retained_bytes\":{d},\"profile_upper_tree_pin_ns\":{d},\"profile_root_load_ns\":{d},\"profile_node_cache_miss_ns\":{d},\"profile_node_cache_misses\":{d},\"profile_quantized_cache_miss_ns\":{d},\"profile_quantized_cache_misses\":{d},\"profile_quantized_internal_cache_miss_ns\":{d},\"profile_quantized_internal_cache_misses\":{d},\"profile_quantized_leaf_cache_miss_ns\":{d},\"profile_quantized_leaf_cache_misses\":{d}",
        .{
            totals.total_ns,
            totals.setup_ns,
            totals.runtime_txn_ns,
            totals.scratch_acquire_ns,
            totals.search_scratch_allocations,
            totals.search_scratch_allocation_bytes,
            totals.search_scratch_retained_bytes,
            totals.upper_tree_pin_ns,
            totals.root_load_ns,
            totals.node_cache_miss_ns,
            totals.node_cache_misses,
            totals.quantized_cache_miss_ns,
            totals.quantized_cache_misses,
            totals.quantized_internal_cache_miss_ns,
            totals.quantized_internal_cache_misses,
            totals.quantized_leaf_cache_miss_ns,
            totals.quantized_leaf_cache_misses,
        },
    );
    try writer.print(
        ",\"profile_child_expand_ns\":{d},\"profile_leaf_score_ns\":{d},\"profile_posting_overlay_ns\":{d},\"profile_posting_overlay_calls\":{d},\"profile_posting_overlay_base_members\":{d},\"profile_posting_base_decode_ns\":{d},\"profile_posting_base_decode_members\":{d},\"profile_posting_delta_replay_ns\":{d},\"profile_posting_delta_replay_records\":{d},\"profile_posting_overlay_delta_records\":{d},\"profile_posting_overlay_delta_scan_skips\":{d},\"profile_posting_overlay_materialized_members\":{d},\"profile_posting_overlay_fallbacks\":{d},\"profile_posting_overlay_cache_hits\":{d},\"profile_posting_overlay_cache_misses\":{d},\"profile_posting_overlay_cache_evictions\":{d},\"profile_posting_overlay_cache_admission_skips\":{d},\"profile_posting_overlay_cache_member_bytes\":{d},\"profile_rerank_ns\":{d},\"profile_rerank_vector_load_ns\":{d},\"profile_rerank_metadata_ns\":{d}",
        .{
            totals.child_expand_ns,
            totals.leaf_score_ns,
            totals.posting_overlay_ns,
            totals.posting_overlay_calls,
            totals.posting_overlay_base_members,
            totals.posting_base_decode_ns,
            totals.posting_base_decode_members,
            totals.posting_delta_replay_ns,
            totals.posting_delta_replay_records,
            totals.posting_overlay_delta_records,
            totals.posting_overlay_delta_scan_skips,
            totals.posting_overlay_materialized_members,
            totals.posting_overlay_fallbacks,
            totals.posting_overlay_cache_hits,
            totals.posting_overlay_cache_misses,
            totals.posting_overlay_cache_evictions,
            totals.posting_overlay_cache_admission_skips,
            totals.posting_overlay_cache_member_bytes,
            totals.rerank_ns,
            totals.rerank_vector_load_ns,
            totals.rerank_metadata_ns,
        },
    );
    try writer.print(
        ",\"nodes_visited\":{d},\"leaves_explored\":{d},\"centroid_directory_blocks_scanned\":{d},\"centroid_directory_blocks_selected\":{d},\"centroid_directory_block_probe_limit\":{d},\"centroid_directory_block_probe_count\":{d},\"centroid_directory_block_centroids_scored\":{d},\"centroid_directory_block_centroid_estimates\":{d},\"centroid_directory_posting_centroids_scored\":{d},\"centroid_directory_posting_centroid_estimates\":{d},\"approx_vectors_scored\":{d},\"exact_vectors_scored\":{d},\"reranked_vectors\":{d},\"approx_candidate_count\":{d},\"rerank_candidate_count\":{d},\"result_count\":{d}}}\n",
        .{
            totals.nodes_visited,
            totals.leaves_explored,
            totals.centroid_directory_blocks_scanned,
            totals.centroid_directory_blocks_selected,
            totals.centroid_directory_block_probe_limit,
            totals.centroid_directory_block_probe_count,
            totals.centroid_directory_block_centroids_scored,
            totals.centroid_directory_block_centroid_estimates,
            totals.centroid_directory_posting_centroids_scored,
            totals.centroid_directory_posting_centroid_estimates,
            totals.approx_vectors_scored,
            totals.exact_vectors_scored,
            totals.reranked_vectors,
            totals.approx_candidate_count,
            totals.rerank_candidate_count,
            totals.result_count,
        },
    );
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

fn makeQueries(allocator: Allocator, cfg: Config, dataset: []const f32) ![]f32 {
    var rng = std.Random.DefaultPrng.init(cfg.seed ^ 0xa11ce);
    const random = rng.random();
    const queries = try allocator.alloc(f32, cfg.queries * cfg.dims);
    for (0..cfg.queries) |i| {
        const source = (i * 9973) % cfg.vectors;
        if (cfg.dataset_mode == .procedural) {
            writeProceduralDatasetVector(cfg.seed, source, queries[i * cfg.dims ..][0..cfg.dims]);
        }
        for (0..cfg.dims) |dim| {
            const source_value = switch (cfg.dataset_mode) {
                .materialized => dataset[source * cfg.dims + dim],
                .procedural => queries[i * cfg.dims + dim],
            };
            queries[i * cfg.dims + dim] = source_value + random.float(f32) * 0.0001;
        }
        _ = vec.normalize(queries[i * cfg.dims ..][0..cfg.dims]);
    }
    return queries;
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

fn freeItems(allocator: Allocator, items: []const hbc.BatchInsertItem) void {
    for (items) |item| allocator.free(item.metadata);
    allocator.free(items);
}

fn parseArgs(proc_args: std.process.Args) !Config {
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
        } else if (std.mem.eql(u8, arg, "--queries")) {
            cfg.queries = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--k")) {
            cfg.k = try parseNextUsize(&args, arg);
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
        } else if (std.mem.eql(u8, arg, "--build")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.build_mode = std.meta.stringToEnum(BuildSelection, value) orelse return error.InvalidArgument;
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
        } else if (std.mem.eql(u8, arg, "--dataset-mode")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.dataset_mode = std.meta.stringToEnum(DatasetMode, value) orelse return error.InvalidArgument;
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
        } else if (std.mem.eql(u8, arg, "--exact-truth-cache-path")) {
            cfg.exact_truth_cache_path = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--no-quantization")) {
            cfg.use_quantization = false;
        } else if (std.mem.eql(u8, arg, "--no-reopen")) {
            cfg.reopen_before_query = false;
        } else if (std.mem.eql(u8, arg, "--repair-postings-after-build")) {
            cfg.repair_postings_after_build = true;
        } else if (std.mem.eql(u8, arg, "--no-prewarm-warm-queries")) {
            cfg.prewarm_warm_queries = false;
        } else if (std.mem.eql(u8, arg, "--random-ortho")) {
            cfg.use_random_ortho_trans = true;
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.samples == 0 or cfg.vectors == 0 or cfg.dims == 0 or cfg.queries == 0 or cfg.k == 0 or cfg.batch_size == 0) return error.InvalidArgument;
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

test "read bench procedural cosine exact distance matches materialized vector" {
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
