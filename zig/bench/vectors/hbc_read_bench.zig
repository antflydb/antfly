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
const recall_common = @import("recall_common.zig");
const posting_segment = antfly.vectorindex.posting_segment;

const Config = struct {
    samples: usize = 3,
    vectors: usize = 10_000,
    dims: usize = 128,
    queries: usize = 200,
    recall_queries: usize = 20,
    k: usize = 10,
    batch_size: usize = 1_000,
    seed: u64 = 42,
    leaf_size: u32 = 128,
    branching_factor: u32 = 128,
    storage_mode: StorageSelection = .host,
    build_mode: BuildSelection = .both,
    split_algo: vec.ClustAlgorithm = .kmeans,
    bulk_build_algo: hbc.BulkBuildAlgo = .hilbert_seeded,
    kmeans_backend: hbc.HBCConfig.KmeansBackend = .auto,
    kmeans_update_strategy: hbc.HBCConfig.KmeansUpdateStrategy = .auto,
    use_quantization: bool = true,
    use_random_ortho_trans: bool = false,
    centroid_directory_mode: hbc.HBCConfig.CentroidDirectoryMode = .hbc,
    flat_centroid_block_size: usize = 8192,
    flat_centroid_probe_count: usize = 0,
    reopen_before_query: bool = true,
    shadow_segment_checkpoint: bool = false,
    segment_store_reads: bool = false,
    segment_wal_shadow_batches: usize = 0,
    segment_wal_mutation_batches: usize = 0,
    segment_wal_sync_each_batch: bool = false,
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
    public_ingest,
    both,
};

const ExternalVectorLoaderContext = struct {
    items: []const hbc.BatchInsertItem,

    fn load(ctx_ptr: *anyopaque, allocator: Allocator, vector_id: u64, _: []const u8) ![]f32 {
        const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (vector_id == 0 or vector_id > ctx.items.len) return error.NotFound;
        return try allocator.dupe(f32, ctx.items[@intCast(vector_id - 1)].vector);
    }
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
        allocator: Allocator,
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

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !lsm_backend.Storage.Trailer {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.read_trailer += 1;
            const trailer = try self.backing.readFileTrailerAlloc(allocator, path, len);
            self.counters.read_bytes += trailer.bytes.len;
            return trailer;
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

        fn appendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.write_file += 1;
            self.counters.write_bytes += contents.len;
            return self.backing.appendFileAbsolute(self.allocator, path, contents, sync);
        }

        fn syncFileContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.syncFileContentsAbsolute(path);
        }

        fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.syncParentAbsolute(path);
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
        .append_file_absolute = CountingStorage.appendFileAbsolute,
        .sync_contents_absolute = CountingStorage.syncFileContentsAbsolute,
        .sync_parent_absolute = CountingStorage.syncParentAbsolute,
        .rename_absolute = CountingStorage.renameAbsolute,
        .delete_file_absolute = CountingStorage.deleteFileAbsolute,
        .delete_tree = CountingStorage.deleteTree,
        .now_ns = CountingStorage.nowNs,
        .rename_is_atomic = true,
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
                    ctx.* = .{ .allocator = allocator, .backing = backing.storage() };
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
                ctx.* = .{ .allocator = allocator, .backing = backing.storage() };
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
    build_kind: BuildSelection,
    root_dir: [:0]u8,
    storage_harness: StorageHarness,
    index: hbc.HBCIndex,

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

        return .{
            .allocator = allocator,
            .cfg = cfg,
            .sample_index = sample_index,
            .storage_kind = storage_kind,
            .build_kind = build_kind,
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

    fn reopen(self: *Scenario) !void {
        self.index.close();
        self.index = try hbc.HBCIndex.openWithLsmStorage(self.allocator, self.root_dir, hbcConfig(self.cfg), self.storage_harness.storage());
    }
};

const ProfileTotals = struct {
    total_ns: u64 = 0,
    setup_ns: u64 = 0,
    root_load_ns: u64 = 0,
    node_cache_miss_ns: u64 = 0,
    node_cache_misses: u64 = 0,
    quantized_cache_miss_ns: u64 = 0,
    quantized_cache_misses: u64 = 0,
    child_expand_ns: u64 = 0,
    leaf_score_ns: u64 = 0,
    rerank_ns: u64 = 0,
    rerank_vector_load_ns: u64 = 0,
    rerank_metadata_ns: u64 = 0,
    nodes_visited: u64 = 0,
    leaves_explored: u64 = 0,
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
        self.root_load_ns += p.root_load_ns;
        self.node_cache_miss_ns += p.node_cache_miss_ns;
        self.node_cache_misses += p.node_cache_misses;
        self.quantized_cache_miss_ns += p.quantized_cache_miss_ns;
        self.quantized_cache_misses += p.quantized_cache_misses;
        self.child_expand_ns += p.child_expand_ns;
        self.leaf_score_ns += p.leaf_score_ns;
        self.rerank_ns += p.rerank_ns;
        self.rerank_vector_load_ns += p.rerank_vector_load_ns;
        self.rerank_metadata_ns += p.rerank_metadata_ns;
        self.nodes_visited += p.nodes_visited;
        self.leaves_explored += p.leaves_explored;
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

    const dataset = try makeDataset(allocator, cfg);
    defer allocator.free(dataset);
    const queries = try makeQueries(allocator, cfg, dataset);
    defer allocator.free(queries);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;

    try out.print(
        "hbc read bench samples={d} vectors={d} dims={d} queries={d} k={d} batch_size={d} leaf_size={d} branching_factor={d} storage={s} build={s} kmeans_backend={s} kmeans_update_strategy={s} centroid_directory={s} flat_centroid_block_size={d} flat_centroid_probe_count={d} reopen_before_query={any} segment_store_reads={any} segment_wal_shadow_batches={d} segment_wal_mutation_batches={d} segment_wal_sync_each_batch={any}\n",
        .{
            cfg.samples,
            cfg.vectors,
            cfg.dims,
            cfg.queries,
            cfg.k,
            cfg.batch_size,
            cfg.leaf_size,
            cfg.branching_factor,
            @tagName(cfg.storage_mode),
            @tagName(cfg.build_mode),
            @tagName(cfg.kmeans_backend),
            @tagName(cfg.kmeans_update_strategy),
            @tagName(cfg.centroid_directory_mode),
            cfg.flat_centroid_block_size,
            cfg.flat_centroid_probe_count,
            cfg.reopen_before_query,
            cfg.segment_store_reads,
            cfg.segment_wal_shadow_batches,
            cfg.segment_wal_mutation_batches,
            cfg.segment_wal_sync_each_batch,
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
        .public_ingest => &[_]BuildSelection{.public_ingest},
        .both => &[_]BuildSelection{ .bulk_build, .online_coalesced },
    };

    for (storage_modes) |storage_mode| {
        for (build_modes) |build_mode| {
            for (0..cfg.samples) |sample_index| {
                const sample_dataset = try allocator.dupe(f32, dataset);
                defer allocator.free(sample_dataset);
                const sample_items = try makeItems(allocator, cfg, sample_dataset);
                defer freeItems(allocator, sample_items);
                var scenario = try Scenario.init(allocator, cfg, sample_index, storage_mode, build_mode);
                defer scenario.deinit();
                var external_loader_ctx: ExternalVectorLoaderContext = .{ .items = sample_items };
                if (build_mode == .public_ingest) scenario.index.setExternalVectorLoader(&external_loader_ctx, ExternalVectorLoaderContext.load);
                const before_build_storage = scenario.storage_harness.snapshotCounters();
                scenario.index.resetWriteProfile();
                const build_start = nanotime();
                try buildIndex(&scenario, sample_items);
                const build_elapsed = nanotime() - build_start;
                const after_build_storage = scenario.storage_harness.snapshotCounters();
                try printBuildResult(out, &scenario, build_elapsed, before_build_storage, after_build_storage);
                try stdout_writer.flush();
                if (cfg.shadow_segment_checkpoint) {
                    try benchShadowSegmentCheckpoint(out, &stdout_writer, &scenario);
                }
                var posting_source_sequence: u64 = @intCast(cfg.vectors);
                if (cfg.segment_store_reads) {
                    const checkpoint_start = nanotime();
                    const checkpoint = try scenario.index.publishExperimentalPostingCheckpoint(@intCast(cfg.vectors));
                    const checkpoint_ns = nanotime() - checkpoint_start;
                    try out.print(
                        "{{\"scenario\":\"{s}_{s}_posting_store_publish\",\"storage\":\"{s}\",\"build\":\"{s}\",\"sample\":{d},\"workload\":\"posting_store_publish\",\"generation\":{d},\"covered_source_sequence\":{d},\"live_postings\":{d},\"segment_bytes\":{d},\"checkpoint_ns\":{d}}}\n",
                        .{
                            @tagName(scenario.storage_kind),
                            @tagName(scenario.build_kind),
                            @tagName(scenario.storage_kind),
                            @tagName(scenario.build_kind),
                            scenario.sample_index,
                            checkpoint.generation,
                            checkpoint.covered_source_sequence,
                            checkpoint.posting_count,
                            checkpoint.segment_bytes,
                            checkpoint_ns,
                        },
                    );
                    try stdout_writer.flush();
                    if (cfg.segment_wal_shadow_batches > 0) {
                        const wal_start = nanotime();
                        var wal_bytes: u64 = 0;
                        var wal_records: u64 = 0;
                        for (0..cfg.segment_wal_shadow_batches) |batch_index| {
                            posting_source_sequence += 1;
                            const posting_id = @as(u64, @intCast(batch_index % @as(usize, @intCast(checkpoint.posting_count)))) + 1;
                            const wal_stats = try scenario.index.appendExperimentalPostingWalSnapshotBatchWithOptions(
                                @intCast(batch_index + 1),
                                posting_source_sequence,
                                &.{posting_id},
                                .{ .sync = cfg.segment_wal_sync_each_batch },
                            );
                            wal_bytes = wal_stats.wal_committed_bytes;
                            wal_records += wal_stats.record_count;
                        }
                        const wal_ns = nanotime() - wal_start;
                        try out.print(
                            "{{\"scenario\":\"{s}_{s}_posting_wal_shadow\",\"storage\":\"{s}\",\"build\":\"{s}\",\"sample\":{d},\"workload\":\"posting_wal_shadow\",\"batches\":{d},\"records\":{d},\"wal_bytes\":{d},\"sync_each_batch\":{any},\"ns\":{d}}}\n",
                            .{
                                @tagName(scenario.storage_kind),
                                @tagName(scenario.build_kind),
                                @tagName(scenario.storage_kind),
                                @tagName(scenario.build_kind),
                                scenario.sample_index,
                                cfg.segment_wal_shadow_batches,
                                wal_records,
                                wal_bytes,
                                cfg.segment_wal_sync_each_batch,
                                wal_ns,
                            },
                        );
                        try stdout_writer.flush();
                    }
                    if (cfg.segment_wal_mutation_batches > 0) {
                        const mutation_storage_before = scenario.storage_harness.snapshotCounters();
                        const mutation_start = nanotime();
                        var apply_ns: u64 = 0;
                        var wal_capture_ns: u64 = 0;
                        var wal_bytes: u64 = 0;
                        var wal_records: u64 = 0;
                        const item_batch_slots = (sample_items.len - 1) / cfg.batch_size + 1;
                        for (0..cfg.segment_wal_mutation_batches) |batch_index| {
                            const offset = (batch_index % item_batch_slots) * cfg.batch_size;
                            const end = @min(offset + cfg.batch_size, sample_items.len);
                            const mutation_count = end - offset;
                            const mutation_vectors = try allocator.alloc(f32, mutation_count * cfg.dims);
                            defer allocator.free(mutation_vectors);
                            const mutation_items = try allocator.alloc(hbc.BatchInsertItem, mutation_count);
                            defer allocator.free(mutation_items);
                            for (mutation_items, 0..) |*item, item_index| {
                                const source = sample_items[offset + item_index];
                                const vector = mutation_vectors[item_index * cfg.dims ..][0..cfg.dims];
                                @memcpy(vector, source.vector);
                                vector[batch_index % cfg.dims] += 0.05;
                                _ = vec.normalize(vector);
                                item.* = .{
                                    .vector_id = source.vector_id,
                                    .vector = vector,
                                    .metadata = source.metadata,
                                };
                            }
                            const apply_start = nanotime();
                            try scenario.index.beginExperimentalPostingMutationCapture();
                            scenario.index.batchInsertWithMetadataOptions(mutation_items, .{
                                .assume_absent_ids = false,
                                .coalesce_leaf_writes = true,
                                .skip_vector_store = true,
                            }) catch |err| {
                                scenario.index.cancelExperimentalPostingMutationCapture();
                                return err;
                            };
                            apply_ns += nanotime() - apply_start;
                            for (mutation_items, 0..) |item, item_index| {
                                @memcpy(sample_dataset[(offset + item_index) * cfg.dims ..][0..cfg.dims], item.vector);
                                scenario.index.invalidateVectorCache(item.vector_id);
                            }
                            posting_source_sequence += 1;
                            const wal_capture_start = nanotime();
                            const wal_stats = try scenario.index.finishExperimentalPostingMutationCapture(
                                @intCast(batch_index + 1),
                                posting_source_sequence,
                                .{ .sync = cfg.segment_wal_sync_each_batch },
                            );
                            wal_capture_ns += nanotime() - wal_capture_start;
                            wal_bytes = wal_stats.wal_committed_bytes;
                            wal_records += wal_stats.record_count;
                        }
                        const mutation_ns = nanotime() - mutation_start;
                        const mutation_storage_after = scenario.storage_harness.snapshotCounters();
                        const mutation_storage = StorageCounters.delta(mutation_storage_after, mutation_storage_before);
                        try out.print(
                            "{{\"scenario\":\"{s}_{s}_posting_wal_mutation\",\"storage\":\"{s}\",\"build\":\"{s}\",\"sample\":{d},\"workload\":\"posting_wal_mutation\",\"batches\":{d},\"batch_size\":{d},\"records\":{d},\"wal_bytes\":{d},\"sync_each_batch\":{any},\"ns\":{d},\"apply_ns\":{d},\"wal_capture_ns\":{d},\"storage_write_file\":{d},\"storage_write_bytes\":{d}}}\n",
                            .{
                                @tagName(scenario.storage_kind),
                                @tagName(scenario.build_kind),
                                @tagName(scenario.storage_kind),
                                @tagName(scenario.build_kind),
                                scenario.sample_index,
                                cfg.segment_wal_mutation_batches,
                                cfg.batch_size,
                                wal_records,
                                wal_bytes,
                                cfg.segment_wal_sync_each_batch,
                                mutation_ns,
                                apply_ns,
                                wal_capture_ns,
                                mutation_storage.write_file,
                                mutation_storage.write_bytes,
                            },
                        );
                        try stdout_writer.flush();
                    }
                }
                try benchQueries(out, &stdout_writer, &scenario, "post_ingest_query_no_metadata", queries, cfg.queries, .{
                    .query = queries[0..cfg.dims],
                    .k = cfg.k,
                    .load_metadata = false,
                }, sample_dataset, true);
                if (cfg.segment_wal_mutation_batches > 0) {
                    try scenario.reopen();
                    if (build_mode == .public_ingest) scenario.index.setExternalVectorLoader(&external_loader_ctx, ExternalVectorLoaderContext.load);
                    try benchQueries(out, &stdout_writer, &scenario, "reopened_lsm_cold_query_no_metadata", queries, 1, .{
                        .query = queries[0..cfg.dims],
                        .k = cfg.k,
                        .load_metadata = false,
                    }, sample_dataset, false);
                    try benchQueries(out, &stdout_writer, &scenario, "reopened_lsm_warm_query_no_metadata", queries, cfg.queries, .{
                        .query = queries[0..cfg.dims],
                        .k = cfg.k,
                        .load_metadata = false,
                    }, sample_dataset, true);
                }
                var reopened = false;
                if (cfg.segment_store_reads) {
                    const reopen_start = nanotime();
                    try scenario.reopen();
                    const reopen_ns = nanotime() - reopen_start;
                    if (build_mode == .public_ingest) scenario.index.setExternalVectorLoader(&external_loader_ctx, ExternalVectorLoaderContext.load);
                    const activate_start = nanotime();
                    try scenario.index.activateExperimentalPostingReads(posting_source_sequence);
                    const activate_ns = nanotime() - activate_start;
                    try out.print(
                        "{{\"scenario\":\"{s}_{s}_posting_store_activate\",\"storage\":\"{s}\",\"build\":\"{s}\",\"sample\":{d},\"workload\":\"posting_store_activate\",\"reopen_ns\":{d},\"activate_ns\":{d}}}\n",
                        .{
                            @tagName(scenario.storage_kind),
                            @tagName(scenario.build_kind),
                            @tagName(scenario.storage_kind),
                            @tagName(scenario.build_kind),
                            scenario.sample_index,
                            reopen_ns,
                            activate_ns,
                        },
                    );
                    try stdout_writer.flush();
                    reopened = true;
                }
                if (cfg.reopen_before_query and !reopened) {
                    try scenario.reopen();
                    if (build_mode == .public_ingest) scenario.index.setExternalVectorLoader(&external_loader_ctx, ExternalVectorLoaderContext.load);
                }

                try benchQueries(out, &stdout_writer, &scenario, "cold_first_query_no_metadata", queries, 1, .{
                    .query = queries[0..cfg.dims],
                    .k = cfg.k,
                    .load_metadata = false,
                }, sample_dataset, false);
                try benchQueries(out, &stdout_writer, &scenario, "warm_query_no_metadata", queries, cfg.queries, .{
                    .query = queries[0..cfg.dims],
                    .k = cfg.k,
                    .load_metadata = false,
                }, sample_dataset, true);
                try benchQueries(out, &stdout_writer, &scenario, "warm_query_metadata", queries, cfg.queries, .{
                    .query = queries[0..cfg.dims],
                    .k = cfg.k,
                    .load_metadata = true,
                }, sample_dataset, false);
                try benchQueries(out, &stdout_writer, &scenario, "warm_query_filter_prefix", queries, cfg.queries, .{
                    .query = queries[0..cfg.dims],
                    .k = cfg.k,
                    .load_metadata = false,
                    .filter_prefix = "doc:0000",
                }, sample_dataset, false);
            }
        }
    }
    try stdout_writer.flush();
}

fn benchShadowSegmentCheckpoint(writer: anytype, stdout_writer: anytype, scenario: *Scenario) !void {
    var segment_writer = posting_segment.Writer.init(scenario.allocator);
    defer segment_writer.deinit();
    var live_ids = std.ArrayListUnmanaged(u64).empty;
    defer live_ids.deinit(scenario.allocator);

    var txn = try scenario.index.beginRuntimeReadTxn();
    defer txn.abort();
    const checkpoint_start = nanotime();
    var node_id: u64 = 1;
    while (node_id <= scenario.index.metadata.node_count) : (node_id += 1) {
        var key_buf: [12]u8 = undefined;
        const packed_value = scenario.index.getNamespaced(&txn, .nodes, antfly.vectorindex.encodeNodeKey(&key_buf, node_id, .packed_node)) catch |err| switch (err) {
            error.NotFound => continue,
            else => return err,
        };
        try segment_writer.appendBase(node_id, packed_value);
        try live_ids.append(scenario.allocator, node_id);

        var quant_key_buf: [10]u8 = undefined;
        if (scenario.index.getNamespaced(&txn, .quant, antfly.vectorindex.encodeQuantKey(&quant_key_buf, node_id))) |quantized| {
            try segment_writer.appendQuantizedCheckpoint(node_id, quantized);
        } else |err| switch (err) {
            error.NotFound => {},
            else => return err,
        }
    }
    const segment_bytes = try segment_writer.build();
    defer scenario.allocator.free(segment_bytes);
    const checkpoint_ns = nanotime() - checkpoint_start;

    const open_start = nanotime();
    var reader = try posting_segment.VerifiedReader.init(scenario.allocator, segment_bytes);
    defer reader.deinit();
    const open_ns = nanotime() - open_start;
    const verification_latencies = try scenario.allocator.alloc(u64, live_ids.items.len);
    defer scenario.allocator.free(verification_latencies);
    for (live_ids.items, 0..) |lookup_id, index| {
        const lookup_start = nanotime();
        _ = (try reader.getBase(lookup_id)).?;
        _ = try reader.getQuantizedCheckpoint(lookup_id);
        verification_latencies[index] = nanotime() - lookup_start;
    }
    std.mem.sort(u64, verification_latencies, {}, std.sort.asc(u64));
    const lookup_count = @max(scenario.cfg.queries, 1000);
    const latencies = try scenario.allocator.alloc(u64, lookup_count);
    defer scenario.allocator.free(latencies);
    var bytes_touched: usize = 0;
    for (0..lookup_count) |index| {
        const lookup_id = live_ids.items[(index * 9973) % live_ids.items.len];
        const lookup_start = nanotime();
        const base = (try reader.getBase(lookup_id)).?;
        const quantized = try reader.getQuantizedCheckpoint(lookup_id);
        latencies[index] = nanotime() - lookup_start;
        bytes_touched +%= base.len;
        if (quantized) |value| bytes_touched +%= value.len;
    }
    std.mem.doNotOptimizeAway(bytes_touched);
    std.mem.sort(u64, latencies, {}, std.sort.asc(u64));
    try writer.print(
        "{{\"scenario\":\"{s}_{s}_shadow_segment_checkpoint\",\"storage\":\"{s}\",\"build\":\"{s}\",\"sample\":{d},\"workload\":\"shadow_segment_checkpoint\",\"live_postings\":{d},\"segment_bytes\":{d},\"checkpoint_ns\":{d},\"open_ns\":{d},\"verification_p50_ns\":{d},\"verification_p95_ns\":{d},\"verification_max_ns\":{d},\"lookups\":{d},\"lookup_p50_ns\":{d},\"lookup_p95_ns\":{d},\"lookup_p99_ns\":{d},\"lookup_max_ns\":{d}}}\n",
        .{
            @tagName(scenario.storage_kind),
            @tagName(scenario.build_kind),
            @tagName(scenario.storage_kind),
            @tagName(scenario.build_kind),
            scenario.sample_index,
            live_ids.items.len,
            segment_bytes.len,
            checkpoint_ns,
            open_ns,
            percentile(verification_latencies, 50),
            percentile(verification_latencies, 95),
            verification_latencies[verification_latencies.len - 1],
            lookup_count,
            percentile(latencies, 50),
            percentile(latencies, 95),
            percentile(latencies, 99),
            latencies[latencies.len - 1],
        },
    );
    try stdout_writer.flush();
}

fn printBuildResult(
    writer: anytype,
    scenario: *const Scenario,
    elapsed_ns: u64,
    before_storage: StorageCounters,
    after_storage: StorageCounters,
) !void {
    const storage_delta = StorageCounters.delta(after_storage, before_storage);
    const profile = scenario.index.getWriteProfile();
    try writer.print(
        "{{\"scenario\":\"{s}_{s}_build\",\"storage\":\"{s}\",\"build\":\"{s}\",\"sample\":{d},\"workload\":\"build\",\"vectors\":{d},\"dims\":{d},\"ns\":{d},\"ns_per_vector\":{d:.2},\"vectors_per_second\":{d:.2},\"storage_write_file\":{d},\"storage_write_bytes\":{d},\"storage_manifest_write_file\":{d},\"storage_manifest_write_bytes\":{d},\"insert_calls\":{d},\"save_node_calls\":{d},\"split_leaf_calls\":{d},\"refresh_quantized_ns\":{d},\"quantized_store_ns\":{d},\"quantized_put_ns\":{d}}}\n",
        .{
            @tagName(scenario.storage_kind),
            @tagName(scenario.build_kind),
            @tagName(scenario.storage_kind),
            @tagName(scenario.build_kind),
            scenario.sample_index,
            scenario.cfg.vectors,
            scenario.cfg.dims,
            elapsed_ns,
            @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(scenario.cfg.vectors)),
            @as(f64, @floatFromInt(scenario.cfg.vectors)) * 1_000_000_000.0 / @as(f64, @floatFromInt(@max(elapsed_ns, 1))),
            storage_delta.write_file,
            storage_delta.write_bytes,
            storage_delta.manifest_write_file,
            storage_delta.manifest_write_bytes,
            profile.insert_calls,
            profile.save_node_calls,
            profile.split_leaf_calls,
            profile.refresh_quantized_ns,
            profile.quantized_store_ns,
            profile.quantized_put_ns,
        },
    );
}

fn buildIndex(scenario: *Scenario, items: []const hbc.BatchInsertItem) !void {
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
        .public_ingest => {
            try scenario.index.beginBulkIngestSession();
            errdefer scenario.index.abortBulkIngestSession();
            var offset: usize = 0;
            while (offset < items.len) {
                const end = @min(offset + scenario.cfg.batch_size, items.len);
                try scenario.index.batchInsertWithMetadataOptions(items[offset..end], .{
                    .assume_absent_ids = true,
                    .coalesce_leaf_writes = true,
                    .defer_quantized_rebuild = true,
                    .skip_vector_store = true,
                    .bulk_ingest = true,
                });
                offset = end;
            }
            try scenario.index.finishBulkIngestSessionWithOptions(.{ .compact = false });
        },
        .both => unreachable,
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
    dataset: []const f32,
    measure_recall: bool,
) !void {
    const before_storage = scenario.storage_harness.snapshotCounters();
    var totals: ProfileTotals = .{};
    const latencies = try scenario.allocator.alloc(u64, query_count);
    defer scenario.allocator.free(latencies);
    const predictions = try scenario.allocator.alloc(u64, query_count * scenario.cfg.k);
    defer scenario.allocator.free(predictions);
    @memset(predictions, 0);
    const prediction_counts = try scenario.allocator.alloc(usize, query_count);
    defer scenario.allocator.free(prediction_counts);
    const start = nanotime();
    for (0..query_count) |i| {
        var req = request_template;
        req.query = queries[(i % scenario.cfg.queries) * scenario.cfg.dims ..][0..scenario.cfg.dims];
        const query_start = nanotime();
        var profiled = try scenario.index.searchProfiledRequest(req);
        latencies[i] = nanotime() - query_start;
        totals.add(&profiled);
        const hits = profiled.results.getHits();
        prediction_counts[i] = @min(hits.len, scenario.cfg.k);
        for (hits[0..prediction_counts[i]], 0..) |hit, hit_index| {
            predictions[i * scenario.cfg.k + hit_index] = hit.vector_id;
        }
        profiled.results.deinit();
    }
    const elapsed = nanotime() - start;
    const after_storage = scenario.storage_harness.snapshotCounters();
    std.mem.sort(u64, latencies, {}, std.sort.asc(u64));

    var recall_sum: f64 = 0;
    const recall_queries = if (measure_recall) @min(query_count, scenario.cfg.recall_queries) else 0;
    for (0..recall_queries) |i| {
        const query = queries[(i % scenario.cfg.queries) * scenario.cfg.dims ..][0..scenario.cfg.dims];
        const truth = try recall_common.calculateTruth(
            scenario.allocator,
            @min(scenario.cfg.k, scenario.cfg.vectors),
            .cosine,
            query,
            .{ .dims = scenario.cfg.dims, .count = scenario.cfg.vectors, .data = @constCast(dataset) },
        );
        defer scenario.allocator.free(truth);
        var hits: usize = 0;
        for (truth) |truth_offset| {
            for (predictions[i * scenario.cfg.k ..][0..prediction_counts[i]]) |prediction| {
                if (prediction == truth_offset + 1) {
                    hits += 1;
                    break;
                }
            }
        }
        recall_sum += @as(f64, @floatFromInt(hits)) / @as(f64, @floatFromInt(truth.len));
    }
    const recall = if (recall_queries > 0) recall_sum / @as(f64, @floatFromInt(recall_queries)) else 0;
    var result_hasher = std.hash.Wyhash.init(0);
    result_hasher.update(std.mem.sliceAsBytes(predictions));
    result_hasher.update(std.mem.sliceAsBytes(prediction_counts));
    try printResult(writer, scenario, workload, query_count, elapsed, before_storage, after_storage, totals, latencies, recall_queries, recall, result_hasher.final());
    try stdout_writer.flush();
}

fn printResult(
    writer: anytype,
    scenario: *const Scenario,
    workload: []const u8,
    queries: usize,
    ns: u64,
    before_storage: StorageCounters,
    after_storage: StorageCounters,
    totals: ProfileTotals,
    sorted_latencies: []const u64,
    recall_queries: usize,
    recall: f64,
    result_digest: u64,
) !void {
    const storage_delta = StorageCounters.delta(after_storage, before_storage);
    const ns_per_query = @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(@max(queries, 1)));
    try writer.print(
        "{{\"scenario\":\"{s}_{s}_{s}\",\"storage\":\"{s}\",\"build\":\"{s}\",\"sample\":{d},\"workload\":\"{s}\",\"vectors\":{d},\"dims\":{d},\"queries\":{d},\"k\":{d},\"ns\":{d},\"ns_per_query\":{d:.2},\"qps\":{d:.2},\"latency_p50_ns\":{d},\"latency_p95_ns\":{d},\"latency_p99_ns\":{d},\"latency_max_ns\":{d},\"recall_queries\":{d},\"recall_at_k\":{d:.6},\"result_digest\":{d}",
        .{
            @tagName(scenario.storage_kind),
            @tagName(scenario.build_kind),
            workload,
            @tagName(scenario.storage_kind),
            @tagName(scenario.build_kind),
            scenario.sample_index,
            workload,
            scenario.cfg.vectors,
            scenario.cfg.dims,
            queries,
            scenario.cfg.k,
            ns,
            ns_per_query,
            @as(f64, @floatFromInt(queries)) * 1_000_000_000.0 / @as(f64, @floatFromInt(@max(ns, 1))),
            percentile(sorted_latencies, 50),
            percentile(sorted_latencies, 95),
            percentile(sorted_latencies, 99),
            sorted_latencies[sorted_latencies.len - 1],
            recall_queries,
            recall,
            result_digest,
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
        ",\"profile_total_ns\":{d},\"profile_setup_ns\":{d},\"profile_root_load_ns\":{d},\"profile_node_cache_miss_ns\":{d},\"profile_node_cache_misses\":{d},\"profile_quantized_cache_miss_ns\":{d},\"profile_quantized_cache_misses\":{d}",
        .{
            totals.total_ns,
            totals.setup_ns,
            totals.root_load_ns,
            totals.node_cache_miss_ns,
            totals.node_cache_misses,
            totals.quantized_cache_miss_ns,
            totals.quantized_cache_misses,
        },
    );
    try writer.print(
        ",\"profile_child_expand_ns\":{d},\"profile_leaf_score_ns\":{d},\"profile_rerank_ns\":{d},\"profile_rerank_vector_load_ns\":{d},\"profile_rerank_metadata_ns\":{d}",
        .{
            totals.child_expand_ns,
            totals.leaf_score_ns,
            totals.rerank_ns,
            totals.rerank_vector_load_ns,
            totals.rerank_metadata_ns,
        },
    );
    try writer.print(
        ",\"nodes_visited\":{d},\"leaves_explored\":{d},\"approx_vectors_scored\":{d},\"exact_vectors_scored\":{d},\"reranked_vectors\":{d},\"approx_candidate_count\":{d},\"rerank_candidate_count\":{d},\"result_count\":{d}}}\n",
        .{
            totals.nodes_visited,
            totals.leaves_explored,
            totals.approx_vectors_scored,
            totals.exact_vectors_scored,
            totals.reranked_vectors,
            totals.approx_candidate_count,
            totals.rerank_candidate_count,
            totals.result_count,
        },
    );
}

fn percentile(sorted: []const u64, percent: usize) u64 {
    std.debug.assert(sorted.len > 0);
    const rank = @max(@as(usize, 1), (sorted.len * percent + 99) / 100);
    return sorted[@min(rank - 1, sorted.len - 1)];
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
        .centroid_directory_mode = cfg.centroid_directory_mode,
        .flat_centroid_block_size = cfg.flat_centroid_block_size,
        .flat_centroid_probe_count = cfg.flat_centroid_probe_count,
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

fn makeQueries(allocator: Allocator, cfg: Config, dataset: []const f32) ![]f32 {
    var rng = std.Random.DefaultPrng.init(cfg.seed ^ 0xa11ce);
    const random = rng.random();
    const queries = try allocator.alloc(f32, cfg.queries * cfg.dims);
    for (0..cfg.queries) |i| {
        const source = (i * 9973) % cfg.vectors;
        for (0..cfg.dims) |dim| {
            queries[i * cfg.dims + dim] = dataset[source * cfg.dims + dim] + random.float(f32) * 0.0001;
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

fn freeItems(allocator: Allocator, items: []hbc.BatchInsertItem) void {
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
        } else if (std.mem.eql(u8, arg, "--recall-queries")) {
            cfg.recall_queries = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--k")) {
            cfg.k = try parseNextUsize(&args, arg);
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
        } else if (std.mem.eql(u8, arg, "--flat-centroid-block-size")) {
            cfg.flat_centroid_block_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--flat-centroid-probe-count")) {
            cfg.flat_centroid_probe_count = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--no-quantization")) {
            cfg.use_quantization = false;
        } else if (std.mem.eql(u8, arg, "--no-reopen")) {
            cfg.reopen_before_query = false;
        } else if (std.mem.eql(u8, arg, "--shadow-segment-checkpoint")) {
            cfg.shadow_segment_checkpoint = true;
        } else if (std.mem.eql(u8, arg, "--segment-store-reads")) {
            cfg.segment_store_reads = true;
        } else if (std.mem.eql(u8, arg, "--segment-wal-shadow-batches")) {
            cfg.segment_wal_shadow_batches = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--segment-wal-mutation-batches")) {
            cfg.segment_wal_mutation_batches = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--segment-wal-sync-each-batch")) {
            cfg.segment_wal_sync_each_batch = true;
        } else if (std.mem.eql(u8, arg, "--random-ortho")) {
            cfg.use_random_ortho_trans = true;
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.samples == 0 or cfg.vectors == 0 or cfg.dims == 0 or cfg.queries == 0 or cfg.recall_queries == 0 or cfg.k == 0 or cfg.batch_size == 0) return error.InvalidArgument;
    if ((cfg.segment_wal_shadow_batches > 0 or cfg.segment_wal_mutation_batches > 0) and !cfg.segment_store_reads) return error.InvalidArgument;
    if (cfg.segment_wal_shadow_batches > 0 and cfg.segment_wal_mutation_batches > 0) return error.InvalidArgument;
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
