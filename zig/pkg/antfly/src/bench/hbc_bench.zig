// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const builtin = @import("builtin");
const std = @import("std");
const antfly = @import("antfly-zig");
const platform_time = antfly.platform_time;

const hbc = antfly.hbc;
const lsm_backend = antfly.lsm_backend;
const vec = antfly.vector;

const BenchConfig = struct {
    docs: usize = 512,
    dims: usize = 128,
    queries: usize = 10,
    k: usize = 10,
    search_repeats: usize = 10,
    warm_queries: usize = 5,
    seed: u64 = 42,
    compare_hilbert: bool = false,
    disable_reranking: bool = false,
    batch_insert: bool = false,
    bulk_build: bool = false,
    bulk_build_hilbert_seeded: bool = false,
    bulk_build_doc_key_seeded: bool = false,
    bulk_build_kmeans: bool = false,
    kmeans_backend: hbc.HBCConfig.KmeansBackend = .auto,
    kmeans_update_strategy: hbc.HBCConfig.KmeansUpdateStrategy = .auto,
    prefer_key_local_leaf_splits: bool = false,
    defer_page_mutation: bool = false,
    in_memory_lsm: bool = false,
    use_random_ortho_trans: bool = false,
};

const BenchResult = struct {
    label: []const u8,
    insert_ns: u64,
    insert_transform_ns: u64,
    insert_store_vector_ns: u64,
    insert_find_leaf_ns: u64,
    insert_mutate_leaf_ns: u64,
    insert_flush_metadata_ns: u64,
    insert_commit_ns: u64,
    save_node_ns: u64,
    refresh_quantized_ns: u64,
    quantized_vector_load_ns: u64,
    quantized_compute_ns: u64,
    quantized_store_ns: u64,
    quantized_encode_ns: u64,
    quantized_put_ns: u64,
    save_split_range_ns: u64,
    update_parent_ns: u64,
    split_leaf_ns: u64,
    split_internal_ns: u64,
    insert_calls: u64,
    save_node_calls: u64,
    update_parent_calls: u64,
    split_leaf_calls: u64,
    split_internal_calls: u64,
    search_ns: u64,
    root_load_ns: u64,
    node_cache_miss_ns: u64,
    quantized_cache_miss_ns: u64,
    child_expand_ns: u64,
    leaf_score_ns: u64,
    rerank_ns: u64,
    rerank_prepare_ns: u64,
    rerank_select_ns: u64,
    rerank_load_ns: u64,
    rerank_prefetch_ns: u64,
    rerank_vector_view_ns: u64,
    rerank_distance_ns: u64,
    rerank_apply_ns: u64,
    rerank_resort_ns: u64,
    rerank_finalize_ns: u64,
    rerank_metadata_ns: u64,
    nodes_visited: u64,
    leaves_explored: u64,
    reranked_vectors: u64,
};

pub fn main(init: std.process.Init) !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const alloc = gpa_state.allocator();

    const cfg = try parseArgs(init.minimal.args);
    const dataset = try makeDataset(alloc, cfg);
    defer alloc.free(dataset);
    const queries = try makeQueries(alloc, dataset, cfg);
    defer alloc.free(queries);

    const kmeans = try runBench(alloc, cfg, dataset, queries, .kmeans);

    std.debug.print(
        "HBC benchmark docs={d} dims={d} queries={d} k={d} repeats={d} storage={s} kmeans_backend={s} kmeans_update_strategy={s}\n",
        .{
            cfg.docs,
            cfg.dims,
            cfg.queries,
            cfg.k,
            cfg.search_repeats,
            if (cfg.in_memory_lsm) "lsm-memory" else "lmdb",
            @tagName(cfg.kmeans_backend),
            @tagName(cfg.kmeans_update_strategy),
        },
    );
    printResult(kmeans);
    if (cfg.compare_hilbert) {
        const hilbert = try runBench(alloc, cfg, dataset, queries, .hilbert);
        printResult(hilbert);
        std.debug.print(
            "hilbert_vs_kmeans insert={d:.2}x search={d:.2}x\n",
            .{
                ratio(hilbert.insert_ns, kmeans.insert_ns),
                ratio(hilbert.search_ns, kmeans.search_ns),
            },
        );
    }
}

fn parseArgs(args_in: std.process.Args) !BenchConfig {
    var cfg = BenchConfig{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, "--docs");
        } else if (std.mem.eql(u8, arg, "--dims")) {
            cfg.dims = try parseNextUsize(&args, "--dims");
        } else if (std.mem.eql(u8, arg, "--queries")) {
            cfg.queries = try parseNextUsize(&args, "--queries");
        } else if (std.mem.eql(u8, arg, "--k")) {
            cfg.k = try parseNextUsize(&args, "--k");
        } else if (std.mem.eql(u8, arg, "--search-repeats")) {
            cfg.search_repeats = try parseNextUsize(&args, "--search-repeats");
        } else if (std.mem.eql(u8, arg, "--warm-queries")) {
            cfg.warm_queries = try parseNextUsize(&args, "--warm-queries");
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = try parseNextU64(&args, "--seed");
        } else if (std.mem.eql(u8, arg, "--compare-hilbert")) {
            cfg.compare_hilbert = true;
        } else if (std.mem.eql(u8, arg, "--disable-reranking")) {
            cfg.disable_reranking = true;
        } else if (std.mem.eql(u8, arg, "--batch-insert")) {
            cfg.batch_insert = true;
        } else if (std.mem.eql(u8, arg, "--bulk-build")) {
            cfg.bulk_build = true;
        } else if (std.mem.eql(u8, arg, "--bulk-build-hilbert-seeded")) {
            cfg.bulk_build = true;
            cfg.bulk_build_hilbert_seeded = true;
        } else if (std.mem.eql(u8, arg, "--bulk-build-doc-key-seeded")) {
            cfg.bulk_build = true;
            cfg.bulk_build_doc_key_seeded = true;
        } else if (std.mem.eql(u8, arg, "--bulk-build-kmeans")) {
            cfg.bulk_build = true;
            cfg.bulk_build_kmeans = true;
        } else if (std.mem.eql(u8, arg, "--kmeans-backend")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.kmeans_backend = std.meta.stringToEnum(hbc.HBCConfig.KmeansBackend, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--kmeans-update-strategy")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.kmeans_update_strategy = std.meta.stringToEnum(hbc.HBCConfig.KmeansUpdateStrategy, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--prefer-key-local-leaf-splits")) {
            cfg.prefer_key_local_leaf_splits = true;
        } else if (std.mem.eql(u8, arg, "--defer-page-mutation")) {
            cfg.defer_page_mutation = true;
        } else if (std.mem.eql(u8, arg, "--in-memory-lsm")) {
            cfg.in_memory_lsm = true;
        } else if (std.mem.eql(u8, arg, "--random-ortho")) {
            cfg.use_random_ortho_trans = true;
        } else {
            return error.InvalidArgument;
        }
    }

    if (cfg.batch_insert and cfg.bulk_build) return error.InvalidArgument;
    const bulk_mode_count =
        @intFromBool(cfg.bulk_build_hilbert_seeded) +
        @intFromBool(cfg.bulk_build_doc_key_seeded) +
        @intFromBool(cfg.bulk_build_kmeans);
    if (bulk_mode_count > 1) return error.InvalidArgument;
    if (cfg.docs == 0 or cfg.dims == 0 or cfg.queries == 0 or cfg.k == 0 or cfg.search_repeats == 0) return error.InvalidArgument;
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(usize, raw, 10);
}

fn parseNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(u64, raw, 10);
}

fn printResult(result: BenchResult) void {
    std.debug.print(
        "{s}: insert={d:.3}ms xform={d:.3}ms store={d:.3}ms find_leaf={d:.3}ms mutate={d:.3}ms flush={d:.3}ms commit={d:.3}ms insert_calls={d} save={d:.3}ms({d}) quant={d:.3}ms(load={d:.3} compute={d:.3} store={d:.3} encode={d:.3} put={d:.3}) split_range={d:.3}ms update_parent={d:.3}ms({d}) split_leaf={d:.3}ms({d}) split_internal={d:.3}ms({d})\n",
        .{
            result.label,
            @as(f64, @floatFromInt(result.insert_ns)) / 1e6,
            @as(f64, @floatFromInt(result.insert_transform_ns)) / 1e6,
            @as(f64, @floatFromInt(result.insert_store_vector_ns)) / 1e6,
            @as(f64, @floatFromInt(result.insert_find_leaf_ns)) / 1e6,
            @as(f64, @floatFromInt(result.insert_mutate_leaf_ns)) / 1e6,
            @as(f64, @floatFromInt(result.insert_flush_metadata_ns)) / 1e6,
            @as(f64, @floatFromInt(result.insert_commit_ns)) / 1e6,
            result.insert_calls,
            @as(f64, @floatFromInt(result.save_node_ns)) / 1e6,
            result.save_node_calls,
            @as(f64, @floatFromInt(result.refresh_quantized_ns)) / 1e6,
            @as(f64, @floatFromInt(result.quantized_vector_load_ns)) / 1e6,
            @as(f64, @floatFromInt(result.quantized_compute_ns)) / 1e6,
            @as(f64, @floatFromInt(result.quantized_store_ns)) / 1e6,
            @as(f64, @floatFromInt(result.quantized_encode_ns)) / 1e6,
            @as(f64, @floatFromInt(result.quantized_put_ns)) / 1e6,
            @as(f64, @floatFromInt(result.save_split_range_ns)) / 1e6,
            @as(f64, @floatFromInt(result.update_parent_ns)) / 1e6,
            result.update_parent_calls,
            @as(f64, @floatFromInt(result.split_leaf_ns)) / 1e6,
            result.split_leaf_calls,
            @as(f64, @floatFromInt(result.split_internal_ns)) / 1e6,
            result.split_internal_calls,
        },
    );
    std.debug.print(
        "{s}: search={d:.3}ms root={d:.3}ms node_miss={d:.3}ms quant_miss={d:.3}ms expand={d:.3}ms\n",
        .{
            result.label,
            @as(f64, @floatFromInt(result.search_ns)) / 1e6,
            @as(f64, @floatFromInt(result.root_load_ns)) / 1e6,
            @as(f64, @floatFromInt(result.node_cache_miss_ns)) / 1e6,
            @as(f64, @floatFromInt(result.quantized_cache_miss_ns)) / 1e6,
            @as(f64, @floatFromInt(result.child_expand_ns)) / 1e6,
        },
    );
    std.debug.print(
        "{s}: leaf={d:.3}ms rerank={d:.3}ms(prepare={d:.3} select={d:.3} load={d:.3} prefetch={d:.3} view={d:.3} dist={d:.3} apply={d:.3} resort={d:.3} finalize={d:.3} meta={d:.3}) nodes={d} leaves={d} reranked={d}\n",
        .{
            result.label,
            @as(f64, @floatFromInt(result.leaf_score_ns)) / 1e6,
            @as(f64, @floatFromInt(result.rerank_ns)) / 1e6,
            @as(f64, @floatFromInt(result.rerank_prepare_ns)) / 1e6,
            @as(f64, @floatFromInt(result.rerank_select_ns)) / 1e6,
            @as(f64, @floatFromInt(result.rerank_load_ns)) / 1e6,
            @as(f64, @floatFromInt(result.rerank_prefetch_ns)) / 1e6,
            @as(f64, @floatFromInt(result.rerank_vector_view_ns)) / 1e6,
            @as(f64, @floatFromInt(result.rerank_distance_ns)) / 1e6,
            @as(f64, @floatFromInt(result.rerank_apply_ns)) / 1e6,
            @as(f64, @floatFromInt(result.rerank_resort_ns)) / 1e6,
            @as(f64, @floatFromInt(result.rerank_finalize_ns)) / 1e6,
            @as(f64, @floatFromInt(result.rerank_metadata_ns)) / 1e6,
            result.nodes_visited,
            result.leaves_explored,
            result.reranked_vectors,
        },
    );
}

fn ratio(numer: u64, denom: u64) f64 {
    if (denom == 0) return 0;
    return @as(f64, @floatFromInt(numer)) / @as(f64, @floatFromInt(denom));
}

fn makeDataset(alloc: std.mem.Allocator, cfg: BenchConfig) ![]f32 {
    var rng = std.Random.DefaultPrng.init(cfg.seed);
    const random = rng.random();

    const data = try alloc.alloc(f32, cfg.docs * cfg.dims);
    for (0..cfg.docs) |doc_idx| {
        const cluster = doc_idx % 8;
        const base = @as(f32, @floatFromInt(cluster)) * 0.25;
        for (0..cfg.dims) |dim_idx| {
            data[doc_idx * cfg.dims + dim_idx] = base + (random.float(f32) * 0.01);
        }
        _ = vec.normalize(data[doc_idx * cfg.dims ..][0..cfg.dims]);
    }
    return data;
}

fn makeQueries(alloc: std.mem.Allocator, dataset: []const f32, cfg: BenchConfig) ![]f32 {
    const queries = try alloc.alloc(f32, cfg.queries * cfg.dims);
    for (0..cfg.queries) |i| {
        const src_idx = (i * 997) % cfg.docs;
        @memcpy(
            queries[i * cfg.dims ..][0..cfg.dims],
            dataset[src_idx * cfg.dims ..][0..cfg.dims],
        );
    }
    return queries;
}

fn runBench(
    alloc: std.mem.Allocator,
    cfg: BenchConfig,
    dataset: []const f32,
    queries: []const f32,
    split_algo: vec.ClustAlgorithm,
) !BenchResult {
    var tp = TestPath{};
    const path = tp.init(split_algo);
    defer tp.cleanup();

    const index_config: hbc.HBCConfig = .{
        .dims = @intCast(cfg.dims),
        .metric = .cosine,
        .split_algo = split_algo,
        .storage_backend = if (cfg.in_memory_lsm) .lsm else .lmdb,
        .branching_factor = 7 * 24,
        .leaf_size = 7 * 24,
        .search_width = 2 * 3 * 7 * 24,
        .epsilon = 7,
        .use_quantization = true,
        .rerank_policy = if (cfg.disable_reranking) .never else .boundary,
        .quantizer_seed = cfg.seed,
        .use_random_ortho_trans = cfg.use_random_ortho_trans,
        .bulk_build_algo = bulkBuildAlgo(cfg),
        .kmeans_backend = cfg.kmeans_backend,
        .kmeans_update_strategy = cfg.kmeans_update_strategy,
        .prefer_key_local_leaf_splits = cfg.prefer_key_local_leaf_splits,
        .defer_page_mutation = cfg.defer_page_mutation,
        .max_cached_nodes = 100_000,
        .max_cached_vectors = 100_000,
    };

    var memory_storage: ?lsm_backend.MemoryStorage = if (cfg.in_memory_lsm)
        lsm_backend.MemoryStorage.init(alloc)
    else
        null;
    defer if (memory_storage) |*backing| backing.deinit();

    var idx = if (memory_storage) |*backing|
        try hbc.HBCIndex.openWithLsmStorage(alloc, path, index_config, backing.storage())
    else
        try hbc.HBCIndex.open(alloc, path, index_config);
    defer idx.close();

    idx.resetWriteProfile();
    const insert_start = nanotime();
    var key_buf: [32]u8 = undefined;
    if (cfg.bulk_build) {
        const items = try alloc.alloc(hbc.BatchInsertItem, cfg.docs);
        defer alloc.free(items);
        for (0..cfg.docs) |i| {
            const doc_key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{i});
            errdefer {
                for (items[0..i]) |item| alloc.free(item.metadata);
            }
            items[i] = .{
                .vector_id = @intCast(i + 1),
                .vector = dataset[i * cfg.dims ..][0..cfg.dims],
                .metadata = doc_key,
            };
        }
        defer for (items) |item| alloc.free(item.metadata);
        try idx.bulkBuildWithMetadata(items);
    } else if (cfg.batch_insert) {
        const items = try alloc.alloc(hbc.BatchInsertItem, cfg.docs);
        defer alloc.free(items);
        for (0..cfg.docs) |i| {
            const doc_key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{i});
            errdefer {
                for (items[0..i]) |item| alloc.free(item.metadata);
            }
            items[i] = .{
                .vector_id = @intCast(i + 1),
                .vector = dataset[i * cfg.dims ..][0..cfg.dims],
                .metadata = doc_key,
            };
        }
        defer for (items) |item| alloc.free(item.metadata);
        try idx.batchInsertWithMetadata(items);
    } else {
        for (0..cfg.docs) |i| {
            const doc_key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{i});
            try idx.insertWithMetadata(@intCast(i + 1), dataset[i * cfg.dims ..][0..cfg.dims], doc_key);
        }
    }
    const insert_ns = nanotime() - insert_start;
    const write_profile = idx.getWriteProfile();

    for (0..cfg.warm_queries) |i| {
        var profiled = try idx.searchProfiledRequest(.{
            .query = queries[(i % cfg.queries) * cfg.dims ..][0..cfg.dims],
            .k = cfg.k,
            .load_metadata = false,
        });
        profiled.results.deinit();
    }

    var search_total: u64 = 0;
    var root_load_total: u64 = 0;
    var node_cache_miss_total: u64 = 0;
    var quantized_cache_miss_total: u64 = 0;
    var child_expand_total: u64 = 0;
    var leaf_score_total: u64 = 0;
    var rerank_total: u64 = 0;
    var rerank_prepare_total: u64 = 0;
    var rerank_select_total: u64 = 0;
    var rerank_load_total: u64 = 0;
    var rerank_prefetch_total: u64 = 0;
    var rerank_vector_view_total: u64 = 0;
    var rerank_distance_total: u64 = 0;
    var rerank_apply_total: u64 = 0;
    var rerank_resort_total: u64 = 0;
    var rerank_finalize_total: u64 = 0;
    var rerank_metadata_total: u64 = 0;
    var nodes_visited_total: u64 = 0;
    var leaves_explored_total: u64 = 0;
    var reranked_vectors_total: u64 = 0;
    const total_searches = cfg.queries * cfg.search_repeats;
    for (0..cfg.search_repeats) |_| {
        for (0..cfg.queries) |i| {
            const search_start = nanotime();
            var profiled = try idx.searchProfiledRequest(.{
                .query = queries[i * cfg.dims ..][0..cfg.dims],
                .k = cfg.k,
                .load_metadata = false,
            });
            defer profiled.results.deinit();
            search_total += nanotime() - search_start;
            root_load_total += profiled.profile.root_load_ns;
            node_cache_miss_total += profiled.profile.node_cache_miss_ns;
            quantized_cache_miss_total += profiled.profile.quantized_cache_miss_ns;
            child_expand_total += profiled.profile.child_expand_ns;
            leaf_score_total += profiled.profile.leaf_score_ns;
            rerank_total += profiled.profile.rerank_ns;
            rerank_prepare_total += profiled.profile.rerank_prepare_ns;
            rerank_select_total += profiled.profile.rerank_select_positions_ns;
            rerank_load_total += profiled.profile.rerank_vector_load_ns;
            rerank_prefetch_total += profiled.profile.rerank_prefetch_ns;
            rerank_vector_view_total += profiled.profile.rerank_vector_view_ns;
            rerank_distance_total += profiled.profile.rerank_distance_ns;
            rerank_apply_total += profiled.profile.rerank_apply_ns;
            rerank_resort_total += profiled.profile.rerank_resort_ns;
            rerank_finalize_total += profiled.profile.rerank_finalize_ns;
            rerank_metadata_total += profiled.profile.rerank_metadata_ns;
            nodes_visited_total += profiled.profile.nodes_visited;
            leaves_explored_total += profiled.profile.leaves_explored;
            reranked_vectors_total += profiled.profile.reranked_vectors;
            std.mem.doNotOptimizeAway(profiled.results.items.items.len);
        }
    }

    return .{
        .label = modeLabel(split_algo, cfg),
        .insert_ns = insert_ns,
        .insert_transform_ns = write_profile.insert_transform_ns,
        .insert_store_vector_ns = write_profile.insert_store_vector_ns,
        .insert_find_leaf_ns = write_profile.insert_find_leaf_ns,
        .insert_mutate_leaf_ns = write_profile.insert_mutate_leaf_ns,
        .insert_flush_metadata_ns = write_profile.insert_flush_metadata_ns,
        .insert_commit_ns = write_profile.insert_commit_ns,
        .save_node_ns = write_profile.save_node_ns,
        .refresh_quantized_ns = write_profile.refresh_quantized_ns,
        .quantized_vector_load_ns = write_profile.quantized_vector_load_ns,
        .quantized_compute_ns = write_profile.quantized_compute_ns,
        .quantized_store_ns = write_profile.quantized_store_ns,
        .quantized_encode_ns = write_profile.quantized_encode_ns,
        .quantized_put_ns = write_profile.quantized_put_ns,
        .save_split_range_ns = write_profile.save_split_range_ns,
        .update_parent_ns = write_profile.update_parent_ns,
        .split_leaf_ns = write_profile.split_leaf_ns,
        .split_internal_ns = write_profile.split_internal_ns,
        .insert_calls = write_profile.insert_calls,
        .save_node_calls = write_profile.save_node_calls,
        .update_parent_calls = write_profile.update_parent_calls,
        .split_leaf_calls = write_profile.split_leaf_calls,
        .split_internal_calls = write_profile.split_internal_calls,
        .search_ns = @divTrunc(search_total, total_searches),
        .root_load_ns = @divTrunc(root_load_total, total_searches),
        .node_cache_miss_ns = @divTrunc(node_cache_miss_total, total_searches),
        .quantized_cache_miss_ns = @divTrunc(quantized_cache_miss_total, total_searches),
        .child_expand_ns = @divTrunc(child_expand_total, total_searches),
        .leaf_score_ns = @divTrunc(leaf_score_total, total_searches),
        .rerank_ns = @divTrunc(rerank_total, total_searches),
        .rerank_prepare_ns = @divTrunc(rerank_prepare_total, total_searches),
        .rerank_select_ns = @divTrunc(rerank_select_total, total_searches),
        .rerank_load_ns = @divTrunc(rerank_load_total, total_searches),
        .rerank_prefetch_ns = @divTrunc(rerank_prefetch_total, total_searches),
        .rerank_vector_view_ns = @divTrunc(rerank_vector_view_total, total_searches),
        .rerank_distance_ns = @divTrunc(rerank_distance_total, total_searches),
        .rerank_apply_ns = @divTrunc(rerank_apply_total, total_searches),
        .rerank_resort_ns = @divTrunc(rerank_resort_total, total_searches),
        .rerank_finalize_ns = @divTrunc(rerank_finalize_total, total_searches),
        .rerank_metadata_ns = @divTrunc(rerank_metadata_total, total_searches),
        .nodes_visited = @divTrunc(nodes_visited_total, total_searches),
        .leaves_explored = @divTrunc(leaves_explored_total, total_searches),
        .reranked_vectors = @divTrunc(reranked_vectors_total, total_searches),
    };
}

fn modeLabel(split_algo: vec.ClustAlgorithm, cfg: BenchConfig) []const u8 {
    if (cfg.bulk_build) {
        if (cfg.bulk_build_hilbert_seeded) {
            return switch (split_algo) {
                .kmeans => "kmeans_bulk_hilbert_seeded",
                .hilbert => "hilbert_bulk_hilbert_seeded",
            };
        }
        if (cfg.bulk_build_doc_key_seeded) {
            return switch (split_algo) {
                .kmeans => "kmeans_bulk_doc_key_seeded",
                .hilbert => "hilbert_bulk_doc_key_seeded",
            };
        }
        if (cfg.bulk_build_kmeans) {
            return switch (split_algo) {
                .kmeans => "kmeans_bulk_kmeans",
                .hilbert => "hilbert_bulk_kmeans",
            };
        }
        return switch (split_algo) {
            .kmeans => "kmeans_bulk",
            .hilbert => "hilbert_bulk",
        };
    }
    if (cfg.batch_insert) {
        return switch (split_algo) {
            .kmeans => "kmeans_batch",
            .hilbert => "hilbert_batch",
        };
    }
    return @tagName(split_algo);
}

fn bulkBuildAlgo(cfg: BenchConfig) hbc.BulkBuildAlgo {
    if (cfg.bulk_build_hilbert_seeded) return .hilbert_seeded;
    if (cfg.bulk_build_doc_key_seeded) return .doc_key_seeded;
    if (cfg.bulk_build_kmeans) return .kmeans;
    return .recursive;
}

const TestPath = struct {
    buf: [256]u8 = undefined,

    fn init(self: *TestPath, split_algo: vec.ClustAlgorithm) [*:0]const u8 {
        const ts = nanotime();
        const slice = std.fmt.bufPrint(
            &self.buf,
            "/tmp/antfly-hbc-bench-{s}-{d}\x00",
            .{ @tagName(split_algo), ts },
        ) catch unreachable;
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch unreachable;
        return @ptrCast(slice.ptr);
    }

    fn cleanup(self: *TestPath) void {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(&self.buf)))) catch {};
    }
};

fn nanotime() u64 {
    _ = builtin;
    return platform_time.monotonicNs();
}
