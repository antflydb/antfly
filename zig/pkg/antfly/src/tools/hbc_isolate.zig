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

const std = @import("std");
const builtin = @import("builtin");

const antfly = @import("antfly_hbc_isolate_root");
const hbc = antfly.hbc;
const vec = antfly.vector;

const Config = struct {
    const Dataset = enum { clustered, random };
    const QuerySource = enum { sample, random };

    docs: usize = 2048,
    dims: usize = 128,
    queries: usize = 25,
    k: usize = 10,
    repeats: usize = 10,
    seed: u64 = 42,
    query_seed: u64 = 69,
    metric: vec.DistanceMetric = .cosine,
    dataset: Dataset = .clustered,
    query_source: QuerySource = .sample,
    storage_backend: hbc.StorageBackend = .lsm,
    rerank_policy: hbc.HBCConfig.RerankPolicy = .boundary,
    use_random_ortho_trans: bool = false,
    search_width: u32 = 2 * 3 * 7 * 24,
    epsilon: f32 = 7,
    search_effort: ?f32 = 0.5,
    defer_page_mutation: bool = false,
};

const Result = struct {
    insert_ns: u64,
    insert_find_leaf_ns: u64,
    insert_mutate_ns: u64,
    insert_quant_ns: u64,
    insert_commit_ns: u64,
    split_leaf_ns: u64,
    resolved_search_width: u32,
    resolved_epsilon: f32,
    search_ns: u64,
    leaf_ns: u64,
    rerank_ns: u64,
    nodes: u64,
    leaves: u64,
    reranked: u64,
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

    const result = try runBench(alloc, cfg, dataset, queries);
    std.debug.print(
        "zig_hbc docs={d} dims={d} queries={d} k={d} repeats={d} effort={d:.3} search_width={d} epsilon={d:.3} rot={any} rerank={s} insert={d:.3}ms search={d:.3}us leaf={d:.3}us rerank={d:.3}us nodes={d} leaves={d} reranked={d}\n",
        .{
            cfg.docs,
            cfg.dims,
            cfg.queries,
            cfg.k,
            cfg.repeats,
            cfg.search_effort orelse -1.0,
            result.resolved_search_width,
            result.resolved_epsilon,
            cfg.use_random_ortho_trans,
            @tagName(cfg.rerank_policy),
            @as(f64, @floatFromInt(result.insert_ns)) / 1e6,
            @as(f64, @floatFromInt(result.search_ns)) / 1e3,
            @as(f64, @floatFromInt(result.leaf_ns)) / 1e3,
            @as(f64, @floatFromInt(result.rerank_ns)) / 1e3,
            result.nodes,
            result.leaves,
            result.reranked,
        },
    );
    std.debug.print(
        "zig_hbc_write find_leaf={d:.3}ms mutate={d:.3}ms quant={d:.3}ms split_leaf={d:.3}ms commit={d:.3}ms\n",
        .{
            @as(f64, @floatFromInt(result.insert_find_leaf_ns)) / 1e6,
            @as(f64, @floatFromInt(result.insert_mutate_ns)) / 1e6,
            @as(f64, @floatFromInt(result.insert_quant_ns)) / 1e6,
            @as(f64, @floatFromInt(result.split_leaf_ns)) / 1e6,
            @as(f64, @floatFromInt(result.insert_commit_ns)) / 1e6,
        },
    );
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
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
        } else if (std.mem.eql(u8, arg, "--repeats")) {
            cfg.repeats = try parseNextUsize(&args, "--repeats");
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = try parseNextU64(&args, "--seed");
        } else if (std.mem.eql(u8, arg, "--query-seed")) {
            cfg.query_seed = try parseNextU64(&args, "--query-seed");
        } else if (std.mem.eql(u8, arg, "--metric")) {
            const raw = args.next() orelse {
                std.debug.print("missing value for --metric\n", .{});
                return error.InvalidArgument;
            };
            cfg.metric = parseMetric(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--dataset")) {
            const raw = args.next() orelse {
                std.debug.print("missing value for --dataset\n", .{});
                return error.InvalidArgument;
            };
            cfg.dataset = parseDataset(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--query-source")) {
            const raw = args.next() orelse {
                std.debug.print("missing value for --query-source\n", .{});
                return error.InvalidArgument;
            };
            cfg.query_source = parseQuerySource(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--storage-backend")) {
            const raw = args.next() orelse {
                std.debug.print("missing value for --storage-backend\n", .{});
                return error.InvalidArgument;
            };
            cfg.storage_backend = parseStorageBackend(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--search-width")) {
            cfg.search_width = try parseNextU32(&args, "--search-width");
        } else if (std.mem.eql(u8, arg, "--epsilon")) {
            cfg.epsilon = try parseNextF32(&args, "--epsilon");
        } else if (std.mem.eql(u8, arg, "--search-effort")) {
            cfg.search_effort = normalizedSearchEffort(try parseNextF32(&args, "--search-effort"));
        } else if (std.mem.eql(u8, arg, "--no-search-effort")) {
            cfg.search_effort = null;
        } else if (std.mem.eql(u8, arg, "--rerank-policy")) {
            const raw = args.next() orelse {
                std.debug.print("missing value for --rerank-policy\n", .{});
                return error.InvalidArgument;
            };
            cfg.rerank_policy = parseRerankPolicy(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--random-ortho")) {
            cfg.use_random_ortho_trans = true;
        } else if (std.mem.eql(u8, arg, "--no-random-ortho")) {
            cfg.use_random_ortho_trans = false;
        } else if (std.mem.eql(u8, arg, "--defer-page-mutation")) {
            cfg.defer_page_mutation = true;
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.dims == 0 or cfg.queries == 0 or cfg.k == 0 or cfg.repeats == 0 or cfg.search_width == 0) {
        return error.InvalidArgument;
    }
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

fn parseNextU32(args: *std.process.Args.Iterator, flag: []const u8) !u32 {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(u32, raw, 10);
}

fn parseNextF32(args: *std.process.Args.Iterator, flag: []const u8) !f32 {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseFloat(f32, raw);
}

fn parseMetric(raw: []const u8) ?vec.DistanceMetric {
    if (std.mem.eql(u8, raw, "cosine")) return .cosine;
    if (std.mem.eql(u8, raw, "l2_squared")) return .l2_squared;
    if (std.mem.eql(u8, raw, "inner_product")) return .inner_product;
    return null;
}

fn parseDataset(raw: []const u8) ?Config.Dataset {
    if (std.mem.eql(u8, raw, "clustered")) return .clustered;
    if (std.mem.eql(u8, raw, "random")) return .random;
    return null;
}

fn parseQuerySource(raw: []const u8) ?Config.QuerySource {
    if (std.mem.eql(u8, raw, "sample")) return .sample;
    if (std.mem.eql(u8, raw, "random")) return .random;
    return null;
}

fn parseStorageBackend(raw: []const u8) ?hbc.StorageBackend {
    if (std.mem.eql(u8, raw, "lmdb")) return .lmdb;
    if (std.mem.eql(u8, raw, "lsm")) return .lsm;
    return null;
}

fn parseRerankPolicy(raw: []const u8) ?hbc.HBCConfig.RerankPolicy {
    if (std.mem.eql(u8, raw, "always")) return .always;
    if (std.mem.eql(u8, raw, "boundary")) return .boundary;
    if (std.mem.eql(u8, raw, "never")) return .never;
    return null;
}

fn normalizedSearchEffort(effort: f32) f32 {
    if (std.math.isNan(effort)) return 0.5;
    if (effort < 0) return 0;
    if (effort > 1) return 1;
    return effort;
}

fn estimateLeafCount(stats: hbc.IndexStats) u32 {
    if (stats.active_count == 0) return 0;
    const leaf_size = @max(stats.leaf_size, 1);
    const estimated = (stats.active_count + leaf_size - 1) / leaf_size;
    return @intCast(@min(estimated, @as(u64, std.math.maxInt(u32))));
}

fn resolveSearchWidth(k: u32, effort: f32, stats: hbc.IndexStats) u32 {
    const default_balanced_search_effort: f32 = 0.5;
    const min_width = @max(k, @as(u32, 64));
    const legacy_max_width = @max(min_width * 20, @as(u32, 4096));
    const legacy_balanced_width = min_width + @as(u32, @intFromFloat(@as(f32, @floatFromInt(legacy_max_width - min_width)) * default_balanced_search_effort));
    const estimated_leaf_count = estimateLeafCount(stats);
    const max_width = if (estimated_leaf_count > 0)
        @max(min_width, @max(estimated_leaf_count, if (stats.node_count > 0 and stats.node_count <= std.math.maxInt(u32)) @as(u32, @intCast(stats.node_count)) else estimated_leaf_count))
    else if (stats.node_count > legacy_max_width and stats.node_count <= std.math.maxInt(u32))
        @as(u32, @intCast(stats.node_count))
    else
        legacy_max_width;
    const balanced_cap = if (estimated_leaf_count > 0) @max(min_width, estimated_leaf_count) else max_width;
    const leaf_balanced_width = min_width + @as(u32, @intFromFloat(@as(f32, @floatFromInt(balanced_cap - min_width)) * default_balanced_search_effort));
    const balanced_width = @min(legacy_balanced_width, leaf_balanced_width);

    if (effort <= default_balanced_search_effort) {
        if (balanced_width <= min_width) return min_width;
        const ratio = effort / default_balanced_search_effort;
        return min_width + @as(u32, @intFromFloat(@as(f32, @floatFromInt(balanced_width - min_width)) * ratio));
    }

    if (max_width <= balanced_width) return max_width;
    const ratio = (effort - default_balanced_search_effort) / (1 - default_balanced_search_effort);
    const width = balanced_width + @as(u32, @intFromFloat(@as(f32, @floatFromInt(max_width - balanced_width)) * ratio));
    return @min(width, max_width);
}

fn resolveSearchEpsilon(effort: f32) f32 {
    if (effort < 0.5) {
        return 1.0 + (effort * 12.0);
    }
    return 7.0 + ((effort - 0.5) * 186.0);
}

fn makeDataset(alloc: std.mem.Allocator, cfg: Config) ![]f32 {
    const data = try alloc.alloc(f32, cfg.docs * cfg.dims);
    switch (cfg.dataset) {
        .clustered => {
            for (0..cfg.docs) |doc_idx| {
                const cluster = @as(f32, @floatFromInt(doc_idx % 8)) * 0.25;
                for (0..cfg.dims) |dim_idx| {
                    data[doc_idx * cfg.dims + dim_idx] = cluster + deterministicNoise(cfg.seed, doc_idx, dim_idx);
                }
                if (cfg.metric == .cosine) {
                    _ = vec.normalize(data[doc_idx * cfg.dims ..][0..cfg.dims]);
                }
            }
        },
        .random => {
            var prng = std.Random.DefaultPrng.init(cfg.seed);
            const random = prng.random();
            for (0..cfg.docs) |doc_idx| {
                const slot = data[doc_idx * cfg.dims ..][0..cfg.dims];
                for (slot) |*value| value.* = random.float(f32);
                if (cfg.metric == .cosine) _ = vec.normalize(slot);
            }
        },
    }
    return data;
}

fn makeQueries(alloc: std.mem.Allocator, dataset: []const f32, cfg: Config) ![]f32 {
    const queries = try alloc.alloc(f32, cfg.queries * cfg.dims);
    switch (cfg.query_source) {
        .sample => {
            for (0..cfg.queries) |i| {
                const src_idx = (i * 997) % cfg.docs;
                @memcpy(
                    queries[i * cfg.dims ..][0..cfg.dims],
                    dataset[src_idx * cfg.dims ..][0..cfg.dims],
                );
            }
        },
        .random => {
            var prng = std.Random.DefaultPrng.init(cfg.query_seed);
            const random = prng.random();
            for (0..cfg.queries) |i| {
                const slot = queries[i * cfg.dims ..][0..cfg.dims];
                for (slot) |*value| value.* = random.float(f32);
                if (cfg.metric == .cosine) _ = vec.normalize(slot);
            }
        },
    }
    return queries;
}

fn deterministicNoise(seed: u64, doc_idx: usize, dim_idx: usize) f32 {
    var x = seed ^
        (@as(u64, @intCast(doc_idx + 1)) *% 0x9E3779B97F4A7C15) ^
        (@as(u64, @intCast(dim_idx + 1)) *% 0xC2B2AE3D27D4EB4F);
    x ^= x >> 33;
    x *%= 0xFF51AFD7ED558CCD;
    x ^= x >> 33;
    x *%= 0xC4CEB9FE1A85EC53;
    x ^= x >> 33;
    const scaled = @as(f32, @floatFromInt(x & 1023)) / 1024.0;
    return scaled * 0.01;
}

fn runBench(alloc: std.mem.Allocator, cfg: Config, dataset: []const f32, queries: []const f32) !Result {
    var tp = TestPath{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try hbc.HBCIndex.open(alloc, path, .{
        .storage_backend = cfg.storage_backend,
        .dims = @intCast(cfg.dims),
        .metric = cfg.metric,
        .split_algo = .kmeans,
        .branching_factor = 7 * 24,
        .leaf_size = 7 * 24,
        .search_width = cfg.search_width,
        .epsilon = cfg.epsilon,
        .use_quantization = true,
        .rerank_policy = cfg.rerank_policy,
        .quantizer_seed = cfg.seed,
        .use_random_ortho_trans = cfg.use_random_ortho_trans,
        .max_cached_nodes = 100_000,
        .max_cached_vectors = 100_000,
        .defer_page_mutation = cfg.defer_page_mutation,
    });
    defer idx.close();

    const items = try alloc.alloc(hbc.BatchInsertItem, cfg.docs);
    defer {
        for (items) |item| alloc.free(item.metadata);
        alloc.free(items);
    }
    for (0..cfg.docs) |i| {
        items[i] = .{
            .vector_id = @intCast(i + 1),
            .vector = dataset[i * cfg.dims ..][0..cfg.dims],
            .metadata = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{i}),
        };
    }

    const insert_start = nowNs();
    try idx.batchInsertWithMetadata(items);
    const insert_ns = elapsedSince(insert_start);
    const write_profile = idx.getWriteProfile();
    const resolved_search_width = if (cfg.search_effort) |effort|
        resolveSearchWidth(@intCast(cfg.k), effort, idx.stats())
    else
        cfg.search_width;
    const resolved_epsilon = if (cfg.search_effort) |effort|
        resolveSearchEpsilon(effort)
    else
        cfg.epsilon;

    var search_total: u64 = 0;
    var leaf_total: u64 = 0;
    var rerank_total: u64 = 0;
    var nodes_total: u64 = 0;
    var leaves_total: u64 = 0;
    var reranked_total: u64 = 0;
    const total = cfg.queries * cfg.repeats;

    for (0..cfg.queries) |i| {
        var warm = try idx.searchProfiledRequest(.{
            .query = queries[i * cfg.dims ..][0..cfg.dims],
            .k = cfg.k,
            .search_width = resolved_search_width,
            .epsilon = resolved_epsilon,
            .load_metadata = false,
        });
        warm.results.deinit();
    }
    for (0..cfg.repeats) |_| {
        for (0..cfg.queries) |i| {
            const query = queries[i * cfg.dims ..][0..cfg.dims];
            const start = nowNs();
            var profiled = try idx.searchProfiledRequest(.{
                .query = query,
                .k = cfg.k,
                .search_width = resolved_search_width,
                .epsilon = resolved_epsilon,
                .load_metadata = false,
            });
            defer profiled.results.deinit();
            search_total += elapsedSince(start);
            leaf_total += profiled.profile.leaf_score_ns;
            rerank_total += profiled.profile.rerank_ns;
            nodes_total += profiled.profile.nodes_visited;
            leaves_total += profiled.profile.leaves_explored;
            reranked_total += profiled.profile.reranked_vectors;
            std.mem.doNotOptimizeAway(profiled.results.items.items.len);
        }
    }

    return .{
        .insert_ns = insert_ns,
        .insert_find_leaf_ns = write_profile.insert_find_leaf_ns,
        .insert_mutate_ns = write_profile.insert_mutate_leaf_ns,
        .insert_quant_ns = write_profile.refresh_quantized_ns,
        .insert_commit_ns = write_profile.insert_commit_ns,
        .split_leaf_ns = write_profile.split_leaf_ns,
        .resolved_search_width = resolved_search_width,
        .resolved_epsilon = resolved_epsilon,
        .search_ns = @divTrunc(search_total, total),
        .leaf_ns = @divTrunc(leaf_total, total),
        .rerank_ns = @divTrunc(rerank_total, total),
        .nodes = @divTrunc(nodes_total, total),
        .leaves = @divTrunc(leaves_total, total),
        .reranked = @divTrunc(reranked_total, total),
    };
}

const TestPath = struct {
    buf: [256]u8 = undefined,

    fn init(self: *TestPath) [*:0]const u8 {
        const ts = nowNs();
        const slice = std.fmt.bufPrint(&self.buf, "/tmp/antfly-hbc-isolate-{d}\x00", .{ts}) catch unreachable;
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

fn nowNs() u64 {
    if (builtin.os.tag == .freestanding) return 0;
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => {},
        else => unreachable,
    }
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn elapsedSince(start_ns: u64) u64 {
    const end_ns = nowNs();
    if (end_ns <= start_ns) return 0;
    return end_ns - start_ns;
}
