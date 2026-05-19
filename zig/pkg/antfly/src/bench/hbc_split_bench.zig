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
const antfly = @import("antfly-zig");
const platform_time = antfly.platform_time;

const hbc = antfly.hbc;
const vec = antfly.vector;

const BenchConfig = struct {
    docs: usize = 512,
    dims: usize = 128,
    samples: usize = 5,
    seed: u64 = 42,
    split_algo: vec.ClustAlgorithm = .kmeans,
    bulk_build_algo: hbc.BulkBuildAlgo = .recursive,
    kmeans_backend: hbc.HBCConfig.KmeansBackend = .auto,
    kmeans_update_strategy: hbc.HBCConfig.KmeansUpdateStrategy = .auto,
    disable_reranking: bool = false,
    prefer_key_local_leaf_splits: bool = false,
    use_random_ortho_trans: bool = false,
};

const PhaseStats = struct {
    total: u64 = 0,
    min: u64 = std.math.maxInt(u64),
    max: u64 = 0,

    fn add(self: *PhaseStats, value: u64) void {
        self.total += value;
        self.min = @min(self.min, value);
        self.max = @max(self.max, value);
    }

    fn avg(self: PhaseStats, samples: usize) u64 {
        return if (samples == 0) 0 else @divTrunc(self.total, samples);
    }
};

pub fn main(init: std.process.Init) !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const alloc = gpa_state.allocator();

    const cfg = try parseArgs(init.minimal.args);
    const dataset = try makeDataset(alloc, cfg);
    defer alloc.free(dataset);

    var source_path = TestPath{};
    const source_dir = source_path.init("src");
    defer source_path.cleanup();

    var source = try hbc.HBCIndex.open(alloc, source_dir, .{
        .dims = @intCast(cfg.dims),
        .metric = .cosine,
        .split_algo = cfg.split_algo,
        .branching_factor = 7 * 24,
        .leaf_size = 7 * 24,
        .search_width = 2 * 3 * 7 * 24,
        .epsilon = 7,
        .use_quantization = true,
        .rerank_policy = if (cfg.disable_reranking) .never else .boundary,
        .quantizer_seed = cfg.seed,
        .use_random_ortho_trans = cfg.use_random_ortho_trans,
        .bulk_build_algo = cfg.bulk_build_algo,
        .kmeans_backend = cfg.kmeans_backend,
        .kmeans_update_strategy = cfg.kmeans_update_strategy,
        .prefer_key_local_leaf_splits = cfg.prefer_key_local_leaf_splits,
        .max_cached_nodes = 100_000,
        .max_cached_vectors = 100_000,
    });
    defer source.close();

    const source_build_ns = try buildSource(alloc, &source, dataset, cfg);

    var key_buf: [32]u8 = undefined;
    const split_key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{cfg.docs / 2});

    const collect_started = nanotime();
    var member_plan = try source.collectSplitMembers(split_key);
    defer member_plan.deinit(alloc);
    const collect_ns = nanotime() - collect_started;

    const planning = try source.splitPlanningStats(split_key);
    const work = try source.estimateSplitRebuildWork(split_key);

    const full_members = try joinMembers(alloc, member_plan.right_only_members, member_plan.mixed_right_members);
    defer alloc.free(full_members);

    var full_stats = PhaseStats{};
    var mixed_stats = PhaseStats{};
    for (0..cfg.samples) |sample| {
        full_stats.add(try benchChildBuild(alloc, cfg, full_members, &source, sample, "full"));
        mixed_stats.add(try benchChildBuild(alloc, cfg, member_plan.mixed_right_members, &source, sample, "mixed"));
    }

    std.debug.print(
        "HBC split bench docs={d} dims={d} samples={d} split_algo={s} bulk_build_algo={s} kmeans_backend={s} kmeans_update_strategy={s}\n",
        .{ cfg.docs, cfg.dims, cfg.samples, @tagName(cfg.split_algo), @tagName(cfg.bulk_build_algo), @tagName(cfg.kmeans_backend), @tagName(cfg.kmeans_update_strategy) },
    );
    std.debug.print(
        "source_build={d:.3}ms collect_members={d:.3}ms split_plan(left={d} right={d} mixed={d} unknown={d} leaves={d} internal={d}) right_members={d} mixed_right_members={d}\n",
        .{
            nsMs(source_build_ns),
            nsMs(collect_ns),
            planning.left_only,
            planning.right_only,
            planning.mixed,
            planning.unknown,
            planning.leaves,
            planning.internal,
            work.right_only_members,
            work.mixed_right_members,
        },
    );
    printPhase("child_full_bulk", full_stats, cfg.samples);
    printPhase("child_mixed_bulk", mixed_stats, cfg.samples);
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
        } else if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args, "--samples");
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = try parseNextU64(&args, "--seed");
        } else if (std.mem.eql(u8, arg, "--split-hilbert")) {
            cfg.split_algo = .hilbert;
        } else if (std.mem.eql(u8, arg, "--bulk-build-hilbert-seeded")) {
            cfg.bulk_build_algo = .hilbert_seeded;
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
        } else if (std.mem.eql(u8, arg, "--disable-reranking")) {
            cfg.disable_reranking = true;
        } else if (std.mem.eql(u8, arg, "--prefer-key-local-leaf-splits")) {
            cfg.prefer_key_local_leaf_splits = true;
        } else if (std.mem.eql(u8, arg, "--random-ortho")) {
            cfg.use_random_ortho_trans = true;
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.dims == 0 or cfg.samples == 0) return error.InvalidArgument;
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const raw = args.next() orelse return error.InvalidArgument;
    _ = flag;
    return try std.fmt.parseInt(usize, raw, 10);
}

fn parseNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const raw = args.next() orelse return error.InvalidArgument;
    _ = flag;
    return try std.fmt.parseInt(u64, raw, 10);
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

fn buildSource(
    alloc: std.mem.Allocator,
    source: *hbc.HBCIndex,
    dataset: []const f32,
    cfg: BenchConfig,
) !u64 {
    const items = try alloc.alloc(hbc.BatchInsertItem, cfg.docs);
    defer alloc.free(items);
    errdefer for (items[0..cfg.docs]) |item| if (item.metadata.len > 0) alloc.free(item.metadata);

    for (0..cfg.docs) |i| {
        const doc_key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{i});
        items[i] = .{
            .vector_id = @intCast(i + 1),
            .vector = dataset[i * cfg.dims ..][0..cfg.dims],
            .metadata = doc_key,
        };
    }
    defer for (items) |item| alloc.free(item.metadata);

    const started = nanotime();
    try source.bulkBuildWithMetadata(items);
    return nanotime() - started;
}

fn joinMembers(alloc: std.mem.Allocator, left: []const u64, right: []const u64) ![]u64 {
    const joined = try alloc.alloc(u64, left.len + right.len);
    @memcpy(joined[0..left.len], left);
    @memcpy(joined[left.len..], right);
    return joined;
}

fn benchChildBuild(
    alloc: std.mem.Allocator,
    cfg: BenchConfig,
    member_ids: []const u64,
    source: *hbc.HBCIndex,
    sample: usize,
    label: []const u8,
) !u64 {
    var tp = TestPath{};
    const path = tp.initChild(label, sample);
    defer tp.cleanup();

    var child = try hbc.HBCIndex.open(alloc, path, .{
        .dims = @intCast(cfg.dims),
        .metric = .cosine,
        .split_algo = cfg.split_algo,
        .branching_factor = 7 * 24,
        .leaf_size = 7 * 24,
        .search_width = 2 * 3 * 7 * 24,
        .epsilon = 7,
        .use_quantization = true,
        .rerank_policy = if (cfg.disable_reranking) .never else .boundary,
        .quantizer_seed = cfg.seed,
        .use_random_ortho_trans = true,
        .bulk_build_algo = cfg.bulk_build_algo,
        .kmeans_backend = cfg.kmeans_backend,
        .kmeans_update_strategy = cfg.kmeans_update_strategy,
        .prefer_key_local_leaf_splits = cfg.prefer_key_local_leaf_splits,
        .max_cached_nodes = 100_000,
        .max_cached_vectors = 100_000,
    });
    defer child.close();

    const started = nanotime();
    try child.bulkBuildMembersFrom(source, member_ids);
    return nanotime() - started;
}

fn printPhase(label: []const u8, stats: PhaseStats, samples: usize) void {
    std.debug.print(
        "{s}: avg={d:.3}ms min={d:.3}ms max={d:.3}ms\n",
        .{
            label,
            nsMs(stats.avg(samples)),
            nsMs(if (stats.min == std.math.maxInt(u64)) 0 else stats.min),
            nsMs(stats.max),
        },
    );
}

fn nsMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1e6;
}

const TestPath = struct {
    buf: [256]u8 = undefined,

    fn init(self: *TestPath, label: []const u8) [*:0]const u8 {
        const ts = nanotime();
        const slice = std.fmt.bufPrint(&self.buf, "/tmp/antfly-hbc-split-{s}-{d}\x00", .{ label, ts }) catch unreachable;
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
        return @ptrCast(slice.ptr);
    }

    fn initChild(self: *TestPath, label: []const u8, sample: usize) [*:0]const u8 {
        const ts = nanotime();
        const slice = std.fmt.bufPrint(&self.buf, "/tmp/antfly-hbc-split-{s}-{d}-{d}\x00", .{ label, sample, ts }) catch unreachable;
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
        return @ptrCast(slice.ptr);
    }

    fn cleanup(self: *TestPath) void {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(&self.buf)))) catch {};
    }
};

fn nanotime() u64 {
    return platform_time.monotonicNs();
}
