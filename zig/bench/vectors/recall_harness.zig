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
const common = @import("recall_common.zig");
const recall_cases = @import("recall_cases.zig");
const platform_time = antfly.platform_time;

const hbc = antfly.hbc;
const quantizer_mod = antfly.quantizer;
const vec = antfly.vector;
const vectorindex = antfly.vectorindex;
const search_mod = vectorindex.search;
const search_types = vectorindex.search_types;
const search_results = vectorindex.search_results;
const hbc_index_mod = vectorindex.hbc_index;

const Suite = enum {
    quantizer,
    hbc,
    both,
};

const Config = struct {
    dataset_dir: []const u8,
    suite: Suite = .both,
    dataset_filter: ?[]const u8 = null,
    bulk_build: bool = false,
    centroid_only_routing: bool = false,
    dump_query_index: ?usize = null,
    dump_metric: ?vec.DistanceMetric = null,
    dump_limit: usize = 30,
    dump_randomize: ?bool = null,
    per_query_metric: ?vec.DistanceMetric = null,
    per_query_only: bool = false,
};

const RecallCase = recall_cases.RecallCase;

pub fn main(init: std.process.Init) !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const alloc = gpa_state.allocator();

    const cfg = try parseArgs(init.minimal.args);
    var ok = true;
    if (cfg.per_query_metric != null and cfg.per_query_only) {
        _ = try runSuite(init.io, alloc, cfg, "hbc-per-query", &recall_cases.hbc_cases, runHBCCase);
        return;
    }
    if (cfg.suite == .quantizer or cfg.suite == .both) {
        ok = (try runSuite(init.io, alloc, cfg, "quantizer", &recall_cases.quantizer_cases, runQuantizerCase)) and ok;
    }
    if (cfg.suite == .hbc or cfg.suite == .both) {
        ok = (try runSuite(init.io, alloc, cfg, if (cfg.bulk_build) "hbc-bulk" else "hbc", &recall_cases.hbc_cases, runHBCCase)) and ok;
    }
    if (!ok) return error.RecallMismatch;
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{
        .dataset_dir = "",
    };
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--dataset-dir")) {
            cfg.dataset_dir = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--suite")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.suite = parseSuite(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--dataset")) {
            cfg.dataset_filter = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--bulk-build")) {
            cfg.bulk_build = true;
        } else if (std.mem.eql(u8, arg, "--centroid-only-routing")) {
            cfg.centroid_only_routing = true;
        } else if (std.mem.eql(u8, arg, "--dump-query-index")) {
            cfg.dump_query_index = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--dump-metric")) {
            cfg.dump_metric = parseMetric(args.next() orelse return error.InvalidArgument) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--dump-limit")) {
            cfg.dump_limit = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--dump-randomize")) {
            const raw = args.next() orelse return error.InvalidArgument;
            if (std.mem.eql(u8, raw, "true")) cfg.dump_randomize = true else if (std.mem.eql(u8, raw, "false")) cfg.dump_randomize = false else return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--per-query-metric")) {
            cfg.per_query_metric = parseMetric(args.next() orelse return error.InvalidArgument) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--per-query-only")) {
            cfg.per_query_only = true;
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.dataset_dir.len == 0) return error.InvalidArgument;
    if ((cfg.dump_query_index == null) != (cfg.dump_metric == null)) return error.InvalidArgument;
    if (cfg.per_query_only and cfg.per_query_metric == null) return error.InvalidArgument;
    return cfg;
}

fn parseSuite(raw: []const u8) ?Suite {
    if (std.mem.eql(u8, raw, "quantizer")) return .quantizer;
    if (std.mem.eql(u8, raw, "hbc")) return .hbc;
    if (std.mem.eql(u8, raw, "both")) return .both;
    return null;
}

fn parseMetric(raw: []const u8) ?vec.DistanceMetric {
    if (std.mem.eql(u8, raw, "l2_squared")) return .l2_squared;
    if (std.mem.eql(u8, raw, "inner_product")) return .inner_product;
    if (std.mem.eql(u8, raw, "cosine")) return .cosine;
    return null;
}

fn runSuite(
    io: std.Io,
    alloc: std.mem.Allocator,
    cfg: Config,
    suite_name: []const u8,
    cases: []const RecallCase,
    comptime runner: fn (std.Io, std.mem.Allocator, Config, []const u8, RecallCase) anyerror!common.MetricStats,
) !bool {
    var all_ok = true;
    for (cases) |case| {
        if (cfg.dataset_filter) |filter| {
            if (!std.mem.eql(u8, filter, case.dataset)) continue;
        }

        const dataset_path = try joinConvertedDatasetPath(alloc, cfg.dataset_dir, case.dataset);
        defer alloc.free(dataset_path);

        const actual = try runner(io, alloc, cfg, dataset_path, case);
        const case_ok = compareMetrics(case, actual);
        all_ok = all_ok and case_ok;

        std.debug.print(
            "{s} dataset={s} randomize={any} count={d} topk={d} recall(E={d:.2} IP={d:.2} C={d:.2}) expected(E={d:.2} IP={d:.2} C={d:.2}) {s}\n",
            .{
                suite_name,
                case.dataset,
                case.randomize,
                case.count,
                case.top_k,
                actual.euclidean,
                actual.inner_product,
                actual.cosine,
                case.expected.euclidean,
                case.expected.inner_product,
                case.expected.cosine,
                if (case_ok) "OK" else "MISMATCH",
            },
        );
    }
    return all_ok;
}

fn compareMetrics(case: RecallCase, actual: common.MetricStats) bool {
    return approxEq(actual.euclidean, case.expected.euclidean, case.tolerance) and
        approxEq(actual.inner_product, case.expected.inner_product, case.tolerance) and
        approxEq(actual.cosine, case.expected.cosine, case.tolerance);
}

fn approxEq(actual: f64, expected: f64, tolerance: f64) bool {
    return @abs(actual - expected) <= tolerance;
}

fn joinConvertedDatasetPath(alloc: std.mem.Allocator, dataset_dir: []const u8, gob_name: []const u8) ![]u8 {
    const converted = try convertedDatasetName(alloc, gob_name);
    defer alloc.free(converted);
    return std.fs.path.join(alloc, &.{ dataset_dir, converted });
}

fn convertedDatasetName(alloc: std.mem.Allocator, gob_name: []const u8) ![]u8 {
    if (std.mem.endsWith(u8, gob_name, ".gob")) {
        return std.fmt.allocPrint(alloc, "{s}.pbvec", .{gob_name[0 .. gob_name.len - 4]});
    }
    return std.fmt.allocPrint(alloc, "{s}.pbvec", .{gob_name});
}

fn runQuantizerCase(io: std.Io, alloc: std.mem.Allocator, cfg: Config, dataset_path: []const u8, case: RecallCase) !common.MetricStats {
    _ = cfg;
    var loaded = try common.loadVectorSet(io, alloc, dataset_path);
    defer loaded.deinit(alloc);

    var working = try common.cloneSet(alloc, loaded.asSet());
    defer working.deinit(alloc);

    const split = try common.splitDataset(working.asSet(), case.count);
    if (case.randomize) {
        try common.applyRandomTransformInPlace(alloc, split.data, 42);
        try common.applyRandomTransformInPlace(alloc, split.queries, 42);
    }

    return .{
        .euclidean = 100.0 * try calculateQuantizerRecallMetric(alloc, split, case.top_k, .l2_squared),
        .inner_product = 100.0 * try calculateQuantizerRecallMetric(alloc, split, case.top_k, .inner_product),
        .cosine = 100.0 * try calculateQuantizerRecallMetric(alloc, split, case.top_k, .cosine),
    };
}

fn calculateQuantizerRecallMetric(
    alloc: std.mem.Allocator,
    split: common.SplitDataset,
    top_k: usize,
    metric: vec.DistanceMetric,
) !f64 {
    var data_owned = try common.cloneSet(alloc, split.data);
    defer data_owned.deinit(alloc);
    var query_owned = try common.cloneSet(alloc, split.queries);
    defer query_owned.deinit(alloc);

    if (metric == .cosine) {
        common.normalizeSetInPlace(data_owned.asSet());
        common.normalizeSetInPlace(query_owned.asSet());
    }

    const data = data_owned.asSet();
    const queries = query_owned.asSet();
    const centroid = try common.computeCentroid(alloc, data);
    defer alloc.free(centroid);

    var quantizer = try quantizer_mod.RaBitQuantizer.init(alloc, data.dims, 42, metric);
    defer quantizer.deinit();

    var quantized = try quantizer.quantize(centroid, data.data, data.count);
    defer quantized.deinit(alloc);

    var scratch = try quantizer_mod.RaBitQuantizer.EstimateScratch.init(alloc, data.dims);
    defer scratch.deinit(alloc);

    const estimated = try alloc.alloc(f32, data.count);
    defer alloc.free(estimated);
    const error_bounds = try alloc.alloc(f32, data.count);
    defer alloc.free(error_bounds);
    const prediction = try alloc.alloc(usize, data.count);
    defer alloc.free(prediction);

    var recall_sum: f64 = 0;
    for (0..queries.count) |query_idx| {
        const query = queries.atConst(query_idx);
        try quantizer.estimateDistancesWithScratch(&quantized, query, estimated, error_bounds, &scratch);
        for (prediction, 0..) |*slot, i| slot.* = i;
        std.mem.sort(usize, prediction, commonDistanceCtx(estimated), distanceLessThan);

        const truth = try common.calculateTruth(alloc, top_k, metric, query, data);
        defer alloc.free(truth);
        recall_sum += common.calculateRecall(prediction[0..top_k], truth);
    }
    return recall_sum / @as(f64, @floatFromInt(queries.count));
}

fn runHBCCase(io: std.Io, alloc: std.mem.Allocator, cfg: Config, dataset_path: []const u8, case: RecallCase) !common.MetricStats {
    var loaded = try common.loadVectorSet(io, alloc, dataset_path);
    defer loaded.deinit(alloc);

    const loaded_set = loaded.asSet();
    const split = try common.splitDataset(loaded_set, case.count);

    if (cfg.dump_query_index) |query_index| {
        if (cfg.dump_randomize) |want_randomize| {
            if (case.randomize != want_randomize) {
                return .{
                    .euclidean = 100.0 * try calculateHBCRecallMetric(alloc, split, case.top_k, case.randomize, .l2_squared, cfg.bulk_build, cfg.centroid_only_routing),
                    .inner_product = 100.0 * try calculateHBCRecallMetric(alloc, split, case.top_k, case.randomize, .inner_product, cfg.bulk_build, cfg.centroid_only_routing),
                    .cosine = 100.0 * try calculateHBCRecallMetric(alloc, split, case.top_k, case.randomize, .cosine, cfg.bulk_build, cfg.centroid_only_routing),
                };
            }
        }
        const metric = cfg.dump_metric orelse return error.InvalidArgument;
        std.debug.print(
            "frontier_dump dataset={s} randomize={any} metric={s} query_index={d}\n",
            .{ dataset_path, case.randomize, @tagName(metric), query_index },
        );
        try dumpHBCFrontier(alloc, split, case.top_k, case.randomize, metric, cfg.bulk_build, cfg.centroid_only_routing, query_index, cfg.dump_limit);
    }

    if (cfg.per_query_metric) |metric| {
        if (cfg.dump_randomize == null or case.randomize == cfg.dump_randomize.?) {
            try dumpHBCPerQueryRecall(alloc, split, case.top_k, case.randomize, metric, cfg.bulk_build, cfg.centroid_only_routing, dataset_path);
        }
    }

    return .{
        .euclidean = 100.0 * try calculateHBCRecallMetric(alloc, split, case.top_k, case.randomize, .l2_squared, cfg.bulk_build, cfg.centroid_only_routing),
        .inner_product = 100.0 * try calculateHBCRecallMetric(alloc, split, case.top_k, case.randomize, .inner_product, cfg.bulk_build, cfg.centroid_only_routing),
        .cosine = 100.0 * try calculateHBCRecallMetric(alloc, split, case.top_k, case.randomize, .cosine, cfg.bulk_build, cfg.centroid_only_routing),
    };
}

fn calculateHBCRecallMetric(
    alloc: std.mem.Allocator,
    split: common.SplitDataset,
    top_k: usize,
    randomize: bool,
    metric: vec.DistanceMetric,
    bulk_build: bool,
    centroid_only_routing: bool,
) !f64 {
    var built = try buildHBCIndex(alloc, split, randomize, metric, bulk_build, centroid_only_routing);
    defer built.deinit();
    const data = built.data();
    const queries = built.queries();

    var recall_sum: f64 = 0;
    for (0..queries.count) |query_idx| {
        const query = queries.atConst(query_idx);
        var results = try built.idx.search(query, top_k);
        defer results.deinit();

        const hits = results.getHits();
        const prediction = try alloc.alloc(usize, @min(top_k, hits.len));
        defer alloc.free(prediction);
        for (prediction, 0..) |*slot, i| {
            slot.* = @intCast(hits[i].vector_id - 1);
        }

        const truth = try common.calculateTruth(alloc, top_k, metric, query, data);
        defer alloc.free(truth);
        recall_sum += common.calculateRecall(prediction, truth);
    }
    return recall_sum / @as(f64, @floatFromInt(queries.count));
}

const BuiltHBC = struct {
    tp: TestPath,
    data_owned: common.OwnedVectorSet,
    query_owned: common.OwnedVectorSet,
    idx: hbc.HBCIndex,

    fn deinit(self: *BuiltHBC) void {
        const alloc = self.idx.alloc;
        self.idx.close();
        self.tp.cleanup();
        self.query_owned.deinit(alloc);
        self.data_owned.deinit(alloc);
        self.* = undefined;
    }

    fn data(self: *BuiltHBC) vec.Set {
        return self.data_owned.asSet();
    }

    fn queries(self: *BuiltHBC) vec.Set {
        return self.query_owned.asSet();
    }
};

fn buildHBCIndex(
    alloc: std.mem.Allocator,
    split: common.SplitDataset,
    randomize: bool,
    metric: vec.DistanceMetric,
    bulk_build: bool,
    centroid_only_routing: bool,
) !BuiltHBC {
    var data_owned = try common.cloneSet(alloc, split.data);
    errdefer data_owned.deinit(alloc);
    var query_owned = try common.cloneSet(alloc, split.queries);
    errdefer query_owned.deinit(alloc);

    if (metric == .cosine) {
        common.normalizeSetInPlace(data_owned.asSet());
        common.normalizeSetInPlace(query_owned.asSet());
    }

    var tp = TestPath{};
    const path = tp.init();

    var idx = try hbc.HBCIndex.open(alloc, path, .{
        .dims = @intCast(data_owned.dims),
        .metric = metric,
        .split_algo = .kmeans,
        .branching_factor = 7 * 24,
        .leaf_size = 7 * 24,
        .search_width = 3 * 7 * 24,
        .epsilon = 1.6,
        .use_quantization = true,
        .rerank_policy = .always,
        .quantizer_seed = 42,
        .use_random_ortho_trans = randomize,
        .max_cached_nodes = 10_000,
        .max_cached_vectors = 10_000,
    });
    errdefer idx.close();

    const data = data_owned.asSet();
    const items = try alloc.alloc(hbc.BatchInsertItem, data.count);
    defer {
        for (items) |item| alloc.free(item.metadata);
        alloc.free(items);
    }
    for (0..data.count) |i| {
        items[i] = .{
            .vector_id = @intCast(i + 1),
            .vector = data.atConst(i),
            .metadata = try std.fmt.allocPrint(alloc, "data_{d}", .{i + 1}),
        };
    }
    if (bulk_build) {
        try idx.bulkBuildWithMetadata(items);
    } else {
        try idx.batchInsertWithMetadataOptions(items, .{ .centroid_only_routing = centroid_only_routing });
    }

    return .{
        .tp = tp,
        .data_owned = data_owned,
        .query_owned = query_owned,
        .idx = idx,
    };
}

fn dumpHBCFrontier(
    alloc: std.mem.Allocator,
    split: common.SplitDataset,
    top_k: usize,
    randomize: bool,
    metric: vec.DistanceMetric,
    bulk_build: bool,
    centroid_only_routing: bool,
    query_index: usize,
    dump_limit: usize,
) !void {
    var built = try buildHBCIndex(alloc, split, randomize, metric, bulk_build, centroid_only_routing);
    defer built.deinit();

    const data = built.data();
    const queries = built.queries();
    if (query_index >= queries.count) return error.InvalidArgument;
    const query = queries.atConst(query_index);
    const truth = try common.calculateTruth(alloc, top_k, metric, query, data);
    defer alloc.free(truth);

    try debugGlobalApprox(alloc, &built.idx, query, top_k, truth, dump_limit);
}

fn debugGlobalApprox(
    alloc: std.mem.Allocator,
    idx: *hbc.HBCIndex,
    query: []const f32,
    top_k: usize,
    truth: []const usize,
    dump_limit: usize,
) !void {
    var txn = try idx.beginRuntimeReadTxn();
    defer txn.abort();

    var filter_state = try search_types.RequestFilterState.init(alloc, .{
        .query = query,
        .k = top_k,
    });
    defer filter_state.deinit(alloc);

    var scratch_handle = try idx.acquireSearchScratch();
    defer idx.releaseSearchScratch(&scratch_handle);
    const scratch = &scratch_handle.scratch;

    const transformed_query = idx.transformVector(query, scratch.transformed_query);
    const transformed_query_measure: f32 = switch (idx.config.metric) {
        .l2_squared => vec.dot(query, query),
        .cosine => vec.norm(transformed_query),
        .inner_product => 0,
    };
    const exact_query_measure: f32 = switch (idx.config.metric) {
        .l2_squared => vec.dot(query, query),
        .cosine => vec.norm(query),
        .inner_product => 0,
    };

    const req = search_types.SearchRequest{
        .query = query,
        .k = top_k,
    };
    const search_width = idx.config.search_width;
    const epsilon = idx.config.epsilon;
    const rerank_factor: usize = search_mod.rerankFactor(epsilon);
    const candidate_limit: usize = top_k * rerank_factor;
    const candidate_capacity: usize = search_mod.candidateCapacity(search_width, idx.metadata.branching_factor);

    var candidates = std.PriorityQueue(hbc.PriorityItem, void, search_types.candidateLessThan).initContext({});
    defer candidates.deinit(alloc);
    try candidates.ensureTotalCapacity(alloc, candidate_capacity);

    var approx_results = try search_results.ApproxSearchResults.initCapacity(alloc, top_k, candidate_limit, candidate_limit);
    defer approx_results.deinit();

    var profile: search_types.SearchProfile = .{};
    const root = try idx.getNodePtrProfiled(&txn, idx.metadata.root_node, &profile);
    if (root.is_leaf) {
        try hbc_index_mod.scoreLeafMembers(idx, &txn, root, transformed_query, transformed_query_measure, query, exact_query_measure, req, &filter_state, &approx_results, scratch, &profile, nowNsU64Fixed, elapsedSinceU64Fixed);
    } else {
        try hbc_index_mod.addChildCandidates(idx, &txn, root, transformed_query, transformed_query_measure, &candidates, scratch, &profile, nowNsU64Fixed, elapsedSinceU64Fixed);
        var beam_state = search_mod.BeamSearchState{};
        while (true) {
            const candidate = candidates.pop() orelse break;
            if (search_mod.shouldStopBeamSearch(&beam_state, search_width)) break;
            const node = idx.getNodePtrProfiled(&txn, candidate.id, &profile) catch continue;
            if (!node.is_leaf and search_mod.shouldBreakOnInternalCandidate(candidate, &approx_results)) break;
            if (!node.is_leaf and search_mod.shouldSkipInternalCandidate(candidate, &approx_results, &beam_state, epsilon)) continue;
            if (node.is_leaf) {
                if (search_mod.shouldSkipLeafCandidate(candidate, &approx_results, &beam_state, epsilon)) continue;
                try hbc_index_mod.scoreLeafMembers(idx, &txn, node, transformed_query, transformed_query_measure, query, exact_query_measure, req, &filter_state, &approx_results, scratch, &profile, nowNsU64Fixed, elapsedSinceU64Fixed);
                search_mod.noteLeafExplored(&beam_state);
            } else {
                try hbc_index_mod.addChildCandidates(idx, &txn, node, transformed_query, transformed_query_measure, &candidates, scratch, &profile, nowNsU64Fixed, elapsedSinceU64Fixed);
            }
        }
    }

    approx_results.sort();
    const ranked = approx_results.items.items;
    std.debug.print("global_approx retained={d} cutoff_rank={d} rerank_factor={d} search_width={d}\n", .{
        ranked.len,
        candidate_limit,
        rerank_factor,
        search_width,
    });
    for (ranked[0..@min(dump_limit, ranked.len)], 0..) |item, i| {
        std.debug.print("approx[{d:0>2}] id={d} dist={d:.6} err={d:.6}\n", .{ i + 1, item.vector_id, item.distance, item.error_bound });
    }
    for (truth, 0..) |offset, i| {
        const vector_id: u64 = @intCast(offset + 1);
        var found_rank: ?usize = null;
        for (ranked, 0..) |item, rank_idx| {
            if (item.vector_id == vector_id) {
                found_rank = rank_idx + 1;
                std.debug.print("truth[{d}] id={d} retained_rank={d} approx={d:.6} err={d:.6}\n", .{
                    i,
                    vector_id,
                    found_rank.?,
                    item.distance,
                    item.error_bound,
                });
                break;
            }
        }
        if (found_rank == null) std.debug.print("truth[{d}] id={d} retained_rank=<dropped>\n", .{ i, vector_id });
    }
}

fn dumpHBCPerQueryRecall(
    alloc: std.mem.Allocator,
    split: common.SplitDataset,
    top_k: usize,
    randomize: bool,
    metric: vec.DistanceMetric,
    bulk_build: bool,
    centroid_only_routing: bool,
    dataset_path: []const u8,
) !void {
    var built = try buildHBCIndex(alloc, split, randomize, metric, bulk_build, centroid_only_routing);
    defer built.deinit();

    const data = built.data();
    const queries = built.queries();
    std.debug.print(
        "per_query dataset={s} randomize={any} metric={s} queries={d}\n",
        .{ dataset_path, randomize, @tagName(metric), queries.count },
    );
    for (0..queries.count) |query_idx| {
        const query = queries.atConst(query_idx);
        var results = try built.idx.search(query, top_k);
        defer results.deinit();
        const hits = results.getHits();
        const prediction = try alloc.alloc(usize, @min(top_k, hits.len));
        defer alloc.free(prediction);
        for (prediction, 0..) |*slot, i| {
            slot.* = @intCast(hits[i].vector_id - 1);
        }
        const truth = try common.calculateTruth(alloc, top_k, metric, query, data);
        defer alloc.free(truth);
        const recall = common.calculateRecall(prediction, truth);
        std.debug.print("query={d} recall={d:.4} pred=", .{ query_idx, recall });
        for (prediction) |id| std.debug.print("{d},", .{id + 1});
        std.debug.print(" truth=", .{});
        for (truth) |id| std.debug.print("{d},", .{id + 1});
        std.debug.print("\n", .{});
    }
}

const DistanceCtx = struct {
    distances: []const f32,
};

fn commonDistanceCtx(distances: []const f32) DistanceCtx {
    return .{ .distances = distances };
}

fn distanceLessThan(ctx: DistanceCtx, lhs: usize, rhs: usize) bool {
    if (ctx.distances[lhs] != ctx.distances[rhs]) return ctx.distances[lhs] < ctx.distances[rhs];
    return lhs < rhs;
}

const TestPath = struct {
    buf: [256]u8 = undefined,

    fn init(self: *TestPath) [*:0]const u8 {
        const ts = tempPathId();
        const slice = std.fmt.bufPrint(&self.buf, "/tmp/antfly-hbc-recall-{d}\x00", .{ts}) catch unreachable;
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

fn tempPathId() u64 {
    return platform_time.monotonicNs();
}

fn nowNsU64Fixed() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSinceU64Fixed(start_ns: u64) u64 {
    return platform_time.monotonicNs() - start_ns;
}
