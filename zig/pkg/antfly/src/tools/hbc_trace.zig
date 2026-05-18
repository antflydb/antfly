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
const common = @import("recall_common");
const platform_time = antfly.platform_time;

const hbc = antfly.hbc;
const vec = antfly.vector;
const vectorindex = antfly.vectorindex;
const search_mod = vectorindex.search;
const search_types = vectorindex.search_types;
const search_results = vectorindex.search_results;
const hbc_index_mod = vectorindex.hbc_index;

const Config = struct {
    input: []const u8 = "",
    count: usize = 1000,
    query_index: usize = 0,
    top_k: usize = 10,
    metric: vec.DistanceMetric = .l2_squared,
    randomize: bool = false,
    use_quantization: bool = true,
    epsilon: f32 = 1.6,
};

pub fn main(init: std.process.Init) !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const alloc = gpa_state.allocator();

    const cfg = try parseArgs(init.minimal.args);

    var loaded = try common.loadVectorSet(init.io, alloc, cfg.input);
    defer loaded.deinit(alloc);

    var owned = try common.cloneSet(alloc, loaded.asSet());
    defer owned.deinit(alloc);

    const split = try common.splitDataset(owned.asSet(), cfg.count);
    if (cfg.query_index >= split.queries.count) return error.InvalidArgument;

    if (cfg.metric == .cosine) {
        common.normalizeSetInPlace(split.data);
        common.normalizeSetInPlace(split.queries);
    }
    var tp = TestPath{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try hbc.HBCIndex.open(alloc, path, .{
        .dims = @intCast(split.data.dims),
        .metric = cfg.metric,
        .split_algo = .kmeans,
        .branching_factor = 7 * 24,
        .leaf_size = 7 * 24,
        .search_width = 3 * 7 * 24,
        .epsilon = cfg.epsilon,
        .use_quantization = cfg.use_quantization,
        .rerank_policy = .boundary,
        .quantizer_seed = 42,
        .use_random_ortho_trans = cfg.randomize,
        .max_cached_nodes = 10_000,
        .max_cached_vectors = 10_000,
    });
    defer idx.close();

    const items = try alloc.alloc(hbc.BatchInsertItem, split.data.count);
    defer {
        for (items) |item| alloc.free(item.metadata);
        alloc.free(items);
    }
    for (0..split.data.count) |i| {
        items[i] = .{
            .vector_id = @intCast(i + 1),
            .vector = split.data.atConst(i),
            .metadata = try std.fmt.allocPrint(alloc, "data_{d}", .{i + 1}),
        };
    }
    try idx.batchInsertWithMetadata(items);

    const query = split.queries.atConst(cfg.query_index);
    const truth = try common.calculateTruth(alloc, cfg.top_k, cfg.metric, query, split.data);
    defer alloc.free(truth);

    var profiled = try idx.searchProfiledRequest(.{
        .query = query,
        .k = cfg.top_k,
        .load_metadata = false,
    });
    defer profiled.results.deinit();
    const hits = profiled.results.getHits();

    std.debug.print(
        "zig_hbc_trace input={s} metric={s} randomize={any} quantized={any} epsilon={d:.2} count={d} data={d} queries={d} query_index={d} topk={d}\n",
        .{ cfg.input, common.metricFileLabel(cfg.metric), cfg.randomize, cfg.use_quantization, cfg.epsilon, cfg.count, split.data.count, split.queries.count, cfg.query_index, cfg.top_k },
    );
    std.debug.print("truth_ids", .{});
    for (truth) |offset| std.debug.print(" {d}", .{offset + 1});
    std.debug.print("\n", .{});
    std.debug.print("truth_leaves", .{});
    for (truth) |offset| std.debug.print(" {any}", .{try idx.debugScanLeafForVector(offset + 1)});
    std.debug.print("\n", .{});
    std.debug.print("result_ids", .{});
    for (hits[0..@min(cfg.top_k, hits.len)]) |hit| std.debug.print(" {d}", .{hit.vector_id});
    std.debug.print("\n", .{});
    std.debug.print("result_leaves", .{});
    for (hits[0..@min(cfg.top_k, hits.len)]) |hit| std.debug.print(" {any}", .{try idx.debugScanLeafForVector(hit.vector_id)});
    std.debug.print("\n", .{});
    try debugGlobalApprox(alloc, &idx, query, cfg.top_k, truth);
    const p = profiled.profile;
    std.debug.print(
        "profile nodes={d} leaves={d} approx_nodes={d} approx_leaves={d} approx_vectors={d} reranked={d} rerank_load_ns={d} rerank_dist_ns={d}\n",
        .{ p.nodes_visited, p.leaves_explored, p.approx_nodes_expanded, p.approx_leaves_scored, p.approx_vectors_scored, p.reranked_vectors, p.rerank_vector_load_ns, p.rerank_distance_ns },
    );
    for (hits[0..@min(cfg.top_k, hits.len)], 0..) |hit, i| {
        std.debug.print("hit[{d}] id={d} dist={d:.6} err={d:.6}\n", .{ i, hit.vector_id, hit.distance, hit.error_bound });
    }
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--input")) {
            cfg.input = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--count")) {
            cfg.count = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--query-index")) {
            cfg.query_index = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--topk")) {
            cfg.top_k = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--epsilon")) {
            cfg.epsilon = try std.fmt.parseFloat(f32, args.next() orelse return error.InvalidArgument);
        } else if (std.mem.eql(u8, arg, "--metric")) {
            cfg.metric = parseMetric(args.next() orelse return error.InvalidArgument) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--randomize")) {
            cfg.randomize = true;
        } else if (std.mem.eql(u8, arg, "--no-quantization")) {
            cfg.use_quantization = false;
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.input.len == 0) return error.InvalidArgument;
    return cfg;
}

fn parseMetric(raw: []const u8) ?vec.DistanceMetric {
    if (std.mem.eql(u8, raw, "l2_squared")) return .l2_squared;
    if (std.mem.eql(u8, raw, "inner_product")) return .inner_product;
    if (std.mem.eql(u8, raw, "cosine")) return .cosine;
    return null;
}

fn debugGlobalApprox(
    alloc: std.mem.Allocator,
    idx: *hbc.HBCIndex,
    query: []const f32,
    top_k: usize,
    truth: []const usize,
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
        .load_metadata = false,
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
    std.debug.print(
        "global_approx retained={d} cutoff_rank={d} rerank_factor={d} search_width={d}\n",
        .{ ranked.len, candidate_limit, rerank_factor, search_width },
    );
    const top = @min(@as(usize, 30), ranked.len);
    for (ranked[0..top], 0..) |item, i| {
        std.debug.print("approx[{d:0>2}] id={d} dist={d:.6} err={d:.6}\n", .{ i + 1, item.vector_id, item.distance, item.error_bound });
    }

    for (truth) |offset| {
        const vector_id: u64 = @intCast(offset + 1);
        var found_rank: ?usize = null;
        for (ranked, 0..) |item, i| {
            if (item.vector_id == vector_id) {
                found_rank = i + 1;
                std.debug.print(
                    "truth_rank id={d} rank={d} approx={d:.6} err={d:.6}\n",
                    .{ vector_id, found_rank.?, item.distance, item.error_bound },
                );
                break;
            }
        }
        if (found_rank == null) std.debug.print("truth_rank id={d} rank=<dropped>\n", .{vector_id});
    }
}

const TestPath = struct {
    buf: [256]u8 = undefined,

    fn init(self: *TestPath) [*:0]const u8 {
        const ts = tempPathId();
        const slice = std.fmt.bufPrint(&self.buf, "/tmp/antfly-hbc-trace-{d}\x00", .{ts}) catch unreachable;
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
