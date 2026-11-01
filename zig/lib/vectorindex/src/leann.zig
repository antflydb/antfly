// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! LEANN: a low-storage graph vector index (arXiv:2506.08276).
//!
//! The index keeps only a pruned proximity graph and product-quantized (PQ)
//! codes. Full-precision embeddings are discarded after the build. At query
//! time the graph is walked with cheap PQ distances, and only the most
//! promising candidates have their exact embeddings recomputed through an
//! `EmbeddingSource` (normally the embedder that produced them).
//!
//! Pieces from the paper implemented here:
//! - An HNSW-style layered graph. Upper layers are kept as built and are
//!   descended with PQ distances (or exact ones with `exact_descent`).
//! - High-degree preserving pruning (Algorithm 3) of layer 0: the top `hub_fraction` of
//!   nodes by in-degree keep up to `max_degree` neighbors; every other node
//!   selects `low_degree` neighbors. Reverse links may grow any list to
//!   `max_degree`, so low-degree nodes stay connected to hubs.
//! - Two-level search with hybrid distance (Algorithm 2): neighbors go into an
//!   approximate queue ordered by PQ distance, and only the top
//!   `rerank_ratio` fraction is promoted to exact recomputation.
//! - Dynamic batching: promoted candidates accumulate across expansion steps
//!   until `batch_size` is reached, so the embedder sees larger batches.
//! - Inserts (Appendix B) with simplified neighbor selection, and O(1) soft
//!   deletes that keep deleted nodes traversable but filter them from results.
//!
//! Not thread-safe for writes. Searches only read the index, so concurrent
//! searches are safe when no insert or delete runs at the same time.

const std = @import("std");
const Allocator = std.mem.Allocator;
const antfly_vector = @import("antfly_vector");
const rq = antfly_vector.quantizer;
const vproto = antfly_vector.proto;
const vmath = antfly_vector.vector;
const vrabitq = antfly_vector.rabitq;

/// Distance metric. Values match `antfly_vector.vector.DistanceMetric`.
pub const Metric = enum(u8) {
    /// Squared Euclidean distance.
    l2_squared = 0,
    /// Negative dot product.
    inner_product = 1,
    /// 1 - cosine similarity. Vectors are normalized on the way in.
    cosine = 2,
};

/// How vectors are compressed for approximate distances.
pub const Quantizer = enum(u8) {
    /// Product quantization, as in the paper. No error bounds.
    pq = 0,
    /// 1-bit RaBitQ with per-candidate error bounds (repo RaBitQ kernel).
    rabitq = 1,
};

pub const Config = struct {
    dims: u32,
    metric: Metric = .l2_squared,
    quantizer: Quantizer = .pq,
    /// M: degree cap for hub nodes and for any list grown by reverse links.
    max_degree: u32 = 30,
    /// m: forward degree for non-hub nodes. Zero means max(2, M / 5).
    low_degree: u32 = 0,
    /// β: fraction of nodes (by in-degree) that keep `max_degree` neighbors.
    hub_fraction: f32 = 0.02,
    /// Beam width used while building and pruning the graph.
    ef_construction: u32 = 128,
    /// Number of PQ subspaces (bytes per code). Zero picks the divisor of
    /// `dims` whose subspace width is closest to 8 floats.
    pq_subspaces: u32 = 0,
    pq_train_iterations: u32 = 10,
    pq_train_sample: u32 = 16384,
    /// When false, the unpruned base graph is kept. Useful for comparisons.
    prune: bool = true,
    seed: u64 = 42,
    /// Experimental: keep full-precision vectors for nodes on layer 1 and
    /// above (about 1 in `max_degree` nodes). Upper-layer descent and linking
    /// then use exact distances, and those nodes never need recomputing.
    /// Not serialized or replicated yet: `serialize` and `Delta.encode`
    /// refuse indexes and deltas that use it.
    store_upper: bool = false,

    pub fn resolved(self: Config) !Config {
        if (self.dims == 0) return error.InvalidConfig;
        if (self.max_degree < 2) return error.InvalidConfig;
        if (self.ef_construction == 0) return error.InvalidConfig;
        if (self.quantizer == .rabitq and self.dims < 2) return error.InvalidConfig;
        if (!(self.hub_fraction >= 0 and self.hub_fraction <= 1)) return error.InvalidConfig;
        var out = self;
        if (out.low_degree == 0) out.low_degree = @max(2, out.max_degree / 5);
        if (out.low_degree > out.max_degree) return error.InvalidConfig;
        if (out.pq_subspaces == 0) {
            // Pick the divisor of dims whose subspace width is closest to 8.
            var best: u32 = 1;
            var best_gap: u32 = std.math.maxInt(u32);
            var s: u32 = 1;
            while (s <= out.dims) : (s += 1) {
                if (out.dims % s != 0) continue;
                const width = out.dims / s;
                const gap = if (width > 8) width - 8 else 8 - width;
                if (gap < best_gap) {
                    best_gap = gap;
                    best = s;
                }
            }
            out.pq_subspaces = best;
        }
        if (out.pq_subspaces > out.dims or out.dims % out.pq_subspaces != 0) return error.InvalidConfig;
        if (out.pq_train_iterations == 0) out.pq_train_iterations = 1;
        if (out.pq_train_sample == 0) return error.InvalidConfig;
        return out;
    }
};

/// Recomputes exact embeddings for node ids at query and insert time. `out`
/// holds `ids.len * dims` floats, row-major in the order of `ids`. In
/// production this calls the embedder on the stored source text; a cache of
/// hot embeddings can sit in front of it.
pub const EmbeddingSource = struct {
    ctx: *anyopaque,
    recompute_fn: *const fn (ctx: *anyopaque, ids: []const u32, out: []f32) anyerror!void,

    pub fn recompute(self: EmbeddingSource, ids: []const u32, out: []f32) !void {
        return self.recompute_fn(self.ctx, ids, out);
    }
};

/// EmbeddingSource over vectors already in memory, such as an embedding
/// cache or a test fixture. Counts how many embeddings were requested.
pub const SliceEmbeddingSource = struct {
    vectors: []const f32,
    dims: usize,
    recomputed: u64 = 0,
    calls: u64 = 0,

    pub fn source(self: *SliceEmbeddingSource) EmbeddingSource {
        return .{ .ctx = self, .recompute_fn = recompute };
    }

    fn recompute(ctx: *anyopaque, ids: []const u32, out: []f32) anyerror!void {
        const self: *SliceEmbeddingSource = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        self.recomputed += ids.len;
        for (ids, 0..) |id, i| {
            const start = @as(usize, id) * self.dims;
            if (start + self.dims > self.vectors.len) return error.UnknownNode;
            @memcpy(out[i * self.dims ..][0..self.dims], self.vectors[start..][0..self.dims]);
        }
    }
};

/// How candidates are chosen for exact recomputation.
pub const Rerank = enum {
    /// `bounds` with RaBitQ, `ratio` with PQ.
    auto,
    /// Promote the top `rerank_ratio` of the approximate queue after each
    /// expansion (the paper's two-level search).
    ratio,
    /// Walk with estimates only, then recompute candidates whose RaBitQ
    /// lower bound can still beat the k-th exact result. RaBitQ only.
    bounds,
};

pub const SearchOptions = struct {
    k: u32 = 10,
    /// Beam width, raised to `k` when smaller. Zero picks a default per
    /// rerank mode: 64 for `ratio`, where every beam slot costs
    /// recomputation, and 512 for `bounds`, where the walk only costs
    /// estimates.
    ef: u32 = 0,
    /// α: fraction of the approximate queue promoted to exact recomputation
    /// after each expansion. At least one candidate is promoted per step.
    rerank_ratio: f32 = 0.1,
    /// Recompute once this many candidates are pending. Zero recomputes after
    /// every expansion step.
    batch_size: u32 = 0,
    /// False disables the approximate queue and recomputes every neighbor,
    /// which is the plain best-first search (Algorithm 1) the paper compares to.
    two_level: bool = true,
    /// Descend the upper layers with recomputed exact distances instead of
    /// PQ distances. Costs roughly levels x M recomputations per query but
    /// avoids starting layer 0 in the wrong region when PQ is coarse.
    exact_descent: bool = false,
    rerank: Rerank = .auto,
    /// Multiplies RaBitQ error bounds in `bounds` mode. Larger is safer and
    /// recomputes more. The kernel's bound is about one standard error.
    bound_scale: f32 = 1.0,
};

pub const SearchStats = struct {
    /// Exact embeddings recomputed through the EmbeddingSource.
    recomputed: u32 = 0,
    /// Recompute calls (batches) made.
    batches: u32 = 0,
    /// Approximate (PQ or RaBitQ) distances evaluated.
    approx_evaluated: u32 = 0,
    /// Nodes whose neighbor lists were expanded.
    expanded: u32 = 0,
};

pub const Result = struct { id: u32, distance: f32 };

pub const SearchResponse = struct {
    results: []Result,
    stats: SearchStats,

    pub fn deinit(self: *SearchResponse, alloc: Allocator) void {
        alloc.free(self.results);
        self.* = undefined;
    }
};

pub const GraphStats = struct {
    nodes: u32,
    /// Bottom-layer edges, which dominate storage.
    edges: u64,
    avg_degree: f64,
    max_degree: u32,
    hubs: u32,
    deleted: u32,
    /// Highest layer (0 when only the bottom layer exists).
    max_level: u8,
    /// Nodes present above the bottom layer, and their edges.
    upper_nodes: u32,
    upper_edges: u64,
};

const flag_hub: u8 = 1;
const flag_deleted: u8 = 2;
/// With M = 30, a level above 6 already has odds of about 1 in 10^9.
const max_level_cap: u8 = 15;

const NeighborList = std.ArrayListUnmanaged(u32);

/// Per-layer adjacency. Layer 0 holds every node; `upper[v]` holds one list
/// for each layer 1..level(v).
const Layers = struct {
    base: []NeighborList,
    upper: []const []NeighborList,

    fn list(self: Layers, v: u32, layer: u8) *NeighborList {
        return if (layer == 0) &self.base[v] else &self.upper[v][layer - 1];
    }

    fn neighbors(self: Layers, v: u32, layer: u8) []const u32 {
        return self.list(v, layer).items;
    }
};

/// HNSW level for a node: floor(-ln(U) / ln(M)), with U derived from a hash
/// of (seed, id) so builds and inserts are reproducible across replicas.
fn levelFor(seed: u64, id: u32, max_degree: u32) u8 {
    var z = seed +% (@as(u64, id) +% 1) *% 0x9E3779B97F4A7C15;
    z = (z ^ (z >> 30)) *% 0xBF58476D1CE4E5B9;
    z = (z ^ (z >> 27)) *% 0x94D049BB133111EB;
    z ^= z >> 31;
    // U in (0, 1].
    const u = (@as(f64, @floatFromInt(z >> 11)) + 1) / 9007199254740992.0;
    const ml = 1.0 / @log(@as(f64, @floatFromInt(max_degree)));
    const level = @floor(-@log(u) * ml);
    return @intFromFloat(@min(level, @as(f64, max_level_cap)));
}

pub const Index = struct {
    alloc: Allocator,
    config: Config,
    node_count: u32 = 0,
    /// Entry point: a node on the highest layer.
    entry: u32 = 0,
    max_level: u8 = 0,
    deleted_count: u32 = 0,
    /// Number of deltas (inserts and deletes) applied since this graph was
    /// built. Deltas carry the version they were planned against.
    version: u64 = 0,
    /// Layer-0 neighbor lists (pruned).
    adjacency: std.ArrayListUnmanaged(NeighborList) = .empty,
    /// Upper-layer neighbor lists per node; `upper.items[v].len` is v's level.
    upper: std.ArrayListUnmanaged([]NeighborList) = .empty,
    flags: std.ArrayListUnmanaged(u8) = .empty,
    /// PQ codes, `pq_subspaces` bytes per node (PQ quantizer only).
    codes: std.ArrayListUnmanaged(u8) = .empty,
    pq: ?ProductQuantizer = null,
    rabitq: ?RaBitQCodes = null,
    /// Build statistic: unreachable layer-0 nodes the connectivity pass
    /// linked. Not serialized.
    repaired: u32 = 0,
    /// Layer-0 in-degree per node, kept in memory (rebuilt on load) so
    /// inserts never evict a node's last inbound edge.
    indegree: std.ArrayListUnmanaged(u32) = .empty,
    /// `Config.store_upper` only: vectors of nodes on layer 1 and above, in
    /// the space `decodeApprox` uses (rotated for RaBitQ).
    upper_vectors: std.AutoHashMapUnmanaged(u32, []f32) = .empty,

    /// Builds an index from `vectors` (row-major, `dims` floats each). The
    /// vectors are only used during the build; the index keeps none of them.
    pub fn build(alloc: Allocator, config: Config, vectors: []const f32) !Index {
        const cfg = try config.resolved();
        const dims: usize = cfg.dims;
        if (vectors.len == 0 or vectors.len % dims != 0) return error.InvalidInput;
        const n_usize = vectors.len / dims;
        if (n_usize > std.math.maxInt(u32) - 1) return error.InvalidInput;
        const n: u32 = @intCast(n_usize);

        const work = try alloc.dupe(f32, vectors);
        defer alloc.free(work);
        if (cfg.metric == .cosine) {
            for (0..n) |i| normalize(work[i * dims ..][0..dims]);
        }

        var index = Index{ .alloc = alloc, .config = cfg };
        errdefer index.deinit();
        switch (cfg.quantizer) {
            .pq => {
                index.pq = try ProductQuantizer.train(alloc, work, dims, cfg.pq_subspaces, cfg.pq_train_iterations, cfg.pq_train_sample, cfg.seed);
                const pq = &index.pq.?;
                try index.codes.resize(alloc, n_usize * cfg.pq_subspaces);
                for (0..n) |i| pq.encode(work[i * dims ..][0..dims], index.codes.items[i * cfg.pq_subspaces ..][0..cfg.pq_subspaces]);
            },
            .rabitq => {
                // Rotation preserves distances, so the graph below is built
                // on the rotated vectors directly.
                index.rabitq = try RaBitQCodes.init(alloc, cfg);
                try index.rabitq.?.rotateInPlace(alloc, work);
                try index.rabitq.?.encodeAll(alloc, work, n_usize);
            },
        }
        try index.buildGraph(work, n);
        return index;
    }

    /// Rebuilds the graph over the live nodes `live` without exact
    /// embeddings, keeping their codes unchanged: new node i is old node
    /// `live[i]`. The graph is built from `graph_vectors` (row i for
    /// `live[i]`, original space) when given, otherwise from each code's own
    /// reconstruction. RaBitQ only.
    pub fn rebuildFromCodes(self: *const Index, alloc: Allocator, live: []const u32, graph_vectors: ?[]const f32) !Index {
        const cfg = self.config;
        if (cfg.store_upper or self.rabitq == null) return error.Unsupported;
        const dims: usize = cfg.dims;
        if (live.len == 0 or live.len > std.math.maxInt(u32) - 1) return error.InvalidInput;
        const n: u32 = @intCast(live.len);
        const old = &self.rabitq.?;
        const work = try alloc.alloc(f32, live.len * dims);
        defer alloc.free(work);
        if (graph_vectors) |gv| {
            if (gv.len != work.len) return error.InvalidInput;
            @memcpy(work, gv);
            if (cfg.metric == .cosine) {
                for (0..live.len) |i| normalize(work[i * dims ..][0..dims]);
            }
            try old.rotateInPlace(alloc, work);
        } else {
            for (live, 0..) |id, i| {
                if (id >= self.node_count) return error.InvalidInput;
                old.decode(id, work[i * dims ..][0..dims]);
            }
        }

        var index = Index{ .alloc = alloc, .config = cfg };
        errdefer index.deinit();
        index.rabitq = try RaBitQCodes.init(alloc, cfg);
        const r = &index.rabitq.?;
        r.centroid = try alloc.dupe(f32, old.centroid);
        r.centroid_norm = old.centroid_norm;
        try r.reserve(alloc, live.len);
        for (live) |id| {
            if (id >= self.node_count) return error.InvalidInput;
            r.appendRowFromAssumeCapacity(old, id);
        }
        try index.buildGraph(work, n);
        return index;
    }

    /// HNSW construction, pruning and connectivity repair over `work` (in the
    /// space `decodeApprox` uses), after the codes are in place.
    fn buildGraph(index: *Index, work: []const f32, n: u32) !void {
        const alloc = index.alloc;
        const cfg = index.config;
        const dims: usize = cfg.dims;
        const n_usize: usize = n;
        try index.flags.appendNTimes(alloc, 0, n_usize);

        try index.upper.ensureTotalCapacity(alloc, n_usize);
        for (0..n) |i| {
            const lists = try alloc.alloc(NeighborList, levelFor(cfg.seed, @intCast(i), cfg.max_degree));
            @memset(lists, NeighborList.empty);
            index.upper.appendAssumeCapacity(lists);
            if (cfg.store_upper and lists.len > 0) {
                const kept = try alloc.dupe(f32, work[i * dims ..][0..dims]);
                errdefer alloc.free(kept);
                try index.upper_vectors.put(alloc, @intCast(i), kept);
            }
        }

        const ctx = BuildContext{ .vectors = work, .dims = dims, .metric = cfg.metric };
        var visited = try VisitSet.init(alloc, n);
        defer visited.deinit(alloc);
        var cands = std.ArrayListUnmanaged(Cand).empty;
        defer cands.deinit(alloc);
        var shrink_scratch = std.ArrayListUnmanaged(Cand).empty;
        defer shrink_scratch.deinit(alloc);
        var scratch = BeamScratch{};
        defer scratch.deinit(alloc);

        // Standard HNSW construction with exact distances.
        const base = try allocLists(alloc, n);
        defer freeLists(alloc, base);
        const layers = Layers{ .base = base, .upper = index.upper.items };
        var entry: u32 = 0;
        var top: u8 = @intCast(index.upper.items[0].len);
        for (1..n) |i| {
            const v: u32 = @intCast(i);
            const level: u8 = @intCast(index.upper.items[v].len);
            var ep = greedyDescentExact(&ctx, layers, ctx.vec(v), entry, top, level);
            var layer = @min(level, top);
            while (true) : (layer -= 1) {
                try beamSearchExact(alloc, &ctx, layers, layer, ctx.vec(v), ep, cfg.ef_construction, &visited, &scratch, &cands);
                const own = layers.list(v, layer);
                try selectHeuristic(alloc, &ctx, cands.items, v, cfg.max_degree, own);
                for (own.items) |u| {
                    const ul = layers.list(u, layer);
                    try ul.append(alloc, v);
                    if (ul.items.len > cfg.max_degree) try shrinkExact(alloc, &ctx, ul, u, cfg.max_degree, &shrink_scratch);
                }
                ep = cands.items[0].id;
                if (layer == 0) break;
            }
            if (level > top) {
                top = level;
                entry = v;
            }
        }
        index.entry = entry;
        index.max_level = top;

        try index.adjacency.ensureTotalCapacity(alloc, n_usize);
        if (!cfg.prune) {
            for (base) |*list| {
                index.adjacency.appendAssumeCapacity(list.*);
                list.* = .empty;
            }
        } else {
            const pruned = try pruneGraph(alloc, &ctx, cfg, layers, entry, top, index.flags.items, &visited, &scratch, &cands, &index.repaired);
            defer alloc.free(pruned);
            for (pruned) |list| index.adjacency.appendAssumeCapacity(list);
        }
        index.node_count = n;
        try index.rebuildIndegree();
    }

    fn rebuildIndegree(self: *Index) !void {
        try self.indegree.resize(self.alloc, self.adjacency.items.len);
        @memset(self.indegree.items, 0);
        for (self.adjacency.items) |list| for (list.items) |nb| {
            self.indegree.items[nb] += 1;
        };
    }

    pub fn deinit(self: *Index) void {
        for (self.adjacency.items) |*list| list.deinit(self.alloc);
        self.adjacency.deinit(self.alloc);
        for (self.upper.items) |lists| freeLists(self.alloc, lists);
        self.upper.deinit(self.alloc);
        self.flags.deinit(self.alloc);
        self.codes.deinit(self.alloc);
        self.indegree.deinit(self.alloc);
        var uv = self.upper_vectors.valueIterator();
        while (uv.next()) |v| self.alloc.free(v.*);
        self.upper_vectors.deinit(self.alloc);
        if (self.pq) |*pq| pq.deinit(self.alloc);
        if (self.rabitq) |*r| r.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn dimension(self: *const Index) usize {
        return self.config.dims;
    }

    pub fn isDeleted(self: *const Index, id: u32) bool {
        return self.flags.items[id] & flag_deleted != 0;
    }

    /// Soft-deletes a node in O(1). It stays in the graph for navigation and
    /// is filtered from results. Rebuild once `deletedFraction` gets large
    /// (the paper suggests about 5%).
    pub fn markDeleted(self: *Index, id: u32) !void {
        if (id >= self.node_count) return error.UnknownNode;
        // Planned inserts skip deleted nodes, so a delete changes what later
        // plans produce and advances the version even when repeated.
        self.version += 1;
        if (self.flags.items[id] & flag_deleted != 0) return;
        self.flags.items[id] |= flag_deleted;
        self.deleted_count += 1;
    }

    pub const ConsolidateStats = struct {
        /// Neighbor lists that lost deleted members and were refilled.
        patched_lists: u32 = 0,
        /// Deleted nodes taken out of the graph by this pass.
        removed: u32 = 0,
    };

    /// Experimental and local only (not yet a replicated delta). Takes deleted
    /// nodes out of the graph, FreshDiskANN-style: every list that points to
    /// a deleted node drops it and refills the freed slots with the closest
    /// live neighbors of its deleted members (up to two hops), by approximate
    /// distance, so it costs no recomputation. Deleted nodes keep their ids
    /// and stay counted in `deletedFraction`, but no search reaches them.
    pub fn consolidateDeletes(self: *Index) !ConsolidateStats {
        const alloc = self.alloc;
        var stats = ConsolidateStats{};
        if (self.deleted_count == 0) return stats;
        const d = self.dimension();
        const a = try alloc.alloc(f32, d);
        defer alloc.free(a);
        const b = try alloc.alloc(f32, d);
        defer alloc.free(b);

        const Patch = struct { v: u32, layer: u8, ids: []u32 };
        var patches = std.ArrayListUnmanaged(Patch).empty;
        defer {
            for (patches.items) |p| alloc.free(p.ids);
            patches.deinit(alloc);
        }
        var keep = std.ArrayListUnmanaged(u32).empty;
        defer keep.deinit(alloc);
        var cands = std.ArrayListUnmanaged(Cand).empty;
        defer cands.deinit(alloc);
        var seen = std.AutoHashMapUnmanaged(u32, void).empty;
        defer seen.deinit(alloc);

        const Ctx = struct {
            index: *const Index,
            seen: *std.AutoHashMapUnmanaged(u32, void),
            cands: *std.ArrayListUnmanaged(Cand),
            a: []const f32,
            b: []f32,
            fn add(ctx: @This(), al: Allocator, w: u32) !void {
                if (ctx.index.isDeleted(w)) return;
                const gop = try ctx.seen.getOrPut(al, w);
                if (gop.found_existing) return;
                ctx.index.decodeApprox(w, ctx.b);
                try ctx.cands.append(al, .{ .dist = distance(ctx.index.config.metric, ctx.a, ctx.b), .id = w });
            }
        };

        for (0..self.node_count) |vi| {
            const v: u32 = @intCast(vi);
            if (self.isDeleted(v)) continue;
            const levels: u8 = @intCast(self.upper.items[v].len);
            var layer: u8 = 0;
            while (layer <= levels) : (layer += 1) {
                const list = self.neighbors(v, layer);
                var has_deleted = false;
                for (list) |x| {
                    if (self.isDeleted(x)) has_deleted = true;
                }
                if (!has_deleted) continue;
                keep.clearRetainingCapacity();
                cands.clearRetainingCapacity();
                seen.clearRetainingCapacity();
                try seen.put(alloc, v, {});
                for (list) |x| if (!self.isDeleted(x)) {
                    try keep.append(alloc, x);
                    try seen.put(alloc, x, {});
                };
                self.decodeApprox(v, a);
                const ctx = Ctx{ .index = self, .seen = &seen, .cands = &cands, .a = a, .b = b };
                for (list) |x| {
                    if (!self.isDeleted(x)) continue;
                    for (self.neighbors(x, layer)) |w| {
                        if (self.isDeleted(w)) {
                            for (self.neighbors(w, layer)) |w2| try ctx.add(alloc, w2);
                        } else try ctx.add(alloc, w);
                    }
                }
                std.mem.sortUnstable(Cand, cands.items, {}, candLessThan);
                for (cands.items) |c| {
                    if (keep.items.len >= list.len) break;
                    try keep.append(alloc, c.id);
                }
                try patches.ensureUnusedCapacity(alloc, 1);
                patches.appendAssumeCapacity(.{ .v = v, .layer = layer, .ids = try alloc.dupe(u32, keep.items) });
            }
        }

        // Lists only shrink or keep their length, so nothing below allocates.
        for (patches.items) |p| {
            const list = if (p.layer == 0) &self.adjacency.items[p.v] else &self.upper.items[p.v][p.layer - 1];
            list.clearRetainingCapacity();
            list.appendSliceAssumeCapacity(p.ids);
        }
        stats.patched_lists = @intCast(patches.items.len);
        for (0..self.node_count) |vi| {
            const v: u32 = @intCast(vi);
            if (!self.isDeleted(v)) continue;
            var had_edges = self.adjacency.items[v].items.len > 0;
            self.adjacency.items[v].clearRetainingCapacity();
            for (self.upper.items[v]) |*list| {
                if (list.items.len > 0) had_edges = true;
                list.clearRetainingCapacity();
            }
            if (had_edges) stats.removed += 1;
        }
        if (self.isDeleted(self.entry)) {
            var best: ?u32 = null;
            var best_level: usize = 0;
            for (0..self.node_count) |vi| {
                const v: u32 = @intCast(vi);
                if (self.isDeleted(v)) continue;
                const lv = self.upper.items[v].len;
                if (best == null or lv > best_level) {
                    best = v;
                    best_level = lv;
                }
            }
            if (best) |e| {
                self.entry = e;
                self.max_level = @intCast(best_level);
            }
        }
        try self.rebuildIndegree();
        self.version += 1;
        return stats;
    }

    pub fn deletedFraction(self: *const Index) f64 {
        if (self.node_count == 0) return 0;
        return @as(f64, @floatFromInt(self.deleted_count)) / @as(f64, @floatFromInt(self.node_count));
    }

    pub fn graphStats(self: *const Index) GraphStats {
        var edges: u64 = 0;
        var max_deg: u32 = 0;
        var hubs: u32 = 0;
        for (self.adjacency.items, self.flags.items) |list, flag| {
            edges += list.items.len;
            max_deg = @max(max_deg, @as(u32, @intCast(list.items.len)));
            if (flag & flag_hub != 0) hubs += 1;
        }
        var upper_nodes: u32 = 0;
        var upper_edges: u64 = 0;
        for (self.upper.items) |lists| {
            if (lists.len > 0) upper_nodes += 1;
            for (lists) |list| upper_edges += list.items.len;
        }
        const avg = if (self.node_count == 0) 0 else @as(f64, @floatFromInt(edges)) / @as(f64, @floatFromInt(self.node_count));
        return .{
            .nodes = self.node_count,
            .edges = edges,
            .avg_degree = avg,
            .max_degree = max_deg,
            .hubs = hubs,
            .deleted = self.deleted_count,
            .max_level = self.max_level,
            .upper_nodes = upper_nodes,
            .upper_edges = upper_edges,
        };
    }

    /// Searches for the `k` nearest live nodes to `query`.
    pub fn search(self: *const Index, alloc: Allocator, source: EmbeddingSource, query: []const f32, opts: SearchOptions) !SearchResponse {
        if (query.len != self.dimension()) return error.DimensionMismatch;
        var stats = SearchStats{};
        if (self.node_count == 0 or opts.k == 0) return .{ .results = try alloc.alloc(Result, 0), .stats = stats };
        const rerank: Rerank = switch (opts.rerank) {
            .auto => if (self.rabitq != null) .bounds else .ratio,
            else => opts.rerank,
        };
        if (rerank == .bounds and self.rabitq == null) return error.BoundsRequireRaBitQ;
        var eff = opts;
        if (eff.ef == 0) eff.ef = if (rerank == .bounds) 512 else 64;

        const q = try alloc.dupe(f32, query);
        defer alloc.free(q);
        if (self.config.metric == .cosine) normalize(q);
        var approx = try Approx.init(alloc, self, q);
        defer approx.deinit();

        const start = if (eff.exact_descent)
            try self.greedyDescentRecompute(alloc, source, q, self.entry, self.max_level, &stats)
        else
            try self.greedyDescentApprox(&approx, self.entry, self.max_level, 0, &stats);

        var found = std.ArrayListUnmanaged(Cand).empty;
        defer found.deinit(alloc);
        switch (rerank) {
            .bounds => try self.boundedSearch(alloc, source, q, &approx, start, eff, &found, &stats),
            else => try self.twoLevelSearch(alloc, source, q, &approx, start, eff, &found, &stats),
        }

        var out = std.ArrayListUnmanaged(Result).empty;
        errdefer out.deinit(alloc);
        for (found.items) |c| {
            if (out.items.len >= opts.k) break;
            if (self.isDeleted(c.id)) continue;
            try out.append(alloc, .{ .id = c.id, .distance = c.dist });
        }
        return .{ .results = try out.toOwnedSlice(alloc), .stats = stats };
    }

    /// Inserts one embedding and returns its node id. Ids are dense and
    /// assigned in insertion order, so the caller's EmbeddingSource must be
    /// able to recompute the new id afterwards. Equivalent to `planInsert`
    /// followed by `applyDelta`.
    pub fn insert(self: *Index, source: EmbeddingSource, vector: []const f32) !u32 {
        var delta = try self.planInsert(self.alloc, source, vector);
        defer delta.deinit(self.alloc);
        try self.applyDelta(delta);
        return delta.id;
    }

    /// Plans an insert without changing the index. Replicated deployments run
    /// this on one node (the leader), replicate the returned delta, and every
    /// replica, the planner included, applies it with `applyDelta` in the
    /// same order. Replicas then hold identical graphs even on hardware whose
    /// floating-point results differ, because only the planner computes
    /// distances.
    ///
    /// Upper layers are linked with approximate distances, so they cost no
    /// recomputation. Layer 0 uses the paper's simplified neighbor selection:
    /// the closest `low_degree` live candidates become forward edges. With PQ
    /// they come from a two-level search (which recomputes); with RaBitQ from
    /// an estimate-only search (which does not). Full reverse lists drop their
    /// farthest member by approximate distance, but never a node's last
    /// inbound layer-0 edge; if every list rejects the new node it is attached
    /// to its closest neighbor over the cap.
    pub fn planInsert(self: *const Index, alloc: Allocator, source: EmbeddingSource, vector: []const f32) !Delta {
        const d = self.dimension();
        if (vector.len != d) return error.DimensionMismatch;
        if (self.node_count == std.math.maxInt(u32) - 1) return error.IndexFull;
        const id = self.node_count;
        const level = levelFor(self.config.seed, id, self.config.max_degree);

        const v = try alloc.dupe(f32, vector);
        defer alloc.free(v);
        if (self.config.metric == .cosine) normalize(v);
        var approx = try Approx.init(alloc, self, v);
        defer approx.deinit();

        var delta = Delta{ .kind = .insert, .base_version = self.version, .id = id, .level = level };
        errdefer delta.deinit(alloc);
        delta.code = try self.encodeCode(alloc, v);
        delta.upper = try alloc.alloc([]u32, level);
        @memset(delta.upper, &.{});
        if (self.config.store_upper and level > 0) delta.upper_vector = try self.toDecodeSpace(alloc, v);

        var stats = SearchStats{};
        var ep = try self.greedyDescentApprox(&approx, self.entry, self.max_level, level, &stats);
        var found = std.ArrayListUnmanaged(Cand).empty;
        defer found.deinit(alloc);
        var picked = std.ArrayListUnmanaged(u32).empty;
        defer picked.deinit(alloc);
        var layer = @min(level, self.max_level);
        while (layer > 0) : (layer -= 1) {
            try self.beamSearchApprox(alloc, &approx, layer, ep, self.config.ef_construction, &found);
            picked.clearRetainingCapacity();
            for (found.items) |c| {
                if (picked.items.len >= self.config.max_degree) break;
                try picked.append(alloc, c.id);
            }
            delta.upper[layer - 1] = try alloc.dupe(u32, picked.items);
            ep = found.items[0].id;
        }

        const ef = self.config.ef_construction;
        if (self.rabitq != null) {
            try self.beamSearchApprox(alloc, &approx, 0, ep, ef, &found);
        } else {
            try self.twoLevelSearch(alloc, source, v, &approx, ep, .{ .k = ef, .ef = ef }, &found, &stats);
        }
        picked.clearRetainingCapacity();
        for (found.items) |c| {
            if (picked.items.len >= self.config.low_degree) break;
            if (self.isDeleted(c.id)) continue;
            try picked.append(alloc, c.id);
        }
        delta.forward = try alloc.dupe(u32, picked.items);

        // Reverse links, simulated against an overlay of in-degree changes so
        // the plan matches what applying the edits in order produces.
        var overlay = std.AutoHashMapUnmanaged(u32, i64).empty;
        defer overlay.deinit(alloc);
        const Deg = struct {
            fn get(index: *const Index, ov: *const std.AutoHashMapUnmanaged(u32, i64), node: u32, new_id: u32) i64 {
                const base: i64 = if (node == new_id) 0 else index.indegree.items[node];
                return base + (ov.get(node) orelse 0);
            }
            fn add(a: Allocator, ov: *std.AutoHashMapUnmanaged(u32, i64), node: u32, by: i64) !void {
                const gop = try ov.getOrPut(a, node);
                if (!gop.found_existing) gop.value_ptr.* = 0;
                gop.value_ptr.* += by;
            }
        };
        for (delta.forward) |u| try Deg.add(alloc, &overlay, u, 1);

        var edits = std.ArrayListUnmanaged(Delta.Edit).empty;
        defer edits.deinit(alloc);
        const a = try alloc.alloc(f32, d);
        defer alloc.free(a);
        const b = try alloc.alloc(f32, d);
        defer alloc.free(b);
        const new_vec = try alloc.alloc(f32, d);
        defer alloc.free(new_vec);
        if (delta.upper_vector.len > 0) @memcpy(new_vec, delta.upper_vector) else self.decodeBytes(delta.code, new_vec);

        for (delta.forward) |u| {
            const list = self.adjacency.items[u].items;
            if (list.len < self.config.max_degree) {
                try edits.append(alloc, .{ .layer = 0, .owner = u, .slot = Delta.append_slot });
                try Deg.add(alloc, &overlay, id, 1);
                continue;
            }
            self.decodeApprox(u, a);
            var worst_i: usize = list.len;
            var worst = distance(self.config.metric, a, new_vec);
            for (list, 0..) |w, i| {
                if (Deg.get(self, &overlay, w, id) <= 1) continue;
                self.decodeApprox(w, b);
                const dist = distance(self.config.metric, a, b);
                if (dist > worst) {
                    worst = dist;
                    worst_i = i;
                }
            }
            if (worst_i == list.len) continue;
            try edits.append(alloc, .{ .layer = 0, .owner = u, .slot = @intCast(worst_i), .old = list[worst_i] });
            try Deg.add(alloc, &overlay, list[worst_i], -1);
            try Deg.add(alloc, &overlay, id, 1);
        }
        if (Deg.get(self, &overlay, id, id) == 0 and delta.forward.len > 0) {
            try edits.append(alloc, .{ .layer = 0, .owner = delta.forward[0], .slot = Delta.append_slot });
        }
        for (delta.upper, 1..) |list_ids, l| {
            for (list_ids) |u| {
                const list = self.upper.items[u][l - 1].items;
                if (list.len < self.config.max_degree) {
                    try edits.append(alloc, .{ .layer = @intCast(l), .owner = u, .slot = Delta.append_slot });
                    continue;
                }
                self.decodeApprox(u, a);
                var worst_i: usize = list.len;
                var worst = distance(self.config.metric, a, new_vec);
                for (list, 0..) |w, i| {
                    self.decodeApprox(w, b);
                    const dist = distance(self.config.metric, a, b);
                    if (dist > worst) {
                        worst = dist;
                        worst_i = i;
                    }
                }
                if (worst_i == list.len) continue;
                try edits.append(alloc, .{ .layer = @intCast(l), .owner = u, .slot = @intCast(worst_i), .old = list[worst_i] });
            }
        }
        delta.edits = try edits.toOwnedSlice(alloc);
        return delta;
    }

    /// Plans a soft delete of a live node.
    pub fn planDelete(self: *const Index, id: u32) !Delta {
        if (id >= self.node_count) return error.UnknownNode;
        return .{ .kind = .delete, .base_version = self.version, .id = id };
    }

    /// Applies a delta planned against this exact graph version. Rejects a
    /// delta planned against any other version with `error.DeltaVersionMismatch`
    /// (the caller then needs a fresh generation) and malformed deltas with
    /// `error.InvalidDelta`; in both cases the index is unchanged.
    pub fn applyDelta(self: *Index, delta: Delta) !void {
        _ = try self.applyDeltaUndoable(delta);
    }

    /// What `revertDelta` needs beyond the delta itself.
    pub const Undo = struct {
        prev_entry: u32,
        prev_max_level: u8,
        /// Delete deltas only: whether the node was live before.
        was_live: bool = false,
    };

    /// `applyDelta`, also returning what is needed to revert it. Planners use
    /// this to plan a batch (each insert sees the previous ones) and then
    /// revert, so their state only changes when the batch is committed.
    pub fn applyDeltaUndoable(self: *Index, delta: Delta) !Undo {
        if (delta.base_version != self.version) return error.DeltaVersionMismatch;
        const undo = Undo{ .prev_entry = self.entry, .prev_max_level = self.max_level };
        switch (delta.kind) {
            .delete => {
                if (delta.id >= self.node_count) return error.InvalidDelta;
                const was_live = !self.isDeleted(delta.id);
                try self.markDeleted(delta.id);
                var out = undo;
                out.was_live = was_live;
                return out;
            },
            .insert => {},
        }
        try self.validateInsertDelta(delta);
        const alloc = self.alloc;
        const id = delta.id;

        // Reserve everything up front: nothing below the commit may fail.
        try self.reserveCode(alloc);
        try self.flags.ensureUnusedCapacity(alloc, 1);
        try self.indegree.ensureUnusedCapacity(alloc, 1);
        try self.adjacency.ensureUnusedCapacity(alloc, 1);
        try self.upper.ensureUnusedCapacity(alloc, 1);
        var forward = try NeighborList.initCapacity(alloc, delta.forward.len);
        var forward_owned = true;
        defer if (forward_owned) forward.deinit(alloc);
        forward.appendSliceAssumeCapacity(delta.forward);
        const new_upper = try alloc.alloc(NeighborList, delta.level);
        @memset(new_upper, NeighborList.empty);
        var upper_owned = true;
        defer if (upper_owned) freeLists(alloc, new_upper);
        for (new_upper, delta.upper) |*list, ids| {
            try list.ensureTotalCapacity(alloc, ids.len);
            list.appendSliceAssumeCapacity(ids);
        }
        for (delta.edits) |e| {
            if (e.slot != Delta.append_slot) continue;
            const list = if (e.layer == 0) &self.adjacency.items[e.owner] else &self.upper.items[e.owner][e.layer - 1];
            try list.ensureUnusedCapacity(alloc, 1);
        }
        var kept_vector: ?[]f32 = null;
        defer if (kept_vector) |kv| alloc.free(kv);
        if (delta.upper_vector.len > 0) {
            kept_vector = try alloc.dupe(f32, delta.upper_vector);
            try self.upper_vectors.ensureUnusedCapacity(alloc, 1);
        }

        self.appendCodeAssumeCapacity(delta.code);
        self.flags.appendAssumeCapacity(0);
        self.indegree.appendAssumeCapacity(0);
        for (delta.forward) |u| self.indegree.items[u] += 1;
        self.adjacency.appendAssumeCapacity(forward);
        forward_owned = false;
        self.upper.appendAssumeCapacity(new_upper);
        upper_owned = false;
        if (kept_vector) |kv| {
            self.upper_vectors.putAssumeCapacity(id, kv);
            kept_vector = null;
        }
        self.node_count += 1;

        for (delta.edits) |e| {
            const list = if (e.layer == 0) &self.adjacency.items[e.owner] else &self.upper.items[e.owner][e.layer - 1];
            if (e.slot == Delta.append_slot) {
                list.appendAssumeCapacity(id);
            } else {
                const old = list.items[e.slot];
                list.items[e.slot] = id;
                if (e.layer == 0) self.indegree.items[old] -= 1;
            }
            if (e.layer == 0) self.indegree.items[id] += 1;
        }
        if (delta.level > self.max_level) {
            self.max_level = delta.level;
            self.entry = id;
        }
        self.version += 1;
        return undo;
    }

    /// Reverts the most recently applied delta. Deltas must be reverted in
    /// reverse order of application.
    pub fn revertDelta(self: *Index, delta: Delta, undo: Undo) void {
        std.debug.assert(self.version == delta.base_version + 1);
        self.version -= 1;
        switch (delta.kind) {
            .delete => {
                if (undo.was_live) {
                    self.flags.items[delta.id] &= ~flag_deleted;
                    self.deleted_count -= 1;
                }
                return;
            },
            .insert => {},
        }
        const id = delta.id;
        std.debug.assert(id + 1 == self.node_count);
        var i = delta.edits.len;
        while (i > 0) {
            i -= 1;
            const e = delta.edits[i];
            const list = if (e.layer == 0) &self.adjacency.items[e.owner] else &self.upper.items[e.owner][e.layer - 1];
            if (e.slot == Delta.append_slot) {
                std.debug.assert(list.items[list.items.len - 1] == id);
                list.items.len -= 1;
            } else {
                list.items[e.slot] = e.old;
                if (e.layer == 0) self.indegree.items[e.old] += 1;
            }
            if (e.layer == 0) self.indegree.items[id] -= 1;
        }
        for (delta.forward) |u| self.indegree.items[u] -= 1;
        var forward = self.adjacency.pop().?;
        forward.deinit(self.alloc);
        freeLists(self.alloc, self.upper.pop().?);
        if (self.upper_vectors.fetchRemove(id)) |kv| self.alloc.free(kv.value);
        self.flags.items.len -= 1;
        self.indegree.items.len -= 1;
        self.popCode();
        self.node_count -= 1;
        self.entry = undo.prev_entry;
        self.max_level = undo.prev_max_level;
    }

    fn popCode(self: *Index) void {
        if (self.rabitq) |*r| return r.popRow();
        self.codes.items.len -= self.config.pq_subspaces;
    }

    fn validateInsertDelta(self: *const Index, delta: Delta) !void {
        const id = delta.id;
        if (id != self.node_count) return error.InvalidDelta;
        if (delta.level != levelFor(self.config.seed, id, self.config.max_degree)) return error.InvalidDelta;
        if (delta.code.len != self.codeByteLen()) return error.InvalidDelta;
        if (delta.upper.len != delta.level) return error.InvalidDelta;
        const want_vector: usize = if (self.config.store_upper and delta.level > 0) self.dimension() else 0;
        if (delta.upper_vector.len != want_vector) return error.InvalidDelta;
        if (delta.forward.len > self.config.max_degree) return error.InvalidDelta;
        for (delta.forward) |u| if (u >= self.node_count) return error.InvalidDelta;
        for (delta.upper, 1..) |ids, l| {
            if (ids.len > self.config.max_degree) return error.InvalidDelta;
            for (ids) |u| if (u >= self.node_count or self.upper.items[u].len < l) return error.InvalidDelta;
        }
        // Slots refer to list positions before this delta; appends only grow
        // lists, so a slot valid before stays valid while edits apply.
        for (delta.edits) |e| {
            if (e.owner >= self.node_count) return error.InvalidDelta;
            if (e.layer > 0 and self.upper.items[e.owner].len < e.layer) return error.InvalidDelta;
            const len = if (e.layer == 0) self.adjacency.items[e.owner].items.len else self.upper.items[e.owner][e.layer - 1].items.len;
            if (e.slot != Delta.append_slot and e.slot >= len) return error.InvalidDelta;
            if (e.slot != Delta.append_slot) {
                const current = if (e.layer == 0) self.adjacency.items[e.owner].items[e.slot] else self.upper.items[e.owner][e.layer - 1].items[e.slot];
                if (current != e.old) return error.InvalidDelta;
                if (e.layer == 0 and self.indegree.items[current] == 0) return error.InvalidDelta;
            }
        }
    }

    /// Bytes of one node's compressed code.
    fn codeByteLen(self: *const Index) usize {
        if (self.rabitq) |*r| return r.rowByteLen();
        return self.config.pq_subspaces;
    }

    /// Encodes a (normalized for cosine) vector into code bytes.
    fn encodeCode(self: *const Index, alloc: Allocator, v: []const f32) ![]u8 {
        if (self.rabitq) |*r| return try r.encodeRowBytes(alloc, v);
        const out = try alloc.alloc(u8, self.config.pq_subspaces);
        self.pq.?.encode(v, out);
        return out;
    }

    /// Approximate reconstruction from code bytes, in the same space as
    /// `decodeApprox`.
    fn decodeBytes(self: *const Index, bytes: []const u8, out: []f32) void {
        if (self.rabitq) |*r| return r.decodeRowBytes(bytes, out);
        self.pq.?.decode(bytes, out);
    }

    fn reserveCode(self: *Index, alloc: Allocator) !void {
        if (self.rabitq) |*r| return r.reserve(alloc, 1);
        try self.codes.ensureUnusedCapacity(alloc, self.config.pq_subspaces);
    }

    fn appendCodeAssumeCapacity(self: *Index, bytes: []const u8) void {
        if (self.rabitq) |*r| return r.appendRowBytesAssumeCapacity(bytes);
        self.codes.appendSliceAssumeCapacity(bytes);
    }

    /// Approximate reconstruction of a stored vector (in rotated space for
    /// RaBitQ). Only used to compare stored vectors with each other.
    fn decodeApprox(self: *const Index, id: u32, out: []f32) void {
        if (self.upper_vectors.get(id)) |v| return @memcpy(out, v);
        if (self.rabitq) |*r| return r.decode(id, out);
        self.pq.?.decode(self.code(id), out);
    }

    /// Copies a (normalized for cosine) vector into the space `decodeApprox`
    /// uses.
    fn toDecodeSpace(self: *const Index, alloc: Allocator, v: []const f32) ![]f32 {
        const out = try alloc.alloc(f32, v.len);
        if (self.rabitq) |*r| {
            _ = r.rotation.transform(v, out);
        } else @memcpy(out, v);
        return out;
    }

    fn code(self: *const Index, id: u32) []const u8 {
        const sub = self.config.pq_subspaces;
        return self.codes.items[@as(usize, id) * sub ..][0..sub];
    }

    /// Greedy descent with approximate distances from layer `from` down to
    /// layer `to + 1`. Costs no recomputation.
    fn greedyDescentApprox(self: *const Index, approx: *Approx, start: u32, from: u8, to: u8, stats: *SearchStats) !u32 {
        if (from <= to) return start;
        var cur = start;
        var cur_d = (try approx.scoreOne(cur)).dist;
        var layer = from;
        while (layer > to) : (layer -= 1) {
            var improved = true;
            while (improved) {
                improved = false;
                const nbs = self.upper.items[cur][layer - 1].items;
                const ests = try approx.score(nbs);
                stats.approx_evaluated += @intCast(nbs.len);
                for (nbs, ests) |nb, e| {
                    if (e.dist < cur_d) {
                        cur_d = e.dist;
                        cur = nb;
                        improved = true;
                    }
                }
            }
        }
        return cur;
    }

    /// Greedy descent to layer 1 with exact distances. Each step recomputes
    /// the unseen neighbors of the current node in one batch.
    fn greedyDescentRecompute(self: *const Index, alloc: Allocator, source: EmbeddingSource, q: []const f32, start: u32, from: u8, stats: *SearchStats) !u32 {
        if (from == 0) return start;
        var seen = std.AutoHashMapUnmanaged(u32, f32).empty;
        defer seen.deinit(alloc);
        var ids = std.ArrayListUnmanaged(u32).empty;
        defer ids.deinit(alloc);
        var buf = std.ArrayListUnmanaged(f32).empty;
        defer buf.deinit(alloc);
        var dists = std.ArrayListUnmanaged(f32).empty;
        defer dists.deinit(alloc);

        try ids.append(alloc, start);
        try dists.resize(alloc, 1);
        try self.recomputeDistances(alloc, source, q, ids.items, &buf, dists.items, stats);
        try seen.put(alloc, start, dists.items[0]);
        var cur = start;
        var cur_d = dists.items[0];
        var layer = from;
        while (layer > 0) : (layer -= 1) {
            var improved = true;
            while (improved) {
                improved = false;
                const nbs = self.upper.items[cur][layer - 1].items;
                ids.clearRetainingCapacity();
                for (nbs) |nb| if (!seen.contains(nb)) try ids.append(alloc, nb);
                try dists.resize(alloc, ids.items.len);
                try self.recomputeDistances(alloc, source, q, ids.items, &buf, dists.items, stats);
                for (ids.items, dists.items) |id, dist| try seen.put(alloc, id, dist);
                for (nbs) |nb| {
                    const dist = seen.get(nb).?;
                    if (dist < cur_d) {
                        cur_d = dist;
                        cur = nb;
                        improved = true;
                    }
                }
            }
        }
        return cur;
    }

    /// Recomputes `ids` in one EmbeddingSource call and writes their exact
    /// distances to `q` into `out`.
    fn recomputeDistances(self: *const Index, alloc: Allocator, source: EmbeddingSource, q: []const f32, ids: []const u32, buf: *std.ArrayListUnmanaged(f32), out: []f32, stats: *SearchStats) !void {
        if (ids.len == 0) return;
        const d = self.dimension();
        try buf.resize(alloc, ids.len * d);
        try source.recompute(ids, buf.items);
        stats.batches += 1;
        stats.recomputed += @intCast(ids.len);
        for (out, 0..) |*o, i| {
            const row = buf.items[i * d ..][0..d];
            if (self.config.metric == .cosine) normalize(row);
            o.* = distance(self.config.metric, q, row);
        }
    }

    fn neighbors(self: *const Index, v: u32, layer: u8) []const u32 {
        return if (layer == 0) self.adjacency.items[v].items else self.upper.items[v][layer - 1].items;
    }

    /// Beam search on one layer with approximate distances only.
    fn beamSearchApprox(self: *const Index, alloc: Allocator, approx: *Approx, layer: u8, start: u32, ef: u32, out: *std.ArrayListUnmanaged(Cand)) !void {
        var visited = std.AutoHashMapUnmanaged(u32, void).empty;
        defer visited.deinit(alloc);
        var candidates = Heap(false){};
        defer candidates.deinit(alloc);
        var results = Heap(true){};
        defer results.deinit(alloc);
        var fresh = std.ArrayListUnmanaged(u32).empty;
        defer fresh.deinit(alloc);
        const first = Cand{ .dist = (try approx.scoreOne(start)).dist, .id = start };
        try visited.put(alloc, start, {});
        try candidates.push(alloc, first);
        try results.push(alloc, first);
        while (candidates.len() > 0) {
            const c = candidates.pop();
            if (results.len() >= ef and c.dist > results.top().dist) break;
            fresh.clearRetainingCapacity();
            for (self.neighbors(c.id, layer)) |nb| {
                const gop = try visited.getOrPut(alloc, nb);
                if (!gop.found_existing) try fresh.append(alloc, nb);
            }
            const ests = try approx.score(fresh.items);
            for (fresh.items, ests) |nb, e| {
                const nc = Cand{ .dist = e.dist, .id = nb };
                if (results.len() < ef) {
                    try results.push(alloc, nc);
                } else if (nc.dist < results.top().dist) {
                    results.replaceTop(nc);
                } else continue;
                try candidates.push(alloc, nc);
            }
        }
        out.clearRetainingCapacity();
        try out.appendSlice(alloc, results.items.items);
        std.mem.sortUnstable(Cand, out.items, {}, candLessThan);
    }

    /// Bound-driven search (RaBitQ only). Phase 1 walks layer 0 with
    /// estimates alone. Phase 2 recomputes candidates in increasing order of
    /// their lower bound (estimate - bound_scale * error bound) and stops once
    /// the next lower bound cannot beat the k-th exact distance found so far.
    /// Fills `out` with up to `k` exact live results in ascending order.
    fn boundedSearch(
        self: *const Index,
        alloc: Allocator,
        source: EmbeddingSource,
        q: []const f32,
        approx: *Approx,
        start: u32,
        opts: SearchOptions,
        out: *std.ArrayListUnmanaged(Cand),
        stats: *SearchStats,
    ) !void {
        const ef: usize = @max(opts.ef, opts.k);
        const k: usize = opts.k;
        const scale = @max(opts.bound_scale, 0);

        const Pooled = struct { id: u32, lower: f32 };
        var pool = std.ArrayListUnmanaged(Pooled).empty;
        defer pool.deinit(alloc);
        var visited = try std.DynamicBitSetUnmanaged.initEmpty(alloc, self.node_count);
        defer visited.deinit(alloc);
        var candidates = Heap(false){};
        defer candidates.deinit(alloc);
        var beam = Heap(true){};
        defer beam.deinit(alloc);
        var fresh = std.ArrayListUnmanaged(u32).empty;
        defer fresh.deinit(alloc);

        const first = try approx.scoreOne(start);
        visited.set(start);
        try pool.append(alloc, .{ .id = start, .lower = first.dist - scale * first.bound });
        try candidates.push(alloc, .{ .dist = first.dist, .id = start });
        try beam.push(alloc, .{ .dist = first.dist, .id = start });
        stats.approx_evaluated += 1;
        while (candidates.len() > 0) {
            const c = candidates.pop();
            if (beam.len() >= ef and c.dist > beam.top().dist) break;
            stats.expanded += 1;
            fresh.clearRetainingCapacity();
            for (self.adjacency.items[c.id].items) |nb| {
                if (visited.isSet(nb)) continue;
                visited.set(nb);
                try fresh.append(alloc, nb);
            }
            const ests = try approx.score(fresh.items);
            stats.approx_evaluated += @intCast(fresh.items.len);
            for (fresh.items, ests) |nb, e| {
                try pool.append(alloc, .{ .id = nb, .lower = e.dist - scale * e.bound });
                const nc = Cand{ .dist = e.dist, .id = nb };
                if (beam.len() < ef) {
                    try beam.push(alloc, nc);
                } else if (nc.dist < beam.top().dist) {
                    beam.replaceTop(nc);
                } else continue;
                try candidates.push(alloc, nc);
            }
        }

        const ByLower = struct {
            fn lessThan(_: void, a: Pooled, b: Pooled) bool {
                return a.lower < b.lower;
            }
        };
        std.mem.sortUnstable(Pooled, pool.items, {}, ByLower.lessThan);

        var top = Heap(true){};
        defer top.deinit(alloc);
        var ids = std.ArrayListUnmanaged(u32).empty;
        defer ids.deinit(alloc);
        var dists = std.ArrayListUnmanaged(f32).empty;
        defer dists.deinit(alloc);
        var buf = std.ArrayListUnmanaged(f32).empty;
        defer buf.deinit(alloc);
        const chunk: usize = if (opts.batch_size > 0) opts.batch_size else @max(k, 1);
        var i: usize = 0;
        while (i < pool.items.len) {
            const tau = if (top.len() >= k) top.top().dist else std.math.inf(f32);
            ids.clearRetainingCapacity();
            while (i < pool.items.len and ids.items.len < chunk) : (i += 1) {
                const p = pool.items[i];
                if (p.lower >= tau) break;
                if (self.isDeleted(p.id)) continue;
                if (self.upper_vectors.contains(p.id)) {
                    // Stored exactly (bound 0): no recomputation needed.
                    const c = Cand{ .dist = p.lower, .id = p.id };
                    if (top.len() < k) {
                        try top.push(alloc, c);
                    } else if (c.dist < top.top().dist) {
                        top.replaceTop(c);
                    }
                    continue;
                }
                try ids.append(alloc, p.id);
            }
            if (ids.items.len == 0) {
                if (i < pool.items.len and pool.items[i].lower >= tau) break;
                continue;
            }
            try dists.resize(alloc, ids.items.len);
            try self.recomputeDistances(alloc, source, q, ids.items, &buf, dists.items, stats);
            for (ids.items, dists.items) |id, dist| {
                const c = Cand{ .dist = dist, .id = id };
                if (top.len() < k) {
                    try top.push(alloc, c);
                } else if (c.dist < top.top().dist) {
                    top.replaceTop(c);
                }
            }
        }

        out.clearRetainingCapacity();
        try out.appendSlice(alloc, top.items.items);
        std.mem.sortUnstable(Cand, out.items, {}, candLessThan);
    }

    /// Algorithm 2 on layer 0, starting at `start`. Fills `out` with up to
    /// `ef` exact candidates in ascending distance order, including deleted
    /// nodes.
    fn twoLevelSearch(
        self: *const Index,
        alloc: Allocator,
        source: EmbeddingSource,
        q: []const f32,
        approx: *Approx,
        start: u32,
        opts: SearchOptions,
        out: *std.ArrayListUnmanaged(Cand),
        stats: *SearchStats,
    ) !void {
        const ef: usize = @max(opts.ef, opts.k);
        const ratio: f32 = std.math.clamp(opts.rerank_ratio, 0, 1);

        var visited = try std.DynamicBitSetUnmanaged.initEmpty(alloc, self.node_count);
        defer visited.deinit(alloc);
        var exact_queue = Heap(false){};
        defer exact_queue.deinit(alloc);
        var approx_queue = Heap(false){};
        defer approx_queue.deinit(alloc);
        var results = Heap(true){};
        defer results.deinit(alloc);
        var pending = std.ArrayListUnmanaged(u32).empty;
        defer pending.deinit(alloc);
        var buf = std.ArrayListUnmanaged(f32).empty;
        defer buf.deinit(alloc);
        var dists = std.ArrayListUnmanaged(f32).empty;
        defer dists.deinit(alloc);
        var fresh = std.ArrayListUnmanaged(u32).empty;
        defer fresh.deinit(alloc);

        const Flush = struct {
            fn run(
                index: *const Index,
                a: Allocator,
                src: EmbeddingSource,
                query: []const f32,
                ids: *std.ArrayListUnmanaged(u32),
                vectors: *std.ArrayListUnmanaged(f32),
                ds: *std.ArrayListUnmanaged(f32),
                eq: *Heap(false),
                res: *Heap(true),
                limit: usize,
                st: *SearchStats,
            ) !void {
                if (ids.items.len == 0) return;
                try ds.resize(a, ids.items.len);
                try index.recomputeDistances(a, src, query, ids.items, vectors, ds.items, st);
                for (ids.items, ds.items) |id, dist| {
                    const c = Cand{ .dist = dist, .id = id };
                    if (res.len() < limit) {
                        try res.push(a, c);
                    } else if (c.dist < res.top().dist) {
                        res.replaceTop(c);
                    } else continue;
                    try eq.push(a, c);
                }
                ids.clearRetainingCapacity();
            }
        };

        visited.set(start);
        try pending.append(alloc, start);
        try Flush.run(self, alloc, source, q, &pending, &buf, &dists, &exact_queue, &results, ef, stats);

        while (true) {
            if (exact_queue.len() == 0) {
                if (pending.items.len > 0) {
                    try Flush.run(self, alloc, source, q, &pending, &buf, &dists, &exact_queue, &results, ef, stats);
                    continue;
                }
                // The exact queue ran dry. Like the paper, stop once the beam
                // is full; only keep promoting while it still has room.
                if (approx_queue.len() == 0 or results.len() >= ef) break;
                try promote(alloc, &approx_queue, &pending, ratio);
                try Flush.run(self, alloc, source, q, &pending, &buf, &dists, &exact_queue, &results, ef, stats);
                continue;
            }

            const c = exact_queue.pop();
            if (results.len() >= ef and c.dist > results.top().dist) {
                if (pending.items.len > 0) {
                    try Flush.run(self, alloc, source, q, &pending, &buf, &dists, &exact_queue, &results, ef, stats);
                    continue;
                }
                break;
            }

            stats.expanded += 1;
            fresh.clearRetainingCapacity();
            for (self.adjacency.items[c.id].items) |nb| {
                if (visited.isSet(nb)) continue;
                visited.set(nb);
                try fresh.append(alloc, nb);
            }
            const ests = try approx.score(fresh.items);
            stats.approx_evaluated += @intCast(fresh.items.len);
            for (fresh.items, ests) |nb, e| try approx_queue.push(alloc, .{ .dist = e.dist, .id = nb });

            if (opts.two_level) {
                try promote(alloc, &approx_queue, &pending, ratio);
            } else {
                while (approx_queue.len() > 0) try pending.append(alloc, approx_queue.pop().id);
            }
            if (opts.batch_size == 0 or pending.items.len >= opts.batch_size) {
                try Flush.run(self, alloc, source, q, &pending, &buf, &dists, &exact_queue, &results, ef, stats);
            }
        }

        try out.ensureTotalCapacity(alloc, results.len());
        out.clearRetainingCapacity();
        out.appendSliceAssumeCapacity(results.items.items);
        std.mem.sortUnstable(Cand, out.items, {}, candLessThan);
    }

    // ---------------------------------------------------------------------
    // Serialization
    // ---------------------------------------------------------------------

    const magic = "LEANNIDX";
    pub const format_version: u32 = 4;

    /// Serializes the graph layers, quantizer state, codes, and flags. No
    /// embeddings are written. Layout after the header: quantizer section,
    /// flags, levels, layer-0 CSR offsets and neighbors, then for each node
    /// with level > 0 and each of its layers a u32 count and the neighbors.
    pub fn serialize(self: *const Index, alloc: Allocator) ![]u8 {
        if (self.config.store_upper) return error.UnsupportedStoreUpper;
        var w: std.Io.Writer.Allocating = .init(alloc);
        defer w.deinit();
        const wr = &w.writer;
        const cfg = self.config;
        try wr.writeAll(magic);
        try wr.writeInt(u32, format_version, .little);
        try wr.writeInt(u32, cfg.dims, .little);
        try wr.writeByte(@backingInt(cfg.metric));
        try wr.writeByte(@backingInt(cfg.quantizer));
        try wr.writeInt(u32, cfg.max_degree, .little);
        try wr.writeInt(u32, cfg.low_degree, .little);
        try wr.writeInt(u32, cfg.ef_construction, .little);
        try wr.writeInt(u32, @bitCast(cfg.hub_fraction), .little);
        try wr.writeInt(u32, cfg.pq_subspaces, .little);
        try wr.writeInt(u64, cfg.seed, .little);
        try wr.writeInt(u32, self.node_count, .little);
        try wr.writeInt(u32, self.entry, .little);
        try wr.writeByte(self.max_level);
        try wr.writeInt(u64, self.version, .little);
        if (self.pq) |*pq| {
            try wr.writeInt(u32, pq.k, .little);
            for (pq.centroids) |f| try wr.writeInt(u32, @bitCast(f), .little);
            try wr.writeAll(self.codes.items);
        } else {
            try self.rabitq.?.write(wr);
        }
        try wr.writeAll(self.flags.items);
        for (self.upper.items) |lists| try wr.writeByte(@intCast(lists.len));
        var offset: u32 = 0;
        try wr.writeInt(u32, 0, .little);
        for (self.adjacency.items) |list| {
            offset += @intCast(list.items.len);
            try wr.writeInt(u32, offset, .little);
        }
        for (self.adjacency.items) |list| {
            for (list.items) |nb| try wr.writeInt(u32, nb, .little);
        }
        for (self.upper.items) |lists| {
            for (lists) |list| {
                try wr.writeInt(u32, @intCast(list.items.len), .little);
                for (list.items) |nb| try wr.writeInt(u32, nb, .little);
            }
        }
        const crc = std.hash.Crc32.hash(w.written());
        try wr.writeInt(u32, crc, .little);
        return w.toOwnedSlice();
    }

    pub fn deserialize(alloc: Allocator, bytes: []const u8) !Index {
        if (bytes.len < magic.len + 4) return error.InvalidFormat;
        const body = bytes[0 .. bytes.len - 4];
        const stored_crc = std.mem.readInt(u32, bytes[bytes.len - 4 ..][0..4], .little);
        if (std.hash.Crc32.hash(body) != stored_crc) return error.InvalidFormat;

        var r: std.Io.Reader = .fixed(body);
        const head = r.take(magic.len) catch return error.InvalidFormat;
        if (!std.mem.eql(u8, head, magic)) return error.InvalidFormat;
        if ((try readU32(&r)) != format_version) return error.UnsupportedVersion;
        var cfg = Config{ .dims = try readU32(&r) };
        cfg.metric = std.enums.fromInt(Metric, r.takeByte() catch return error.InvalidFormat) orelse return error.InvalidFormat;
        cfg.quantizer = std.enums.fromInt(Quantizer, r.takeByte() catch return error.InvalidFormat) orelse return error.InvalidFormat;
        cfg.max_degree = try readU32(&r);
        cfg.low_degree = try readU32(&r);
        cfg.ef_construction = try readU32(&r);
        cfg.hub_fraction = @bitCast(try readU32(&r));
        cfg.pq_subspaces = try readU32(&r);
        cfg.seed = r.takeInt(u64, .little) catch return error.InvalidFormat;
        cfg = cfg.resolved() catch return error.InvalidFormat;
        const n = try readU32(&r);
        const entry = try readU32(&r);
        const max_level = r.takeByte() catch return error.InvalidFormat;
        const version = r.takeInt(u64, .little) catch return error.InvalidFormat;
        if (n == 0 or entry >= n or max_level > max_level_cap) return error.InvalidFormat;

        var index = Index{ .alloc = alloc, .config = cfg, .node_count = n, .entry = entry, .max_level = max_level, .version = version };
        errdefer index.deinit();

        switch (cfg.quantizer) {
            .pq => {
                const k = try readU32(&r);
                if (k == 0 or k > 256) return error.InvalidFormat;
                const sub: usize = cfg.pq_subspaces;
                const centroid_len = @as(usize, k) * cfg.dims;
                if (r.bufferedLen() < centroid_len * 4 + @as(usize, n) * sub) return error.InvalidFormat;
                const centroids = try alloc.alloc(f32, centroid_len);
                for (centroids) |*f| f.* = @bitCast(readU32(&r) catch unreachable);
                index.pq = .{ .dims = cfg.dims, .subspaces = cfg.pq_subspaces, .sub_dims = cfg.dims / cfg.pq_subspaces, .k = k, .centroids = centroids };
                try index.codes.appendSlice(alloc, r.take(@as(usize, n) * sub) catch unreachable);
                for (index.codes.items) |c| if (c >= k) return error.InvalidFormat;
            },
            .rabitq => {
                index.rabitq = try RaBitQCodes.init(alloc, cfg);
                try index.rabitq.?.read(alloc, &r, n);
            },
        }

        // Fixed-size tail before allocating per-node structures.
        if (r.bufferedLen() < @as(usize, n) * 2 + (@as(usize, n) + 1) * 4) return error.InvalidFormat;
        try index.flags.appendSlice(alloc, r.take(n) catch unreachable);
        for (index.flags.items) |f| {
            if (f & ~(flag_hub | flag_deleted) != 0) return error.InvalidFormat;
            if (f & flag_deleted != 0) index.deleted_count += 1;
        }
        const levels = r.take(n) catch unreachable;
        for (levels) |l| if (l > max_level) return error.InvalidFormat;
        if (levels[entry] != max_level) return error.InvalidFormat;

        const offsets = try alloc.alloc(u32, @as(usize, n) + 1);
        defer alloc.free(offsets);
        for (offsets) |*o| o.* = try readU32(&r);
        if (offsets[0] != 0) return error.InvalidFormat;
        for (offsets[1..], offsets[0..n]) |hi, lo| if (hi < lo) return error.InvalidFormat;
        if (r.bufferedLen() < @as(usize, offsets[n]) * 4) return error.InvalidFormat;

        try index.adjacency.ensureTotalCapacity(alloc, n);
        for (0..n) |i| {
            const len = offsets[i + 1] - offsets[i];
            index.adjacency.appendAssumeCapacity(.empty);
            const list = &index.adjacency.items[i];
            try list.ensureTotalCapacity(alloc, len);
            for (0..len) |_| {
                const nb = try readU32(&r);
                if (nb >= n) return error.InvalidFormat;
                list.appendAssumeCapacity(nb);
            }
        }

        try index.upper.ensureTotalCapacity(alloc, n);
        for (levels) |level| {
            const lists = try alloc.alloc(NeighborList, level);
            @memset(lists, NeighborList.empty);
            index.upper.appendAssumeCapacity(lists);
            for (lists, 1..) |*list, layer| {
                const len = try readU32(&r);
                if (len > cfg.max_degree) return error.InvalidFormat;
                try list.ensureTotalCapacity(alloc, len);
                for (0..len) |_| {
                    const nb = try readU32(&r);
                    if (nb >= n or levels[nb] < layer) return error.InvalidFormat;
                    list.appendAssumeCapacity(nb);
                }
            }
        }
        if (r.bufferedLen() != 0) return error.InvalidFormat;
        try index.rebuildIndegree();
        return index;
    }
};

/// A replicable change to an index: an insert planned by `planInsert` or a
/// delete planned by `planDelete`. Apply with `Index.applyDelta`. Owned
/// slices are freed by `deinit`.
pub const Delta = struct {
    kind: Kind,
    /// Index version the delta was planned against.
    base_version: u64,
    /// New node id (insert) or deleted node id (delete).
    id: u32,
    level: u8 = 0,
    /// Compressed code bytes for the new node.
    code: []u8 = &.{},
    /// Layer-0 forward edges.
    forward: []u32 = &.{},
    /// Forward edges for layers 1..level.
    upper: [][]u32 = &.{},
    /// Reverse-link edits, applied in order. Each writes the new id into
    /// `owner`'s list on `layer`, at `slot` or appended.
    edits: []Edit = &.{},
    /// `Config.store_upper` only: the new node's vector when it lands on an
    /// upper layer. Not encoded yet.
    upper_vector: []f32 = &.{},

    pub const Kind = enum(u8) { insert = 1, delete = 2 };
    pub const append_slot: u32 = std.math.maxInt(u32);
    /// `old` is the id being replaced (unused for appends). Applying checks
    /// it, which catches a replica whose graph has diverged, and reverting
    /// restores it.
    pub const Edit = struct { layer: u8, owner: u32, slot: u32, old: u32 = 0 };

    pub fn deinit(self: *Delta, alloc: Allocator) void {
        if (self.code.len > 0) alloc.free(self.code);
        if (self.forward.len > 0) alloc.free(self.forward);
        for (self.upper) |ids| if (ids.len > 0) alloc.free(ids);
        if (self.upper.len > 0) alloc.free(self.upper);
        if (self.edits.len > 0) alloc.free(self.edits);
        if (self.upper_vector.len > 0) alloc.free(self.upper_vector);
        self.* = undefined;
    }

    pub fn encode(self: Delta, alloc: Allocator) ![]u8 {
        if (self.upper_vector.len > 0) return error.UnsupportedStoreUpper;
        var w: std.Io.Writer.Allocating = .init(alloc);
        defer w.deinit();
        const wr = &w.writer;
        try wr.writeByte(@backingInt(self.kind));
        try wr.writeInt(u64, self.base_version, .little);
        try wr.writeInt(u32, self.id, .little);
        if (self.kind == .insert) {
            try wr.writeByte(self.level);
            try wr.writeInt(u32, @intCast(self.code.len), .little);
            try wr.writeAll(self.code);
            try writeIds(wr, self.forward);
            for (self.upper) |ids| try writeIds(wr, ids);
            try wr.writeInt(u32, @intCast(self.edits.len), .little);
            for (self.edits) |e| {
                try wr.writeByte(e.layer);
                try wr.writeInt(u32, e.owner, .little);
                try wr.writeInt(u32, e.slot, .little);
                try wr.writeInt(u32, e.old, .little);
            }
        }
        return w.toOwnedSlice();
    }

    fn writeIds(wr: *std.Io.Writer, ids: []const u32) !void {
        try wr.writeInt(u32, @intCast(ids.len), .little);
        for (ids) |id| try wr.writeInt(u32, id, .little);
    }

    /// Decodes a delta. Structural limits keep a corrupt record from
    /// allocating unboundedly; `applyDelta` validates it against the index.
    pub fn decode(alloc: Allocator, bytes: []const u8) !Delta {
        var r: std.Io.Reader = .fixed(bytes);
        const kind = std.enums.fromInt(Kind, r.takeByte() catch return error.InvalidDelta) orelse return error.InvalidDelta;
        var out = Delta{
            .kind = kind,
            .base_version = r.takeInt(u64, .little) catch return error.InvalidDelta,
            .id = r.takeInt(u32, .little) catch return error.InvalidDelta,
        };
        errdefer out.deinit(alloc);
        if (kind == .insert) {
            out.level = r.takeByte() catch return error.InvalidDelta;
            if (out.level > max_level_cap) return error.InvalidDelta;
            const code_len = r.takeInt(u32, .little) catch return error.InvalidDelta;
            if (code_len > 1 << 20) return error.InvalidDelta;
            out.code = try alloc.dupe(u8, r.take(code_len) catch return error.InvalidDelta);
            out.forward = try readIds(alloc, &r);
            out.upper = try alloc.alloc([]u32, out.level);
            @memset(out.upper, &.{});
            for (out.upper) |*ids| ids.* = try readIds(alloc, &r);
            const n_edits = r.takeInt(u32, .little) catch return error.InvalidDelta;
            if (n_edits > 4096) return error.InvalidDelta;
            out.edits = try alloc.alloc(Edit, n_edits);
            for (out.edits) |*e| e.* = .{
                .layer = r.takeByte() catch return error.InvalidDelta,
                .owner = r.takeInt(u32, .little) catch return error.InvalidDelta,
                .slot = r.takeInt(u32, .little) catch return error.InvalidDelta,
                .old = r.takeInt(u32, .little) catch return error.InvalidDelta,
            };
        }
        if (r.bufferedLen() != 0) return error.InvalidDelta;
        return out;
    }

    fn readIds(alloc: Allocator, r: *std.Io.Reader) ![]u32 {
        const n = r.takeInt(u32, .little) catch return error.InvalidDelta;
        if (n > 1024) return error.InvalidDelta;
        const ids = try alloc.alloc(u32, n);
        errdefer alloc.free(ids);
        for (ids) |*id| id.* = r.takeInt(u32, .little) catch return error.InvalidDelta;
        return ids;
    }
};

const Est = struct { dist: f32, bound: f32 };

/// Per-query approximate scorer over PQ codes (ADC table, bound 0) or RaBitQ
/// codes (repo RaBitQ kernel, with error bounds). Not thread-safe; create one
/// per query.
const Approx = struct {
    alloc: Allocator,
    index: *const Index,
    table: []f32 = &.{},
    scratch: ?*rq.RaBitQuantizer.EstimateScratch = null,
    prepared: rq.RaBitQuantizer.PreparedEstimate = undefined,
    view: vproto.RaBitQuantizedVectorSet = .{},
    pairs: std.ArrayListUnmanaged(Pair) = .empty,
    ranges: std.ArrayListUnmanaged(rq.ScoreRange) = .empty,
    out: std.ArrayListUnmanaged(Est) = .empty,
    /// Query in the space of `Index.upper_vectors`, when the index has any.
    exact_q: []f32 = &.{},

    const Pair = struct { id: u32, slot: u32 };

    /// `q` must already be normalized for cosine and is in the original
    /// (unrotated) space.
    fn init(alloc: Allocator, index: *const Index, q: []const f32) !Approx {
        var self = Approx{ .alloc = alloc, .index = index };
        errdefer self.deinit();
        if (index.config.store_upper) self.exact_q = try index.toDecodeSpace(alloc, q);
        if (index.pq) |*pq| {
            self.table = try alloc.alloc(f32, pq.tableLen());
            pq.buildTable(index.config.metric, q, self.table);
            return self;
        }
        const r = &index.rabitq.?;
        const rotated = try alloc.alloc(f32, q.len);
        defer alloc.free(rotated);
        _ = r.rotation.transform(q, rotated);
        const scratch = try alloc.create(rq.RaBitQuantizer.EstimateScratch);
        errdefer alloc.destroy(scratch);
        scratch.* = try rq.RaBitQuantizer.EstimateScratch.init(alloc, q.len);
        self.scratch = scratch;
        self.view = r.view();
        self.prepared = try r.quantizer.prepareEstimate(&self.view, rotated, scratch, null);
        return self;
    }

    fn deinit(self: *Approx) void {
        if (self.table.len > 0) self.alloc.free(self.table);
        if (self.exact_q.len > 0) self.alloc.free(self.exact_q);
        if (self.scratch) |s| {
            s.deinit(self.alloc);
            self.alloc.destroy(s);
        }
        self.pairs.deinit(self.alloc);
        self.ranges.deinit(self.alloc);
        self.out.deinit(self.alloc);
    }

    /// Scores `ids` (unique). The returned slice is valid until the next call.
    fn score(self: *Approx, ids: []const u32) ![]const Est {
        const out = try self.scoreCodes(ids);
        if (self.exact_q.len == 0) return out;
        for (ids, out) |id, *o| {
            if (self.index.upper_vectors.get(id)) |v| o.* = .{ .dist = distance(self.index.config.metric, self.exact_q, v), .bound = 0 };
        }
        return out;
    }

    fn scoreCodes(self: *Approx, ids: []const u32) ![]Est {
        try self.out.resize(self.alloc, ids.len);
        const out = self.out.items;
        if (ids.len == 0) return out;
        if (self.index.pq) |*pq| {
            for (ids, out) |id, *o| o.* = .{ .dist = pq.adc(self.table, self.index.code(id)), .bound = 0 };
            return out;
        }
        // The RaBitQ kernel scores sorted, non-overlapping ranges.
        self.pairs.clearRetainingCapacity();
        try self.pairs.ensureTotalCapacity(self.alloc, ids.len);
        for (ids, 0..) |id, slot| self.pairs.appendAssumeCapacity(.{ .id = id, .slot = @intCast(slot) });
        const ById = struct {
            fn lessThan(_: void, a: Pair, b: Pair) bool {
                return a.id < b.id;
            }
        };
        std.mem.sortUnstable(Pair, self.pairs.items, {}, ById.lessThan);
        self.ranges.clearRetainingCapacity();
        for (self.pairs.items) |p| {
            const id: usize = p.id;
            if (self.ranges.items.len > 0) {
                const last = &self.ranges.items[self.ranges.items.len - 1];
                if (id < last.end) continue; // duplicate id
                if (id == last.end) {
                    last.end += 1;
                    continue;
                }
            }
            try self.ranges.append(self.alloc, .{ .start = id, .end = id + 1 });
        }
        const Sink = struct {
            pairs: []const Pair,
            out: []Est,
            cursor: usize = 0,
            pub fn write(sink: *@This(), i: usize, d: f32, b: f32) void {
                while (sink.pairs[sink.cursor].id != i) sink.cursor += 1;
                while (sink.cursor < sink.pairs.len and sink.pairs[sink.cursor].id == i) : (sink.cursor += 1) {
                    sink.out[sink.pairs[sink.cursor].slot] = .{ .dist = d, .bound = b };
                }
            }
        };
        var sink = Sink{ .pairs = self.pairs.items, .out = out };
        try self.index.rabitq.?.quantizer.estimatePreparedDistancesInRangesTo(&self.view, self.prepared, null, self.ranges.items, &sink);
        return out;
    }

    fn scoreOne(self: *Approx, id: u32) !Est {
        return (try self.score(&.{id}))[0];
    }
};

/// 1-bit RaBitQ codes (Gao & Long, 2024) using the repo's RaBitQ kernel,
/// stored in growable lists so inserts can append. Vectors are rotated with
/// a seeded random orthogonal (Givens) transform and encoded relative to the
/// mean of the build set.
pub const RaBitQCodes = struct {
    quantizer: rq.RaBitQuantizer,
    rotation: vmath.RandomOrthogonalTransformer,
    metric: vmath.DistanceMetric,
    dims: usize,
    width: usize,
    centroid: []f32 = &.{},
    centroid_norm: f32 = 0,
    codes: std.ArrayListUnmanaged(u64) = .empty,
    code_counts: std.ArrayListUnmanaged(u32) = .empty,
    centroid_distances: std.ArrayListUnmanaged(f32) = .empty,
    quantized_dot_products: std.ArrayListUnmanaged(f32) = .empty,
    /// Empty for l2_squared.
    centroid_dot_products: std.ArrayListUnmanaged(f32) = .empty,

    fn init(alloc: Allocator, cfg: Config) !RaBitQCodes {
        const metric: vmath.DistanceMetric = @fromBackingInt(@intCast(@backingInt(cfg.metric)));
        var quantizer = try rq.RaBitQuantizer.init(alloc, cfg.dims, cfg.seed, metric);
        errdefer quantizer.deinit();
        const rotation = try vmath.RandomOrthogonalTransformer.init(alloc, .givens, cfg.dims, cfg.seed);
        return .{ .quantizer = quantizer, .rotation = rotation, .metric = metric, .dims = cfg.dims, .width = vrabitq.codeWidth(cfg.dims) };
    }

    fn deinit(self: *RaBitQCodes, alloc: Allocator) void {
        self.quantizer.deinit();
        self.rotation.deinit();
        if (self.centroid.len > 0) alloc.free(self.centroid);
        self.codes.deinit(alloc);
        self.code_counts.deinit(alloc);
        self.centroid_distances.deinit(alloc);
        self.quantized_dot_products.deinit(alloc);
        self.centroid_dot_products.deinit(alloc);
    }

    fn count(self: *const RaBitQCodes) usize {
        return self.code_counts.items.len;
    }

    fn rotateInPlace(self: *const RaBitQCodes, alloc: Allocator, rows: []f32) !void {
        const tmp = try alloc.alloc(f32, self.dims);
        defer alloc.free(tmp);
        var i: usize = 0;
        while (i < rows.len) : (i += self.dims) {
            const row = rows[i..][0..self.dims];
            _ = self.rotation.transform(row, tmp);
            @memcpy(row, tmp);
        }
    }

    /// Sets the centroid to the mean of `rotated` and encodes every row.
    fn encodeAll(self: *RaBitQCodes, alloc: Allocator, rotated: []const f32, n: usize) !void {
        const sum = try alloc.alloc(f64, self.dims);
        defer alloc.free(sum);
        @memset(sum, 0);
        for (0..n) |i| for (rotated[i * self.dims ..][0..self.dims], sum) |x, *s| {
            s.* += x;
        };
        self.centroid = try alloc.alloc(f32, self.dims);
        for (self.centroid, sum) |*c, s| c.* = @floatCast(s / @as(f64, @floatFromInt(n)));
        self.centroid_norm = if (self.metric == .l2_squared) 0 else @sqrt(dot(self.centroid, self.centroid));

        var set = try self.quantizer.quantize(self.centroid, rotated, n);
        defer set.deinit(alloc);
        try self.codes.appendSlice(alloc, set.codes.data);
        try self.code_counts.appendSlice(alloc, set.code_counts);
        try self.centroid_distances.appendSlice(alloc, set.centroid_distances);
        try self.quantized_dot_products.appendSlice(alloc, set.quantized_dot_products);
        try self.centroid_dot_products.appendSlice(alloc, set.centroid_dot_products);
    }

    const Row = struct {
        set: vproto.RaBitQuantizedVectorSet,

        fn deinit(self: *Row, alloc: Allocator) void {
            self.set.deinit(alloc);
        }
    };

    /// Encodes one unrotated vector without touching the stored codes.
    fn encodeRow(self: *const RaBitQCodes, alloc: Allocator, v: []const f32) !Row {
        const rotated = try alloc.alloc(f32, self.dims);
        defer alloc.free(rotated);
        _ = self.rotation.transform(v, rotated);
        return .{ .set = try self.quantizer.quantize(self.centroid, rotated, 1) };
    }

    /// Row byte layout: `width` u64 code words, u32 code count, f32 centroid
    /// distance, f32 quantized dot product, then f32 centroid dot product
    /// for inner product and cosine.
    fn rowByteLen(self: *const RaBitQCodes) usize {
        return self.width * 8 + 12 + (if (self.metric != .l2_squared) @as(usize, 4) else 0);
    }

    fn encodeRowBytes(self: *const RaBitQCodes, alloc: Allocator, v: []const f32) ![]u8 {
        var row = try self.encodeRow(alloc, v);
        defer row.deinit(alloc);
        const out = try alloc.alloc(u8, self.rowByteLen());
        var pos: usize = 0;
        for (row.set.codes.data) |word| {
            std.mem.writeInt(u64, out[pos..][0..8], word, .little);
            pos += 8;
        }
        std.mem.writeInt(u32, out[pos..][0..4], row.set.code_counts[0], .little);
        std.mem.writeInt(u32, out[pos + 4 ..][0..4], @bitCast(row.set.centroid_distances[0]), .little);
        std.mem.writeInt(u32, out[pos + 8 ..][0..4], @bitCast(row.set.quantized_dot_products[0]), .little);
        if (self.metric != .l2_squared) std.mem.writeInt(u32, out[pos + 12 ..][0..4], @bitCast(row.set.centroid_dot_products[0]), .little);
        return out;
    }

    fn appendRowBytesAssumeCapacity(self: *RaBitQCodes, bytes: []const u8) void {
        var pos: usize = 0;
        for (0..self.width) |_| {
            self.codes.appendAssumeCapacity(std.mem.readInt(u64, bytes[pos..][0..8], .little));
            pos += 8;
        }
        self.code_counts.appendAssumeCapacity(std.mem.readInt(u32, bytes[pos..][0..4], .little));
        self.centroid_distances.appendAssumeCapacity(@bitCast(std.mem.readInt(u32, bytes[pos + 4 ..][0..4], .little)));
        self.quantized_dot_products.appendAssumeCapacity(@bitCast(std.mem.readInt(u32, bytes[pos + 8 ..][0..4], .little)));
        if (self.metric != .l2_squared) self.centroid_dot_products.appendAssumeCapacity(@bitCast(std.mem.readInt(u32, bytes[pos + 12 ..][0..4], .little)));
    }

    /// Appends a copy of `other`'s row `id`. Both must share a centroid.
    fn appendRowFromAssumeCapacity(self: *RaBitQCodes, other: *const RaBitQCodes, id: u32) void {
        const i: usize = id;
        self.codes.appendSliceAssumeCapacity(other.codes.items[i * self.width ..][0..self.width]);
        self.code_counts.appendAssumeCapacity(other.code_counts.items[i]);
        self.centroid_distances.appendAssumeCapacity(other.centroid_distances.items[i]);
        self.quantized_dot_products.appendAssumeCapacity(other.quantized_dot_products.items[i]);
        if (self.metric != .l2_squared) self.centroid_dot_products.appendAssumeCapacity(other.centroid_dot_products.items[i]);
    }

    fn popRow(self: *RaBitQCodes) void {
        self.codes.items.len -= self.width;
        self.code_counts.items.len -= 1;
        self.centroid_distances.items.len -= 1;
        self.quantized_dot_products.items.len -= 1;
        if (self.metric != .l2_squared) self.centroid_dot_products.items.len -= 1;
    }

    fn decodeRowBytes(self: *const RaBitQCodes, bytes: []const u8, out: []f32) void {
        const cd: f32 = @bitCast(std.mem.readInt(u32, bytes[self.width * 8 + 4 ..][0..4], .little));
        const scale = cd / @sqrt(@as(f32, @floatFromInt(self.dims)));
        for (out, 0..) |*o, j| {
            const word = std.mem.readInt(u64, bytes[(j / 64) * 8 ..][0..8], .little);
            const bit = (word >> @intCast(63 - j % 64)) & 1;
            o.* = self.centroid[j] + (if (bit == 1) scale else -scale);
        }
    }

    fn reserve(self: *RaBitQCodes, alloc: Allocator, extra: usize) !void {
        try self.codes.ensureUnusedCapacity(alloc, extra * self.width);
        try self.code_counts.ensureUnusedCapacity(alloc, extra);
        try self.centroid_distances.ensureUnusedCapacity(alloc, extra);
        try self.quantized_dot_products.ensureUnusedCapacity(alloc, extra);
        if (self.metric != .l2_squared) try self.centroid_dot_products.ensureUnusedCapacity(alloc, extra);
    }

    fn appendRowAssumeCapacity(self: *RaBitQCodes, row: *const Row) void {
        self.codes.appendSliceAssumeCapacity(row.set.codes.data);
        self.code_counts.appendSliceAssumeCapacity(row.set.code_counts);
        self.centroid_distances.appendSliceAssumeCapacity(row.set.centroid_distances);
        self.quantized_dot_products.appendSliceAssumeCapacity(row.set.quantized_dot_products);
        if (self.metric != .l2_squared) self.centroid_dot_products.appendSliceAssumeCapacity(row.set.centroid_dot_products);
    }

    /// Borrowed set view for the RaBitQ kernel. Valid until the next append.
    fn view(self: *const RaBitQCodes) vproto.RaBitQuantizedVectorSet {
        return .{
            .metric = self.metric,
            .centroid = self.centroid,
            .codes = .{ .count = @intCast(self.count()), .width = @intCast(self.width), .data = self.codes.items },
            .code_counts = self.code_counts.items,
            .centroid_distances = self.centroid_distances.items,
            .quantized_dot_products = self.quantized_dot_products.items,
            .centroid_dot_products = self.centroid_dot_products.items,
            .centroid_norm = self.centroid_norm,
        };
    }

    /// Reconstruction in rotated space: centroid + |r| * sign(code) / sqrt(D).
    fn decode(self: *const RaBitQCodes, id: u32, out: []f32) void {
        const words = self.codes.items[@as(usize, id) * self.width ..][0..self.width];
        const scale = self.centroid_distances.items[id] / @sqrt(@as(f32, @floatFromInt(self.dims)));
        for (out, 0..) |*o, j| {
            const bit = (words[j / 64] >> @intCast(63 - j % 64)) & 1;
            o.* = self.centroid[j] + (if (bit == 1) scale else -scale);
        }
    }

    fn write(self: *const RaBitQCodes, wr: *std.Io.Writer) !void {
        for (self.centroid) |f| try wr.writeInt(u32, @bitCast(f), .little);
        try wr.writeInt(u32, @bitCast(self.centroid_norm), .little);
        for (self.codes.items) |c| try wr.writeInt(u64, c, .little);
        for (self.code_counts.items) |c| try wr.writeInt(u32, c, .little);
        for (self.centroid_distances.items) |f| try wr.writeInt(u32, @bitCast(f), .little);
        for (self.quantized_dot_products.items) |f| try wr.writeInt(u32, @bitCast(f), .little);
        for (self.centroid_dot_products.items) |f| try wr.writeInt(u32, @bitCast(f), .little);
    }

    fn read(self: *RaBitQCodes, alloc: Allocator, r: *std.Io.Reader, n: u32) !void {
        const per_node: usize = self.width * 8 + 4 * 3 + (if (self.metric != .l2_squared) @as(usize, 4) else 0);
        if (r.bufferedLen() < self.dims * 4 + 4 + per_node * n) return error.InvalidFormat;
        self.centroid = try alloc.alloc(f32, self.dims);
        for (self.centroid) |*f| f.* = @bitCast(readU32(r) catch unreachable);
        self.centroid_norm = @bitCast(readU32(r) catch unreachable);
        try self.codes.resize(alloc, @as(usize, n) * self.width);
        for (self.codes.items) |*c| c.* = r.takeInt(u64, .little) catch unreachable;
        try self.code_counts.resize(alloc, n);
        for (self.code_counts.items) |*c| {
            c.* = readU32(r) catch unreachable;
            if (c.* > self.dims) return error.InvalidFormat;
        }
        inline for (.{ &self.centroid_distances, &self.quantized_dot_products }) |list| {
            try list.resize(alloc, n);
            for (list.items) |*f| f.* = @bitCast(readU32(r) catch unreachable);
        }
        if (self.metric != .l2_squared) {
            try self.centroid_dot_products.resize(alloc, n);
            for (self.centroid_dot_products.items) |*f| f.* = @bitCast(readU32(r) catch unreachable);
        }
    }
};

fn readU32(r: *std.Io.Reader) !u32 {
    return r.takeInt(u32, .little) catch error.InvalidFormat;
}

fn promote(alloc: Allocator, approx_queue: *Heap(false), pending: *std.ArrayListUnmanaged(u32), ratio: f32) !void {
    if (approx_queue.len() == 0) return;
    const want_f = @ceil(ratio * @as(f32, @floatFromInt(approx_queue.len())));
    const want: usize = @max(1, @as(usize, @intFromFloat(want_f)));
    var i: usize = 0;
    while (i < want and approx_queue.len() > 0) : (i += 1) try pending.append(alloc, approx_queue.pop().id);
}

// -------------------------------------------------------------------------
// Graph construction
// -------------------------------------------------------------------------

const BuildContext = struct {
    vectors: []const f32,
    dims: usize,
    metric: Metric,

    fn vec(self: *const BuildContext, id: u32) []const f32 {
        return self.vectors[@as(usize, id) * self.dims ..][0..self.dims];
    }

    fn dist(self: *const BuildContext, a: u32, b: u32) f32 {
        return distance(self.metric, self.vec(a), self.vec(b));
    }
};

fn allocLists(alloc: Allocator, n: u32) ![]NeighborList {
    const lists = try alloc.alloc(NeighborList, n);
    @memset(lists, NeighborList.empty);
    return lists;
}

fn freeLists(alloc: Allocator, lists: []NeighborList) void {
    for (lists) |*list| list.deinit(alloc);
    alloc.free(lists);
}

const BeamScratch = struct {
    candidates: Heap(false) = .{},
    results: Heap(true) = .{},

    fn deinit(self: *BeamScratch, alloc: Allocator) void {
        self.candidates.deinit(alloc);
        self.results.deinit(alloc);
    }
};

/// Greedy descent with exact distances from layer `from` down to `to + 1`.
fn greedyDescentExact(ctx: *const BuildContext, layers: Layers, query: []const f32, start: u32, from: u8, to: u8) u32 {
    var cur = start;
    var cur_d = distance(ctx.metric, query, ctx.vec(cur));
    var layer = from;
    while (layer > to) : (layer -= 1) {
        var improved = true;
        while (improved) {
            improved = false;
            for (layers.neighbors(cur, layer)) |nb| {
                const d = distance(ctx.metric, query, ctx.vec(nb));
                if (d < cur_d) {
                    cur_d = d;
                    cur = nb;
                    improved = true;
                }
            }
        }
    }
    return cur;
}

/// Best-first search on one layer with exact distances (Algorithm 1).
fn beamSearchExact(
    alloc: Allocator,
    ctx: *const BuildContext,
    layers: Layers,
    layer: u8,
    query: []const f32,
    entry: u32,
    ef: u32,
    visited: *VisitSet,
    scratch: *BeamScratch,
    out: *std.ArrayListUnmanaged(Cand),
) !void {
    visited.reset();
    scratch.candidates.clear();
    scratch.results.clear();
    const start = Cand{ .dist = distance(ctx.metric, query, ctx.vec(entry)), .id = entry };
    visited.set(entry);
    try scratch.candidates.push(alloc, start);
    try scratch.results.push(alloc, start);
    while (scratch.candidates.len() > 0) {
        const c = scratch.candidates.pop();
        if (scratch.results.len() >= ef and c.dist > scratch.results.top().dist) break;
        for (layers.neighbors(c.id, layer)) |nb| {
            if (!visited.testAndSet(nb)) continue;
            const nc = Cand{ .dist = distance(ctx.metric, query, ctx.vec(nb)), .id = nb };
            if (scratch.results.len() < ef) {
                try scratch.results.push(alloc, nc);
            } else if (nc.dist < scratch.results.top().dist) {
                scratch.results.replaceTop(nc);
            } else continue;
            try scratch.candidates.push(alloc, nc);
        }
    }
    out.clearRetainingCapacity();
    try out.appendSlice(alloc, scratch.results.items.items);
    std.mem.sortUnstable(Cand, out.items, {}, candLessThan);
}

/// HNSW neighbor-selection heuristic: keep a candidate only when it is closer
/// to `base` than to every neighbor already kept. `candidates` must be sorted
/// by distance to `base`.
fn selectHeuristic(
    alloc: Allocator,
    ctx: *const BuildContext,
    candidates: []const Cand,
    base: u32,
    cap: u32,
    out: *NeighborList,
) !void {
    out.clearRetainingCapacity();
    for (candidates) |c| {
        if (out.items.len >= cap) break;
        if (c.id == base) continue;
        var keep = true;
        for (out.items) |s| {
            if (ctx.dist(c.id, s) < c.dist) {
                keep = false;
                break;
            }
        }
        if (keep) try out.append(alloc, c.id);
    }
}

fn shrinkExact(
    alloc: Allocator,
    ctx: *const BuildContext,
    list: *NeighborList,
    base: u32,
    cap: u32,
    scratch: *std.ArrayListUnmanaged(Cand),
) !void {
    scratch.clearRetainingCapacity();
    for (list.items) |nb| try scratch.append(alloc, .{ .dist = ctx.dist(base, nb), .id = nb });
    std.mem.sortUnstable(Cand, scratch.items, {}, candLessThan);
    try selectHeuristic(alloc, ctx, scratch.items, base, cap, list);
}

fn addUnique(alloc: Allocator, list: *NeighborList, id: u32) !bool {
    for (list.items) |x| if (x == id) return false;
    try list.append(alloc, id);
    return true;
}

/// High-degree preserving pruning (Algorithm 3) of layer 0. Upper layers are
/// left alone. Returns the pruned layer-0 lists and marks hubs in `flags`.
fn pruneGraph(
    alloc: Allocator,
    ctx: *const BuildContext,
    cfg: Config,
    layers: Layers,
    entry: u32,
    top: u8,
    flags: []u8,
    visited: *VisitSet,
    scratch: *BeamScratch,
    cands: *std.ArrayListUnmanaged(Cand),
    repaired: *u32,
) ![]NeighborList {
    const n: u32 = @intCast(layers.base.len);
    const indegree = try alloc.alloc(u32, n);
    defer alloc.free(indegree);
    @memset(indegree, 0);
    for (layers.base) |list| for (list.items) |nb| {
        indegree[nb] += 1;
    };
    const order = try alloc.alloc(u32, n);
    defer alloc.free(order);
    for (order, 0..) |*o, i| o.* = @intCast(i);
    const ByDegree = struct {
        deg: []const u32,
        fn lessThan(self: @This(), a: u32, b: u32) bool {
            if (self.deg[a] != self.deg[b]) return self.deg[a] > self.deg[b];
            return a < b;
        }
    };
    std.mem.sortUnstable(u32, order, ByDegree{ .deg = indegree }, ByDegree.lessThan);
    const hub_count: u32 = @intFromFloat(@ceil(cfg.hub_fraction * @as(f32, @floatFromInt(n))));
    for (order[0..@min(hub_count, n)]) |id| flags[id] |= flag_hub;

    const pruned = try allocLists(alloc, n);
    errdefer freeLists(alloc, pruned);
    var selected = NeighborList.empty;
    defer selected.deinit(alloc);
    var shrink_scratch = std.ArrayListUnmanaged(Cand).empty;
    defer shrink_scratch.deinit(alloc);

    for (0..n) |i| {
        const v: u32 = @intCast(i);
        const ep = greedyDescentExact(ctx, layers, ctx.vec(v), entry, top, 0);
        try beamSearchExact(alloc, ctx, layers, 0, ctx.vec(v), ep, cfg.ef_construction, visited, scratch, cands);
        const cap = if (flags[v] & flag_hub != 0) cfg.max_degree else cfg.low_degree;
        try selectHeuristic(alloc, ctx, cands.items, v, cap, &selected);
        for (selected.items) |s| _ = try addUnique(alloc, &pruned[v], s);
        if (pruned[v].items.len > cfg.max_degree) try shrinkExact(alloc, ctx, &pruned[v], v, cfg.max_degree, &shrink_scratch);
        for (selected.items) |u| {
            if (!try addUnique(alloc, &pruned[u], v)) continue;
            if (pruned[u].items.len > cfg.max_degree) try shrinkExact(alloc, ctx, &pruned[u], u, cfg.max_degree, &shrink_scratch);
        }
    }
    repaired.* = try repairConnectivity(alloc, ctx, cfg, .{ .base = pruned, .upper = layers.upper }, entry, visited, scratch, cands);
    return pruned;
}

/// Safety net (as in NSG): links every layer-0 node the traversal cannot
/// reach from `entry` to its nearest reachable node. Returns the number of
/// nodes linked; normally zero or close to it.
fn repairConnectivity(
    alloc: Allocator,
    ctx: *const BuildContext,
    cfg: Config,
    layers: Layers,
    entry: u32,
    visited: *VisitSet,
    scratch: *BeamScratch,
    cands: *std.ArrayListUnmanaged(Cand),
) !u32 {
    const adj = layers.base;
    const n: u32 = @intCast(adj.len);
    var reached = try std.DynamicBitSetUnmanaged.initEmpty(alloc, n);
    defer reached.deinit(alloc);
    var stack = std.ArrayListUnmanaged(u32).empty;
    defer stack.deinit(alloc);
    try markReachable(alloc, adj, entry, &reached, &stack);
    var linked: u32 = 0;
    for (0..n) |i| {
        const u: u32 = @intCast(i);
        if (reached.isSet(u)) continue;
        try beamSearchExact(alloc, ctx, layers, 0, ctx.vec(u), entry, cfg.ef_construction, visited, scratch, cands);
        // Prefer a reachable node with spare capacity; otherwise exceed the
        // cap on the nearest one rather than leave `u` stranded.
        var parent = cands.items[0].id;
        for (cands.items) |c| {
            if (adj[c.id].items.len < cfg.max_degree) {
                parent = c.id;
                break;
            }
        }
        try adj[parent].append(alloc, u);
        try markReachable(alloc, adj, u, &reached, &stack);
        linked += 1;
    }
    return linked;
}

fn markReachable(alloc: Allocator, adj: []const NeighborList, start: u32, reached: *std.DynamicBitSetUnmanaged, stack: *std.ArrayListUnmanaged(u32)) !void {
    if (reached.isSet(start)) return;
    reached.set(start);
    stack.clearRetainingCapacity();
    try stack.append(alloc, start);
    while (stack.pop()) |v| for (adj[v].items) |nb| {
        if (reached.isSet(nb)) continue;
        reached.set(nb);
        try stack.append(alloc, nb);
    };
}

const VisitSet = struct {
    marks: []u32,
    epoch: u32 = 0,

    fn init(alloc: Allocator, n: u32) !VisitSet {
        const marks = try alloc.alloc(u32, n);
        @memset(marks, 0);
        return .{ .marks = marks };
    }

    fn deinit(self: *VisitSet, alloc: Allocator) void {
        alloc.free(self.marks);
    }

    fn reset(self: *VisitSet) void {
        self.epoch +%= 1;
        if (self.epoch == 0) {
            @memset(self.marks, 0);
            self.epoch = 1;
        }
    }

    fn set(self: *VisitSet, id: u32) void {
        self.marks[id] = self.epoch;
    }

    /// Marks `id` and returns true when it was not yet visited.
    fn testAndSet(self: *VisitSet, id: u32) bool {
        if (self.marks[id] == self.epoch) return false;
        self.marks[id] = self.epoch;
        return true;
    }
};

// -------------------------------------------------------------------------
// Product quantization
// -------------------------------------------------------------------------

pub const ProductQuantizer = struct {
    dims: u32,
    subspaces: u32,
    sub_dims: u32,
    /// Centroids per subspace (at most 256, one byte per code).
    k: u32,
    /// subspaces * k * sub_dims floats.
    centroids: []f32,

    pub fn deinit(self: *ProductQuantizer, alloc: Allocator) void {
        alloc.free(self.centroids);
        self.* = undefined;
    }

    fn centroid(self: *const ProductQuantizer, s: usize, c: usize) []const f32 {
        return self.centroids[(s * self.k + c) * self.sub_dims ..][0..self.sub_dims];
    }

    /// Trains one k-means codebook per subspace (L2, random init, Lloyd).
    pub fn train(alloc: Allocator, vectors: []const f32, dims: usize, subspaces: u32, iterations: u32, sample_limit: u32, seed: u64) !ProductQuantizer {
        const n = vectors.len / dims;
        const sub_dims = dims / subspaces;
        var prng = std.Random.DefaultPrng.init(seed);
        const rand = prng.random();

        const perm = try alloc.alloc(u32, n);
        defer alloc.free(perm);
        for (perm, 0..) |*p, i| p.* = @intCast(i);
        const sample_n = @min(n, sample_limit);
        for (0..sample_n) |i| {
            const j = i + rand.uintLessThan(usize, n - i);
            std.mem.swap(u32, &perm[i], &perm[j]);
        }
        const k: u32 = @intCast(@min(256, sample_n));

        const centroids = try alloc.alloc(f32, @as(usize, subspaces) * k * sub_dims);
        errdefer alloc.free(centroids);
        const data = try alloc.alloc(f32, sample_n * sub_dims);
        defer alloc.free(data);
        const assign = try alloc.alloc(u32, sample_n);
        defer alloc.free(assign);
        const sums = try alloc.alloc(f64, @as(usize, k) * sub_dims);
        defer alloc.free(sums);
        const counts = try alloc.alloc(u32, k);
        defer alloc.free(counts);

        for (0..subspaces) |s| {
            for (0..sample_n) |i| {
                @memcpy(data[i * sub_dims ..][0..sub_dims], vectors[@as(usize, perm[i]) * dims + s * sub_dims ..][0..sub_dims]);
            }
            const cs = centroids[s * k * sub_dims ..][0 .. k * sub_dims];
            // The sample is already shuffled, so its first k rows are a
            // random initialization.
            @memcpy(cs, data[0 .. k * sub_dims]);
            for (0..iterations) |_| {
                @memset(sums, 0);
                @memset(counts, 0);
                for (0..sample_n) |i| {
                    const x = data[i * sub_dims ..][0..sub_dims];
                    const best = nearest(cs, sub_dims, x);
                    assign[i] = best;
                    counts[best] += 1;
                    for (x, 0..) |v, j| sums[best * sub_dims + j] += v;
                }
                for (0..k) |c| {
                    const dst = cs[c * sub_dims ..][0..sub_dims];
                    if (counts[c] == 0) {
                        const pick = rand.uintLessThan(usize, sample_n);
                        @memcpy(dst, data[pick * sub_dims ..][0..sub_dims]);
                        continue;
                    }
                    const inv = 1.0 / @as(f64, @floatFromInt(counts[c]));
                    for (dst, 0..) |*v, j| v.* = @floatCast(sums[c * sub_dims + j] * inv);
                }
            }
        }
        return .{ .dims = @intCast(dims), .subspaces = subspaces, .sub_dims = @intCast(sub_dims), .k = k, .centroids = centroids };
    }

    fn nearest(cs: []const f32, sub_dims: usize, x: []const f32) u32 {
        const k = cs.len / sub_dims;
        var best: u32 = 0;
        var best_d: f32 = std.math.inf(f32);
        for (0..k) |c| {
            const d = l2Squared(x, cs[c * sub_dims ..][0..sub_dims]);
            if (d < best_d) {
                best_d = d;
                best = @intCast(c);
            }
        }
        return best;
    }

    pub fn encode(self: *const ProductQuantizer, v: []const f32, out: []u8) void {
        const sd: usize = self.sub_dims;
        for (0..self.subspaces) |s| {
            const cs = self.centroids[s * self.k * sd ..][0 .. self.k * sd];
            out[s] = @intCast(nearest(cs, sd, v[s * sd ..][0..sd]));
        }
    }

    pub fn decode(self: *const ProductQuantizer, c: []const u8, out: []f32) void {
        const sd: usize = self.sub_dims;
        for (0..self.subspaces) |s| @memcpy(out[s * sd ..][0..sd], self.centroid(s, c[s]));
    }

    pub fn tableLen(self: *const ProductQuantizer) usize {
        return @as(usize, self.subspaces) * self.k;
    }

    /// Per-subspace distance lookup table for asymmetric distance (ADC).
    pub fn buildTable(self: *const ProductQuantizer, metric: Metric, q: []const f32, table: []f32) void {
        const sd: usize = self.sub_dims;
        for (0..self.subspaces) |s| {
            const qs = q[s * sd ..][0..sd];
            for (0..self.k) |c| {
                const cv = self.centroid(s, c);
                table[s * self.k + c] = switch (metric) {
                    .l2_squared => l2Squared(qs, cv),
                    .inner_product, .cosine => -dot(qs, cv),
                };
            }
        }
        // Cosine distance is 1 - dot; fold the constant into subspace 0.
        if (metric == .cosine) {
            for (table[0..self.k]) |*t| t.* += 1;
        }
    }

    pub fn adc(self: *const ProductQuantizer, table: []const f32, c: []const u8) f32 {
        var sum: f32 = 0;
        for (c, 0..) |b, s| sum += table[s * self.k + b];
        return sum;
    }
};

// -------------------------------------------------------------------------
// Small helpers
// -------------------------------------------------------------------------

const Cand = struct { dist: f32, id: u32 };

fn candLessThan(_: void, a: Cand, b: Cand) bool {
    if (a.dist != b.dist) return a.dist < b.dist;
    return a.id < b.id;
}

/// Binary heap over candidates: a min-heap, or a max-heap when `is_max`.
fn Heap(comptime is_max: bool) type {
    return struct {
        items: std.ArrayListUnmanaged(Cand) = .empty,
        const Self = @This();

        fn before(a: Cand, b: Cand) bool {
            return if (is_max) a.dist > b.dist else a.dist < b.dist;
        }

        fn deinit(self: *Self, alloc: Allocator) void {
            self.items.deinit(alloc);
        }

        fn clear(self: *Self) void {
            self.items.clearRetainingCapacity();
        }

        fn len(self: *const Self) usize {
            return self.items.items.len;
        }

        fn top(self: *const Self) Cand {
            return self.items.items[0];
        }

        fn push(self: *Self, alloc: Allocator, c: Cand) !void {
            try self.items.append(alloc, c);
            var i = self.items.items.len - 1;
            const xs = self.items.items;
            while (i > 0) {
                const p = (i - 1) / 2;
                if (!before(xs[i], xs[p])) break;
                std.mem.swap(Cand, &xs[i], &xs[p]);
                i = p;
            }
        }

        fn pop(self: *Self) Cand {
            const xs = self.items.items;
            const out = xs[0];
            const last = self.items.pop().?;
            if (self.items.items.len > 0) {
                self.items.items[0] = last;
                self.siftDown(0);
            }
            return out;
        }

        fn replaceTop(self: *Self, c: Cand) void {
            self.items.items[0] = c;
            self.siftDown(0);
        }

        fn siftDown(self: *Self, start: usize) void {
            const xs = self.items.items;
            var i = start;
            while (true) {
                const l = 2 * i + 1;
                if (l >= xs.len) break;
                var best = l;
                if (l + 1 < xs.len and before(xs[l + 1], xs[l])) best = l + 1;
                if (!before(xs[best], xs[i])) break;
                std.mem.swap(Cand, &xs[i], &xs[best]);
                i = best;
            }
        }
    };
}

const lanes = 8;
const V = @Vector(lanes, f32);

fn dot(a: []const f32, b: []const f32) f32 {
    var acc: V = @splat(0);
    var i: usize = 0;
    while (i + lanes <= a.len) : (i += lanes) {
        const va: V = a[i..][0..lanes].*;
        const vb: V = b[i..][0..lanes].*;
        acc += va * vb;
    }
    var sum = @reduce(.Add, acc);
    while (i < a.len) : (i += 1) sum += a[i] * b[i];
    return sum;
}

fn l2Squared(a: []const f32, b: []const f32) f32 {
    var acc: V = @splat(0);
    var i: usize = 0;
    while (i + lanes <= a.len) : (i += lanes) {
        const va: V = a[i..][0..lanes].*;
        const vb: V = b[i..][0..lanes].*;
        const d = va - vb;
        acc += d * d;
    }
    var sum = @reduce(.Add, acc);
    while (i < a.len) : (i += 1) {
        const d = a[i] - b[i];
        sum += d * d;
    }
    return sum;
}

pub fn distance(metric: Metric, a: []const f32, b: []const f32) f32 {
    return switch (metric) {
        .l2_squared => l2Squared(a, b),
        .inner_product => -dot(a, b),
        .cosine => 1 - dot(a, b),
    };
}

fn normalize(v: []f32) void {
    const n = @sqrt(dot(v, v));
    if (n == 0) return;
    for (v) |*x| x.* /= n;
}

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

const testing = std.testing;

const Fixture = struct {
    data: []f32,
    queries: []f32,
    dims: usize,

    /// Gaussian clusters, which are closer to real embeddings than uniform
    /// noise and make graph navigation non-trivial.
    fn init(alloc: Allocator, n: usize, nq: usize, dims: usize, clusters: usize, seed: u64) !Fixture {
        var prng = std.Random.DefaultPrng.init(seed);
        const rand = prng.random();
        const centers = try alloc.alloc(f32, clusters * dims);
        defer alloc.free(centers);
        for (centers) |*c| c.* = rand.floatNorm(f32) * 3;
        const data = try alloc.alloc(f32, n * dims);
        errdefer alloc.free(data);
        const queries = try alloc.alloc(f32, nq * dims);
        for ([_][]f32{ data, queries }) |buf| {
            var i: usize = 0;
            while (i < buf.len) : (i += dims) {
                const c = rand.uintLessThan(usize, clusters);
                for (0..dims) |j| buf[i + j] = centers[c * dims + j] + rand.floatNorm(f32);
            }
        }
        return .{ .data = data, .queries = queries, .dims = dims };
    }

    fn deinit(self: *Fixture, alloc: Allocator) void {
        alloc.free(self.data);
        alloc.free(self.queries);
    }

    fn query(self: *const Fixture, i: usize) []const f32 {
        return self.queries[i * self.dims ..][0..self.dims];
    }

    fn truth(self: *const Fixture, alloc: Allocator, metric: Metric, q: []const f32, k: usize, n_limit: usize, skip: ?u32) ![]u32 {
        const n = @min(n_limit, self.data.len / self.dims);
        const all = try alloc.alloc(Cand, n);
        defer alloc.free(all);
        const qn = try alloc.dupe(f32, q);
        defer alloc.free(qn);
        const row = try alloc.alloc(f32, self.dims);
        defer alloc.free(row);
        if (metric == .cosine) normalize(qn);
        for (0..n) |i| {
            @memcpy(row, self.data[i * self.dims ..][0..self.dims]);
            if (metric == .cosine) normalize(row);
            const d = distance(metric, qn, row);
            all[i] = .{ .dist = if (skip != null and skip.? == i) std.math.inf(f32) else d, .id = @intCast(i) };
        }
        std.mem.sortUnstable(Cand, all, {}, candLessThan);
        const out = try alloc.alloc(u32, k);
        for (out, all[0..k]) |*o, c| o.* = c.id;
        return out;
    }
};

fn recallAt(alloc: Allocator, index: *const Index, fx: *const Fixture, src: EmbeddingSource, opts: SearchOptions, nq: usize, n_limit: usize) !struct { recall: f64, recomputed: f64, batches: f64 } {
    var hits: usize = 0;
    var recomputed: u64 = 0;
    var batches: u64 = 0;
    for (0..nq) |qi| {
        const q = fx.query(qi);
        var resp = try index.search(alloc, src, q, opts);
        defer resp.deinit(alloc);
        recomputed += resp.stats.recomputed;
        batches += resp.stats.batches;
        const gt = try fx.truth(alloc, index.config.metric, q, opts.k, n_limit, null);
        defer alloc.free(gt);
        for (resp.results) |r| {
            for (gt) |g| if (g == r.id) {
                hits += 1;
                break;
            };
        }
    }
    const nqf: f64 = @floatFromInt(nq);
    return .{
        .recall = @as(f64, @floatFromInt(hits)) / (nqf * @as(f64, @floatFromInt(opts.k))),
        .recomputed = @as(f64, @floatFromInt(recomputed)) / nqf,
        .batches = @as(f64, @floatFromInt(batches)) / nqf,
    };
}

test "leann: config resolution and validation" {
    const c = try (Config{ .dims = 96 }).resolved();
    try testing.expectEqual(@as(u32, 6), c.low_degree);
    try testing.expectEqual(@as(u32, 12), c.pq_subspaces);
    try testing.expectEqual(@as(u32, 5), (try (Config{ .dims = 25 }).resolved()).pq_subspaces);
    try testing.expectEqual(@as(u32, 96), (try (Config{ .dims = 768 }).resolved()).pq_subspaces);
    try testing.expectError(error.InvalidConfig, (Config{ .dims = 0 }).resolved());
    try testing.expectError(error.InvalidConfig, (Config{ .dims = 16, .pq_subspaces = 5 }).resolved());
    try testing.expectError(error.InvalidConfig, (Config{ .dims = 16, .max_degree = 4, .low_degree = 8 }).resolved());
    try testing.expectError(error.InvalidConfig, (Config{ .dims = 16, .hub_fraction = 1.5 }).resolved());
}

test "leann: recall with recomputation across metrics" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 2000, 40, 32, 24, 7);
    defer fx.deinit(alloc);
    for ([_]Metric{ .l2_squared, .cosine, .inner_product }) |metric| {
        var index = try Index.build(alloc, .{ .dims = 32, .metric = metric, .max_degree = 24, .ef_construction = 96 }, fx.data);
        defer index.deinit();
        var src = SliceEmbeddingSource{ .vectors = fx.data, .dims = 32 };
        const r = try recallAt(alloc, &index, &fx, src.source(), .{ .k = 10, .ef = 64 }, 40, std.math.maxInt(usize));
        testing.expect(r.recall >= 0.9) catch |err| {
            std.debug.print("metric={s} recall={d:.3}\n", .{ @tagName(metric), r.recall });
            return err;
        };
        // Only a small part of the corpus should ever be recomputed.
        try testing.expect(r.recomputed < 2000 / 4);
    }
}

test "leann: pruning keeps hubs and lowers average degree" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 1500, 1, 32, 16, 11);
    defer fx.deinit(alloc);
    var full = try Index.build(alloc, .{ .dims = 32, .max_degree = 30, .prune = false }, fx.data);
    defer full.deinit();
    var pruned = try Index.build(alloc, .{ .dims = 32, .max_degree = 30 }, fx.data);
    defer pruned.deinit();
    const fs = full.graphStats();
    const ps = pruned.graphStats();
    try testing.expectEqual(@as(u32, 0), fs.hubs);
    try testing.expectEqual(@as(u32, 30), ps.hubs); // ceil(0.02 * 1500)
    try testing.expect(ps.avg_degree < fs.avg_degree);
    try testing.expect(ps.max_degree <= 30);
    // Every node must be reachable from the entry point.
    var seen = try std.DynamicBitSetUnmanaged.initEmpty(alloc, ps.nodes);
    defer seen.deinit(alloc);
    var stack = std.ArrayListUnmanaged(u32).empty;
    defer stack.deinit(alloc);
    try stack.append(alloc, pruned.entry);
    seen.set(pruned.entry);
    while (stack.pop()) |v| for (pruned.adjacency.items[v].items) |nb| {
        if (seen.isSet(nb)) continue;
        seen.set(nb);
        try stack.append(alloc, nb);
    };
    try testing.expectEqual(@as(usize, ps.nodes), seen.count());
}

test "leann: level assignment follows the HNSW distribution" {
    var above: usize = 0;
    var max_seen: u8 = 0;
    const n = 100_000;
    for (0..n) |i| {
        const l = levelFor(42, @intCast(i), 30);
        if (l > 0) above += 1;
        max_seen = @max(max_seen, l);
        try testing.expectEqual(l, levelFor(42, @intCast(i), 30));
    }
    // P(level >= 1) = 1 / M.
    const frac = @as(f64, @floatFromInt(above)) / n;
    try testing.expect(frac > 0.8 / 30.0 and frac < 1.2 / 30.0);
    try testing.expect(max_seen >= 2 and max_seen <= max_level_cap);
}

test "leann: upper layers are unpruned and well formed" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 3000, 20, 32, 24, 31);
    defer fx.deinit(alloc);
    var index = try Index.build(alloc, .{ .dims = 32, .max_degree = 16, .ef_construction = 96 }, fx.data);
    defer index.deinit();
    const gs = index.graphStats();
    try testing.expect(gs.max_level >= 1);
    try testing.expect(gs.upper_nodes > 0 and gs.upper_nodes < gs.nodes / 4);
    try testing.expectEqual(@as(usize, gs.max_level), index.upper.items[index.entry].len);
    for (index.upper.items, 0..) |lists, v| {
        for (lists, 1..) |list, layer| {
            try testing.expect(list.items.len <= 16);
            for (list.items) |nb| {
                try testing.expect(nb != v);
                try testing.expect(index.upper.items[nb].len >= layer);
            }
        }
    }
    var src = SliceEmbeddingSource{ .vectors = fx.data, .dims = 32 };
    const approx = try recallAt(alloc, &index, &fx, src.source(), .{ .ef = 64 }, 20, std.math.maxInt(usize));
    const exact = try recallAt(alloc, &index, &fx, src.source(), .{ .ef = 64, .exact_descent = true }, 20, std.math.maxInt(usize));
    try testing.expect(approx.recall >= 0.85);
    try testing.expect(exact.recall >= 0.85);
}

test "leann: two-level search recomputes less than plain search" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 2000, 30, 32, 24, 3);
    defer fx.deinit(alloc);
    var index = try Index.build(alloc, .{ .dims = 32, .max_degree = 24, .ef_construction = 96, .prune = false }, fx.data);
    defer index.deinit();
    var src = SliceEmbeddingSource{ .vectors = fx.data, .dims = 32 };
    const plain = try recallAt(alloc, &index, &fx, src.source(), .{ .ef = 32, .two_level = false }, 30, std.math.maxInt(usize));
    const two = try recallAt(alloc, &index, &fx, src.source(), .{ .ef = 32, .rerank_ratio = 0.1 }, 30, std.math.maxInt(usize));
    testing.expect(two.recomputed < plain.recomputed and two.recall >= plain.recall - 0.05) catch |err| {
        std.debug.print("plain recall={d:.3} recomputed={d:.1} | two-level recall={d:.3} recomputed={d:.1}\n", .{ plain.recall, plain.recomputed, two.recall, two.recomputed });
        return err;
    };
}

test "leann: dynamic batching reduces recompute calls" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 2000, 30, 32, 24, 5);
    defer fx.deinit(alloc);
    var index = try Index.build(alloc, .{ .dims = 32, .max_degree = 24, .ef_construction = 96 }, fx.data);
    defer index.deinit();
    var src = SliceEmbeddingSource{ .vectors = fx.data, .dims = 32 };
    const step = try recallAt(alloc, &index, &fx, src.source(), .{ .ef = 64 }, 30, std.math.maxInt(usize));
    const batched = try recallAt(alloc, &index, &fx, src.source(), .{ .ef = 64, .batch_size = 32 }, 30, std.math.maxInt(usize));
    try testing.expect(batched.batches < step.batches / 2);
    try testing.expect(batched.recall >= 0.85);
}

test "leann: storage is a fraction of raw embeddings" {
    const alloc = testing.allocator;
    const dims = 128;
    const n = 1500;
    var fx = try Fixture.init(alloc, n, 1, dims, 16, 13);
    defer fx.deinit(alloc);
    var index = try Index.build(alloc, .{ .dims = dims }, fx.data);
    defer index.deinit();
    const bytes = try index.serialize(alloc);
    defer alloc.free(bytes);
    // The PQ codebook is a fixed cost that does not grow with the corpus.
    const codebook = index.pq.?.centroids.len * @sizeOf(f32);
    const per_vector = (bytes.len - codebook) / n;
    testing.expect(per_vector * 8 < dims * @sizeOf(f32)) catch |err| {
        std.debug.print("per-vector bytes={d} raw={d}\n", .{ per_vector, dims * @sizeOf(f32) });
        return err;
    };
}

test "leann: serialization round-trips and rejects corruption" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 800, 10, 16, 8, 17);
    defer fx.deinit(alloc);
    var index = try Index.build(alloc, .{ .dims = 16, .metric = .cosine, .max_degree = 16 }, fx.data);
    defer index.deinit();
    try index.markDeleted(5);
    const bytes = try index.serialize(alloc);
    defer alloc.free(bytes);
    var copy = try Index.deserialize(alloc, bytes);
    defer copy.deinit();
    try testing.expectEqual(index.node_count, copy.node_count);
    try testing.expectEqual(index.entry, copy.entry);
    try testing.expectEqual(@as(u32, 1), copy.deleted_count);
    try testing.expectEqual(index.config.low_degree, copy.config.low_degree);

    var src = SliceEmbeddingSource{ .vectors = fx.data, .dims = 16 };
    for (0..10) |qi| {
        var a = try index.search(alloc, src.source(), fx.query(qi), .{});
        defer a.deinit(alloc);
        var b = try copy.search(alloc, src.source(), fx.query(qi), .{});
        defer b.deinit(alloc);
        try testing.expectEqualSlices(Result, a.results, b.results);
    }

    const bad = try alloc.dupe(u8, bytes);
    defer alloc.free(bad);
    bad[bad.len / 2] ^= 0xff;
    try testing.expectError(error.InvalidFormat, Index.deserialize(alloc, bad));
    try testing.expectError(error.InvalidFormat, Index.deserialize(alloc, bytes[0 .. bytes.len - 9]));
}

test "leann: soft deletes are filtered from results" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 1000, 5, 16, 8, 19);
    defer fx.deinit(alloc);
    var index = try Index.build(alloc, .{ .dims = 16, .max_degree = 16 }, fx.data);
    defer index.deinit();
    var src = SliceEmbeddingSource{ .vectors = fx.data, .dims = 16 };
    for (0..5) |qi| {
        const gt = try fx.truth(alloc, .l2_squared, fx.query(qi), 1, 1000, null);
        defer alloc.free(gt);
        try index.markDeleted(gt[0]);
        var resp = try index.search(alloc, src.source(), fx.query(qi), .{ .k = 10 });
        defer resp.deinit(alloc);
        try testing.expectEqual(@as(usize, 10), resp.results.len);
        for (resp.results) |r| try testing.expect(!index.isDeleted(r.id));
    }
    try testing.expect(index.deletedFraction() > 0);
    try testing.expectError(error.UnknownNode, index.markDeleted(5000));
}

test "leann: inserts are searchable" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 2400, 30, 32, 24, 23);
    defer fx.deinit(alloc);
    var index = try Index.build(alloc, .{ .dims = 32, .max_degree = 24, .ef_construction = 96 }, fx.data[0 .. 1600 * 32]);
    defer index.deinit();
    var src = SliceEmbeddingSource{ .vectors = fx.data, .dims = 32 };
    for (1600..2400) |i| {
        const id = try index.insert(src.source(), fx.data[i * 32 ..][0..32]);
        try testing.expectEqual(@as(u32, @intCast(i)), id);
    }
    const r = try recallAt(alloc, &index, &fx, src.source(), .{ .k = 10, .ef = 64 }, 30, std.math.maxInt(usize));
    testing.expect(r.recall >= 0.85) catch |err| {
        std.debug.print("insert recall={d:.3}\n", .{r.recall});
        return err;
    };
    // Each inserted vector should find itself.
    for ([_]usize{ 1600, 1999, 2399 }) |i| {
        var resp = try index.search(alloc, src.source(), fx.data[i * 32 ..][0..32], .{ .k = 1 });
        defer resp.deinit(alloc);
        try testing.expectEqual(@as(u32, @intCast(i)), resp.results[0].id);
    }
}

test "leann: consolidating deletes removes them from the graph and keeps recall" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 2000, 20, 32, 24, 31);
    defer fx.deinit(alloc);
    var index = try Index.build(alloc, .{ .dims = 32, .max_degree = 24, .quantizer = .rabitq }, fx.data);
    defer index.deinit();
    var src = SliceEmbeddingSource{ .vectors = fx.data, .dims = 32 };
    var prng = std.Random.DefaultPrng.init(5);
    var deleted: usize = 0;
    while (deleted < 600) {
        const id = prng.random().uintLessThan(u32, 2000);
        if (index.isDeleted(id)) continue;
        try index.markDeleted(id);
        deleted += 1;
    }
    if (index.isDeleted(index.entry) == false) try index.markDeleted(index.entry);
    const st = try index.consolidateDeletes();
    try testing.expect(st.patched_lists > 0);
    try testing.expect(!index.isDeleted(index.entry));
    for (index.adjacency.items, 0..) |list, v| {
        if (index.isDeleted(@intCast(v))) try testing.expectEqual(@as(usize, 0), list.items.len);
        for (list.items) |nb| try testing.expect(!index.isDeleted(nb));
    }
    for (index.upper.items) |lists| for (lists) |list| for (list.items) |nb| try testing.expect(!index.isDeleted(nb));
    // Searches still find live neighbors and inserts still plan and apply.
    var hits: usize = 0;
    for (0..20) |qi| {
        var resp = try index.search(alloc, src.source(), fx.query(qi), .{ .k = 10 });
        defer resp.deinit(alloc);
        for (resp.results) |r| try testing.expect(!index.isDeleted(r.id));
        hits += resp.results.len;
    }
    try testing.expectEqual(@as(usize, 200), hits);
}

test "leann: stored upper vectors avoid recomputing upper nodes" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 2400, 20, 32, 24, 37);
    defer fx.deinit(alloc);
    var index = try Index.build(alloc, .{ .dims = 32, .max_degree = 24, .quantizer = .rabitq, .store_upper = true }, fx.data[0 .. 2000 * 32]);
    defer index.deinit();
    try testing.expect(index.upper_vectors.count() > 0);
    var src = SliceEmbeddingSource{ .vectors = fx.data, .dims = 32 };
    for (2000..2400) |i| _ = try index.insert(src.source(), fx.data[i * 32 ..][0..32]);
    var upper_nodes: usize = 0;
    for (index.upper.items) |lists| {
        if (lists.len > 0) upper_nodes += 1;
    }
    try testing.expectEqual(upper_nodes, index.upper_vectors.count());
    const r = try recallAt(alloc, &index, &fx, src.source(), .{ .k = 10, .ef = 128 }, 20, std.math.maxInt(usize));
    try testing.expect(r.recall >= 0.85);
    try testing.expectError(error.UnsupportedStoreUpper, index.serialize(alloc));
    // Reverting an insert drops its stored vector.
    var delta = try index.planInsert(alloc, src.source(), fx.query(0));
    defer delta.deinit(alloc);
    const before = index.upper_vectors.count();
    const undo = try index.applyDeltaUndoable(delta);
    index.revertDelta(delta, undo);
    try testing.expectEqual(before, index.upper_vectors.count());
}

test "leann: rebuild from codes keeps codes and live nodes" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 2000, 20, 32, 24, 41);
    defer fx.deinit(alloc);
    var index = try Index.build(alloc, .{ .dims = 32, .max_degree = 24, .quantizer = .rabitq }, fx.data);
    defer index.deinit();
    var live = std.ArrayListUnmanaged(u32).empty;
    defer live.deinit(alloc);
    var live_vectors = std.ArrayListUnmanaged(f32).empty;
    defer live_vectors.deinit(alloc);
    for (0..2000) |i| {
        if (i % 4 == 0) {
            try index.markDeleted(@intCast(i));
            continue;
        }
        try live.append(alloc, @intCast(i));
        try live_vectors.appendSlice(alloc, fx.data[i * 32 ..][0..32]);
    }
    for ([_]?[]const f32{ null, live_vectors.items }) |graph_vectors| {
        var rebuilt = try index.rebuildFromCodes(alloc, live.items, graph_vectors);
        defer rebuilt.deinit();
        try testing.expectEqual(@as(u32, @intCast(live.items.len)), rebuilt.node_count);
        try testing.expectEqual(@as(u32, 0), rebuilt.deleted_count);
        const width = index.rabitq.?.width;
        for (live.items, 0..) |old, new| {
            try testing.expectEqualSlices(u64, index.rabitq.?.codes.items[old * width ..][0..width], rebuilt.rabitq.?.codes.items[new * width ..][0..width]);
        }
        var src = SliceEmbeddingSource{ .vectors = live_vectors.items, .dims = 32 };
        // Each live vector finds itself through the rebuilt graph.
        for ([_]usize{ 0, 700, 1499 }) |new| {
            var resp = try rebuilt.search(alloc, src.source(), live_vectors.items[new * 32 ..][0..32], .{ .k = 1, .ef = 256 });
            defer resp.deinit(alloc);
            try testing.expectEqual(@as(u32, @intCast(new)), resp.results[0].id);
        }
    }
    try testing.expectError(error.InvalidInput, index.rebuildFromCodes(alloc, &.{}, null));
}

test "leann: recompute failures propagate" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 300, 1, 8, 4, 29);
    defer fx.deinit(alloc);
    var index = try Index.build(alloc, .{ .dims = 8, .max_degree = 8 }, fx.data);
    defer index.deinit();
    var src = SliceEmbeddingSource{ .vectors = fx.data[0 .. 10 * 8], .dims = 8 };
    try testing.expectError(error.UnknownNode, index.search(alloc, src.source(), fx.query(0), .{}));
    try testing.expectError(error.DimensionMismatch, index.search(alloc, src.source(), fx.data[0..4], .{}));
}

test "leann: rabitq bounds search recall and recompute budget" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 2000, 30, 128, 24, 37);
    defer fx.deinit(alloc);
    var index = try Index.build(alloc, .{ .dims = 128, .quantizer = .rabitq, .max_degree = 24, .ef_construction = 96 }, fx.data);
    defer index.deinit();
    var src = SliceEmbeddingSource{ .vectors = fx.data, .dims = 128 };
    const tight = try recallAt(alloc, &index, &fx, src.source(), .{ .ef = 64, .bound_scale = 0.5 }, 30, std.math.maxInt(usize));
    const loose = try recallAt(alloc, &index, &fx, src.source(), .{ .ef = 64, .bound_scale = 2 }, 30, std.math.maxInt(usize));
    testing.expect(loose.recall >= 0.85) catch |err| {
        std.debug.print("rabitq recall tight={d:.3}/{d:.1} loose={d:.3}/{d:.1}\n", .{ tight.recall, tight.recomputed, loose.recall, loose.recomputed });
        return err;
    };
    // Wider bounds can only admit more candidates.
    try testing.expect(loose.recomputed >= tight.recomputed);
    try testing.expect(loose.recall >= tight.recall);
    // Bound-driven verification recomputes a fraction of what it scores.
    try testing.expect(loose.recomputed < 2000 / 4);

    var pq_index = try Index.build(alloc, .{ .dims = 128, .max_degree = 8 }, fx.data[0 .. 300 * 128]);
    defer pq_index.deinit();
    try testing.expectError(error.BoundsRequireRaBitQ, pq_index.search(alloc, src.source(), fx.query(0), .{ .rerank = .bounds }));
}

test "leann: rabitq serialization and inserts without recomputation" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 1500, 10, 64, 12, 41);
    defer fx.deinit(alloc);
    for ([_]Metric{ .l2_squared, .cosine }) |metric| {
        var index = try Index.build(alloc, .{ .dims = 64, .metric = metric, .quantizer = .rabitq, .max_degree = 16 }, fx.data[0 .. 1000 * 64]);
        defer index.deinit();
        var src = SliceEmbeddingSource{ .vectors = fx.data, .dims = 64 };
        for (1000..1500) |i| _ = try index.insert(src.source(), fx.data[i * 64 ..][0..64]);
        // RaBitQ inserts link with estimates only.
        try testing.expectEqual(@as(u64, 0), src.recomputed);
        for ([_]usize{ 1000, 1499 }) |i| {
            var resp = try index.search(alloc, src.source(), fx.data[i * 64 ..][0..64], .{ .k = 1, .ef = 256 });
            defer resp.deinit(alloc);
            try testing.expectEqual(@as(u32, @intCast(i)), resp.results[0].id);
        }

        const bytes = try index.serialize(alloc);
        defer alloc.free(bytes);
        var copy = try Index.deserialize(alloc, bytes);
        defer copy.deinit();
        try testing.expect(copy.rabitq != null);
        for (0..10) |qi| {
            var a = try index.search(alloc, src.source(), fx.query(qi), .{});
            defer a.deinit(alloc);
            var b = try copy.search(alloc, src.source(), fx.query(qi), .{});
            defer b.deinit(alloc);
            try testing.expectEqualSlices(Result, a.results, b.results);
        }
        const bad = try alloc.dupe(u8, bytes);
        defer alloc.free(bad);
        bad[bad.len / 3] ^= 0x10;
        try testing.expectError(error.InvalidFormat, Index.deserialize(alloc, bad));
    }
}

test "leann: replicas applying the leader's deltas stay byte-identical" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 900, 1, 32, 12, 43);
    defer fx.deinit(alloc);
    for ([_]Quantizer{ .pq, .rabitq }) |quantizer| {
        var leader = try Index.build(alloc, .{ .dims = 32, .metric = .cosine, .quantizer = quantizer, .max_degree = 12 }, fx.data[0 .. 500 * 32]);
        defer leader.deinit();
        const snapshot = try leader.serialize(alloc);
        defer alloc.free(snapshot);
        var follower_a = try Index.deserialize(alloc, snapshot);
        defer follower_a.deinit();
        var follower_b = try Index.deserialize(alloc, snapshot);
        defer follower_b.deinit();

        // Node id -> vector, so recomputation reads the right rows.
        var node_vectors = std.ArrayListUnmanaged(f32).empty;
        defer node_vectors.deinit(alloc);
        try node_vectors.appendSlice(alloc, fx.data[0 .. 500 * 32]);
        var last_insert: u32 = 0;
        for (500..900) |i| {
            var src = SliceEmbeddingSource{ .vectors = node_vectors.items, .dims = 32 };
            var delta = if (i % 7 == 0)
                try leader.planDelete(@intCast(i / 3))
            else
                try leader.planInsert(alloc, src.source(), fx.data[i * 32 ..][0..32]);
            defer delta.deinit(alloc);
            if (delta.kind == .insert) {
                last_insert = delta.id;
                try node_vectors.appendSlice(alloc, fx.data[i * 32 ..][0..32]);
            }
            // Replicate through the wire encoding, as a log entry would.
            const wire = try delta.encode(alloc);
            defer alloc.free(wire);
            for ([_]*Index{ &leader, &follower_a, &follower_b }) |replica| {
                var copy = try Delta.decode(alloc, wire);
                defer copy.deinit(alloc);
                try replica.applyDelta(copy);
            }
        }
        const a = try leader.serialize(alloc);
        defer alloc.free(a);
        const b = try follower_a.serialize(alloc);
        defer alloc.free(b);
        const c = try follower_b.serialize(alloc);
        defer alloc.free(c);
        try testing.expectEqualSlices(u8, a, b);
        try testing.expectEqualSlices(u8, a, c);
        try testing.expectEqual(@as(u64, 400), leader.version);

        // The replicated graph still answers queries: the last inserted
        // vector (i = 899) finds its own node.
        var src = SliceEmbeddingSource{ .vectors = node_vectors.items, .dims = 32 };
        var resp = try follower_b.search(alloc, src.source(), fx.data[899 * 32 ..][0..32], .{ .k = 1, .ef = 256 });
        defer resp.deinit(alloc);
        try testing.expectEqual(last_insert, resp.results[0].id);
    }
}

test "leann: stale and malformed deltas are rejected without changes" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 400, 1, 16, 6, 47);
    defer fx.deinit(alloc);
    var index = try Index.build(alloc, .{ .dims = 16, .quantizer = .rabitq, .max_degree = 8 }, fx.data[0 .. 300 * 16]);
    defer index.deinit();
    var src = SliceEmbeddingSource{ .vectors = fx.data, .dims = 16 };

    var first = try index.planInsert(alloc, src.source(), fx.data[300 * 16 ..][0..16]);
    defer first.deinit(alloc);
    var stale = try index.planInsert(alloc, src.source(), fx.data[301 * 16 ..][0..16]);
    defer stale.deinit(alloc);
    try index.applyDelta(first);

    const before = try index.serialize(alloc);
    defer alloc.free(before);
    try testing.expectError(error.DeltaVersionMismatch, index.applyDelta(stale));
    try testing.expectError(error.DeltaVersionMismatch, index.applyDelta(first));

    var fresh = try index.planInsert(alloc, src.source(), fx.data[301 * 16 ..][0..16]);
    defer fresh.deinit(alloc);
    const wire = try fresh.encode(alloc);
    defer alloc.free(wire);
    // Corrupt one edit slot, one forward id, and the code length.
    {
        var bad = try Delta.decode(alloc, wire);
        defer bad.deinit(alloc);
        if (bad.edits.len > 0) {
            bad.edits[0].slot = 9999;
            try testing.expectError(error.InvalidDelta, index.applyDelta(bad));
        }
    }
    {
        var bad = try Delta.decode(alloc, wire);
        defer bad.deinit(alloc);
        bad.forward[0] = 1_000_000;
        try testing.expectError(error.InvalidDelta, index.applyDelta(bad));
    }
    {
        var bad = try Delta.decode(alloc, wire);
        defer bad.deinit(alloc);
        bad.id += 1;
        try testing.expectError(error.InvalidDelta, index.applyDelta(bad));
    }
    try testing.expectError(error.InvalidDelta, Delta.decode(alloc, wire[0 .. wire.len - 3]));
    try testing.expectError(error.UnknownNode, index.planDelete(5000));

    const after = try index.serialize(alloc);
    defer alloc.free(after);
    try testing.expectEqualSlices(u8, before, after);

    var ok = try Delta.decode(alloc, wire);
    defer ok.deinit(alloc);
    try index.applyDelta(ok);
    try testing.expectEqual(@as(u64, 2), index.version);
}

test "leann: a planned batch reverts exactly, then applies identically" {
    const alloc = testing.allocator;
    var fx = try Fixture.init(alloc, 600, 1, 32, 8, 53);
    defer fx.deinit(alloc);
    for ([_]Quantizer{ .pq, .rabitq }) |quantizer| {
        var leader = try Index.build(alloc, .{ .dims = 32, .metric = .cosine, .quantizer = quantizer, .max_degree = 12 }, fx.data[0 .. 400 * 32]);
        defer leader.deinit();
        const committed = try leader.serialize(alloc);
        defer alloc.free(committed);
        var follower = try Index.deserialize(alloc, committed);
        defer follower.deinit();

        var node_vectors = std.ArrayListUnmanaged(f32).empty;
        defer node_vectors.deinit(alloc);
        try node_vectors.appendSlice(alloc, fx.data[0 .. 400 * 32]);
        var deltas = std.ArrayListUnmanaged(Delta).empty;
        defer {
            for (deltas.items) |*d| d.deinit(alloc);
            deltas.deinit(alloc);
        }
        var undos = std.ArrayListUnmanaged(Index.Undo).empty;
        defer undos.deinit(alloc);

        // Plan a batch of 200 writes, each seeing the previous ones.
        for (400..600) |i| {
            var src = SliceEmbeddingSource{ .vectors = node_vectors.items, .dims = 32 };
            var delta = if (i % 9 == 0) try leader.planDelete(@intCast(i - 300)) else try leader.planInsert(alloc, src.source(), fx.data[i * 32 ..][0..32]);
            errdefer delta.deinit(alloc);
            const undo = try leader.applyDeltaUndoable(delta);
            if (delta.kind == .insert) try node_vectors.appendSlice(alloc, fx.data[i * 32 ..][0..32]);
            try deltas.append(alloc, delta);
            try undos.append(alloc, undo);
        }
        // Revert the whole batch: the leader is back at its committed state.
        var i = deltas.items.len;
        while (i > 0) {
            i -= 1;
            leader.revertDelta(deltas.items[i], undos.items[i]);
        }
        const reverted = try leader.serialize(alloc);
        defer alloc.free(reverted);
        try testing.expectEqualSlices(u8, committed, reverted);

        // "Commit": both replicas apply the batch.
        for (deltas.items) |d| {
            try leader.applyDelta(d);
            try follower.applyDelta(d);
        }
        const a = try leader.serialize(alloc);
        defer alloc.free(a);
        const b = try follower.serialize(alloc);
        defer alloc.free(b);
        try testing.expectEqualSlices(u8, a, b);
    }
}
