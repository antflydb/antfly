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
//! - High-degree preserving pruning (Algorithm 3): the top `hub_fraction` of
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

/// Distance metric. Values match `antfly_vector.vector.DistanceMetric`.
pub const Metric = enum(u8) {
    /// Squared Euclidean distance.
    l2_squared = 0,
    /// Negative dot product.
    inner_product = 1,
    /// 1 - cosine similarity. Vectors are normalized on the way in.
    cosine = 2,
};

pub const Config = struct {
    dims: u32,
    metric: Metric = .l2_squared,
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

    pub fn resolved(self: Config) !Config {
        if (self.dims == 0) return error.InvalidConfig;
        if (self.max_degree < 2) return error.InvalidConfig;
        if (self.ef_construction == 0) return error.InvalidConfig;
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

pub const SearchOptions = struct {
    k: u32 = 10,
    /// Exact-result beam width. Raised to `k` when smaller.
    ef: u32 = 64,
    /// α: fraction of the approximate queue promoted to exact recomputation
    /// after each expansion. At least one candidate is promoted per step.
    rerank_ratio: f32 = 0.1,
    /// Recompute once this many candidates are pending. Zero recomputes after
    /// every expansion step.
    batch_size: u32 = 0,
    /// False disables the approximate queue and recomputes every neighbor,
    /// which is the plain best-first search (Algorithm 1) the paper compares to.
    two_level: bool = true,
};

pub const SearchStats = struct {
    /// Exact embeddings recomputed through the EmbeddingSource.
    recomputed: u32 = 0,
    /// Recompute calls (batches) made.
    batches: u32 = 0,
    /// PQ distances evaluated.
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
    edges: u64,
    avg_degree: f64,
    max_degree: u32,
    hubs: u32,
    deleted: u32,
};

const flag_hub: u8 = 1;
const flag_deleted: u8 = 2;

const NeighborList = std.ArrayListUnmanaged(u32);

pub const Index = struct {
    alloc: Allocator,
    config: Config,
    node_count: u32 = 0,
    entry: u32 = 0,
    deleted_count: u32 = 0,
    adjacency: std.ArrayListUnmanaged(NeighborList) = .empty,
    flags: std.ArrayListUnmanaged(u8) = .empty,
    codes: std.ArrayListUnmanaged(u8) = .empty,
    pq: ProductQuantizer,

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

        var pq = try ProductQuantizer.train(alloc, work, dims, cfg.pq_subspaces, cfg.pq_train_iterations, cfg.pq_train_sample, cfg.seed);
        errdefer pq.deinit(alloc);

        var index = Index{ .alloc = alloc, .config = cfg, .pq = pq };
        errdefer {
            for (index.adjacency.items) |*list| list.deinit(alloc);
            index.adjacency.deinit(alloc);
            index.flags.deinit(alloc);
            index.codes.deinit(alloc);
        }

        try index.codes.resize(alloc, n_usize * cfg.pq_subspaces);
        for (0..n) |i| pq.encode(work[i * dims ..][0..dims], index.codes.items[i * cfg.pq_subspaces ..][0..cfg.pq_subspaces]);
        try index.flags.appendNTimes(alloc, 0, n_usize);

        const ctx = BuildContext{ .vectors = work, .dims = dims, .metric = cfg.metric };
        const entry = try ctx.medoid(alloc, n);
        var visited = try VisitSet.init(alloc, n);
        defer visited.deinit(alloc);
        var cands = std.ArrayListUnmanaged(Cand).empty;
        defer cands.deinit(alloc);
        var scratch = BeamScratch{};
        defer scratch.deinit(alloc);

        // Base graph: single-layer HNSW-style incremental construction,
        // starting from the medoid so it is a good entry point.
        const base = try allocLists(alloc, n);
        defer freeLists(alloc, base);
        for (0..n) |step| {
            const v: u32 = if (step == 0) entry else if (step <= entry) @intCast(step - 1) else @intCast(step);
            if (step == 0) continue;
            try beamSearchExact(alloc, &ctx, base, ctx.vec(v), entry, cfg.ef_construction, &visited, &scratch, &cands);
            try selectHeuristic(alloc, &ctx, cands.items, v, cfg.max_degree, &base[v]);
            for (base[v].items) |u| {
                try base[u].append(alloc, v);
                if (base[u].items.len > cfg.max_degree) try shrinkExact(alloc, &ctx, &base[u], u, cfg.max_degree, &cands);
            }
        }

        if (!cfg.prune) {
            try index.adjacency.ensureTotalCapacity(alloc, n_usize);
            for (base) |*list| {
                index.adjacency.appendAssumeCapacity(list.*);
                list.* = .empty;
            }
        } else {
            const pruned = try pruneGraph(alloc, &ctx, cfg, base, entry, index.flags.items, &visited, &scratch, &cands);
            defer alloc.free(pruned);
            try index.adjacency.ensureTotalCapacity(alloc, n_usize);
            for (pruned) |list| index.adjacency.appendAssumeCapacity(list);
        }

        index.node_count = n;
        index.entry = entry;
        return index;
    }

    pub fn deinit(self: *Index) void {
        for (self.adjacency.items) |*list| list.deinit(self.alloc);
        self.adjacency.deinit(self.alloc);
        self.flags.deinit(self.alloc);
        self.codes.deinit(self.alloc);
        self.pq.deinit(self.alloc);
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
        if (self.flags.items[id] & flag_deleted != 0) return;
        self.flags.items[id] |= flag_deleted;
        self.deleted_count += 1;
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
        const avg = if (self.node_count == 0) 0 else @as(f64, @floatFromInt(edges)) / @as(f64, @floatFromInt(self.node_count));
        return .{ .nodes = self.node_count, .edges = edges, .avg_degree = avg, .max_degree = max_deg, .hubs = hubs, .deleted = self.deleted_count };
    }

    /// Searches for the `k` nearest live nodes to `query`.
    pub fn search(self: *const Index, alloc: Allocator, source: EmbeddingSource, query: []const f32, opts: SearchOptions) !SearchResponse {
        if (query.len != self.dimension()) return error.DimensionMismatch;
        var stats = SearchStats{};
        if (self.node_count == 0 or opts.k == 0) return .{ .results = try alloc.alloc(Result, 0), .stats = stats };

        const q = try alloc.dupe(f32, query);
        defer alloc.free(q);
        if (self.config.metric == .cosine) normalize(q);

        var found = std.ArrayListUnmanaged(Cand).empty;
        defer found.deinit(alloc);
        try self.twoLevelSearch(alloc, source, q, opts, &found, &stats);

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
    /// able to recompute the new id afterwards. Uses the paper's simplified
    /// neighbor selection: the closest `low_degree` live candidates become
    /// forward edges, and full reverse lists drop their farthest neighbor by
    /// PQ distance, so no neighbor embeddings are recomputed for shrinking.
    pub fn insert(self: *Index, source: EmbeddingSource, vector: []const f32) !u32 {
        const alloc = self.alloc;
        const d = self.dimension();
        if (vector.len != d) return error.DimensionMismatch;
        if (self.node_count == std.math.maxInt(u32) - 1) return error.IndexFull;
        const sub = self.config.pq_subspaces;

        const v = try alloc.dupe(f32, vector);
        defer alloc.free(v);
        if (self.config.metric == .cosine) normalize(v);

        var found = std.ArrayListUnmanaged(Cand).empty;
        defer found.deinit(alloc);
        if (self.node_count > 0) {
            var stats = SearchStats{};
            const ef = self.config.ef_construction;
            try self.twoLevelSearch(alloc, source, v, .{ .k = ef, .ef = ef }, &found, &stats);
        }

        const id = self.node_count;
        try self.codes.ensureUnusedCapacity(alloc, sub);
        try self.flags.ensureUnusedCapacity(alloc, 1);
        try self.adjacency.ensureUnusedCapacity(alloc, 1);
        var forward = NeighborList.empty;
        errdefer forward.deinit(alloc);
        for (found.items) |c| {
            if (forward.items.len >= self.config.low_degree) break;
            if (self.isDeleted(c.id)) continue;
            try forward.append(alloc, c.id);
        }
        // Reserve everything up front: nothing below the commit may fail.
        for (forward.items) |u| try self.adjacency.items[u].ensureUnusedCapacity(alloc, 1);
        const a = try alloc.alloc(f32, d);
        defer alloc.free(a);
        const b = try alloc.alloc(f32, d);
        defer alloc.free(b);

        const code_start = self.codes.items.len;
        self.codes.items.len += sub;
        self.pq.encode(v, self.codes.items[code_start..][0..sub]);
        self.flags.appendAssumeCapacity(0);
        self.adjacency.appendAssumeCapacity(forward);
        self.node_count += 1;

        for (forward.items) |u| {
            const list = &self.adjacency.items[u];
            if (list.items.len < self.config.max_degree) {
                list.appendAssumeCapacity(id);
                continue;
            }
            self.pq.decode(self.code(u), a);
            self.pq.decode(self.code(id), b);
            var worst_i: usize = list.items.len;
            var worst = distance(self.config.metric, a, b);
            for (list.items, 0..) |w, i| {
                self.pq.decode(self.code(w), b);
                const dist = distance(self.config.metric, a, b);
                if (dist > worst) {
                    worst = dist;
                    worst_i = i;
                }
            }
            if (worst_i < list.items.len) list.items[worst_i] = id;
        }
        return id;
    }

    fn code(self: *const Index, id: u32) []const u8 {
        const sub = self.config.pq_subspaces;
        return self.codes.items[@as(usize, id) * sub ..][0..sub];
    }

    /// Algorithm 2. Fills `out` with up to `ef` exact candidates in ascending
    /// distance order, including deleted nodes.
    fn twoLevelSearch(
        self: *const Index,
        alloc: Allocator,
        source: EmbeddingSource,
        q: []const f32,
        opts: SearchOptions,
        out: *std.ArrayListUnmanaged(Cand),
        stats: *SearchStats,
    ) !void {
        const ef: usize = @max(opts.ef, opts.k);
        const ratio: f32 = std.math.clamp(opts.rerank_ratio, 0, 1);

        const table = try alloc.alloc(f32, self.pq.tableLen());
        defer alloc.free(table);
        self.pq.buildTable(self.config.metric, q, table);

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

        const Flush = struct {
            fn run(
                index: *const Index,
                a: Allocator,
                src: EmbeddingSource,
                query: []const f32,
                ids: *std.ArrayListUnmanaged(u32),
                vectors: *std.ArrayListUnmanaged(f32),
                eq: *Heap(false),
                res: *Heap(true),
                limit: usize,
                st: *SearchStats,
            ) !void {
                if (ids.items.len == 0) return;
                const dd = index.dimension();
                try vectors.resize(a, ids.items.len * dd);
                try src.recompute(ids.items, vectors.items);
                st.batches += 1;
                st.recomputed += @intCast(ids.items.len);
                for (ids.items, 0..) |id, i| {
                    const row = vectors.items[i * dd ..][0..dd];
                    if (index.config.metric == .cosine) normalize(row);
                    const c = Cand{ .dist = distance(index.config.metric, query, row), .id = id };
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

        visited.set(self.entry);
        try pending.append(alloc, self.entry);
        try Flush.run(self, alloc, source, q, &pending, &buf, &exact_queue, &results, ef, stats);

        while (true) {
            if (exact_queue.len() == 0) {
                if (pending.items.len > 0) {
                    try Flush.run(self, alloc, source, q, &pending, &buf, &exact_queue, &results, ef, stats);
                    continue;
                }
                // The exact queue ran dry. Like the paper, stop once the beam
                // is full; only keep promoting while it still has room.
                if (approx_queue.len() == 0 or results.len() >= ef) break;
                try promote(alloc, &approx_queue, &pending, ratio);
                try Flush.run(self, alloc, source, q, &pending, &buf, &exact_queue, &results, ef, stats);
                continue;
            }

            const c = exact_queue.pop();
            if (results.len() >= ef and c.dist > results.top().dist) {
                if (pending.items.len > 0) {
                    try Flush.run(self, alloc, source, q, &pending, &buf, &exact_queue, &results, ef, stats);
                    continue;
                }
                break;
            }

            stats.expanded += 1;
            for (self.adjacency.items[c.id].items) |nb| {
                if (visited.isSet(nb)) continue;
                visited.set(nb);
                stats.approx_evaluated += 1;
                try approx_queue.push(alloc, .{ .dist = self.pq.adc(table, self.code(nb)), .id = nb });
            }

            if (opts.two_level) {
                try promote(alloc, &approx_queue, &pending, ratio);
            } else {
                while (approx_queue.len() > 0) try pending.append(alloc, approx_queue.pop().id);
            }
            if (opts.batch_size == 0 or pending.items.len >= opts.batch_size) {
                try Flush.run(self, alloc, source, q, &pending, &buf, &exact_queue, &results, ef, stats);
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
    pub const format_version: u32 = 1;

    /// Serializes the graph (CSR), PQ codebooks, codes, and flags. No
    /// embeddings are written.
    pub fn serialize(self: *const Index, alloc: Allocator) ![]u8 {
        var w: std.Io.Writer.Allocating = .init(alloc);
        defer w.deinit();
        const wr = &w.writer;
        const cfg = self.config;
        try wr.writeAll(magic);
        try wr.writeInt(u32, format_version, .little);
        try wr.writeInt(u32, cfg.dims, .little);
        try wr.writeByte(@backingInt(cfg.metric));
        try wr.writeInt(u32, cfg.max_degree, .little);
        try wr.writeInt(u32, cfg.low_degree, .little);
        try wr.writeInt(u32, cfg.ef_construction, .little);
        try wr.writeInt(u32, @bitCast(cfg.hub_fraction), .little);
        try wr.writeInt(u32, cfg.pq_subspaces, .little);
        try wr.writeInt(u32, self.pq.k, .little);
        try wr.writeInt(u32, self.node_count, .little);
        try wr.writeInt(u32, self.entry, .little);
        for (self.pq.centroids) |f| try wr.writeInt(u32, @bitCast(f), .little);
        try wr.writeAll(self.codes.items);
        try wr.writeAll(self.flags.items);
        var offset: u32 = 0;
        try wr.writeInt(u32, 0, .little);
        for (self.adjacency.items) |list| {
            offset += @intCast(list.items.len);
            try wr.writeInt(u32, offset, .little);
        }
        for (self.adjacency.items) |list| {
            for (list.items) |nb| try wr.writeInt(u32, nb, .little);
        }
        const body = w.written();
        const crc = std.hash.Crc32.hash(body);
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
        const metric_byte = r.takeByte() catch return error.InvalidFormat;
        cfg.metric = std.enums.fromInt(Metric, metric_byte) orelse return error.InvalidFormat;
        cfg.max_degree = try readU32(&r);
        cfg.low_degree = try readU32(&r);
        cfg.ef_construction = try readU32(&r);
        cfg.hub_fraction = @bitCast(try readU32(&r));
        cfg.pq_subspaces = try readU32(&r);
        cfg = cfg.resolved() catch return error.InvalidFormat;
        const k = try readU32(&r);
        const n = try readU32(&r);
        const entry = try readU32(&r);
        if (k == 0 or k > 256) return error.InvalidFormat;
        if (n == 0 or entry >= n) return error.InvalidFormat;

        const sub: usize = cfg.pq_subspaces;
        const centroid_len = @as(usize, k) * cfg.dims;
        // Cheap size check before allocating anything large.
        const fixed_len = centroid_len * 4 + @as(usize, n) * (sub + 1) + (@as(usize, n) + 1) * 4;
        if (r.bufferedLen() < fixed_len) return error.InvalidFormat;

        var pq = ProductQuantizer{ .dims = cfg.dims, .subspaces = cfg.pq_subspaces, .sub_dims = cfg.dims / cfg.pq_subspaces, .k = k, .centroids = try alloc.alloc(f32, centroid_len) };
        errdefer pq.deinit(alloc);
        for (pq.centroids) |*f| f.* = @bitCast(try readU32(&r));

        var index = Index{ .alloc = alloc, .config = cfg, .pq = pq, .node_count = n, .entry = entry };
        errdefer {
            for (index.adjacency.items) |*list| list.deinit(alloc);
            index.adjacency.deinit(alloc);
            index.flags.deinit(alloc);
            index.codes.deinit(alloc);
        }
        try index.codes.appendSlice(alloc, r.take(@as(usize, n) * sub) catch return error.InvalidFormat);
        for (index.codes.items) |c| if (c >= k) return error.InvalidFormat;
        try index.flags.appendSlice(alloc, r.take(n) catch return error.InvalidFormat);
        for (index.flags.items) |f| {
            if (f & ~(flag_hub | flag_deleted) != 0) return error.InvalidFormat;
            if (f & flag_deleted != 0) index.deleted_count += 1;
        }

        const offsets = try alloc.alloc(u32, @as(usize, n) + 1);
        defer alloc.free(offsets);
        for (offsets) |*o| o.* = try readU32(&r);
        if (offsets[0] != 0) return error.InvalidFormat;
        for (offsets[1..], offsets[0..n]) |hi, lo| if (hi < lo) return error.InvalidFormat;
        if (r.bufferedLen() != @as(usize, offsets[n]) * 4) return error.InvalidFormat;

        try index.adjacency.ensureTotalCapacity(alloc, n);
        for (0..n) |i| {
            const len = offsets[i + 1] - offsets[i];
            var list = try NeighborList.initCapacity(alloc, len);
            for (0..len) |_| {
                const nb = try readU32(&r);
                if (nb >= n) {
                    list.deinit(alloc);
                    return error.InvalidFormat;
                }
                list.appendAssumeCapacity(nb);
            }
            index.adjacency.appendAssumeCapacity(list);
        }
        return index;
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

    /// Node closest to the mean vector, used as the search entry point.
    fn medoid(self: *const BuildContext, alloc: Allocator, n: u32) !u32 {
        const sum = try alloc.alloc(f64, self.dims);
        defer alloc.free(sum);
        @memset(sum, 0);
        for (0..n) |i| {
            for (self.vec(@intCast(i)), 0..) |x, j| sum[j] += x;
        }
        const inv = 1.0 / @as(f64, @floatFromInt(n));
        for (sum) |*x| x.* *= inv;
        var best: u32 = 0;
        var best_d: f64 = std.math.inf(f64);
        for (0..n) |i| {
            var d: f64 = 0;
            for (self.vec(@intCast(i)), sum) |x, m| {
                const diff = @as(f64, x) - m;
                d += diff * diff;
            }
            if (d < best_d) {
                best_d = d;
                best = @intCast(i);
            }
        }
        return best;
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

/// Best-first search with exact distances (Algorithm 1), used while building.
fn beamSearchExact(
    alloc: Allocator,
    ctx: *const BuildContext,
    adj: []const NeighborList,
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
        for (adj[c.id].items) |nb| {
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

/// High-degree preserving pruning (Algorithm 3). Returns the pruned lists and
/// marks hubs in `flags`.
fn pruneGraph(
    alloc: Allocator,
    ctx: *const BuildContext,
    cfg: Config,
    base: []const NeighborList,
    entry: u32,
    flags: []u8,
    visited: *VisitSet,
    scratch: *BeamScratch,
    cands: *std.ArrayListUnmanaged(Cand),
) ![]NeighborList {
    const n: u32 = @intCast(base.len);
    const indegree = try alloc.alloc(u32, n);
    defer alloc.free(indegree);
    @memset(indegree, 0);
    for (base) |list| for (list.items) |nb| {
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

    // Hubs keep their base edges. In a single-layer graph the long-range
    // links that HNSW would keep in upper layers sit on early, high in-degree
    // nodes, so preserving hub lists preserves the routing backbone.
    for (0..n) |i| {
        if (flags[i] & flag_hub == 0) continue;
        try pruned[i].appendSlice(alloc, base[i].items);
    }
    for (0..n) |i| {
        const v: u32 = @intCast(i);
        if (flags[v] & flag_hub != 0) continue;
        try beamSearchExact(alloc, ctx, base, ctx.vec(v), entry, cfg.ef_construction, visited, scratch, cands);
        // The base graph's own edges include the long-range links made early
        // in construction. Without them a single-layer graph splits into
        // per-cluster islands once degrees drop to `low_degree`.
        for (base[v].items) |nb| {
            if (!visited.testAndSet(nb)) continue;
            try cands.append(alloc, .{ .dist = ctx.dist(v, nb), .id = nb });
        }
        std.mem.sortUnstable(Cand, cands.items, {}, candLessThan);
        try selectHeuristic(alloc, ctx, cands.items, v, cfg.low_degree, &selected);
        for (selected.items) |s| _ = try addUnique(alloc, &pruned[v], s);
        if (pruned[v].items.len > cfg.max_degree) try shrinkExact(alloc, ctx, &pruned[v], v, cfg.max_degree, &shrink_scratch);
        for (selected.items) |u| {
            if (!try addUnique(alloc, &pruned[u], v)) continue;
            if (pruned[u].items.len > cfg.max_degree) try shrinkExact(alloc, ctx, &pruned[u], u, cfg.max_degree, &shrink_scratch);
        }
    }
    try repairConnectivity(alloc, ctx, cfg, pruned, entry, visited, scratch, cands);
    return pruned;
}

/// Makes every node reachable from `entry` (as in NSG). For each node the
/// traversal cannot reach, search the reachable part of the graph for its
/// nearest reachable node and link that node to it.
fn repairConnectivity(
    alloc: Allocator,
    ctx: *const BuildContext,
    cfg: Config,
    adj: []NeighborList,
    entry: u32,
    visited: *VisitSet,
    scratch: *BeamScratch,
    cands: *std.ArrayListUnmanaged(Cand),
) !void {
    const n: u32 = @intCast(adj.len);
    var reached = try std.DynamicBitSetUnmanaged.initEmpty(alloc, n);
    defer reached.deinit(alloc);
    var stack = std.ArrayListUnmanaged(u32).empty;
    defer stack.deinit(alloc);
    try markReachable(alloc, adj, entry, &reached, &stack);
    for (0..n) |i| {
        const u: u32 = @intCast(i);
        if (reached.isSet(u)) continue;
        try beamSearchExact(alloc, ctx, adj, ctx.vec(u), entry, cfg.ef_construction, visited, scratch, cands);
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
    }
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
    const codebook = index.pq.centroids.len * @sizeOf(f32);
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
