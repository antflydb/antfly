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

//! LEANN recall / recomputation benchmark.
//!
//!   zig run -OReleaseFast lib/vectorindex/src/leann_bench.zig -- \
//!       [--data PREFIX] [--n 100000] [--queries 200] [--dims 64] \
//!       [--metric l2|cosine|ip] [--m 30] [--efc 128] [--quantizer pq|rabitq|both] \
//!       [--unpruned 1] [--cost FILE]
//!       [--churn ROUNDS] [--churn-frac 0.05] [--rebuild-at 0.2]
//!       [--churn-set maintenance|rebuild|codes|int4] [--bits 1..8]
//!
//! --churn switches to the mixed-workload benchmark: after the initial build,
//! each round deletes `churn-frac * n` random live vectors and inserts as many
//! new ones (rows after the first n of the training set), then measures
//! recall and recomputations against exact ground truth over the live set.
//! It compares tombstones with rebuilds at `rebuild-at` deleted, delete
//! consolidation, and both with stored upper-layer vectors. With
//! `--churn-set rebuild` it compares where rebuilds get their vectors from:
//! exact embeddings, the RaBitQ codes alone, or an int8 copy.
//!
//! --cost names a measured embedder cost curve: one "batch_size milliseconds"
//! pair per line (see scripts/leann_embed_cost.py). When given, the bench
//! reports modeled embedding time per query, interpolating each recompute
//! call's cost from the curve.
//!
//! PREFIX names two files, PREFIX.train.f32 and PREFIX.test.f32, each a
//! little-endian u32 row count and u32 dimension followed by row-major f32s.
//! Without --data the benchmark uses Gaussian clusters.

const std = @import("std");
const leann = @import("leann.zig");
const Allocator = std.mem.Allocator;

const Args = struct {
    data: ?[]const u8 = null,
    n: usize = 100_000,
    queries: usize = 200,
    dims: usize = 64,
    metric: leann.Metric = .l2_squared,
    m: u32 = 30,
    efc: u32 = 128,
    quantizer: ?leann.Quantizer = null,
    unpruned: bool = false,
    cost: ?[]const u8 = null,
    churn: usize = 0,
    churn_frac: f64 = 0.05,
    rebuild_at: f64 = 0.2,
    churn_set: ChurnSet = .maintenance,
    /// RaBitQ bits per dimension.
    bits: u8 = 1,
};

const ChurnSet = enum { maintenance, rebuild, codes, int4 };

/// Embedder cost curve: milliseconds per call at measured batch sizes,
/// linearly interpolated (and extrapolated from the last two points).
const CostCurve = struct {
    sizes: []f64,
    ms: []f64,

    fn at(self: CostCurve, b: usize) f64 {
        const x: f64 = @floatFromInt(b);
        const n = self.sizes.len;
        if (n == 1) return self.ms[0] * x / self.sizes[0];
        var i: usize = 1;
        while (i < n - 1 and self.sizes[i] < x) i += 1;
        const x0 = self.sizes[i - 1];
        const x1 = self.sizes[i];
        const t = (x - x0) / (x1 - x0);
        return @max(self.ms[i - 1] + t * (self.ms[i] - self.ms[i - 1]), 0);
    }
};

/// EmbeddingSource that serves stored vectors and adds the modeled cost of
/// each call.
const CostSource = struct {
    inner: leann.SliceEmbeddingSource,
    curve: ?CostCurve,
    modeled_ms: f64 = 0,

    fn source(self: *CostSource) leann.EmbeddingSource {
        return .{ .ctx = self, .recompute_fn = recompute };
    }

    fn recompute(ctx: *anyopaque, ids: []const u32, out: []f32) anyerror!void {
        const self: *CostSource = @ptrCast(@alignCast(ctx));
        if (self.curve) |c| self.modeled_ms += c.at(ids.len);
        return self.inner.source().recompute(ids, out);
    }
};

const Matrix = struct {
    rows: usize,
    dims: usize,
    data: []f32,

    fn row(self: Matrix, i: usize) []const f32 {
        return self.data[i * self.dims ..][0..self.dims];
    }
};

pub fn main(init: std.process.Init) !void {
    var gpa_state: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{});
    defer _ = gpa_state.deinit();
    const alloc = gpa_state.allocator();
    const io = init.io;

    var stdout_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buffer);
    const out = &stdout_writer.interface;
    defer out.flush() catch {};

    var args = Args{};
    var it = std.process.Args.Iterator.init(init.minimal.args);
    _ = it.skip();
    while (it.next()) |flag| {
        const value = it.next() orelse return error.MissingValue;
        if (std.mem.eql(u8, flag, "--data")) {
            args.data = value;
        } else if (std.mem.eql(u8, flag, "--n")) {
            args.n = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, flag, "--queries")) {
            args.queries = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, flag, "--dims")) {
            args.dims = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, flag, "--m")) {
            args.m = try std.fmt.parseInt(u32, value, 10);
        } else if (std.mem.eql(u8, flag, "--efc")) {
            args.efc = try std.fmt.parseInt(u32, value, 10);
        } else if (std.mem.eql(u8, flag, "--quantizer")) {
            args.quantizer = if (std.mem.eql(u8, value, "pq")) .pq else if (std.mem.eql(u8, value, "rabitq")) .rabitq else null;
        } else if (std.mem.eql(u8, flag, "--unpruned")) {
            args.unpruned = std.mem.eql(u8, value, "1");
        } else if (std.mem.eql(u8, flag, "--churn")) {
            args.churn = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, flag, "--churn-frac")) {
            args.churn_frac = try std.fmt.parseFloat(f64, value);
        } else if (std.mem.eql(u8, flag, "--bits")) {
            args.bits = try std.fmt.parseInt(u8, value, 10);
        } else if (std.mem.eql(u8, flag, "--churn-set")) {
            args.churn_set = std.meta.stringToEnum(ChurnSet, value) orelse return error.UnknownChurnSet;
        } else if (std.mem.eql(u8, flag, "--rebuild-at")) {
            args.rebuild_at = try std.fmt.parseFloat(f64, value);
        } else if (std.mem.eql(u8, flag, "--cost")) {
            args.cost = value;
        } else if (std.mem.eql(u8, flag, "--metric")) {
            args.metric = if (std.mem.eql(u8, value, "cosine")) .cosine else if (std.mem.eql(u8, value, "ip")) .inner_product else .l2_squared;
        } else return error.UnknownFlag;
    }

    if (args.churn > 0) return runChurn(alloc, io, out, args);

    var train: Matrix = undefined;
    var test_set: Matrix = undefined;
    if (args.data) |prefix| {
        train = try loadMatrix(alloc, io, prefix, "train", args.n);
        test_set = try loadMatrix(alloc, io, prefix, "test", args.queries);
    } else {
        train = try synthetic(alloc, args.n, args.dims, 1);
        test_set = try synthetic(alloc, args.queries, args.dims, 2);
    }
    defer alloc.free(train.data);
    defer alloc.free(test_set.data);
    const dims = train.dims;
    const k = 10;

    try out.print("dataset rows={d} dims={d} queries={d} metric={s} M={d} efC={d}\n", .{ train.rows, dims, test_set.rows, @tagName(args.metric), args.m, args.efc });
    try out.flush();

    const truth = try groundTruth(alloc, train, test_set, args.metric, k);
    defer alloc.free(truth);

    var curve: ?CostCurve = null;
    defer if (curve) |c| {
        alloc.free(c.sizes);
        alloc.free(c.ms);
    };
    if (args.cost) |path| {
        curve = try loadCost(alloc, io, path);
        try out.print("embedder cost curve:", .{});
        for (curve.?.sizes, curve.?.ms) |b, m| try out.print(" b={d}:{d:.1}ms", .{ b, m });
        try out.print("\n", .{});
    }
    var src = CostSource{ .inner = .{ .vectors = train.data, .dims = dims }, .curve = curve };
    const raw_bytes = train.rows * dims * @sizeOf(f32);

    const Variant = struct { quantizer: leann.Quantizer, prune: bool };
    var variants: [4]Variant = undefined;
    var nv: usize = 0;
    for ([_]leann.Quantizer{ .pq, .rabitq }) |qz| {
        if (args.quantizer != null and args.quantizer.? != qz) continue;
        variants[nv] = .{ .quantizer = qz, .prune = true };
        nv += 1;
        if (args.unpruned) {
            variants[nv] = .{ .quantizer = qz, .prune = false };
            nv += 1;
        }
    }

    for (variants[0..nv]) |variant| {
        const t0 = std.Io.Timestamp.now(io, .awake);
        var index = try leann.Index.build(alloc, .{ .dims = @intCast(dims), .metric = args.metric, .quantizer = variant.quantizer, .max_degree = args.m, .ef_construction = args.efc, .prune = variant.prune, .rabitq_bits = if (variant.quantizer == .rabitq) args.bits else 1 }, train.data);
        defer index.deinit();
        const build_ms = @as(f64, @floatFromInt(t0.untilNow(io, .awake).nanoseconds)) / 1e6;
        const bytes = try index.serialize(alloc);
        defer alloc.free(bytes);
        const gs = index.graphStats();
        try out.print("\n== {s} (bits {d}) {s} graph: build {d:.1}s, avg degree {d:.2}, max {d}, hubs {d}, levels {d}, upper nodes {d}, repaired {d}, index {d:.2} MiB = {d:.1}% of raw f32 ({d:.2} MiB)\n", .{
            @tagName(variant.quantizer),
            if (variant.quantizer == .rabitq) args.bits else 0,
            if (variant.prune) "pruned" else "unpruned",
            build_ms / 1000,
            gs.avg_degree,
            gs.max_degree,
            gs.hubs,
            gs.max_level,
            gs.upper_nodes,
            index.repaired,
            mib(bytes.len),
            100 * @as(f64, @floatFromInt(bytes.len)) / @as(f64, @floatFromInt(raw_bytes)),
            mib(raw_bytes),
        });
        try out.print("{s:<22} {s:>5} {s:>9} {s:>12} {s:>9} {s:>9} {s:>11}\n", .{ "mode", "ef", "recall@10", "recomputed/q", "batches/q", "cpu ms/q", "embed ms/q" });
        try out.flush();

        const Mode = struct { name: []const u8, opts: leann.SearchOptions };
        const pq_modes = [_]Mode{
            .{ .name = "plain", .opts = .{ .two_level = false } },
            .{ .name = "two-level a=0.1", .opts = .{ .rerank = .ratio, .rerank_ratio = 0.1 } },
            .{ .name = "two-level+batch16", .opts = .{ .rerank = .ratio, .rerank_ratio = 0.1, .batch_size = 16 } },
            .{ .name = "two-level+batch64", .opts = .{ .rerank = .ratio, .rerank_ratio = 0.3, .batch_size = 64 } },
        };
        const rq_modes = [_]Mode{
            .{ .name = "two-level a=0.1", .opts = .{ .rerank = .ratio, .rerank_ratio = 0.1 } },
            .{ .name = "bounds s=0.5", .opts = .{ .rerank = .bounds, .bound_scale = 0.5 } },
            .{ .name = "bounds s=1", .opts = .{ .rerank = .bounds, .bound_scale = 1 } },
            .{ .name = "bounds s=1.5", .opts = .{ .rerank = .bounds, .bound_scale = 1.5 } },
            .{ .name = "bounds s=2", .opts = .{ .rerank = .bounds, .bound_scale = 2 } },
            .{ .name = "bounds s=1 batch32", .opts = .{ .rerank = .bounds, .bound_scale = 1, .batch_size = 32 } },
        };
        const modes: []const Mode = if (variant.quantizer == .pq) &pq_modes else &rq_modes;
        for (modes) |mode| {
            const pq_efs = [_]u32{ 16, 32, 64, 128, 256 };
            // The estimate-only walk costs no recomputation, so wider beams
            // are cheap for bound-driven search.
            const bound_efs = [_]u32{ 32, 64, 128, 256, 512, 1024 };
            const efs: []const u32 = if (mode.opts.rerank == .bounds) &bound_efs else &pq_efs;
            for (efs) |ef| {
                var opts = mode.opts;
                opts.k = k;
                opts.ef = ef;
                var hits: usize = 0;
                var recomputed: u64 = 0;
                var batches: u64 = 0;
                src.modeled_ms = 0;
                const s0 = std.Io.Timestamp.now(io, .awake);
                for (0..test_set.rows) |qi| {
                    var resp = try index.search(alloc, src.source(), test_set.row(qi), opts);
                    defer resp.deinit(alloc);
                    recomputed += resp.stats.recomputed;
                    batches += resp.stats.batches;
                    const gt = truth[qi * k ..][0..k];
                    for (resp.results) |r| {
                        if (std.mem.indexOfScalar(u32, gt, r.id) != null) hits += 1;
                    }
                }
                const ns = s0.untilNow(io, .awake).nanoseconds;
                const nq: f64 = @floatFromInt(test_set.rows);
                try out.print("{s:<22} {d:>5} {d:>9.3} {d:>12.1} {d:>9.1} {d:>9.3} {d:>11.1}\n", .{
                    mode.name,
                    ef,
                    @as(f64, @floatFromInt(hits)) / (nq * k),
                    @as(f64, @floatFromInt(recomputed)) / nq,
                    @as(f64, @floatFromInt(batches)) / nq,
                    @as(f64, @floatFromInt(ns)) / 1e6 / nq,
                    src.modeled_ms / nq,
                });
                try out.flush();
            }
        }
    }
}

fn loadCost(alloc: Allocator, io: std.Io, path: []const u8) !CostCurve {
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(1 << 20));
    defer alloc.free(raw);
    var sizes = std.ArrayListUnmanaged(f64).empty;
    errdefer sizes.deinit(alloc);
    var ms = std.ArrayListUnmanaged(f64).empty;
    errdefer ms.deinit(alloc);
    var lines = std.mem.tokenizeScalar(u8, raw, '\n');
    while (lines.next()) |line| {
        if (line.len == 0 or line[0] == '#') continue;
        var fields = std.mem.tokenizeAny(u8, line, " \t,");
        const b = fields.next() orelse continue;
        const m = fields.next() orelse return error.InvalidCost;
        try sizes.append(alloc, try std.fmt.parseFloat(f64, b));
        try ms.append(alloc, try std.fmt.parseFloat(f64, m));
    }
    if (sizes.items.len == 0) return error.InvalidCost;
    return .{ .sizes = try sizes.toOwnedSlice(alloc), .ms = try ms.toOwnedSlice(alloc) };
}

fn mib(n: usize) f64 {
    return @as(f64, @floatFromInt(n)) / (1024 * 1024);
}

fn loadMatrix(alloc: Allocator, io: std.Io, prefix: []const u8, part: []const u8, limit: usize) !Matrix {
    const path = try std.fmt.allocPrint(alloc, "{s}.{s}.f32", .{ prefix, part });
    defer alloc.free(path);
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(8 * 1024 * 1024 * 1024));
    defer alloc.free(raw);
    if (raw.len < 8) return error.InvalidData;
    const rows_all = std.mem.readInt(u32, raw[0..4], .little);
    const dims = std.mem.readInt(u32, raw[4..8], .little);
    const rows = @min(rows_all, limit);
    if (raw.len < 8 + @as(usize, rows_all) * dims * 4) return error.InvalidData;
    const data = try alloc.alloc(f32, rows * dims);
    @memcpy(std.mem.sliceAsBytes(data), raw[8..][0 .. data.len * 4]);
    return .{ .rows = rows, .dims = dims, .data = data };
}

fn synthetic(alloc: Allocator, rows: usize, dims: usize, stream: u64) !Matrix {
    var centers_rng = std.Random.DefaultPrng.init(99);
    const clusters = 64;
    const centers = try alloc.alloc(f32, clusters * dims);
    defer alloc.free(centers);
    for (centers) |*c| c.* = centers_rng.random().floatNorm(f32) * 3;
    var prng = std.Random.DefaultPrng.init(stream);
    const rand = prng.random();
    const data = try alloc.alloc(f32, rows * dims);
    for (0..rows) |i| {
        const c = rand.uintLessThan(usize, clusters);
        for (0..dims) |j| data[i * dims + j] = centers[c * dims + j] + rand.floatNorm(f32);
    }
    return .{ .rows = rows, .dims = dims, .data = data };
}

fn groundTruth(alloc: Allocator, train: Matrix, queries: Matrix, metric: leann.Metric, k: usize) ![]u32 {
    const truth = try alloc.alloc(u32, queries.rows * k);
    const dists = try alloc.alloc(f32, k);
    defer alloc.free(dists);
    const qbuf = try alloc.alloc(f32, train.dims);
    defer alloc.free(qbuf);
    const rbuf = try alloc.alloc(f32, train.dims);
    defer alloc.free(rbuf);
    for (0..queries.rows) |qi| {
        @memcpy(qbuf, queries.row(qi));
        if (metric == .cosine) normalize(qbuf);
        const ids = truth[qi * k ..][0..k];
        @memset(dists, std.math.inf(f32));
        @memset(ids, 0);
        for (0..train.rows) |i| {
            @memcpy(rbuf, train.row(i));
            if (metric == .cosine) normalize(rbuf);
            const d = leann.distance(metric, qbuf, rbuf);
            if (d >= dists[k - 1]) continue;
            var pos = k - 1;
            while (pos > 0 and dists[pos - 1] > d) : (pos -= 1) {
                dists[pos] = dists[pos - 1];
                ids[pos] = ids[pos - 1];
            }
            dists[pos] = d;
            ids[pos] = @intCast(i);
        }
    }
    return truth;
}

fn normalize(v: []f32) void {
    var s: f32 = 0;
    for (v) |x| s += x * x;
    const n = @sqrt(s);
    if (n == 0) return;
    for (v) |*x| x.* /= n;
}

// ---------------------------------------------------------------------------
// Mixed-workload (churn) benchmark.

/// Counts recomputations served from node-id-indexed vectors.
const CountingSource = struct {
    vectors: []const f32,
    dims: usize,
    count: u64 = 0,

    fn source(self: *CountingSource) leann.EmbeddingSource {
        return .{ .ctx = self, .recompute_fn = recompute };
    }

    fn recompute(ctx: *anyopaque, ids: []const u32, out: []f32) anyerror!void {
        const self: *CountingSource = @ptrCast(@alignCast(ctx));
        self.count += ids.len;
        for (ids, 0..) |id, i| {
            const start = @as(usize, id) * self.dims;
            if (start + self.dims > self.vectors.len) return error.UnknownNode;
            @memcpy(out[i * self.dims ..][0..self.dims], self.vectors[start..][0..self.dims]);
        }
    }
};

/// The same deletes and inserts (by dataset row) for every variant, plus the
/// exact top-k rows over the live set after each round.
const Workload = struct {
    deletes: [][]u32,
    inserts: [][]u32,
    /// truth[r]: k rows per query after round r (r = 0 is the initial set).
    truth: [][]u32,

    fn deinit(self: *Workload, alloc: Allocator) void {
        for (self.deletes) |d| alloc.free(d);
        for (self.inserts) |d| alloc.free(d);
        for (self.truth) |d| alloc.free(d);
        alloc.free(self.deletes);
        alloc.free(self.inserts);
        alloc.free(self.truth);
    }
};

fn makeWorkload(alloc: Allocator, rows: Matrix, queries: Matrix, metric: leann.Metric, n0: usize, rounds: usize, per_round: usize, k: usize) !Workload {
    var live = std.ArrayListUnmanaged(u32).empty;
    defer live.deinit(alloc);
    for (0..n0) |i| try live.append(alloc, @intCast(i));
    var w = Workload{
        .deletes = try alloc.alloc([]u32, rounds),
        .inserts = try alloc.alloc([]u32, rounds),
        .truth = try alloc.alloc([]u32, rounds + 1),
    };
    var prng = std.Random.DefaultPrng.init(7);
    const rand = prng.random();
    var next: u32 = @intCast(n0);
    w.truth[0] = try truthOver(alloc, rows, queries, metric, live.items, k);
    for (0..rounds) |r| {
        w.deletes[r] = try alloc.alloc(u32, per_round);
        for (w.deletes[r]) |*d| {
            const i = rand.uintLessThan(usize, live.items.len);
            d.* = live.swapRemove(i);
        }
        w.inserts[r] = try alloc.alloc(u32, per_round);
        for (w.inserts[r]) |*d| {
            d.* = next;
            next += 1;
            try live.append(alloc, d.*);
        }
        w.truth[r + 1] = try truthOver(alloc, rows, queries, metric, live.items, k);
    }
    return w;
}

fn truthOver(alloc: Allocator, rows: Matrix, queries: Matrix, metric: leann.Metric, live: []const u32, k: usize) ![]u32 {
    const truth = try alloc.alloc(u32, queries.rows * k);
    const dists = try alloc.alloc(f32, k);
    defer alloc.free(dists);
    const qbuf = try alloc.alloc(f32, rows.dims);
    defer alloc.free(qbuf);
    const rbuf = try alloc.alloc(f32, rows.dims);
    defer alloc.free(rbuf);
    for (0..queries.rows) |qi| {
        @memcpy(qbuf, queries.row(qi));
        if (metric == .cosine) normalize(qbuf);
        const ids = truth[qi * k ..][0..k];
        @memset(dists, std.math.inf(f32));
        @memset(ids, 0);
        for (live) |row_id| {
            @memcpy(rbuf, rows.row(row_id));
            if (metric == .cosine) normalize(rbuf);
            const d = leann.distance(metric, qbuf, rbuf);
            if (d >= dists[k - 1]) continue;
            var pos = k - 1;
            while (pos > 0 and dists[pos - 1] > d) : (pos -= 1) {
                dists[pos] = dists[pos - 1];
                ids[pos] = ids[pos - 1];
            }
            dists[pos] = d;
            ids[pos] = row_id;
        }
    }
    return truth;
}

/// Where a rebuild gets the vectors it builds the graph from.
const RebuildSource = enum {
    /// Exact embeddings: re-embedding every live document.
    exact,
    /// Reconstructions of the RaBitQ codes already in the index.
    codes,
    /// A per-dimension int8 copy (dims bytes per vector of extra storage).
    int8,
    /// A per-dimension 4-bit copy (dims / 2 bytes per vector).
    int4,
};

const ChurnVariant = struct {
    name: []const u8,
    store_upper: bool,
    consolidate: bool,
    rebuild: bool,
    source: RebuildSource = .exact,
};

/// Round-trips rows through per-dimension scalar quantization to `levels`
/// values (256 for int8, 16 for 4 bits).
fn scalarRoundTrip(alloc: Allocator, rows: []const f32, dims: usize, metric: leann.Metric, levels: f32) ![]f32 {
    const out = try alloc.dupe(f32, rows);
    errdefer alloc.free(out);
    const n = rows.len / dims;
    if (metric == .cosine) {
        for (0..n) |i| normalize(out[i * dims ..][0..dims]);
    }
    const lo = try alloc.alloc(f32, dims);
    defer alloc.free(lo);
    const hi = try alloc.alloc(f32, dims);
    defer alloc.free(hi);
    @memset(lo, std.math.inf(f32));
    @memset(hi, -std.math.inf(f32));
    for (0..n) |i| for (out[i * dims ..][0..dims], lo, hi) |x, *l, *h| {
        l.* = @min(l.*, x);
        h.* = @max(h.*, x);
    };
    for (0..n) |i| for (out[i * dims ..][0..dims], lo, hi) |*x, l, h| {
        const step = (h - l) / (levels - 1);
        if (step == 0) continue;
        x.* = l + @round((x.* - l) / step) * step;
    };
    return out;
}

/// One variant's index plus the node id <-> dataset row mapping.
const ChurnState = struct {
    index: leann.Index,
    /// Node-id-indexed vectors served to the EmbeddingSource.
    vecs: std.ArrayListUnmanaged(f32) = .empty,
    node_row: std.ArrayListUnmanaged(u32) = .empty,
    row_node: []u32,

    fn deinit(self: *ChurnState, alloc: Allocator) void {
        self.index.deinit();
        self.vecs.deinit(alloc);
        self.node_row.deinit(alloc);
        alloc.free(self.row_node);
    }

    /// Builds over `live_rows`; node i is live_rows[i].
    fn build(alloc: Allocator, cfg: leann.Config, rows: Matrix, live_rows: []const u32, total_rows: usize) !ChurnState {
        var st = ChurnState{ .index = undefined, .row_node = try alloc.alloc(u32, total_rows) };
        errdefer alloc.free(st.row_node);
        @memset(st.row_node, std.math.maxInt(u32));
        for (live_rows, 0..) |r, i| {
            try st.vecs.appendSlice(alloc, rows.row(r));
            try st.node_row.append(alloc, r);
            st.row_node[r] = @intCast(i);
        }
        st.index = try leann.Index.build(alloc, cfg, st.vecs.items);
        return st;
    }

    /// Rebuilds over `live_rows` (dataset rows, all live) from the existing
    /// codes, optionally guided by an int8 copy of their vectors.
    fn rebuildFromCodes(self: *const ChurnState, alloc: Allocator, rows: Matrix, live_rows: []const u32, total_rows: usize, source: RebuildSource) !ChurnState {
        const nodes = try alloc.alloc(u32, live_rows.len);
        defer alloc.free(nodes);
        var st = ChurnState{ .index = undefined, .row_node = try alloc.alloc(u32, total_rows) };
        errdefer alloc.free(st.row_node);
        @memset(st.row_node, std.math.maxInt(u32));
        for (live_rows, 0..) |r, i| {
            nodes[i] = self.row_node[r];
            try st.vecs.appendSlice(alloc, rows.row(r));
            try st.node_row.append(alloc, r);
            st.row_node[r] = @intCast(i);
        }
        var guide: ?[]f32 = null;
        defer if (guide) |g| alloc.free(g);
        if (source == .int8 or source == .int4) guide = try scalarRoundTrip(alloc, st.vecs.items, rows.dims, self.index.config.metric, if (source == .int8) 256 else 16);
        st.index = try self.index.rebuildFromCodes(alloc, nodes, guide);
        return st;
    }

    fn liveRows(self: *const ChurnState, alloc: Allocator) ![]u32 {
        var out = std.ArrayListUnmanaged(u32).empty;
        errdefer out.deinit(alloc);
        for (self.node_row.items, 0..) |r, node| {
            if (!self.index.isDeleted(@intCast(node))) try out.append(alloc, r);
        }
        return out.toOwnedSlice(alloc);
    }
};

const Measured = struct { recall: f64, recomputed: f64, cpu_ms: f64 };

fn measure(alloc: Allocator, io: std.Io, st: *ChurnState, queries: Matrix, truth: []const u32, k: usize, ef: u32) !Measured {
    var src = CountingSource{ .vectors = st.vecs.items, .dims = queries.dims };
    var hits: usize = 0;
    var recomputed: u64 = 0;
    const t0 = std.Io.Timestamp.now(io, .awake);
    for (0..queries.rows) |qi| {
        var resp = try st.index.search(alloc, src.source(), queries.row(qi), .{ .k = @intCast(k), .ef = ef, .rerank = .bounds, .bound_scale = 1 });
        defer resp.deinit(alloc);
        recomputed += resp.stats.recomputed;
        const gt = truth[qi * k ..][0..k];
        for (resp.results) |r| {
            if (std.mem.indexOfScalar(u32, gt, st.node_row.items[r.id]) != null) hits += 1;
        }
    }
    const nq: f64 = @floatFromInt(queries.rows);
    return .{
        .recall = @as(f64, @floatFromInt(hits)) / (nq * @as(f64, @floatFromInt(k))),
        .recomputed = @as(f64, @floatFromInt(recomputed)) / nq,
        .cpu_ms = @as(f64, @floatFromInt(t0.untilNow(io, .awake).nanoseconds)) / 1e6 / nq,
    };
}

fn msSince(io: std.Io, t0: std.Io.Timestamp) f64 {
    return @as(f64, @floatFromInt(t0.untilNow(io, .awake).nanoseconds)) / 1e6;
}

fn runChurn(alloc: Allocator, io: std.Io, out: *std.Io.Writer, args: Args) !void {
    const prefix = args.data orelse return error.ChurnNeedsData;
    const per_round: usize = @intFromFloat(@as(f64, @floatFromInt(args.n)) * args.churn_frac);
    const total = args.n + per_round * args.churn;
    const rows = try loadMatrix(alloc, io, prefix, "train", total);
    defer alloc.free(rows.data);
    if (rows.rows < total) return error.NotEnoughRows;
    const queries = try loadMatrix(alloc, io, prefix, "test", args.queries);
    defer alloc.free(queries.data);
    const k = 10;
    const quantizer = args.quantizer orelse .rabitq;
    if (quantizer != .rabitq) return error.ChurnNeedsRaBitQ;

    try out.print("churn: n={d} dims={d} queries={d} rounds={d} per round: {d} deletes + {d} inserts, rebuild at {d:.0}% deleted, M={d} efC={d}, rabitq bits {d}\n", .{ args.n, rows.dims, queries.rows, args.churn, per_round, per_round, args.rebuild_at * 100, args.m, args.efc, args.bits });
    try out.flush();
    const t_truth = std.Io.Timestamp.now(io, .awake);
    var w = try makeWorkload(alloc, rows, queries, args.metric, args.n, args.churn, per_round, k);
    defer w.deinit(alloc);
    try out.print("ground truth for {d} states in {d:.1}s\n", .{ args.churn + 1, msSince(io, t_truth) / 1000 });

    const maintenance_set = [_]ChurnVariant{
        .{ .name = "tombstones + rebuild", .store_upper = false, .consolidate = false, .rebuild = true },
        .{ .name = "stored upper + rebuild", .store_upper = true, .consolidate = false, .rebuild = true },
        .{ .name = "consolidate", .store_upper = false, .consolidate = true, .rebuild = false },
        .{ .name = "stored upper + consolidate", .store_upper = true, .consolidate = true, .rebuild = false },
    };
    const rebuild_set = [_]ChurnVariant{
        .{ .name = "tombstones + rebuild from exact", .store_upper = false, .consolidate = false, .rebuild = true, .source = .exact },
        .{ .name = "tombstones + rebuild from codes", .store_upper = false, .consolidate = false, .rebuild = true, .source = .codes },
        .{ .name = "tombstones + rebuild from int8", .store_upper = false, .consolidate = false, .rebuild = true, .source = .int8 },
        .{ .name = "consolidate + rebuild from codes", .store_upper = false, .consolidate = true, .rebuild = true, .source = .codes },
    };
    const codes_set = [_]ChurnVariant{
        .{ .name = "tombstones + rebuild from exact", .store_upper = false, .consolidate = false, .rebuild = true, .source = .exact },
        .{ .name = "tombstones + rebuild from codes", .store_upper = false, .consolidate = false, .rebuild = true, .source = .codes },
    };
    const int4_set = [_]ChurnVariant{
        .{ .name = "tombstones + rebuild from int4", .store_upper = false, .consolidate = false, .rebuild = true, .source = .int4 },
    };
    const variants: []const ChurnVariant = switch (args.churn_set) {
        .maintenance => &maintenance_set,
        .rebuild => &rebuild_set,
        .codes => &codes_set,
        .int4 => &int4_set,
    };
    const initial = try alloc.alloc(u32, args.n);
    defer alloc.free(initial);
    for (initial, 0..) |*r, i| r.* = @intCast(i);

    for (variants) |variant| {
        const cfg = leann.Config{ .dims = @intCast(rows.dims), .metric = args.metric, .quantizer = .rabitq, .max_degree = args.m, .ef_construction = args.efc, .store_upper = variant.store_upper, .rabitq_bits = args.bits };
        const tb = std.Io.Timestamp.now(io, .awake);
        var st = try ChurnState.build(alloc, cfg, rows, initial, total);
        defer st.deinit(alloc);
        try out.print("\n== {s}: initial build {d:.1}s, upper vectors {d}\n", .{ variant.name, msSince(io, tb) / 1000, st.index.upper_vectors.count() });
        try out.print("{s:>5} {s:>7} {s:>6} {s:>9} {s:>9} {s:>9} {s:>8} {s:>13} {s:>8} {s:>13} {s:>8}\n", .{ "round", "live", "del%", "write emb", "ins ms", "maint ms", "rebuilt", "recall@512", "rec/q", "recall@1024", "rec/q" });
        var rebuild_embeds: u64 = 0;
        var rebuilds: usize = 0;
        for (0..args.churn + 1) |r| {
            var write_embeds: u64 = 0;
            var insert_ms: f64 = 0;
            var maint_ms: f64 = 0;
            var rebuilt = false;
            if (r > 0) {
                for (w.deletes[r - 1]) |row_id| {
                    const node = st.row_node[row_id];
                    var delta = try st.index.planDelete(node);
                    defer delta.deinit(alloc);
                    try st.index.applyDelta(delta);
                    st.row_node[row_id] = std.math.maxInt(u32);
                }
                const ti = std.Io.Timestamp.now(io, .awake);
                for (w.inserts[r - 1]) |row_id| {
                    var src = CountingSource{ .vectors = st.vecs.items, .dims = rows.dims };
                    const node = try st.index.insert(src.source(), rows.row(row_id));
                    write_embeds += src.count;
                    try st.vecs.appendSlice(alloc, rows.row(row_id));
                    try st.node_row.append(alloc, row_id);
                    std.debug.assert(node + 1 == st.node_row.items.len);
                    st.row_node[row_id] = node;
                }
                insert_ms = msSince(io, ti) / @as(f64, @floatFromInt(per_round));
                const tm = std.Io.Timestamp.now(io, .awake);
                if (variant.consolidate) _ = try st.index.consolidateDeletes();
                if (variant.rebuild and st.index.deletedFraction() >= args.rebuild_at) {
                    const live = try st.liveRows(alloc);
                    defer alloc.free(live);
                    var fresh = switch (variant.source) {
                        .exact => try ChurnState.build(alloc, cfg, rows, live, total),
                        .codes, .int8, .int4 => try st.rebuildFromCodes(alloc, rows, live, total, variant.source),
                    };
                    st.deinit(alloc);
                    st = fresh;
                    fresh = undefined;
                    if (variant.source == .exact) rebuild_embeds += live.len;
                    rebuilds += 1;
                    rebuilt = true;
                }
                maint_ms = msSince(io, tm);
            }
            const m512 = try measure(alloc, io, &st, queries, w.truth[r], k, 512);
            const m1024 = try measure(alloc, io, &st, queries, w.truth[r], k, 1024);
            try out.print("{d:>5} {d:>7} {d:>6.1} {d:>9} {d:>9.2} {d:>9.0} {s:>8} {d:>13.3} {d:>8.1} {d:>13.3} {d:>8.1}\n", .{
                r,
                st.index.node_count - st.index.deleted_count,
                st.index.deletedFraction() * 100,
                write_embeds,
                insert_ms,
                maint_ms,
                if (rebuilt) "yes" else "",
                m512.recall,
                m512.recomputed,
                m1024.recall,
                m1024.recomputed,
            });
            try out.flush();
        }
        try out.print("rebuilds {d} (from {s}), embeddings for rebuilds {d}\n", .{ rebuilds, @tagName(variant.source), rebuild_embeds });
        try out.flush();
    }
}
