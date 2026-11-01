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

//! Simulates HBC's leaf scoring to compare code types before changing HBC.
//!
//!   zig build-exe -OReleaseFast bench/vectors/rabitq_hbc_sim.zig ... -- \
//!       --data PREFIX [--n 100000] [--queries 200] [--leaf 168] [--k 10] \
//!       [--rerank-factor 8] [--metric cosine|l2]
//!
//! Vectors are clustered into leaves of about `--leaf` members (k-means on
//! a sample, like HBC's postings), each leaf coded relative to its own
//! centroid. Each query probes the closest leaves by centroid distance and
//! scores every member with each code type:
//!
//! - rabitq1: the 1-bit kernel as HBC runs it by default (no rotation)
//! - rabitq1-rot: the same with a random rotation
//! - ex2/ex3/ex4: extended RaBitQ (rotated)
//! - f16: half-precision copies, like HBC's native projection plane, with
//!   a Cauchy-Schwarz bound on the rounding error
//!
//! It reports admission recall (true top-k found among the k * rerank-factor
//! best estimates, HBC's candidate heap), how many admitted candidates the
//! boundary rule would send to exact rerank (lower bound at or below the
//! k-th smallest upper bound), and bytes per vector.

const std = @import("std");
const av = @import("antfly_vector");
const rq = av.quantizer;
const rqx = av.rabitq_ex;
const vec = av.vector;
const Allocator = std.mem.Allocator;

const Matrix = struct {
    rows: usize,
    dims: usize,
    data: []f32,
    fn row(self: Matrix, i: usize) []f32 {
        return self.data[i * self.dims ..][0..self.dims];
    }
};

const Method = enum { rabitq1, rabitq1_rot, ex2, ex3, ex4, f16 };

const Leaf = struct {
    members: std.ArrayListUnmanaged(u32) = .empty,
};

pub fn main(init: std.process.Init) !void {
    var gpa_state: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{});
    defer _ = gpa_state.deinit();
    const alloc = gpa_state.allocator();
    const io = init.io;
    var buf: [16 * 1024]u8 = undefined;
    var w = std.Io.File.stdout().writer(io, &buf);
    const out = &w.interface;
    defer out.flush() catch {};

    var prefix: ?[]const u8 = null;
    var n: usize = 100_000;
    var nq: usize = 200;
    var leaf_size: usize = 168;
    var k: usize = 10;
    var rerank_factor: usize = 8;
    var metric: vec.DistanceMetric = .cosine;
    var it = std.process.Args.Iterator.init(init.minimal.args);
    _ = it.skip();
    while (it.next()) |flag| {
        const v = it.next() orelse return error.MissingValue;
        if (std.mem.eql(u8, flag, "--data")) prefix = v else if (std.mem.eql(u8, flag, "--n")) n = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, flag, "--queries")) nq = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, flag, "--leaf")) leaf_size = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, flag, "--k")) k = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, flag, "--rerank-factor")) rerank_factor = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, flag, "--metric")) metric = if (std.mem.eql(u8, v, "l2")) .l2_squared else .cosine else return error.UnknownFlag;
    }
    const data = try loadMatrix(alloc, io, prefix orelse return error.NeedData, "train", n);
    defer alloc.free(data.data);
    const queries = try loadMatrix(alloc, io, prefix.?, "test", nq);
    defer alloc.free(queries.data);
    const d = data.dims;
    if (metric == .cosine) {
        for (0..data.rows) |i| normalize(data.row(i));
        for (0..queries.rows) |i| normalize(queries.row(i));
    }

    // Leaves: k-means on a sample, then assign everything.
    const n_leaves = @max(1, data.rows / leaf_size);
    const t0 = std.Io.Timestamp.now(io, .awake);
    const centroids = try kmeans(alloc, data, n_leaves, 8);
    defer alloc.free(centroids);
    const leaves = try alloc.alloc(Leaf, n_leaves);
    defer {
        for (leaves) |*l| l.members.deinit(alloc);
        alloc.free(leaves);
    }
    @memset(leaves, .{});
    for (0..data.rows) |i| {
        const c = nearest(centroids, d, data.row(i));
        try leaves[c].members.append(alloc, @intCast(i));
    }
    // Leaf centroids are the means of their members.
    for (leaves, 0..) |l, c| {
        const cen = centroids[c * d ..][0..d];
        if (l.members.items.len == 0) continue;
        @memset(cen, 0);
        for (l.members.items) |m| for (cen, data.row(m)) |*a, b| {
            a.* += b;
        };
        for (cen) |*a| a.* /= @floatFromInt(l.members.items.len);
    }
    try out.print("rows={d} dims={d} queries={d} leaves={d} (target {d}) k={d} heap={d} metric={s}; clustering {d:.1}s\n", .{ data.rows, d, queries.rows, n_leaves, leaf_size, k, k * rerank_factor, @tagName(metric), msSince(io, t0) / 1000 });
    try out.flush();

    // Rotated copies of data, centroids and queries (one rotation for all).
    var rot = try vec.RandomOrthogonalTransformer.init(alloc, .givens, d, 42);
    defer rot.deinit();
    const rdata = try rotateAll(alloc, &rot, data.data, d);
    defer alloc.free(rdata);
    const rcent = try rotateAll(alloc, &rot, centroids, d);
    defer alloc.free(rcent);
    const rqry = try rotateAll(alloc, &rot, queries.data, d);
    defer alloc.free(rqry);

    // Exact top-k per query over everything.
    const truth = try alloc.alloc(u32, queries.rows * k);
    defer alloc.free(truth);
    for (0..queries.rows) |qi| try topK(alloc, data, queries.row(qi), metric, k, truth[qi * k ..][0..k]);

    // Encode every leaf with each method.
    var quantizer = try rq.RaBitQuantizer.init(alloc, d, 42, metric);
    defer quantizer.deinit();
    const sets_plain = try alloc.alloc(av.proto.RaBitQuantizedVectorSet, n_leaves);
    const sets_rot = try alloc.alloc(av.proto.RaBitQuantizedVectorSet, n_leaves);
    defer {
        for (sets_plain) |*s| s.deinit(alloc);
        for (sets_rot) |*s| s.deinit(alloc);
        alloc.free(sets_plain);
        alloc.free(sets_rot);
    }
    const gather = try alloc.alloc(f32, 4096 * d);
    defer alloc.free(gather);
    for (leaves, 0..) |l, c| {
        const m = l.members.items.len;
        const g = try alloc.alloc(f32, @max(m, 1) * d);
        defer alloc.free(g);
        for (l.members.items, 0..) |id, j| @memcpy(g[j * d ..][0..d], data.row(id));
        sets_plain[c] = try quantizer.quantize(centroids[c * d ..][0..d], g, m);
        for (l.members.items, 0..) |id, j| @memcpy(g[j * d ..][0..d], rdata[@as(usize, id) * d ..][0..d]);
        sets_rot[c] = try quantizer.quantize(rcent[c * d ..][0..d], g, m);
    }
    var ex_rows: [3][]u8 = undefined;
    var codecs: [3]rqx.Codec = undefined;
    for (0..3) |b| {
        codecs[b] = try rqx.Codec.init(d, @intCast(b + 2), metric);
        ex_rows[b] = try alloc.alloc(u8, data.rows * codecs[b].rowBytes());
        const rb = codecs[b].rowBytes();
        for (leaves, 0..) |l, c| for (l.members.items) |id| {
            try codecs[b].encodeAlloc(alloc, rcent[c * d ..][0..d], rdata[@as(usize, id) * d ..][0..d], ex_rows[b][@as(usize, id) * rb ..][0..rb]);
        };
    }
    defer for (ex_rows) |r| alloc.free(r);
    const halves = try alloc.alloc(f16, data.data.len);
    defer alloc.free(halves);
    for (halves, data.data) |*h, x| h.* = @floatCast(x);
    // Per-vector f16 rounding error norm, for the bound.
    const herr = try alloc.alloc(f32, data.rows);
    defer alloc.free(herr);
    for (0..data.rows) |i| {
        var e: f32 = 0;
        for (halves[i * d ..][0..d], data.row(i)) |h, x| {
            const diff = @as(f32, @floatCast(h)) - x;
            e += diff * diff;
        }
        herr[i] = @sqrt(e);
    }

    const methods = [_]Method{ .rabitq1, .rabitq1_rot, .ex2, .ex3, .ex4, .f16 };
    const bytes_per = [_]f64{
        @floatFromInt(rabitqBytes(d, metric)),
        @floatFromInt(rabitqBytes(d, metric)),
        @floatFromInt(codecs[0].rowBytes()),
        @floatFromInt(codecs[1].rowBytes()),
        @floatFromInt(codecs[2].rowBytes()),
        @floatFromInt(d * 2),
    };

    const probes = [_]usize{ 32, 64, 128, 256 };
    try out.print("\n{s:<12} {s:>6} {s:>8} {s:>9} {s:>10} {s:>10} {s:>10} {s:>9}\n", .{ "method", "probe", "scanned", "bytes/vec", "probe rec", "admit rec", "rerank/q", "score us/q" });
    const heap = k * rerank_factor;
    var est = std.ArrayListUnmanaged(Scored).empty;
    defer est.deinit(alloc);
    var dist = std.ArrayListUnmanaged(f32).empty;
    defer dist.deinit(alloc);
    var bnd = std.ArrayListUnmanaged(f32).empty;
    defer bnd.deinit(alloc);
    var scratch = try rq.RaBitQuantizer.EstimateScratch.init(alloc, d);
    defer scratch.deinit(alloc);
    const leaf_order = try alloc.alloc(Scored, n_leaves);
    defer alloc.free(leaf_order);
    for (probes) |probe| {
        for (methods, 0..) |method, mi| {
            var scanned: usize = 0;
            var probe_hits: usize = 0;
            var admit_hits: usize = 0;
            var rerank: usize = 0;
            var score_ns: u64 = 0;
            for (0..queries.rows) |qi| {
                const q = queries.row(qi);
                const rqv = rqry[qi * d ..][0..d];
                for (leaf_order, 0..) |*s, c| s.* = .{ .id = @intCast(c), .dist = metricDistance(metric, q, centroids[c * d ..][0..d]), .bound = 0 };
                std.mem.sortUnstable(Scored, leaf_order, {}, Scored.lessThan);
                est.clearRetainingCapacity();
                const ts = std.Io.Timestamp.now(io, .awake);
                for (leaf_order[0..@min(probe, n_leaves)]) |lo| {
                    const c = lo.id;
                    const members = leaves[c].members.items;
                    if (members.len == 0) continue;
                    switch (method) {
                        .rabitq1, .rabitq1_rot => {
                            try dist.resize(alloc, members.len);
                            try bnd.resize(alloc, members.len);
                            const set = if (method == .rabitq1) &sets_plain[c] else &sets_rot[c];
                            try quantizer.estimateDistancesWithScratch(set, if (method == .rabitq1) q else rqv, dist.items, bnd.items, &scratch);
                            for (members, dist.items, bnd.items) |id, dd, bb| try est.append(alloc, .{ .id = id, .dist = dd, .bound = bb });
                        },
                        .ex2, .ex3, .ex4 => {
                            const b = @backingInt(method) - @backingInt(Method.ex2);
                            const codec = codecs[b];
                            var query = try codec.prepare(alloc, rcent[c * d ..][0..d], rqv);
                            defer query.deinit(alloc);
                            const rb = codec.rowBytes();
                            for (members) |id| {
                                const e = codec.estimate(&query, ex_rows[b][@as(usize, id) * rb ..][0..rb]);
                                try est.append(alloc, .{ .id = id, .dist = e.distance, .bound = e.bound });
                            }
                        },
                        .f16 => {
                            const qn = vec.norm(q);
                            for (members) |id| {
                                var acc: f32 = 0;
                                var l2: f32 = 0;
                                for (halves[@as(usize, id) * d ..][0..d], q) |h, x| {
                                    const hv: f32 = @floatCast(h);
                                    acc += hv * x;
                                    l2 += (hv - x) * (hv - x);
                                }
                                const e = herr[id];
                                const dd: f32 = switch (metric) {
                                    .l2_squared => l2,
                                    .inner_product => -acc,
                                    .cosine => 1 - acc,
                                };
                                // |<q, x - h>| <= |q| |x - h|; l2 adds the square term.
                                const bb: f32 = if (metric == .l2_squared) 2 * @sqrt(l2) * e + e * e else qn * e;
                                try est.append(alloc, .{ .id = id, .dist = dd, .bound = bb });
                            }
                        },
                    }
                }
                score_ns += @intCast(ts.untilNow(io, .awake).nanoseconds);
                scanned += est.items.len;
                const gt = truth[qi * k ..][0..k];
                for (est.items) |e| {
                    if (std.mem.indexOfScalar(u32, gt, e.id) != null) probe_hits += 1;
                }
                std.mem.sortUnstable(Scored, est.items, {}, Scored.lessThan);
                const admitted = est.items[0..@min(heap, est.items.len)];
                for (admitted) |e| {
                    if (std.mem.indexOfScalar(u32, gt, e.id) != null) admit_hits += 1;
                }
                // Boundary rule: k-th smallest upper bound among admitted.
                if (admitted.len > k) {
                    const uppers = try alloc.alloc(f32, admitted.len);
                    defer alloc.free(uppers);
                    for (admitted, uppers) |e, *u| u.* = e.dist + e.bound;
                    std.mem.sortUnstable(f32, uppers, {}, std.sort.asc(f32));
                    const kth = uppers[k - 1];
                    for (admitted) |e| {
                        if (e.dist - e.bound <= kth) rerank += 1;
                    }
                } else rerank += admitted.len;
            }
            const nqf: f64 = @floatFromInt(queries.rows);
            const tk: f64 = @floatFromInt(queries.rows * k);
            try out.print("{s:<12} {d:>6} {d:>8.0} {d:>9.0} {d:>10.3} {d:>10.3} {d:>10.1} {d:>9.0}\n", .{
                @tagName(method),
                probe,
                @as(f64, @floatFromInt(scanned)) / nqf,
                bytes_per[mi],
                @as(f64, @floatFromInt(probe_hits)) / tk,
                @as(f64, @floatFromInt(admit_hits)) / tk,
                @as(f64, @floatFromInt(rerank)) / nqf,
                @as(f64, @floatFromInt(score_ns)) / 1e3 / nqf,
            });
            try out.flush();
        }
    }
}

const Scored = struct {
    id: u32,
    dist: f32,
    bound: f32,
    fn lessThan(_: void, a: Scored, b: Scored) bool {
        return a.dist < b.dist;
    }
};

/// 1-bit code words plus code count, centroid distance, quantized dot
/// product, and centroid dot product for inner product and cosine.
fn rabitqBytes(d: usize, metric: vec.DistanceMetric) usize {
    return av.rabitq.codeWidth(d) * 8 + 12 + (if (metric != .l2_squared) @as(usize, 4) else 0);
}

fn rotateAll(alloc: Allocator, rot: *const vec.RandomOrthogonalTransformer, rows: []const f32, d: usize) ![]f32 {
    const outv = try alloc.alloc(f32, rows.len);
    var i: usize = 0;
    while (i < rows.len) : (i += d) _ = rot.transform(rows[i..][0..d], outv[i..][0..d]);
    return outv;
}

fn nearest(centroids: []const f32, d: usize, x: []const f32) usize {
    var best: usize = 0;
    var best_d = std.math.inf(f32);
    var c: usize = 0;
    while (c * d < centroids.len) : (c += 1) {
        var s: f32 = 0;
        for (centroids[c * d ..][0..d], x) |a, b| s += (a - b) * (a - b);
        if (s < best_d) {
            best_d = s;
            best = c;
        }
    }
    return best;
}

fn kmeans(alloc: Allocator, data: Matrix, kc: usize, iters: usize) ![]f32 {
    const d = data.dims;
    const sample_n = @min(data.rows, kc * 40);
    var prng = std.Random.DefaultPrng.init(11);
    const rand = prng.random();
    const sample = try alloc.alloc(u32, sample_n);
    defer alloc.free(sample);
    for (sample) |*s| s.* = rand.uintLessThan(u32, @intCast(data.rows));
    const cent = try alloc.alloc(f32, kc * d);
    for (0..kc) |c| @memcpy(cent[c * d ..][0..d], data.row(sample[c % sample_n]));
    const sums = try alloc.alloc(f64, kc * d);
    defer alloc.free(sums);
    const counts = try alloc.alloc(u32, kc);
    defer alloc.free(counts);
    for (0..iters) |_| {
        @memset(sums, 0);
        @memset(counts, 0);
        for (sample) |s| {
            const c = nearest(cent, d, data.row(s));
            counts[c] += 1;
            for (sums[c * d ..][0..d], data.row(s)) |*a, b| a.* += b;
        }
        for (0..kc) |c| {
            if (counts[c] == 0) {
                @memcpy(cent[c * d ..][0..d], data.row(sample[rand.uintLessThan(usize, sample_n)]));
                continue;
            }
            for (cent[c * d ..][0..d], sums[c * d ..][0..d]) |*a, b| a.* = @floatCast(b / @as(f64, @floatFromInt(counts[c])));
        }
    }
    return cent;
}

fn topK(alloc: Allocator, data: Matrix, q: []const f32, metric: vec.DistanceMetric, k: usize, ids: []u32) !void {
    const dists = try alloc.alloc(f32, k);
    defer alloc.free(dists);
    @memset(dists, std.math.inf(f32));
    @memset(ids, 0);
    for (0..data.rows) |i| {
        const dd = metricDistance(metric, q, data.row(i));
        if (dd >= dists[k - 1]) continue;
        var pos = k - 1;
        while (pos > 0 and dists[pos - 1] > dd) : (pos -= 1) {
            dists[pos] = dists[pos - 1];
            ids[pos] = ids[pos - 1];
        }
        dists[pos] = dd;
        ids[pos] = @intCast(i);
    }
}

/// Antfly's conventions: l2 squared, negative inner product, 1 - cosine
/// (inputs already normalized for cosine).
fn metricDistance(metric: vec.DistanceMetric, a: []const f32, b: []const f32) f32 {
    var acc: f32 = 0;
    switch (metric) {
        .l2_squared => {
            for (a, b) |x, y| acc += (x - y) * (x - y);
            return acc;
        },
        .inner_product => {
            for (a, b) |x, y| acc += x * y;
            return -acc;
        },
        .cosine => {
            for (a, b) |x, y| acc += x * y;
            return 1 - acc;
        },
    }
}

fn normalize(v: []f32) void {
    var s: f32 = 0;
    for (v) |x| s += x * x;
    const nn = @sqrt(s);
    if (nn == 0) return;
    for (v) |*x| x.* /= nn;
}

fn msSince(io: std.Io, t0: std.Io.Timestamp) f64 {
    return @as(f64, @floatFromInt(t0.untilNow(io, .awake).nanoseconds)) / 1e6;
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
    const data = try alloc.alloc(f32, rows * dims);
    @memcpy(std.mem.sliceAsBytes(data), raw[8..][0 .. data.len * 4]);
    return .{ .rows = rows, .dims = dims, .data = data };
}
