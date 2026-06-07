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

//! Benchmark for the SPFresh-style posting shape (lib/vectorindex/spfresh_shape).
//!
//! Phases:
//!   - build:     insert N vectors            (write IO / cpu / throughput)
//!   - overwrite: overwrite frac*N vectors    ("during writes", the hot case)
//!   - repair:    fold deltas + refresh centroids (background maintenance cost)
//!   - query:     Q searches, recall vs brute  ("after": qps / read IO / recall)
//!
//! Compare across --repair {none,full,interleave} and --nprobe to see the
//! recall-vs-maintenance-backlog and query-IO tradeoffs. IO is logical block
//! bytes; pair with an external RSS poller for memory.

const std = @import("std");
const shape = @import("antfly_spfresh_shape");

const Allocator = std.mem.Allocator;
const VectorId = shape.VectorId;

const RepairMode = enum { none, full, interleave };
const OverwriteMode = enum { drift, teleport };

const Config = struct {
    vectors: usize = 100_000,
    dim: u32 = 128,
    postings: u32 = 1024,
    clusters: u32 = 1024,
    queries: usize = 200,
    k: usize = 10,
    nprobe: usize = 16,
    overwrite_fraction: f64 = 0.5,
    repair: RepairMode = .full,
    repair_budget: u32 = 0, // 0 => postings/16
    fold_ratio: u32 = 100, // interleave: fold a posting when delta >= ratio% of base
    overwrite_mode: OverwriteMode = .drift, // drift = re-embed near original (realistic)
    overwrite_sigma: f64 = 0.3,
    seed: u64 = 0x5f3e_5102,
};

fn fillGaussian(out: []f32, rnd: std.Random, center: []const f32, sigma: f32) void {
    for (out, 0..) |*o, i| o.* = center[i] + rnd.floatNorm(f32) * sigma;
}

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(alloc, init.minimal.args);

    var stdout_buffer: [8192]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;

    const dim = cfg.dim;
    var prng = std.Random.DefaultPrng.init(cfg.seed);
    const rnd = prng.random();

    // ---- dataset: mixture of `clusters` gaussians --------------------------
    const centers = try alloc.alloc(f32, cfg.clusters * dim);
    defer alloc.free(centers);
    for (centers) |*c| c.* = rnd.float(f32) * 20.0 - 10.0;

    // live vector store owned by the bench (for ground-truth recall + overwrite)
    const data = try alloc.alloc(f32, cfg.vectors * dim);
    defer alloc.free(data);
    for (0..cfg.vectors) |i| {
        const cl = rnd.uintLessThan(u32, cfg.clusters);
        fillGaussian(data[i * dim ..][0..dim], rnd, centers[cl * dim ..][0..dim], 1.0);
    }

    // seeds for postings = sampled dataset vectors
    const seeds = try alloc.alloc([]const f32, cfg.postings);
    defer alloc.free(seeds);
    for (seeds) |*s| {
        const j = rnd.uintLessThan(usize, cfg.vectors);
        s.* = data[j * dim ..][0..dim];
    }

    var idx = try shape.Index.init(alloc, .{ .dim = dim, .num_postings = cfg.postings }, seeds);
    defer idx.deinit();

    const budget: u32 = if (cfg.repair_budget != 0) cfg.repair_budget else @max(@as(u32, 1), cfg.postings / 16);
    const logical_vec_bytes: u64 = @as(u64, dim) * 4;

    // ---- build phase -------------------------------------------------------
    var c0 = idx.store.counters;
    var t0 = monotonicNs();
    {
        var i: usize = 0;
        const chunk: usize = @max(1, cfg.vectors / 64);
        while (i < cfg.vectors) : (i += 1) {
            try idx.insert(@intCast(i), data[i * dim ..][0..dim]);
            if (cfg.repair == .interleave and (i + 1) % chunk == 0) {
                _ = try idx.repairTriggered(cfg.fold_ratio, 4096);
            }
        }
    }
    const build_ns = monotonicNs() - t0;
    const build_io = shape.IoCounters.sub(idx.store.counters, c0);

    // ---- overwrite phase ("during writes") ---------------------------------
    const n_over: usize = @intFromFloat(@as(f64, @floatFromInt(cfg.vectors)) * cfg.overwrite_fraction);
    c0 = idx.store.counters;
    t0 = monotonicNs();
    {
        const tmp = try alloc.alloc(f32, dim);
        defer alloc.free(tmp);
        const chunk: usize = @max(1, n_over / 64);
        var i: usize = 0;
        while (i < n_over) : (i += 1) {
            const id = rnd.uintLessThan(usize, cfg.vectors);
            switch (cfg.overwrite_mode) {
                .teleport => {
                    const cl = rnd.uintLessThan(u32, cfg.clusters);
                    fillGaussian(tmp, rnd, centers[cl * dim ..][0..dim], 1.0);
                },
                .drift => {
                    const old = data[id * dim ..][0..dim];
                    const sig: f32 = @floatCast(cfg.overwrite_sigma);
                    for (tmp, old) |*o, ov| o.* = ov + rnd.floatNorm(f32) * sig;
                },
            }
            @memcpy(data[id * dim ..][0..dim], tmp); // keep ground truth current
            try idx.update(@intCast(id), data[id * dim ..][0..dim]);
            if (cfg.repair == .interleave and (i + 1) % chunk == 0) {
                _ = try idx.repairTriggered(cfg.fold_ratio, 4096);
            }
        }
    }
    const over_ns = monotonicNs() - t0;
    const over_io = shape.IoCounters.sub(idx.store.counters, c0);
    const dirty_before_repair = idx.dirtyPostings();
    const deltas_before_repair = idx.unfoldedDeltas();
    const stale_before_repair = idx.avgStaleness();

    // ---- repair phase ------------------------------------------------------
    c0 = idx.store.counters;
    t0 = monotonicNs();
    var folded: u32 = 0;
    if (cfg.repair == .full) {
        const r = try idx.repair(cfg.postings); // drain everything
        folded = r.folded;
    }
    const repair_ns = monotonicNs() - t0;
    const repair_io = shape.IoCounters.sub(idx.store.counters, c0);

    // ---- query phase ("after") --------------------------------------------
    const out_ids = try alloc.alloc(VectorId, cfg.k);
    defer alloc.free(out_ids);
    const out_d = try alloc.alloc(f32, cfg.k);
    defer alloc.free(out_d);
    const truth = try alloc.alloc(VectorId, cfg.k);
    defer alloc.free(truth);

    var qstats: shape.QueryStats = .{};
    var recall_sum: f64 = 0;
    c0 = idx.store.counters;
    t0 = monotonicNs();
    var qi: usize = 0;
    while (qi < cfg.queries) : (qi += 1) {
        const cl = rnd.uintLessThan(u32, cfg.clusters);
        const q = try alloc.alloc(f32, dim);
        defer alloc.free(q);
        fillGaussian(q, rnd, centers[cl * dim ..][0..dim], 1.0);
        const n = try idx.query(q, cfg.k, cfg.nprobe, out_ids, out_d, &qstats);
        const tn = bruteforce(data, cfg.vectors, dim, q, cfg.k, truth);
        recall_sum += recallAt(out_ids[0..n], truth[0..tn]);
    }
    const query_ns = monotonicNs() - t0;
    const query_io = shape.IoCounters.sub(idx.store.counters, c0);

    // ---- accounting --------------------------------------------------------
    const build_logical: u64 = @as(u64, cfg.vectors) * logical_vec_bytes;
    const over_logical: u64 = @as(u64, n_over) * logical_vec_bytes;
    const build_phys = build_io.base_write_bytes + build_io.delta_append_bytes;
    const over_phys = over_io.base_write_bytes + over_io.delta_append_bytes;
    const total_write_phys = build_phys + over_phys + repair_io.base_write_bytes + repair_io.delta_append_bytes;
    const total_logical = build_logical + over_logical;
    const resident = idx.store.residentBytes();
    const index_mem = resident + @as(u64, cfg.postings) * dim * 4 + idx.assign.count() * 16;

    try out.print(
        "{{\"cfg\":{{\"vectors\":{d},\"dim\":{d},\"postings\":{d},\"clusters\":{d},\"queries\":{d},\"k\":{d},\"nprobe\":{d},\"overwrite_fraction\":{d:.3},\"repair\":\"{s}\",\"repair_budget\":{d}}},",
        .{ cfg.vectors, cfg.dim, cfg.postings, cfg.clusters, cfg.queries, cfg.k, cfg.nprobe, cfg.overwrite_fraction, @tagName(cfg.repair), budget },
    );
    try out.print(
        "\"build\":{{\"ns\":{d},\"ops_per_sec\":{d:.0},\"write_bytes\":{d},\"base_write_bytes\":{d},\"delta_append_bytes\":{d},\"write_amp\":{d:.3}}},",
        .{ build_ns, opsPerSec(cfg.vectors, build_ns), build_phys, build_io.base_write_bytes, build_io.delta_append_bytes, ratio(build_phys, build_logical) },
    );
    try out.print(
        "\"overwrite\":{{\"ops\":{d},\"ns\":{d},\"ops_per_sec\":{d:.0},\"write_bytes\":{d},\"base_write_bytes\":{d},\"delta_append_bytes\":{d},\"write_amp\":{d:.3},\"dirty_postings\":{d},\"unfolded_deltas\":{d},\"avg_staleness\":{d:.1}}},",
        .{ n_over, over_ns, opsPerSec(n_over, over_ns), over_phys, over_io.base_write_bytes, over_io.delta_append_bytes, ratio(over_phys, over_logical), dirty_before_repair, deltas_before_repair, stale_before_repair },
    );
    try out.print(
        "\"repair\":{{\"ns\":{d},\"folds\":{d},\"folded\":{d},\"fold_input_bytes\":{d},\"base_write_bytes\":{d}}},",
        .{ repair_ns, repair_io.folds, folded, repair_io.fold_input_bytes, repair_io.base_write_bytes },
    );
    try out.print(
        "\"query\":{{\"queries\":{d},\"ns\":{d},\"qps\":{d:.0},\"recall@k\":{d:.4},\"read_bytes\":{d},\"reads\":{d},\"distances\":{d},\"avg_members_scanned\":{d:.0},\"avg_postings_scanned\":{d:.1}}},",
        .{ cfg.queries, query_ns, opsPerSec(cfg.queries, query_ns), recall_sum / @as(f64, @floatFromInt(cfg.queries)), query_io.read_bytes, query_io.reads, qstats.distances, avgf(qstats.members_scanned, cfg.queries), avgf(qstats.postings_scanned, cfg.queries) },
    );
    try out.print(
        "\"totals\":{{\"logical_write_bytes\":{d},\"physical_write_bytes\":{d},\"write_amp\":{d:.3},\"index_resident_bytes\":{d},\"assignments\":{d},\"max_posting_members\":{d},\"avg_posting_members\":{d:.0},\"peak_rss_bytes\":{d}}}}}\n",
        .{ total_logical, total_write_phys, ratio(total_write_phys, total_logical), index_mem, idx.assign.count(), idx.maxPostingCount(), avgf(idx.assign.count(), cfg.postings), maxRssBytes() },
    );
    try stdout_writer.flush();
}

fn monotonicNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn maxRssBytes() u64 {
    const usage = std.posix.getrusage(std.posix.rusage.SELF);
    if (usage.maxrss <= 0) return 0;
    return @as(u64, @intCast(usage.maxrss)) * 1024; // linux reports KiB
}

fn opsPerSec(ops: usize, ns: u64) f64 {
    if (ns == 0) return 0;
    return @as(f64, @floatFromInt(ops)) * 1e9 / @as(f64, @floatFromInt(ns));
}

fn ratio(num: u64, den: u64) f64 {
    if (den == 0) return 0;
    return @as(f64, @floatFromInt(num)) / @as(f64, @floatFromInt(den));
}

fn avgf(total: u64, n: usize) f64 {
    if (n == 0) return 0;
    return @as(f64, @floatFromInt(total)) / @as(f64, @floatFromInt(n));
}

fn bruteforce(data: []const f32, n: usize, dim: u32, q: []const f32, k: usize, truth: []VectorId) usize {
    var dists = [_]f32{std.math.floatMax(f32)} ** 256;
    const kk = @min(k, dists.len);
    var found: usize = 0;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var d: f32 = 0;
        const v = data[i * dim ..][0..dim];
        for (q, v) |a, b| {
            const diff = a - b;
            d += diff * diff;
        }
        if (found >= kk and d >= dists[found - 1]) continue;
        var nn = found;
        if (nn < kk) nn += 1;
        var j: usize = nn - 1;
        while (j > 0 and dists[j - 1] > d) : (j -= 1) {
            dists[j] = dists[j - 1];
            truth[j] = truth[j - 1];
        }
        dists[j] = d;
        truth[j] = @intCast(i);
        found = nn;
    }
    return found;
}

fn recallAt(got: []const VectorId, truth: []const VectorId) f64 {
    if (truth.len == 0) return 1.0;
    var hits: usize = 0;
    for (truth) |t| {
        for (got) |g| {
            if (g == t) {
                hits += 1;
                break;
            }
        }
    }
    return @as(f64, @floatFromInt(hits)) / @as(f64, @floatFromInt(truth.len));
}

fn parseArgs(alloc: Allocator, proc_args: std.process.Args) !Config {
    var cfg = Config{};
    var args = try std.process.Args.Iterator.initAllocator(proc_args, alloc);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--vectors")) {
            cfg.vectors = try nextUsize(&args);
        } else if (std.mem.eql(u8, arg, "--dim")) {
            cfg.dim = @intCast(try nextUsize(&args));
        } else if (std.mem.eql(u8, arg, "--postings")) {
            cfg.postings = @intCast(try nextUsize(&args));
        } else if (std.mem.eql(u8, arg, "--clusters")) {
            cfg.clusters = @intCast(try nextUsize(&args));
        } else if (std.mem.eql(u8, arg, "--queries")) {
            cfg.queries = try nextUsize(&args);
        } else if (std.mem.eql(u8, arg, "--k")) {
            cfg.k = try nextUsize(&args);
        } else if (std.mem.eql(u8, arg, "--nprobe")) {
            cfg.nprobe = try nextUsize(&args);
        } else if (std.mem.eql(u8, arg, "--overwrite-fraction")) {
            cfg.overwrite_fraction = try nextFloat(&args);
        } else if (std.mem.eql(u8, arg, "--repair")) {
            const v = args.next() orelse return error.InvalidArgument;
            cfg.repair = std.meta.stringToEnum(RepairMode, v) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--repair-budget")) {
            cfg.repair_budget = @intCast(try nextUsize(&args));
        } else if (std.mem.eql(u8, arg, "--fold-ratio")) {
            cfg.fold_ratio = @intCast(try nextUsize(&args));
        } else if (std.mem.eql(u8, arg, "--overwrite-mode")) {
            const v = args.next() orelse return error.InvalidArgument;
            cfg.overwrite_mode = std.meta.stringToEnum(OverwriteMode, v) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--overwrite-sigma")) {
            cfg.overwrite_sigma = try nextFloat(&args);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = @intCast(try nextUsize(&args));
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.dim == 0 or cfg.vectors == 0 or cfg.postings == 0 or cfg.clusters == 0) return error.InvalidArgument;
    return cfg;
}

fn nextUsize(args: *std.process.Args.Iterator) !usize {
    const v = args.next() orelse return error.InvalidArgument;
    return std.fmt.parseInt(usize, v, 10);
}

fn nextFloat(args: *std.process.Args.Iterator) !f64 {
    const v = args.next() orelse return error.InvalidArgument;
    return std.fmt.parseFloat(f64, v);
}
