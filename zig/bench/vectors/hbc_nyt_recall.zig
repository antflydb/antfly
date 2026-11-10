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

//! Real-HBC recall versus candidate-heap size on an ann-benchmarks style
//! dataset (PREFIX.train.f32 / PREFIX.test.f32: u32 rows, u32 dims, f32s).
//!
//!   zig build hbc-nyt-recall -- --data PREFIX [--n 100000] [--queries 200]
//!
//! HBC admits candidates by 1-bit RaBitQ estimates into a heap of
//! k * rerank_factor before exact rerank. If recall keeps rising as the heap
//! grows at a fixed search width, the 1-bit admission is losing true
//! neighbors, which multi-bit codes (antfly_vector.rabitq_ex) would recover.

const std = @import("std");
const antfly = @import("antfly-zig");
const hbc = antfly.hbc;
const search_types = antfly.vectorindex.search_types;
const platform_time = antfly.platform_time;

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
    var it = std.process.Args.Iterator.init(init.minimal.args);
    _ = it.skip();
    while (it.next()) |flag| {
        const v = it.next() orelse return error.MissingValue;
        if (std.mem.eql(u8, flag, "--data")) prefix = v else if (std.mem.eql(u8, flag, "--n")) n = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, flag, "--queries")) nq = try std.fmt.parseInt(usize, v, 10) else return error.UnknownFlag;
    }
    const data = try loadMatrix(alloc, io, prefix orelse return error.NeedData, "train", n);
    defer alloc.free(data.data);
    const queries = try loadMatrix(alloc, io, prefix.?, "test", nq);
    defer alloc.free(queries.data);
    const d = data.dims;
    for (0..data.rows) |i| normalize(data.data[i * d ..][0..d]);
    for (0..queries.rows) |i| normalize(queries.data[i * d ..][0..d]);

    var path_buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrintZ(&path_buf, "/tmp/antfly-hbc-nyt-{d}", .{platform_time.monotonicNs()});
    try std.Io.Dir.cwd().createDirPath(io, path);
    defer std.Io.Dir.cwd().deleteTree(io, path) catch {};

    // Production defaults from the index manager's DenseConfig.
    var idx = try hbc.HBCIndex.open(alloc, path.ptr, .{
        .dims = @intCast(d),
        .metric = .cosine,
        .split_algo = .kmeans,
        .branching_factor = 7 * 24,
        .leaf_size = 7 * 24,
        .search_width = 2 * 3 * 7 * 24,
        .epsilon = 7,
        .use_quantization = true,
        .rerank_policy = .boundary,
        .quantizer_seed = 42,
        .use_random_ortho_trans = false,
        .max_cached_nodes = 100_000,
        .max_cached_vectors = 200_000,
    });
    defer idx.close();
    const items = try alloc.alloc(hbc.BatchInsertItem, data.rows);
    defer {
        for (items) |item| alloc.free(item.metadata);
        alloc.free(items);
    }
    for (items, 0..) |*item, i| item.* = .{
        .vector_id = @intCast(i + 1),
        .vector = data.data[i * d ..][0..d],
        .metadata = try std.fmt.allocPrint(alloc, "d{d}", .{i + 1}),
    };
    const t0 = platform_time.monotonicNs();
    try idx.bulkBuildWithMetadata(items);
    try out.print("HBC bulk build: rows={d} dims={d} in {d:.1}s\n", .{ data.rows, d, @as(f64, @floatFromInt(platform_time.monotonicNs() - t0)) / 1e9 });
    try out.flush();

    const k = 10;
    const truth = try alloc.alloc(u32, queries.rows * k);
    defer alloc.free(truth);
    for (0..queries.rows) |qi| topK(data.data, d, queries.data[qi * d ..][0..d], truth[qi * k ..][0..k]);

    try out.print("{s:>13} {s:>13} {s:>9} {s:>9}\n", .{ "search_width", "rerank_factor", "recall@10", "ms/query" });
    for ([_]u32{ 168, 1008 }) |width| {
        for ([_]usize{ 2, 4, 8, 16, 32, 64 }) |factor| {
            var hits: usize = 0;
            const ts = platform_time.monotonicNs();
            for (0..queries.rows) |qi| {
                var res = try idx.searchWithRequest(.{
                    .query = queries.data[qi * d ..][0..d],
                    .k = k,
                    .search_width = width,
                    .rerank_factor = factor,
                    .load_metadata = false,
                });
                defer res.deinit();
                const gt = truth[qi * k ..][0..k];
                for (res.getHits()) |h| {
                    if (std.mem.indexOfScalar(u32, gt, @intCast(h.vector_id - 1)) != null) hits += 1;
                }
            }
            const ms = @as(f64, @floatFromInt(platform_time.monotonicNs() - ts)) / 1e6 / @as(f64, @floatFromInt(queries.rows));
            try out.print("{d:>13} {d:>13} {d:>9.3} {d:>9.2}\n", .{ width, factor, @as(f64, @floatFromInt(hits)) / @as(f64, @floatFromInt(queries.rows * k)), ms });
            try out.flush();
        }
    }
}

const Matrix = struct { rows: usize, dims: usize, data: []f32 };

fn topK(data: []const f32, d: usize, q: []const f32, ids: []u32) void {
    var dists: [64]f32 = undefined;
    const k = ids.len;
    @memset(dists[0..k], std.math.inf(f32));
    @memset(ids, 0);
    var i: usize = 0;
    while (i * d < data.len) : (i += 1) {
        var dot: f32 = 0;
        for (data[i * d ..][0..d], q) |a, b| dot += a * b;
        const dd = 1 - dot;
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

fn normalize(v: []f32) void {
    var s: f32 = 0;
    for (v) |x| s += x * x;
    const nn = @sqrt(s);
    if (nn == 0) return;
    for (v) |*x| x.* /= nn;
}

fn loadMatrix(alloc: std.mem.Allocator, io: std.Io, prefix: []const u8, part: []const u8, limit: usize) !Matrix {
    const p = try std.fmt.allocPrint(alloc, "{s}.{s}.f32", .{ prefix, part });
    defer alloc.free(p);
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, p, alloc, .limited(8 * 1024 * 1024 * 1024));
    defer alloc.free(raw);
    if (raw.len < 8) return error.InvalidData;
    const rows_all = std.mem.readInt(u32, raw[0..4], .little);
    const dims = std.mem.readInt(u32, raw[4..8], .little);
    const rows = @min(rows_all, limit);
    const out = try alloc.alloc(f32, rows * dims);
    @memcpy(std.mem.sliceAsBytes(out), raw[8..][0 .. out.len * 4]);
    return .{ .rows = rows, .dims = dims, .data = out };
}
