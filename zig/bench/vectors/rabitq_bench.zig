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
const platform_time = antfly.platform_time;
const vec = @import("antfly_vector").vector;
const quantizer_mod = @import("antfly_vector").quantizer;
const proto = @import("antfly_vector").proto;
const rabitq = @import("antfly_vector").rabitq;

const BenchConfig = struct {
    dims: usize = 128,
    count: usize = 256,
    repeats: usize = 1000,
    seed: u64 = 42,
};

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const cfg = try parseArgs(init.minimal.args);

    var rng = std.Random.Pcg.init(cfg.seed);
    const random = rng.random();

    const width = rabitq.codeWidth(cfg.dims);

    const code = try alloc.alloc(u64, width);
    defer alloc.free(code);
    const q1 = try alloc.alloc(u64, width);
    defer alloc.free(q1);
    const q2 = try alloc.alloc(u64, width);
    defer alloc.free(q2);
    const q3 = try alloc.alloc(u64, width);
    defer alloc.free(q3);
    const q4 = try alloc.alloc(u64, width);
    defer alloc.free(q4);

    for (code) |*v| v.* = random.int(u64);
    for (q1) |*v| v.* = random.int(u64);
    for (q2) |*v| v.* = random.int(u64);
    for (q3) |*v| v.* = random.int(u64);
    for (q4) |*v| v.* = random.int(u64);

    const bit_start = monotonicTime();
    var bit_accum: u32 = 0;
    for (0..cfg.repeats) |_| {
        bit_accum +%= rabitq.bitProduct(code, q1, q2, q3, q4);
    }
    const bit_end = monotonicTime();
    const bit_ns = diffNs(bit_start, bit_end);

    const centroid = try alloc.alloc(f32, cfg.dims);
    defer alloc.free(centroid);
    const vectors = try alloc.alloc(f32, cfg.count * cfg.dims);
    defer alloc.free(vectors);
    const query = try alloc.alloc(f32, cfg.dims);
    defer alloc.free(query);

    fillRandomVector(random, centroid);
    fillRandomUnitVectors(random, cfg.dims, cfg.count, vectors);
    fillRandomVector(random, query);

    var q = try quantizer_mod.RaBitQuantizer.init(alloc, cfg.dims, cfg.seed, .l2_squared);
    defer q.deinit();

    var qs = try q.quantize(centroid, vectors, cfg.count);
    defer qs.deinit(alloc);

    const distances = try alloc.alloc(f32, cfg.count);
    defer alloc.free(distances);
    const error_bounds = try alloc.alloc(f32, cfg.count);
    defer alloc.free(error_bounds);
    var scratch = try quantizer_mod.RaBitQuantizer.EstimateScratch.init(alloc, cfg.dims);
    defer scratch.deinit(alloc);

    const est_start = monotonicTime();
    for (0..cfg.repeats) |_| {
        try q.estimateDistancesWithScratch(&qs, query, distances, error_bounds, &scratch);
    }
    const est_end = monotonicTime();
    const est_ns = diffNs(est_start, est_end);

    std.mem.doNotOptimizeAway(bit_accum);
    std.mem.doNotOptimizeAway(distances[0]);
    std.debug.print(
        "RaBitQ bench dims={d} count={d} repeats={d} width={d}\nbit_product={d:.3}ns estimate={d:.3}us\n",
        .{
            cfg.dims,
            cfg.count,
            cfg.repeats,
            width,
            @as(f64, @floatFromInt(bit_ns)) / @as(f64, @floatFromInt(cfg.repeats)),
            (@as(f64, @floatFromInt(est_ns)) / @as(f64, @floatFromInt(cfg.repeats))) / 1e3,
        },
    );
}

fn parseArgs(args_in: std.process.Args) !BenchConfig {
    var cfg = BenchConfig{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--dims")) {
            cfg.dims = try parseNextUsize(&args, "--dims");
        } else if (std.mem.eql(u8, arg, "--count")) {
            cfg.count = try parseNextUsize(&args, "--count");
        } else if (std.mem.eql(u8, arg, "--repeats")) {
            cfg.repeats = try parseNextUsize(&args, "--repeats");
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = try parseNextU64(&args, "--seed");
        } else {
            return error.InvalidArgument;
        }
    }
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const value = args.next() orelse {
        std.log.err("missing value for {s}", .{flag});
        return error.InvalidArgument;
    };
    return std.fmt.parseInt(usize, value, 10);
}

fn parseNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const value = args.next() orelse {
        std.log.err("missing value for {s}", .{flag});
        return error.InvalidArgument;
    };
    return std.fmt.parseInt(u64, value, 10);
}

fn fillRandomVector(random: anytype, dst: []f32) void {
    for (dst) |*v| v.* = random.float(f32) * 2.0 - 1.0;
}

fn fillRandomUnitVectors(random: anytype, dims: usize, count: usize, out: []f32) void {
    for (0..count) |i| {
        const slice = out[i * dims ..][0..dims];
        fillRandomVector(random, slice);
        _ = vec.normalize(slice);
    }
}

fn monotonicTime() u64 {
    return platform_time.monotonicNs();
}

fn diffNs(start: u64, end: u64) u64 {
    return end - start;
}
