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
const turboquant = @import("../runtime/kv/turboquant.zig");

const Config = struct {
    samples: usize = 4096,
    num_kv_heads: u32 = 8,
    head_dim: u32 = 128,
    scale_min: f32 = -2.0,
    scale_max: f32 = 2.0,
    scale_step: f32 = 0.125,
};

const Metrics = struct {
    count: usize = 0,
    sum_abs: f64 = 0.0,
    sum_sq: f64 = 0.0,
    max_abs: f32 = 0.0,

    fn add(self: *Metrics, exact: f32, estimate: f32) void {
        const diff = @abs(exact - estimate);
        self.count += 1;
        self.sum_abs += diff;
        self.sum_sq += @as(f64, diff) * @as(f64, diff);
        self.max_abs = @max(self.max_abs, diff);
    }

    fn mae(self: Metrics) f64 {
        return self.sum_abs / @as(f64, @floatFromInt(self.count));
    }

    fn rmse(self: Metrics) f64 {
        return @sqrt(self.sum_sq / @as(f64, @floatFromInt(self.count)));
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    const cfg = try parseArgs(init);
    if (!turboquant.isSupportedHeadDim(cfg.head_dim)) return error.UnsupportedKvHeadDim;
    if (cfg.samples == 0 or cfg.num_kv_heads == 0) return error.InvalidBenchShape;
    if (cfg.scale_step <= 0.0 or cfg.scale_max < cfg.scale_min) return error.InvalidScaleSweep;

    const token_values = @as(usize, cfg.num_kv_heads) * cfg.head_dim;
    const key = try allocator.alloc(f32, token_values);
    defer allocator.free(key);
    const query = try allocator.alloc(f32, cfg.head_dim);
    defer allocator.free(query);
    const polar4_key = try allocator.alloc(u8, turboquant.polar4KeyBytes(cfg.num_kv_heads, cfg.head_dim));
    defer allocator.free(polar4_key);
    const turbo3_key = try allocator.alloc(u8, turboquant.turbo3KeyBytes(cfg.num_kv_heads, cfg.head_dim));
    defer allocator.free(turbo3_key);
    const turbo3_residual = try allocator.alloc(u8, turboquant.turbo3ResidualBytes(cfg.num_kv_heads, cfg.head_dim));
    defer allocator.free(turbo3_residual);

    var polar4_metrics = Metrics{};
    var turbo3_base_metrics = Metrics{};

    var scale_count: usize = 0;
    var scale = cfg.scale_min;
    while (scale <= cfg.scale_max + 0.00001) : (scale += cfg.scale_step) scale_count += 1;

    const residual_metrics = try allocator.alloc(Metrics, scale_count);
    defer allocator.free(residual_metrics);
    @memset(residual_metrics, Metrics{});

    for (0..cfg.samples) |sample| {
        fillKey(key, sample);
        try turboquant.encodePolar4Key(key, polar4_key, cfg.num_kv_heads, cfg.head_dim);
        try turboquant.encodeTurbo3Key(key, turbo3_key, cfg.num_kv_heads, cfg.head_dim);
        try turboquant.encodeTurbo3ResidualSketch(key, turbo3_key, turbo3_residual, cfg.num_kv_heads, cfg.head_dim);

        for (0..@as(usize, cfg.num_kv_heads)) |head| {
            fillQuery(query, sample, head);
            const exact = exactDot(query, key[head * cfg.head_dim ..][0..cfg.head_dim]);
            const polar4_dot = try turboquant.dotPolar4KeyFast(query, polar4_key, cfg.num_kv_heads, cfg.head_dim, head);
            const turbo3_dot = try turboquant.dotTurbo3KeyFast(query, turbo3_key, cfg.num_kv_heads, cfg.head_dim, head);
            const residual_dot = try turboquant.dotTurbo3ResidualSketch(query, turbo3_residual, cfg.num_kv_heads, cfg.head_dim, head);

            polar4_metrics.add(exact, polar4_dot);
            turbo3_base_metrics.add(exact, turbo3_dot);

            scale = cfg.scale_min;
            for (residual_metrics) |*metrics| {
                metrics.add(exact, turbo3_dot + scale * residual_dot);
                scale += cfg.scale_step;
            }
        }
    }

    var best_idx: usize = 0;
    for (residual_metrics, 0..) |metrics, i| {
        if (metrics.rmse() < residual_metrics[best_idx].rmse()) best_idx = i;
    }
    const best_scale = cfg.scale_min + cfg.scale_step * @as(f32, @floatFromInt(best_idx));
    const best = residual_metrics[best_idx];

    std.debug.print(
        \\turboquant_distortion=true
        \\samples={}
        \\num_kv_heads={}
        \\head_dim={}
        \\dots={}
        \\polar4_mae={d:.6}
        \\polar4_rmse={d:.6}
        \\polar4_max_abs={d:.6}
        \\turbo3_base_mae={d:.6}
        \\turbo3_base_rmse={d:.6}
        \\turbo3_base_max_abs={d:.6}
        \\turbo3_residual_best_scale={d:.6}
        \\turbo3_residual_best_mae={d:.6}
        \\turbo3_residual_best_rmse={d:.6}
        \\turbo3_residual_best_max_abs={d:.6}
        \\
    , .{
        cfg.samples,
        cfg.num_kv_heads,
        cfg.head_dim,
        polar4_metrics.count,
        polar4_metrics.mae(),
        polar4_metrics.rmse(),
        polar4_metrics.max_abs,
        turbo3_base_metrics.mae(),
        turbo3_base_metrics.rmse(),
        turbo3_base_metrics.max_abs,
        best_scale,
        best.mae(),
        best.rmse(),
        best.max_abs,
    });

    std.debug.print("scale_sweep=scale,mae,rmse,max_abs\n", .{});
    scale = cfg.scale_min;
    for (residual_metrics) |metrics| {
        std.debug.print("scale_row scale={d:.6} mae={d:.6} rmse={d:.6} max_abs={d:.6}\n", .{
            scale,
            metrics.mae(),
            metrics.rmse(),
            metrics.max_abs,
        });
        scale += cfg.scale_step;
    }
}

fn parseArgs(init: std.process.Init) !Config {
    var cfg = Config{};
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    var args_buf: [64][]const u8 = undefined;
    var args_len: usize = 0;
    while (args_iter.next()) |arg| {
        if (args_len < args_buf.len) {
            args_buf[args_len] = arg;
            args_len += 1;
        }
    }
    const args = args_buf[0..args_len];

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--samples")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.samples = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--num-kv-heads")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.num_kv_heads = try std.fmt.parseInt(u32, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--head-dim")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.head_dim = try std.fmt.parseInt(u32, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--scale-min")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.scale_min = try std.fmt.parseFloat(f32, args[i]);
        } else if (std.mem.eql(u8, arg, "--scale-max")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.scale_max = try std.fmt.parseFloat(f32, args[i]);
        } else if (std.mem.eql(u8, arg, "--scale-step")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.scale_step = try std.fmt.parseFloat(f32, args[i]);
        } else if (std.mem.eql(u8, arg, "--help")) {
            printUsage();
            std.process.exit(0);
        } else {
            return error.UnknownArgument;
        }
    }
    return cfg;
}

fn printUsage() void {
    std.debug.print(
        \\Usage: termite-turboquant-distortion-bench [options]
        \\  --samples N
        \\  --num-kv-heads N
        \\  --head-dim 64|128
        \\  --scale-min V
        \\  --scale-max V
        \\  --scale-step V
        \\
    , .{});
}

fn fillKey(dst: []f32, sample: usize) void {
    for (dst, 0..) |*value, i| {
        value.* = sampleValue(sample, i, 0x1234abcd);
    }
}

fn fillQuery(dst: []f32, sample: usize, head: usize) void {
    for (dst, 0..) |*value, i| {
        value.* = sampleValue(sample +% head *% 131, i, 0xfeedface);
    }
}

fn exactDot(query: []const f32, key: []const f32) f32 {
    var sum: f32 = 0.0;
    for (query, key) |q, k| sum += q * k;
    return sum;
}

fn sampleValue(sample: usize, index: usize, seed: u64) f32 {
    const mixed = splitmix64(seed ^ (@as(u64, sample + 1) *% 0x9e3779b97f4a7c15) ^ (@as(u64, index + 1) *% 0xbf58476d1ce4e5b9));
    const u = @as(f32, @floatFromInt(mixed >> 40)) / @as(f32, @floatFromInt(@as(u64, 1) << 24));
    return u * 2.0 - 1.0;
}

fn splitmix64(input: u64) u64 {
    var x = input +% 0x9e3779b97f4a7c15;
    x = (x ^ (x >> 30)) *% 0xbf58476d1ce4e5b9;
    x = (x ^ (x >> 27)) *% 0x94d049bb133111eb;
    return x ^ (x >> 31);
}
