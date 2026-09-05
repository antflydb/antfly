// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

const std = @import("std");
const antfly = @import("antfly-zig");

const vector = antfly.vector;
const vector_block = antfly.vectorindex.vector_block;
const platform_time = antfly.platform_time;

const Config = struct {
    vectors: usize = 4096,
    dims: usize = 768,
    samples: usize = 20,
};

fn baselineEncodeFloat16(source: []const f32, out: []f16) f32 {
    var max_abs: f32 = 0;
    for (source) |value| max_abs = @max(max_abs, @abs(value));
    const candidate_scale = if (max_abs > 65_000) max_abs / 65_000 else 1;
    for (source, out) |value, *encoded| encoded.* = @floatCast(value / candidate_scale);
    return candidate_scale;
}

fn parseArgs(proc_args: std.process.Args) !Config {
    var cfg: Config = .{};
    var args = std.process.Args.Iterator.init(proc_args);
    _ = args.skip();
    while (args.next()) |arg| {
        const value = args.next() orelse return error.InvalidArgument;
        const parsed = try std.fmt.parseInt(usize, value, 10);
        if (std.mem.eql(u8, arg, "--vectors")) cfg.vectors = parsed else if (std.mem.eql(u8, arg, "--dims")) cfg.dims = parsed else if (std.mem.eql(u8, arg, "--samples")) cfg.samples = parsed else return error.InvalidArgument;
    }
    if (cfg.vectors == 0 or cfg.dims == 0 or cfg.samples == 0) return error.InvalidArgument;
    return cfg;
}

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);
    const component_count = try std.math.mul(usize, cfg.vectors, cfg.dims);
    const sources = try alloc.alloc(f32, component_count);
    defer alloc.free(sources);
    const encoded = try alloc.alignedAlloc(f16, .of(f16), component_count);
    defer alloc.free(encoded);
    const baseline = try alloc.alignedAlloc(f16, .of(f16), component_count);
    defer alloc.free(baseline);
    const stats = try alloc.alloc(vector_block.QuantizationStats, cfg.vectors);
    defer alloc.free(stats);
    const scales = try alloc.alloc(f32, cfg.vectors);
    defer alloc.free(scales);
    const residual_stride = try vector_block.exactResidualMaxBytes(cfg.dims);
    const residuals = try alloc.alloc(u8, try std.math.mul(usize, cfg.vectors, residual_stride));
    defer alloc.free(residuals);
    const residual_lens = try alloc.alloc(usize, cfg.vectors);
    defer alloc.free(residual_lens);
    const exact_scratch = try alloc.alloc(f32, cfg.dims);
    defer alloc.free(exact_scratch);
    const query = try alloc.alloc(f32, cfg.dims);
    defer alloc.free(query);

    var state: u64 = 0x9e3779b97f4a7c15;
    for (sources) |*value| {
        state = state *% 6364136223846793005 +% 1442695040888963407;
        value.* = (@as(f32, @floatFromInt(state >> 40)) / 8_388_608.0 - 1.0) * 10;
    }
    for (query) |*value| {
        state = state *% 6364136223846793005 +% 1442695040888963407;
        value.* = (@as(f32, @floatFromInt(state >> 40)) / 8_388_608.0 - 1.0) * 10;
    }

    var baseline_encode_ns: u64 = 0;
    var metadata_encode_ns: u64 = 0;
    var residual_encode_ns: u64 = 0;
    for (0..cfg.samples) |_| {
        var start = platform_time.monotonicNs();
        for (0..cfg.vectors) |i| {
            const offset = i * cfg.dims;
            scales[i] = baselineEncodeFloat16(sources[offset..][0..cfg.dims], baseline[offset..][0..cfg.dims]);
        }
        baseline_encode_ns += platform_time.monotonicNs() - start;
        std.mem.doNotOptimizeAway(baseline.ptr);

        start = platform_time.monotonicNs();
        for (0..cfg.vectors) |i| {
            const offset = i * cfg.dims;
            const result = try vector_block.encodeVectorIntoWithStats(
                .float16,
                sources[offset..][0..cfg.dims],
                std.mem.sliceAsBytes(encoded[offset..][0..cfg.dims]),
            );
            scales[i] = result.scale;
            stats[i] = result.quantization;
        }
        metadata_encode_ns += platform_time.monotonicNs() - start;
        std.mem.doNotOptimizeAway(stats.ptr);

        start = platform_time.monotonicNs();
        for (0..cfg.vectors) |i| {
            const offset = i * cfg.dims;
            residual_lens[i] = try vector_block.encodeExactResidualInto(
                sources[offset..][0..cfg.dims],
                std.mem.sliceAsBytes(encoded[offset..][0..cfg.dims]),
                scales[i],
                residuals[i * residual_stride ..][0..residual_stride],
            );
        }
        residual_encode_ns += platform_time.monotonicNs() - start;
        std.mem.doNotOptimizeAway(residuals.ptr);
    }

    const query_norm = vector.norm(query);
    var legacy_query_ns: u64 = 0;
    var metadata_query_ns: u64 = 0;
    var exact_decode_ns: u64 = 0;
    var checksum: f32 = 0;
    for (0..cfg.samples) |_| {
        var start = platform_time.monotonicNs();
        for (0..cfg.vectors) |i| {
            const offset = i * cfg.dims;
            const bounded = vector.distanceToQueryF16BoundedWithQueryNorm(
                query,
                0,
                query_norm,
                encoded[offset..][0..cfg.dims],
                scales[i],
                .inner_product,
            );
            checksum += bounded.distance + bounded.error_bound;
        }
        legacy_query_ns += platform_time.monotonicNs() - start;
        std.mem.doNotOptimizeAway(checksum);

        start = platform_time.monotonicNs();
        for (0..cfg.vectors) |i| {
            const offset = i * cfg.dims;
            const distance = vector.distanceToQueryF16(query, 0, encoded[offset..][0..cfg.dims], scales[i], .inner_product);
            const bounded = vector.boundedDistanceFromProjectionMetadata(
                distance,
                query_norm,
                stats[i].error_norm,
                stats[i].decoded_norm_lower_bound,
                .inner_product,
            );
            checksum += bounded.distance + bounded.error_bound;
        }
        metadata_query_ns += platform_time.monotonicNs() - start;
        std.mem.doNotOptimizeAway(checksum);

        start = platform_time.monotonicNs();
        for (0..cfg.vectors) |i| {
            const offset = i * cfg.dims;
            const decoded = try vector_block.decodeExactResidualInto(
                std.mem.sliceAsBytes(encoded[offset..][0..cfg.dims]),
                cfg.dims,
                scales[i],
                residuals[i * residual_stride ..][0..residual_lens[i]],
                exact_scratch,
            );
            checksum += vector.distanceToQuery(query, 0, decoded, .inner_product);
        }
        exact_decode_ns += platform_time.monotonicNs() - start;
        std.mem.doNotOptimizeAway(checksum);
    }

    var residual_bytes: usize = 0;
    for (residual_lens) |len| residual_bytes += len;
    for (0..cfg.vectors) |i| {
        const offset = i * cfg.dims;
        const decoded = try vector_block.decodeExactResidualInto(
            std.mem.sliceAsBytes(encoded[offset..][0..cfg.dims]),
            cfg.dims,
            scales[i],
            residuals[i * residual_stride ..][0..residual_lens[i]],
            exact_scratch,
        );
        for (sources[offset..][0..cfg.dims], decoded) |expected, found| {
            if (@as(u32, @bitCast(expected)) != @as(u32, @bitCast(found))) return error.ExactResidualMismatch;
        }
    }

    const operations: f64 = @floatFromInt(cfg.vectors * cfg.samples);
    const baseline_encode_per_vector = @as(f64, @floatFromInt(baseline_encode_ns)) / operations;
    const metadata_encode_per_vector = @as(f64, @floatFromInt(metadata_encode_ns)) / operations;
    const residual_encode_per_vector = @as(f64, @floatFromInt(residual_encode_ns)) / operations;
    const legacy_query_per_vector = @as(f64, @floatFromInt(legacy_query_ns)) / operations;
    const metadata_query_per_vector = @as(f64, @floatFromInt(metadata_query_ns)) / operations;
    const exact_decode_per_vector = @as(f64, @floatFromInt(exact_decode_ns)) / operations;
    const residual_bytes_per_vector = @as(f64, @floatFromInt(residual_bytes)) / @as(f64, @floatFromInt(cfg.vectors));
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;
    try out.print(
        "vector projection bounds vectors={d} dims={d} samples={d}\n" ++
            "encode baseline_ns_per_vector={d:.2} metadata_ns_per_vector={d:.2} ratio={d:.3}\n" ++
            "residual encode_ns_per_vector={d:.2} bytes_per_vector={d:.2} bytes_per_dimension={d:.3}\n" ++
            "query legacy_scan_ns_per_vector={d:.2} metadata_ns_per_vector={d:.2} speedup={d:.3}\n" ++
            "exact residual_decode_and_float32_score_ns_per_vector={d:.2}\n",
        .{
            cfg.vectors,
            cfg.dims,
            cfg.samples,
            baseline_encode_per_vector,
            metadata_encode_per_vector,
            metadata_encode_per_vector / baseline_encode_per_vector,
            residual_encode_per_vector,
            residual_bytes_per_vector,
            residual_bytes_per_vector / @as(f64, @floatFromInt(cfg.dims)),
            legacy_query_per_vector,
            metadata_query_per_vector,
            legacy_query_per_vector / metadata_query_per_vector,
            exact_decode_per_vector,
        },
    );
    try out.flush();
}
