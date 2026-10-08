// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Independent F64 checks of sampled output rows from full 8192-row attention.
const std = @import("std");
const internal = @import("inference_internal");
const factory = internal.architectures.session_factory;
const rows = 8192;
const heads = 4;
const sampled = [_]usize{ 0, 5, 127, 128, 255, 256, 511, 512, 4095, 4096, 8191 };

fn referenceError(a: std.mem.Allocator, q: []const f32, k: []const f32, v: []const f32, actual: []const f32, r: internal.ops.SegmentAttentionGrouped) !f64 {
    const scores = try a.alloc(f64, rows);
    defer a.free(scores);
    const d = r.visibility.head_dim;
    const kv = r.num_kv_heads;
    var worst: f64 = 0;
    for (sampled) |row| for (0..heads) |head| {
        const kh = head / (heads / kv);
        var best: f64 = -std.math.inf(f64);
        for (scores, 0..) |*score, key| {
            const ranges = r.visibility.ranges[row * 6 ..][0..6];
            const visible = (key >= ranges[0] and key < ranges[1]) or (key >= ranges[2] and key < ranges[3]) or (key >= ranges[4] and key < ranges[5]);
            const distance = @abs(@as(i64, r.visibility.query_positions[row]) - r.visibility.key_positions[key]);
            score.* = -std.math.inf(f64);
            if (!visible or distance > r.visibility.window) continue;
            var dot: f64 = 0;
            for (0..d) |col| dot += @as(f64, q[(row * heads + head) * d + col]) * k[(key * kv + kh) * d + col];
            score.* = dot * r.score_scale;
            best = @max(best, score.*);
        }
        var denominator: f64 = 0;
        for (scores) |*score| {
            score.* = if (score.* == -std.math.inf(f64)) 0 else @exp(score.* - best);
            denominator += score.*;
        }
        for (0..d) |col| {
            var expected: f64 = 0;
            if (denominator > 0) for (scores, 0..) |score, key| {
                expected += score / denominator * v[(key * kv + kh) * d + col];
            };
            const value = actual[(row * heads + head) * d + col];
            if (!std.math.isFinite(value)) return error.NonfiniteAttentionOutput;
            worst = @max(worst, @abs(expected - value));
        }
    };
    return worst;
}

const Result = struct { head_dim: usize, kv_heads: usize, window: u32, workspace_limit_bytes: usize, max_abs_error: f64, frame_retained_after: u64, scratch_pending_after: u64 };

pub fn main(init: std.process.Init) !void {
    if (comptime !@import("build_options").enable_metal) return error.MetalRequired;
    const a = std.heap.c_allocator;
    const args = try init.minimal.args.toSlice(a);
    defer a.free(args);
    if (args.len != 3) return error.ExpectedModelAndOutput;
    const session = try factory.createMetalSession(a, args[1]);
    defer session.close();
    var cb = try factory.getComputeBackend(session, a);
    defer cb.deinit();
    var results = std.ArrayList(Result).empty;
    defer results.deinit(a);
    var rng = std.Random.DefaultPrng.init(81137);
    const random = rng.random();
    const ranges = try a.alloc(u32, rows * 6);
    defer a.free(ranges);
    const positions = try a.alloc(i32, rows);
    defer a.free(positions);
    for (0..rows) |row| {
        const lo: u32 = if (row < rows / 2) 0 else rows / 2;
        ranges[row * 6 ..][0..6].* = .{ lo, lo + 1024, lo + 1025, lo + 3072, lo + 3073, lo + 4096 };
        positions[row] = @intCast(row % (rows / 2));
    }
    @memset(ranges[5 * 6 ..][0..6], 0);
    for ([_][2]usize{ .{ 256, 2 }, .{ 512, 1 } }) |shape| {
        const d = shape[0];
        const kv = shape[1];
        const q = try a.alloc(f32, rows * heads * d);
        defer a.free(q);
        const k = try a.alloc(f32, rows * kv * d);
        defer a.free(k);
        const v = try a.alloc(f32, rows * kv * d);
        defer a.free(v);
        for (q) |*value| value.* = (random.float(f32) - 0.5) * 2;
        for (k) |*value| value.* = (random.float(f32) - 0.5) * 2;
        for (v) |*value| value.* = random.float(f32) - 0.5;
        const qc = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(q, &.{ rows, @intCast(heads * d) }));
        defer cb.free(qc);
        const kc = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(k, &.{ rows, @intCast(kv * d) }));
        defer cb.free(kc);
        const vc = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(v, &.{ rows, @intCast(kv * d) }));
        defer cb.free(vc);
        for ([_]usize{ 0, 256 * 1024 * 1024 }) |limit| {
            const window: u32 = if (d == 256) 512 else std.math.maxInt(u32);
            const request = internal.ops.SegmentAttentionGrouped{ .visibility = .{ .ranges = ranges, .query_positions = positions, .key_positions = positions, .queries = rows, .keys = rows, .num_heads = heads, .head_dim = d, .window = window }, .num_kv_heads = kv, .score_scale = 1, .workspace_limit_bytes = limit };
            const out = (try cb.vtable.segmentAttentionGrouped.?(cb.ptr, qc, kc, vc, &request)) orelse return error.AttentionUnsupported;
            defer cb.free(out);
            const actual = try cb.toFloat32(out, a);
            defer a.free(actual);
            const error_max = try referenceError(a, q, k, v, actual, request);
            std.debug.print("independent8192: D={d} KV={d} limit={d} max_abs={d}\n", .{ d, kv, limit, error_max });
            if (error_max > 2e-5) return error.AttentionParityFailed;
            const compute: *internal.native_compute.metal.MetalCompute = @ptrCast(@alignCast(cb.ptr));
            const memory = internal.metal_runtime.runtimeMemorySnapshot(compute.provider_impl.raw_decode_runtime);
            if (memory.frame_retained_bytes != 0 or memory.scratch_pool_pending_slots != 0) return error.AttentionFrameNotDrained;
            try results.append(a, .{ .head_dim = d, .kv_heads = kv, .window = window, .workspace_limit_bytes = limit, .max_abs_error = error_max, .frame_retained_after = memory.frame_retained_bytes, .scratch_pending_after = memory.scratch_pool_pending_slots });
        }
    }
    const json = try std.json.Stringify.valueAlloc(a, .{ .status = "pass", .rows = rows, .heads = heads, .sampled_rows = sampled, .max_error_allowed = 2e-5, .results = results.items }, .{ .whitespace = .indent_2 });
    defer a.free(json);
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = args[2], .data = json });
}
