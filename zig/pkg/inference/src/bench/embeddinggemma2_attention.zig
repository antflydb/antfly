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

//! Independent F64 checks of sampled output rows from full 8192-row attention.
const std = @import("std");
const internal = @import("inference_internal");
const factory = internal.architectures.session_factory;
const rows = 8192;
const heads = 4;
const sampled = [_]usize{ 0, 5, 127, 128, 255, 256, 511, 512, 4095, 4096, 8191 };

fn referenceError(a: std.mem.Allocator, q: []const f32, k: []const f32, v: []const f32, actual: []const f32, r: internal.ops.SegmentAttentionGrouped) !f64 {
    const keys = r.visibility.keys;
    const scores = try a.alloc(f64, keys);
    defer a.free(scores);
    const d = r.visibility.head_dim;
    const kv = r.num_kv_heads;
    var worst: f64 = 0;
    for (sampled) |row| {
        if (row >= r.visibility.queries) continue;
        for (0..heads) |head| {
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
        }
    }
    return worst;
}

const Result = struct { head_dim: usize, kv_heads: usize, window: u32, workspace_limit_bytes: usize, max_abs_error: f64, frame_retained_after: u64, scratch_pending_after: u64 };

pub fn main(init: std.process.Init) !void {
    if (comptime !@import("build_options").enable_metal) return error.MetalRequired;
    const a = std.heap.c_allocator;
    const args = try init.minimal.args.toSlice(a);
    defer a.free(args);
    if (args.len == 4 and std.mem.eql(u8, args[3], "--benchmark")) return benchmark(init.io, a, args[1], args[2]);
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

fn now() !u64 {
    var ts: std.posix.timespec = undefined;
    if (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts)) != .SUCCESS) return error.ClockFailed;
    return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec);
}
extern fn termite_metal_host_vm_counters(out: *[3]u64) c_int;
const Timing = struct {
    queries: usize,
    head_dim: usize,
    kv_heads: usize,
    items: usize,
    strategy: []const u8,
    query_tile: usize,
    max_abs_error: f64,
    wall_ns: [10]u64,
    gpu_ns: [10]u64,
    vm_before: [3]u64,
    vm_after: [3]u64,
};

// ABBA-style interleaved candidates, completed GPU fences, VM counters, and
// independent F64 outputs. No model-level inference or HTTP timing is implied.
fn benchmark(io: std.Io, a: std.mem.Allocator, model: []const u8, output: []const u8) !void {
    const session = try factory.createMetalSession(a, model);
    defer session.close();
    var cb = try factory.getComputeBackend(session, a);
    defer cb.deinit();
    const compute: *internal.native_compute.metal.MetalCompute = @ptrCast(@alignCast(cb.ptr));
    const runtime = compute.provider_impl.raw_decode_runtime;
    var device: internal.metal_runtime.MetalDeviceInfo = .{};
    if (internal.metal_runtime.termite_metal_device_info_get(&device) != 0) return error.RuntimeUnavailable;
    var results = std.ArrayList(Timing).empty;
    defer results.deinit(a);
    var random_source = std.Random.DefaultPrng.init(14517);
    const random = random_source.random();
    for ([_]usize{ 1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 512, 2048, 8192 }) |queries| {
        for ([_][2]usize{ .{ 256, 2 }, .{ 512, 1 } }) |shape| {
            for ([_]usize{ 1, 4 }) |items| {
                if (queries % items != 0) continue;
                const d = shape[0];
                const kv = shape[1];
                const q = try a.alloc(f32, queries * heads * d);
                defer a.free(q);
                const k = try a.alloc(f32, queries * kv * d);
                defer a.free(k);
                const v = try a.alloc(f32, k.len);
                defer a.free(v);
                for (q) |*x| x.* = (random.float(f32) - 0.5) * 2;
                for (k) |*x| x.* = (random.float(f32) - 0.5) * 2;
                for (v) |*x| x.* = random.float(f32) - 0.5;
                const ranges = try a.alloc(u32, queries * 6);
                defer a.free(ranges);
                const positions = try a.alloc(i32, queries);
                defer a.free(positions);
                for (0..queries) |row| {
                    const lo = row / (queries / items) * (queries / items);
                    const lower = if (d == 256) @max(lo, row -| 512) else lo;
                    const upper = if (d == 256) @min(lo + queries / items, row + 513) else lo + queries / items;
                    ranges[row * 6 ..][0..6].* = .{ @intCast(lower), @intCast(upper), 0, 0, 0, 0 };
                    positions[row] = @intCast(row - lo);
                }
                const qc = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(q, &.{ @intCast(queries), @intCast(heads * d) }));
                defer cb.free(qc);
                const kc = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(k, &.{ @intCast(queries), @intCast(kv * d) }));
                defer cb.free(kc);
                const vc = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(v, &.{ @intCast(queries), @intCast(kv * d) }));
                defer cb.free(vc);
                const query_at = std.mem.alignForward(usize, ranges.len, 64);
                const key_at = std.mem.alignForward(usize, query_at + positions.len, 64);
                const words = try a.alloc(i32, key_at + positions.len);
                defer a.free(words);
                @memset(words, 0);
                for (ranges, words[0..ranges.len]) |v_range, *word| word.* = @intCast(v_range);
                @memcpy(words[query_at..][0..positions.len], positions);
                @memcpy(words[key_at..], positions);
                const metadata = (try cb.fromInt32Shape(words, &.{@intCast(words.len)})) orelse return error.TypedMetadataUnavailable;
                defer cb.free(metadata);
                const first = results.items.len;
                var candidates: [8]usize = undefined;
                var candidate_count: usize = 0;
                for (0..8) |candidate| {
                    if (candidate == 7 and queries > 128) continue;
                    if (d != 512 and (candidate == 4 or candidate == 5)) continue;
                    if (candidate == 5 and (queries % 16 != 0)) continue;
                    candidates[candidate_count] = candidate;
                    candidate_count += 1;
                }
                for (candidates[0..candidate_count]) |candidate| try results.append(a, .{
                    .queries = queries,
                    .head_dim = d,
                    .kv_heads = kv,
                    .items = items,
                    .strategy = if (candidate == 7) "flash_short" else if (candidate == 6) "automatic" else if (candidate >= 3) (if (candidate == 5) "flash_matrix64" else if (candidate == 4) "flash_matrix32" else "flash") else "gemm",
                    .query_tile = if (candidate == 6) (if (queries <= 512) @as(usize, 512) else @as(usize, 256)) else switch (candidate) {
                        0 => 128,
                        1 => 256,
                        else => 512,
                    },
                    .max_abs_error = 0,
                    .wall_ns = @splat(0),
                    .gpu_ns = @splat(0),
                    .vm_before = @splat(0),
                    .vm_after = @splat(0),
                });
                // Two warm passes, then ten measurements per candidate.
                for (0..12) |iteration| for (0..candidate_count) |order| {
                    const index = if (iteration % 2 == 0) order else candidate_count - 1 - order;
                    const candidate = candidates[index];
                    const result = &results.items[first + index];
                    const request = internal.ops.SegmentAttentionGrouped{
                        .visibility = .{ .ranges = ranges, .query_positions = positions, .key_positions = positions, .queries = queries, .keys = queries, .num_heads = heads, .head_dim = d, .window = if (d == 256) 512 else std.math.maxInt(u32) },
                        .metadata = metadata,
                        .num_kv_heads = kv,
                        .score_scale = 1,
                        .metal_strategy = if (candidate == 6) .automatic else if (candidate >= 3) .flash else .gemm,
                        .metal_query_tile = if (candidate == 6) null else result.query_tile,
                        .metal_key_block = if (candidate == 6) null else if (candidate == 7) 1 else if (candidate == 5) 64 else if (candidate == 4) 32 else 8,
                    };
                    if (iteration == 2 and termite_metal_host_vm_counters(&result.vm_before) != 0) return error.VmCountersUnavailable;
                    const start = try now();
                    if (!try cb.decoderRuntimeBeginFrame()) return error.FrameUnavailable;
                    var active = true;
                    defer if (active) cb.decoderRuntimeCancelFrame() catch {};
                    const out = (try cb.vtable.segmentAttentionGrouped.?(cb.ptr, qc, kc, vc, &request)) orelse return error.AttentionUnsupported;
                    defer cb.free(out);
                    try cb.decoderRuntimeSubmitAndWaitFrame();
                    active = false;
                    const elapsed = try now() - start;
                    if (iteration >= 2) {
                        result.wall_ns[iteration - 2] = elapsed;
                        result.gpu_ns[iteration - 2] = internal.metal_runtime.lastFrameGpuNanos(runtime);
                        if (result.gpu_ns[iteration - 2] == 0) return error.ClockFailed;
                    }
                    if (iteration == 11 and termite_metal_host_vm_counters(&result.vm_after) != 0) return error.VmCountersUnavailable;
                    if (iteration == 1) {
                        const actual = try cb.toFloat32(out, a);
                        defer a.free(actual);
                        result.max_abs_error = try referenceError(a, q, k, v, actual, request);
                        if (result.max_abs_error > 2e-5) return error.AttentionParityFailed;
                    }
                    const memory = internal.metal_runtime.runtimeMemorySnapshot(runtime);
                    if (memory.frame_retained_bytes != 0 or memory.scratch_pool_pending_slots != 0) return error.AttentionFrameNotDrained;
                };
                std.debug.print("attention_benchmark: queries={d} D={d} items={d} done\n", .{ queries, d, items });
            }
        }
    }
    const json = try std.json.Stringify.valueAlloc(a, .{ .schema_version = 1, .status = "correctness_pass", .timing_scope = "completed_attention_only", .device = device, .production_qualified = false, .warmups = 2, .iterations = 10, .results = results.items }, .{ .whitespace = .indent_2 });
    defer a.free(json);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = output, .data = json });
}
