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

//! Small concrete CUDA harness: avoids the full model/session vtable so kernel
//! and library scheduling changes can be checked without a full server rebuild.
const std = @import("std");
const Context = @import("ops/cuda/context.zig").CudaContext;
const Buffer = @import("ops/cuda/buffer.zig").DeviceBuffer;
const Encoder = @import("ops/cuda/embedding_gemma2.zig");
const Blas = @import("ops/cuda/cublaslt.zig").CublasLt;
const Global = @import("ops/cuda/embedding_gemma2_global.zig");
const A = std.heap.c_allocator;
fn bf16(x: f32) u16 {
    const bits: u32 = @bitCast(x);
    return @truncate((bits + 0x7fff + ((bits >> 16) & 1)) >> 16);
}
fn float(x: u16) f32 {
    return @bitCast(@as(u32, x) << 16);
}
fn upload(ctx: *Context, bytes: []const u8) !Buffer {
    const result = try Buffer.alloc(ctx, bytes.len);
    errdefer {
        var b = result;
        b.free(ctx);
    }
    try result.copyFromHost(ctx, bytes);
    try ctx.synchronize();
    return result;
}
fn median(x: []f64) f64 {
    std.mem.sort(f64, x, {}, std.sort.asc(f64));
    return x[x.len / 2];
}
const Result = struct { json: []u8, digest: [32]u8 };
const Job = struct { n: usize, tile: usize, mask: []const u8, result: ?Result = null, err: ?anyerror = null };
extern "c" fn pthread_create(thread: *usize, attr: ?*const anyopaque, entry: *const fn (?*anyopaque) callconv(.c) ?*anyopaque, data: ?*anyopaque) c_int;
extern "c" fn pthread_join(thread: usize, result: ?*?*anyopaque) c_int;
fn worker(data: ?*anyopaque) callconv(.c) ?*anyopaque {
    const job: *Job = @ptrCast(@alignCast(data.?));
    job.result = runCase(job.n, job.tile, job.mask) catch |err| {
        job.err = err;
        return null;
    };
    return null;
}
pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(A);
    defer A.free(args);
    if (args.len < 4 or args.len > 6) return error.ExpectedSequenceTileOutputMaskWorkers;
    const n = try std.fmt.parseInt(usize, args[1], 10);
    const tile = try std.fmt.parseInt(usize, args[2], 10);
    const mask_kind = if (args.len >= 5) args[4] else "none";
    const workers = if (args.len == 6) try std.fmt.parseInt(usize, args[5], 10) else 1;
    if (!std.mem.eql(u8, mask_kind, "none") and !std.mem.eql(u8, mask_kind, "holes") and !std.mem.eql(u8, mask_kind, "all")) return error.InvalidMask;
    if (workers == 1) {
        const result = try runCase(n, tile, mask_kind);
        defer A.free(result.json);
        try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = args[3], .data = result.json });
    } else if (workers == 2) {
        var jobs = [_]Job{ .{ .n = n, .tile = tile, .mask = mask_kind }, .{ .n = n, .tile = tile, .mask = mask_kind } };
        defer for (&jobs) |job| {
            if (job.result) |result| A.free(result.json);
        };
        var threads: [2]usize = undefined;
        var started: usize = 0;
        errdefer for (threads[0..started]) |thread| {
            _ = pthread_join(thread, null);
        };
        for (&jobs, &threads) |*job, *thread| {
            if (pthread_create(thread, null, worker, job) != 0) return error.ThreadCreationFailed;
            started += 1;
        }
        for (threads) |thread| {
            if (pthread_join(thread, null) != 0) return error.ThreadJoinFailed;
        }
        started = 0;
        for (jobs) |job| if (job.err) |err| return err;
        if (!std.mem.eql(u8, &jobs[0].result.?.digest, &jobs[1].result.?.digest)) return error.ConcurrentOutputDrift;
        const first = try std.json.parseFromSlice(std.json.Value, A, jobs[0].result.?.json, .{});
        defer first.deinit();
        const second = try std.json.parseFromSlice(std.json.Value, A, jobs[1].result.?.json, .{});
        defer second.deinit();
        const report = try std.json.Stringify.valueAlloc(A, .{ .workers = 2, .bitwise_equal = true, .runs = .{ first.value, second.value } }, .{ .whitespace = .indent_2 });
        defer A.free(report);
        try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = args[3], .data = report });
    } else return error.InvalidWorkerCount;
}
fn runCase(n: usize, tile: usize, mask_kind: []const u8) !Result {
    const layout = try Global.Layout.init(n, tile);
    var ctx = try Context.initDefault();
    defer ctx.deinit();
    var module = try Encoder.Module.init(&ctx);
    defer module.deinit(&ctx);
    var blas = try Blas.openWithAllocator(A, &ctx);
    defer blas.deinit();
    blas.attention_plan_cache_enabled = true;
    const hq = try A.alloc(u16, n * 4 * 512);
    defer A.free(hq);
    const hk = try A.alloc(u16, n * 512);
    defer A.free(hk);
    const hv = try A.alloc(u16, n * 512);
    defer A.free(hv);
    const hm = try A.alloc(i64, n);
    defer A.free(hm);
    var rng = std.Random.DefaultPrng.init(33);
    const random = rng.random();
    const scale = std.math.pow(f32, 512, -0.25);
    for (hq) |*x| x.* = bf16((random.float(f32) * 2 - 1) * 2 * scale);
    for (hk) |*x| x.* = bf16((random.float(f32) * 2 - 1) * 2 * scale);
    for (hv) |*x| x.* = bf16(random.float(f32) * 2 - 1);
    for (hm, 0..) |*x, i| x.* = if (std.mem.eql(u8, mask_kind, "all") or (std.mem.eql(u8, mask_kind, "holes") and i % 7 == 0)) 0 else 1;
    var q = try upload(&ctx, std.mem.sliceAsBytes(hq));
    defer q.free(&ctx);
    var k = try upload(&ctx, std.mem.sliceAsBytes(hk));
    defer k.free(&ctx);
    var v = try upload(&ctx, std.mem.sliceAsBytes(hv));
    defer v.free(&ctx);
    var mask = try upload(&ctx, std.mem.sliceAsBytes(hm));
    defer mask.free(&ctx);
    var output = try Buffer.alloc(&ctx, hq.len * 2);
    defer output.free(&ctx);
    var scratch = try Buffer.alloc(&ctx, layout.total_bytes);
    defer scratch.free(&ctx);
    var workspace = try Buffer.alloc(&ctx, (@import("antfly_platform").env.getenvUsize("ANTFLY_GLOBAL_BENCH_WORKSPACE_MB") orelse 0) * 1024 * 1024);
    defer workspace.free(&ctx);
    const reference = try A.alloc(u16, hq.len);
    defer A.free(reference);
    const actual = try A.alloc(u16, hq.len);
    defer A.free(actual);
    const shape = Encoder.Attention{ .batch = 1, .seq = @intCast(n), .q_heads = 4, .kv_heads = 1, .head_dim = 512, .local_radius = 0 };
    var times: [2][9]f64 = undefined;
    for (0..2) |mode| {
        for (0..12) |rep| {
            const pair = try ctx.beginProfileEventPair();
            if (mode == 0) try module.launchLocalFlashAttention(&ctx, output, q, k, v, mask, shape) else try Global.execute(&ctx, &module, &blas, output, q, k, v, mask, n, scratch, workspace, tile, null);
            const us = try ctx.endProfileEventPairUs(pair);
            if (rep >= 3) times[mode][rep - 3] = @as(f64, @floatFromInt(us)) / 1000;
        }
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(if (mode == 0) reference else actual));
        try ctx.synchronize();
    }
    var dot: f64 = 0;
    var na: f64 = 0;
    var nb: f64 = 0;
    var max_abs: f64 = 0;
    for (reference, actual) |x, y| {
        const a: f64 = float(x);
        const b: f64 = float(y);
        if (!std.math.isFinite(b)) return error.NonFiniteOutput;
        dot += a * b;
        na += a * a;
        nb += b * b;
        max_abs = @max(max_abs, @abs(a - b));
    }
    const cosine = if (na == 0 and nb == 0) @as(f64, 1) else dot / @sqrt(na * nb);
    if (!std.math.isFinite(cosine) or cosine < 0.9998) return error.VectorMismatch;
    // Repeat after all plans are cached, and compare every output bit.
    for (0..6) |_| {
        try Global.execute(&ctx, &module, &blas, output, q, k, v, mask, n, scratch, workspace, tile, null);
        const repeated = try A.alloc(u16, hq.len);
        defer A.free(repeated);
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(repeated));
        try ctx.synchronize();
        if (!std.mem.eql(u16, repeated, actual)) return error.NonDeterministicOutput;
    }
    var cpu_max_abs: f64 = 0;
    const cpu_scores = try A.alloc(f64, n);
    defer A.free(cpu_scores);
    for ([_]usize{ 0, @min(n - 1, n / 3 + 1), @min(n - 1, n / 2 + 1), n - 1 }) |query| {
        for (0..4) |head| {
            var maximum = -std.math.inf(f64);
            for (0..n) |key| {
                var score: f64 = 0;
                if (hm[query] != 0 and hm[key] != 0) {
                    for (0..512) |d| score += @as(f64, float(hq[(query * 4 + head) * 512 + d])) * @as(f64, float(hk[key * 512 + d]));
                } else score = -std.math.inf(f64);
                cpu_scores[key] = score;
                maximum = @max(maximum, score);
            }
            var denominator: f64 = 0;
            for (cpu_scores) |*score| {
                score.* = if (std.math.isFinite(score.*)) @exp(score.* - maximum) else 0;
                denominator += score.*;
            }
            if (denominator > 0) for (cpu_scores) |*probability| {
                probability.* /= denominator;
            };
            for (0..512) |d| {
                var expected: f64 = 0;
                if (denominator > 0) for (cpu_scores, 0..) |probability, key| {
                    expected += probability * @as(f64, float(hv[key * 512 + d]));
                };
                cpu_max_abs = @max(cpu_max_abs, @abs(expected - @as(f64, float(actual[(query * 4 + head) * 512 + d]))));
            }
        }
    }
    if (cpu_max_abs > 0.01) return error.CpuOracleMismatch;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(std.mem.sliceAsBytes(actual), &digest, .{});
    const hash = std.fmt.bytesToHex(digest, .lower);
    const report = try std.json.Stringify.valueAlloc(A, .{ .sequence = n, .tile = tile, .mask = mask_kind, .old_ms = median(&times[0]), .new_ms = median(&times[1]), .cosine = cosine, .max_abs = max_abs, .cpu_max_abs = cpu_max_abs, .scratch_bytes = layout.total_bytes, .workspace_bytes = workspace.len, .sha256 = hash, .plan_hits = blas.attention_plan_cache_hits, .plan_count = blas.attention_plans.count() }, .{ .whitespace = .indent_2 });
    std.debug.print("global MQA sequence={d} tile={d} old={d} new={d} cosine={d} cpu_max_abs={d}\n", .{ n, tile, median(&times[0]), median(&times[1]), cosine, cpu_max_abs });
    return .{ .json = report, .digest = digest };
}
