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

const std = @import("std");
const Params = @import("ops.zig").Gemma4AudioLocalAttentionParams;

/// Chunk-local relative attention. Workers own disjoint output rows and score
/// scratch; no allocation occurs after launch. Generic SIMD works on every CPU.
pub fn forward(a: std.mem.Allocator, io: ?std.Io, p: Params, q: []const f32, k: []const f32, v: []const f32, rel: []const f32, scales: []const f32, valid: []const f32) ![]f32 {
    if (p.heads == 0 or p.head_dim == 0 or p.chunk == 0 or p.context_left <= @intFromBool(p.exclude_farthest_key) or p.logit_cap <= 0 or
        p.heads * p.head_dim != p.hidden or q.len != p.rows * p.hidden or k.len != q.len or v.len != q.len or
        rel.len != p.context_left * p.hidden or scales.len != p.head_dim or valid.len != p.rows) return error.InvalidTensorShape;
    const out = try a.alloc(f32, q.len);
    errdefer a.free(out);
    @memset(out, 0);
    const count = if (io != null and p.rows >= 32) @min(@as(usize, 4), p.rows) else 1;
    const scratch = try a.alloc(f32, count * p.context);
    defer a.free(scratch);
    var jobs: [4]Job = undefined;
    for (jobs[0..count], 0..) |*job, index| job.* = .{
        .p = p,
        .q = q,
        .k = k,
        .v = v,
        .rel = rel,
        .scales = scales,
        .valid = valid,
        .out = out,
        .scores = scratch[index * p.context ..][0..p.context],
        .first = p.rows * index / count,
        .last = p.rows * (index + 1) / count,
        .io = io,
    };
    if (count == 1) {
        jobs[0].run();
    } else {
        const runtime = io.?;
        var group: std.Io.Group = .init;
        defer group.cancel(runtime);
        for (jobs[0..count]) |*job| group.async(runtime, Job.run, .{job});
        try group.await(runtime);
    }
    for (jobs[0..count]) |job| if (job.failure) |err| return err;
    return out;
}

const Job = struct {
    p: Params,
    q: []const f32,
    k: []const f32,
    v: []const f32,
    rel: []const f32,
    scales: []const f32,
    valid: []const f32,
    out: []f32,
    scores: []f32,
    first: usize,
    last: usize,
    io: ?std.Io,
    failure: ?anyerror = null,
    fn run(self: *Job) void {
        self.execute() catch |err| {
            self.failure = err;
        };
    }
    fn execute(self: *Job) !void {
        const p = self.p;
        const past = p.context_left - 1;
        for (self.first..self.last) |row| {
            if (self.io) |io| try io.checkCancel();
            if (self.valid[row] == 0) continue;
            const block = row / p.chunk * p.chunk;
            const offset = row - block;
            // Intersect the legal chunk-relative, causal and visible key ranges
            // once per query rather than branching for every head/dimension.
            const first = @max(offset, past -| block);
            const last = @min(p.context, @min(offset + p.context_left, (p.rows - block) + past));
            for (0..p.heads) |head| {
                const qb = row * p.hidden + head * p.head_dim;
                @memset(self.scores, p.invalid_value);
                var maximum: f32 = -std.math.inf(f32);
                var keys: usize = 0;
                for (first..@max(first, last)) |c| {
                    const key = block + c - past;
                    if (key > row or row - key > past - @intFromBool(p.exclude_farthest_key) or self.valid[key] == 0) continue;
                    const kb = key * p.hidden + head * p.head_dim;
                    const rb = (c - offset) * p.hidden + head * p.head_dim;
                    const score = dot(self.q[qb..][0..p.head_dim], self.k[kb..][0..p.head_dim], self.rel[rb..][0..p.head_dim], self.scales, p.k_scale);
                    const capped = std.math.tanh(score / p.logit_cap) * p.logit_cap;
                    self.scores[c] = capped;
                    maximum = @max(maximum, capped);
                    keys += 1;
                }
                if (keys == 0) continue;
                var sum: f32 = 0;
                for (self.scores) |*score| {
                    score.* = @exp(score.* - maximum);
                    sum += score.*;
                }
                if (sum == 0) continue;
                const dst = self.out[qb..][0..p.head_dim];
                for (first..@max(first, last)) |c| {
                    const probability = self.scores[c] / sum;
                    if (probability == 0) continue;
                    const key = block + c - past;
                    if (key > row or row - key > past or self.valid[key] == 0) continue;
                    axpy(dst, self.v[key * p.hidden + head * p.head_dim ..][0..p.head_dim], probability);
                }
            }
        }
    }
};

fn dot(q: []const f32, k: []const f32, rel: []const f32, scales: []const f32, scale: f32) f32 {
    const V = @Vector(8, f32);
    var sums: V = @splat(0);
    var d: usize = 0;
    while (d + 8 <= q.len) : (d += 8) {
        const scaled = @as(V, q[d..][0..8].*) * @as(V, scales[d..][0..8].*);
        sums += scaled * (@as(V, k[d..][0..8].*) * @as(V, @splat(scale)));
        sums += scaled * @as(V, rel[d..][0..8].*);
    }
    var result = @reduce(.Add, sums);
    while (d < q.len) : (d += 1) {
        result += q[d] * scales[d] * (k[d] * scale);
        result += q[d] * scales[d] * rel[d];
    }
    return result;
}
fn axpy(dst: []f32, src: []const f32, scale: f32) void {
    const V = @Vector(8, f32);
    var d: usize = 0;
    while (d + 8 <= dst.len) : (d += 8) dst[d..][0..8].* = @as(V, dst[d..][0..8].*) + @as(V, src[d..][0..8].*) * @as(V, @splat(scale));
    while (d < dst.len) : (d += 1) dst[d] += src[d] * scale;
}
