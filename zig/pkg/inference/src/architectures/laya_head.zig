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

//! Laya's pre-norm TransformerEncoder (ReLU), marker scorer (GELU), and action head.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const Config = @import("../models/laya.zig").Config;
const Tensor = @import("../backends/tensor.zig").Tensor;
const CB = ops.ComputeBackend;
const CT = ops.CT;

fn weight(cb: *const CB, prefix: []const u8, suffix: []const u8) !CT {
    var buf: [256]u8 = undefined;
    return cb.getWeight(try std.fmt.bufPrint(&buf, "model.{s}.{s}", .{ prefix, suffix }));
}
fn linear(cb: *const CB, x: CT, prefix: []const u8, rows: usize, input: usize, output: usize) !CT {
    const w = try weight(cb, prefix, "weight");
    defer cb.free(w);
    const b = try weight(cb, prefix, "bias");
    defer cb.free(b);
    return cb.linear(x, w, b, rows, input, output);
}
fn norm(cb: *const CB, x: CT, prefix: []const u8, dim: usize) !CT {
    const w = try weight(cb, prefix, "weight");
    defer cb.free(w);
    const b = try weight(cb, prefix, "bias");
    defer cb.free(b);
    return cb.layerNorm(x, w, b, dim, 1e-5);
}
fn exactGelu(cb: *const CB, x: CT) !CT {
    return (try cb.geluExact(x)) orelse return error.UnsupportedLayaBackend;
}

pub fn forward(cb: *const CB, a: std.mem.Allocator, cfg: Config, encoder: CT, mask: []const i64, kinds: []const i64, markers: []const i64, batch: usize, seq: usize, count: usize, dim: usize) ![]Tensor {
    if (batch == 0 or seq == 0 or mask.len != batch * seq or count < 2 or count > 20 or dim < 64 or dim % 64 != 0 or seq > cfg.max_len or kinds.len != batch or markers.len != batch * count) return error.InvalidLayaInputs;
    for (kinds) |kind| if (kind < 0 or kind > 2) return error.InvalidLayaInputs;
    for (0..batch) |row| {
        var valid: usize = 0;
        for (markers[row * count ..][0..count]) |pos| {
            if (pos == -1) continue;
            if (pos < 0 or pos >= seq or mask[row * seq + @as(usize, @intCast(pos))] != 1) return error.InvalidLayaInputs;
            valid += 1;
        }
        if (valid < 2) return error.InvalidLayaInputs;
    }
    const hidden = try transform(cb, a, cfg, encoder, mask, kinds, batch, seq, dim);
    defer cb.free(hidden);
    if (cb.kind() == .cuda) return forwardCudaTail(cb, a, cfg, hidden, markers, batch, seq, count, dim);
    const host = try cb.toFloat32(hidden, a);
    defer a.free(host);
    if (host.len != batch * seq * dim) return error.UnexpectedOutputShape;
    const gathered = try a.alloc(f32, batch * count * dim);
    defer a.free(gathered);
    for (markers, 0..) |pos, i| {
        const row = i / count;
        const offset = (row * seq + @as(usize, @intCast(@max(pos, 0)))) * dim;
        @memcpy(gathered[i * dim ..][0..dim], host[offset..][0..dim]);
    }
    const m = try cb.fromFloat32Shape(gathered, &.{ @intCast(batch * count), @intCast(dim) });
    defer cb.free(m);
    const n = try norm(cb, m, "scorer.0", dim);
    defer cb.free(n);
    const s1 = try linear(cb, n, "scorer.1", batch * count, dim, dim);
    defer cb.free(s1);
    const sg = try exactGelu(cb, s1);
    defer cb.free(sg);
    const s2 = try linear(cb, sg, "scorer.3", batch * count, dim, 1);
    defer cb.free(s2);
    const logits = try cb.toFloat32(s2, a);
    defer a.free(logits);
    if (logits.len != batch * count) return error.UnexpectedOutputShape;
    for (markers, logits) |pos, *logit| if (pos < 0) {
        logit.* = -1e4;
    };
    const features = try a.alloc(f32, batch * (dim + 4));
    defer a.free(features);
    for (0..batch) |row| {
        const dst = features[row * (dim + 4) ..][0 .. dim + 4];
        @memcpy(dst[0..dim], host[row * seq * dim ..][0..dim]);
        const z = logits[row * count ..][0..count];
        var max: f32 = -std.math.inf(f32);
        var valid: usize = 0;
        for (z, markers[row * count ..][0..count]) |value, pos| {
            max = @max(max, value);
            valid += @intFromBool(pos >= 0);
        }
        var sum: f32 = 0;
        for (z) |value| {
            sum += @exp(value - max);
        }
        var first: f32 = 0;
        var second: f32 = 0;
        var entropy: f32 = 0;
        for (z) |value| {
            const p = @exp(value - max) / sum;
            entropy -= p * @log(@max(p, 1e-9));
            if (p > first) {
                second = first;
                first = p;
            } else {
                second = @max(second, p);
            }
        }
        dst[dim] = first;
        dst[dim + 1] = first - second;
        dst[dim + 2] = entropy / @log(@as(f32, @floatFromInt(valid)));
        dst[dim + 3] = @as(f32, @floatFromInt(valid)) / 255;
    }
    const f = try cb.fromFloat32Shape(features, &.{ @intCast(batch), @intCast(dim + 4) });
    defer cb.free(f);
    const act1 = try linear(cb, f, "act_head.0", batch, dim + 4, 256);
    defer cb.free(act1);
    const actg = try exactGelu(cb, act1);
    defer cb.free(actg);
    const act2 = try linear(cb, actg, "act_head.2", batch, 256, cfg.n_act);
    defer cb.free(act2);
    const act = try cb.toFloat32(act2, a);
    defer a.free(act);
    var result = try a.alloc(Tensor, 2);
    errdefer a.free(result);
    result[0] = try Tensor.initFloat32(a, "logits", &.{ @intCast(batch), @intCast(count) }, logits);
    errdefer result[0].deinit();
    result[1] = try Tensor.initFloat32(a, "action_logits", &.{ @intCast(batch), @intCast(cfg.n_act) }, act);
    return result;
}

fn forwardCudaTail(cb: *const CB, a: std.mem.Allocator, cfg: Config, hidden: CT, markers: []const i64, batch: usize, seq: usize, count: usize, dim: usize) ![]Tensor {
    const indices = try a.alloc(i64, markers.len);
    defer a.free(indices);
    for (markers, indices, 0..) |pos, *index, i| index.* = @intCast((i / count) * seq + @as(usize, @intCast(@max(pos, 0))));
    const gathered = try cb.embeddingLookup(hidden, indices, batch * count, dim);
    defer cb.free(gathered);
    const normalized = try norm(cb, gathered, "scorer.0", dim);
    defer cb.free(normalized);
    const s1 = try linear(cb, normalized, "scorer.1", batch * count, dim, dim);
    defer cb.free(s1);
    const sg = try exactGelu(cb, s1);
    defer cb.free(sg);
    const scores = try linear(cb, sg, "scorer.3", batch * count, dim, 1);
    defer cb.free(scores);
    const features = (try cb.layaActionFeatures(&.{ .hidden = hidden, .logits = scores, .markers = markers, .batch = batch, .sequence = seq, .options = count, .hidden_size = dim })) orelse return error.UnsupportedLayaBackend;
    defer cb.free(features);
    const act1 = try linear(cb, features, "act_head.0", batch, dim + 4, 256);
    defer cb.free(act1);
    const actg = try exactGelu(cb, act1);
    defer cb.free(actg);
    const actions = try linear(cb, actg, "act_head.2", batch, 256, cfg.n_act);
    defer cb.free(actions);
    try cb.checkExecutionControl();
    // These are the only CUDA tensor readbacks in the complete Laya forward.
    const logits = try cb.toFloat32(scores, a);
    defer a.free(logits);
    const acts = try cb.toFloat32(actions, a);
    defer a.free(acts);
    if (logits.len != batch * count or acts.len != batch * cfg.n_act) return error.UnexpectedOutputShape;
    for (markers, logits) |pos, *logit| if (pos < 0) {
        logit.* = -1e4;
    };
    const result = try a.alloc(Tensor, 2);
    errdefer a.free(result);
    result[0] = try Tensor.initFloat32(a, "logits", &.{ @intCast(batch), @intCast(count) }, logits);
    errdefer result[0].deinit();
    result[1] = try Tensor.initFloat32(a, "action_logits", &.{ @intCast(batch), @intCast(cfg.n_act) }, acts);
    return result;
}

/// Resident TransformerEncoder portion, also used by intermediate parity tests.
pub fn transform(cb: *const CB, a: std.mem.Allocator, cfg: Config, encoder: CT, mask: []const i64, kinds: []const i64, batch: usize, seq: usize, dim: usize) !CT {
    const repeated = try a.alloc(i64, batch * seq);
    defer a.free(repeated);
    for (kinds, 0..) |kind, row| @memset(repeated[row * seq ..][0..seq], kind);
    const type_weight = try cb.getWeight("model.type_emb.weight");
    defer cb.free(type_weight);
    const types = try cb.embeddingLookup(type_weight, repeated, batch * seq, dim);
    defer cb.free(types);
    var hidden = try cb.add(encoder, types);
    errdefer cb.free(hidden);
    for (0..cfg.head_layers) |layer| {
        try cb.checkExecutionControl();
        var name: [128]u8 = undefined;
        const prefix = try std.fmt.bufPrint(&name, "head.layers.{d}", .{layer});
        var buf: [160]u8 = undefined;
        const n1 = try norm(cb, hidden, try std.fmt.bufPrint(&buf, "{s}.norm1", .{prefix}), dim);
        defer cb.free(n1);
        const qw = try weight(cb, prefix, "self_attn.in_proj_weight");
        defer cb.free(qw);
        const qb = try weight(cb, prefix, "self_attn.in_proj_bias");
        defer cb.free(qb);
        const qkv = try cb.linear(n1, qw, qb, batch * seq, dim, dim * 3);
        defer cb.free(qkv);
        const q = try cb.sliceLastDim(qkv, 0, dim);
        defer cb.free(q);
        const k = try cb.sliceLastDim(qkv, dim, dim * 2);
        defer cb.free(k);
        const v = try cb.sliceLastDim(qkv, dim * 2, dim * 3);
        defer cb.free(v);
        const attn = try cb.scaledDotProductAttention(q, k, v, mask, null, batch, seq, dim / 64, 64);
        defer cb.free(attn);
        const proj = try linear(cb, attn, try std.fmt.bufPrint(&buf, "{s}.self_attn.out_proj", .{prefix}), batch * seq, dim, dim);
        defer cb.free(proj);
        const residual = try cb.add(hidden, proj);
        defer cb.free(residual);
        const n2 = try norm(cb, residual, try std.fmt.bufPrint(&buf, "{s}.norm2", .{prefix}), dim);
        defer cb.free(n2);
        const up = try linear(cb, n2, try std.fmt.bufPrint(&buf, "{s}.linear1", .{prefix}), batch * seq, dim, dim * 4);
        defer cb.free(up);
        const relu = try cb.relu(up);
        defer cb.free(relu);
        const down = try linear(cb, relu, try std.fmt.bufPrint(&buf, "{s}.linear2", .{prefix}), batch * seq, dim * 4, dim);
        defer cb.free(down);
        const next = try cb.add(residual, down);
        cb.free(hidden);
        hidden = next;
    }
    return hidden;
}
