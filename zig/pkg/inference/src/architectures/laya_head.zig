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
const modern = @import("modern_bert.zig");
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
    const tokens = try a.alloc(i64, markers.len);
    defer a.free(tokens);
    for (markers, tokens, 0..) |pos, *token, i| token.* = if (pos < 0) -1 else @intCast((i / count) * seq + @as(usize, @intCast(pos)));
    const anchors = try a.alloc(usize, batch);
    defer a.free(anchors);
    for (anchors, 0..) |*anchor, row| anchor.* = row * seq;
    return scoreHost(cb, a, cfg, host, tokens, anchors, count, dim);
}

/// Score `anchors.len` decisions from host hidden states `[tokens, dim]`.
/// `markers` holds `[decisions * count]` token indices into `host`, -1 padded;
/// each decision's action features read the hidden state at its anchor.
fn scoreHost(cb: *const CB, a: std.mem.Allocator, cfg: Config, host: []const f32, markers: []const i64, anchors: []const usize, count: usize, dim: usize) ![]Tensor {
    const batch = anchors.len;
    if (markers.len != batch * count) return error.UnexpectedOutputShape;
    const gathered = try a.alloc(f32, batch * count * dim);
    defer a.free(gathered);
    for (markers, 0..) |token, i| {
        const offset = @as(usize, @intCast(@max(token, 0))) * dim;
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
        @memcpy(dst[0..dim], host[anchors[row] * dim ..][0..dim]);
        actionStats(logits[row * count ..][0..count], markers[row * count ..][0..count], dst[dim..][0..4]);
    }
    const f = try cb.fromFloat32Shape(features, &.{ @intCast(batch), @intCast(dim + 4) });
    defer cb.free(f);
    return actionHead(cb, a, cfg, f, logits, batch, count, dim);
}

/// `scoreHost` with the hidden states left on the device: marker and anchor
/// rows are gathered there, so only the scorer logits (for the action
/// statistics) and the action logits are read back. `markers` indexes rows
/// of `hidden`, -1 padded.
fn scoreDevice(cb: *const CB, a: std.mem.Allocator, cfg: Config, hidden: CT, markers: []const i64, anchors: []const i64, count: usize, dim: usize) ![]Tensor {
    const batch = anchors.len;
    if (markers.len != batch * count) return error.UnexpectedOutputShape;
    const rows = try a.alloc(i64, markers.len);
    defer a.free(rows);
    for (markers, rows) |marker, *row| row.* = @max(marker, 0);
    const gathered = try cb.embeddingLookup(hidden, rows, rows.len, dim);
    defer cb.free(gathered);
    const n = try norm(cb, gathered, "scorer.0", dim);
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
    const stats = try a.alloc(f32, batch * 4);
    defer a.free(stats);
    for (0..batch) |row| actionStats(logits[row * count ..][0..count], markers[row * count ..][0..count], stats[row * 4 ..][0..4]);
    const anchored = try cb.embeddingLookup(hidden, anchors, batch, dim);
    defer cb.free(anchored);
    const stats_ct = try cb.fromFloat32Shape(stats, &.{ @intCast(batch), 4 });
    defer cb.free(stats_ct);
    const f = try cb.concat(anchored, stats_ct, batch, dim, 4);
    defer cb.free(f);
    return actionHead(cb, a, cfg, f, logits, batch, count, dim);
}

/// Top probability, margin, normalized entropy and option count of one
/// decision's (masked) scorer logits; the action head's non-hidden features.
fn actionStats(z: []const f32, markers: []const i64, dst: *[4]f32) void {
    var max: f32 = -std.math.inf(f32);
    var valid: usize = 0;
    for (z, markers) |value, pos| {
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
    dst[0] = first;
    dst[1] = first - second;
    dst[2] = entropy / @log(@as(f32, @floatFromInt(valid)));
    dst[3] = @as(f32, @floatFromInt(valid)) / 255;
}

/// Action logits from `[batch, dim + 4]` features, returned with `logits`.
fn actionHead(cb: *const CB, a: std.mem.Allocator, cfg: Config, f: CT, logits: []const f32, batch: usize, count: usize, dim: usize) ![]Tensor {
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

/// Tree-packed decision head for one row (pipelines/laya_tree.zig). `encoder`
/// is `[seq, dim]`; `segments` gives each row's visible key ranges.
/// mask. Trunk tokens (`kinds` = -1) receive no question-type embedding.
/// Returns logits `[questions, width]` and action logits `[questions, n_act]`.
pub fn forwardPacked(cb: *const CB, a: std.mem.Allocator, cfg: Config, encoder: CT, segments: modern.Packed, kinds: []const i64, markers: []const i64, anchors: []const i64, width: usize, dim: usize) ![]Tensor {
    return packedHead(cb, a, cfg, encoder, segments, kinds, markers, anchors, width, dim, null);
}

/// `forwardPacked` for the branch rows of a row whose trunk keys and values
/// are cached (`prefix`). `encoder` and `kinds` cover the branch rows only;
/// `markers` and `anchors` stay row-global; `segments` covers the branch rows.
pub fn forwardPackedBranches(cb: *const CB, a: std.mem.Allocator, cfg: Config, encoder: CT, segments: modern.Packed, kinds: []const i64, markers: []const i64, anchors: []const i64, width: usize, dim: usize, prefix: Prefix) ![]Tensor {
    const local_markers = try a.alloc(i64, markers.len);
    defer a.free(local_markers);
    for (markers, local_markers) |marker, *local| {
        if (marker >= 0 and marker < prefix.rows) return error.InvalidLayaInputs;
        local.* = if (marker < 0) -1 else marker - @as(i64, @intCast(prefix.rows));
    }
    const local_anchors = try a.alloc(i64, anchors.len);
    defer a.free(local_anchors);
    for (anchors, local_anchors) |anchor, *local| {
        if (anchor < prefix.rows) return error.InvalidLayaInputs;
        local.* = anchor - @as(i64, @intCast(prefix.rows));
    }
    return packedHead(cb, a, cfg, encoder, segments, kinds, local_markers, local_anchors, width, dim, prefix);
}

/// Run the head layers over trunk-only encoder rows (`[rows, dim]`, no type
/// embedding, full visibility) and copy each layer's keys and values.
pub fn captureTrunk(cb: *const CB, a: std.mem.Allocator, cfg: Config, encoder: CT, rows: usize, dim: usize, capture: Capture) !void {
    if (@max(capture.keys.len, capture.key_tensors.len) != cfg.head_layers or @max(capture.values.len, capture.value_tensors.len) != cfg.head_layers) return error.InvalidLayaInputs;
    const zeros = try a.alloc(f32, rows * dim);
    defer a.free(zeros);
    @memset(zeros, 0);
    const zero_ct = try cb.fromFloat32Shape(zeros, &.{ @intCast(rows), @intCast(dim) });
    defer cb.free(zero_ct);
    const mask = try a.alloc(i64, rows);
    defer a.free(mask);
    @memset(mask, 1);
    const hidden = try layers(cb, a, cfg, try cb.add(encoder, zero_ct), mask, null, 1, rows, dim, null, capture);
    cb.free(hidden);
}

/// Per-token question-type embeddings `[rows, dim]`; trunk tokens (kind -1)
/// get zeros. Gathered on the backend from `[type_emb; 0]`.
fn packedTypes(cb: *const CB, a: std.mem.Allocator, kinds: []const i64, dim: usize) !CT {
    const type_weight = try cb.getWeight("model.type_emb.weight");
    defer cb.free(type_weight);
    const zeros = try a.alloc(f32, dim);
    defer a.free(zeros);
    @memset(zeros, 0);
    const zero_row = try cb.fromFloat32Shape(zeros, &.{ 1, @intCast(dim) });
    defer cb.free(zero_row);
    // An in-stream join; Metal's axis-0 concat blits outside the ordered
    // stream and could read the zero row before its upload lands.
    const table = try modern.joinRows(cb, a, type_weight, 3, zero_row, 1, dim);
    defer cb.free(table);
    const ids = try a.alloc(i64, kinds.len);
    defer a.free(ids);
    for (kinds, ids) |kind, *id| {
        if (kind > 2) return error.InvalidLayaInputs;
        id.* = if (kind < 0) 3 else kind;
    }
    return cb.embeddingLookup(table, ids, ids.len, dim);
}

fn packedHead(cb: *const CB, a: std.mem.Allocator, cfg: Config, encoder: CT, segments: modern.Packed, kinds: []const i64, markers: []const i64, anchors: []const i64, width: usize, dim: usize, prefix: ?Prefix) ![]Tensor {
    const rows = kinds.len;
    const seq = rows + if (prefix) |p| p.rows else 0;
    const questions = anchors.len;
    if (rows == 0 or questions == 0 or width < 2 or width > cfg.maxOptions() or markers.len != questions * width or dim < 64 or dim % 64 != 0) return error.InvalidLayaInputs;
    const type_ct = try packedTypes(cb, a, kinds, dim);
    defer cb.free(type_ct);
    const mask = try a.alloc(i64, seq);
    defer a.free(mask);
    @memset(mask, 1);
    const hidden = try layers(cb, a, cfg, try cb.add(encoder, type_ct), mask, segments, 1, seq, dim, prefix, null);
    defer cb.free(hidden);
    for (anchors) |anchor| if (anchor < 0 or anchor >= rows) return error.InvalidLayaInputs;
    for (markers) |marker| if (marker >= rows) return error.InvalidLayaInputs;
    // Metal keeps the hidden states on the device; a full readback would
    // synchronize the frame and copy every row to score a few of them.
    if (cb.kind() == .metal) return scoreDevice(cb, a, cfg, hidden, markers, anchors, width, dim);
    const host = try cb.toFloat32(hidden, a);
    defer a.free(host);
    if (host.len != rows * dim) return error.UnexpectedOutputShape;
    const offsets = try a.alloc(usize, questions);
    defer a.free(offsets);
    for (anchors, offsets) |anchor, *offset| {
        if (anchor < 0 or anchor >= rows) return error.InvalidLayaInputs;
        offset.* = @intCast(anchor);
    }
    for (markers) |marker| if (marker >= rows) return error.InvalidLayaInputs;
    return scoreHost(cb, a, cfg, host, markers, offsets, width, dim);
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
    return layers(cb, a, cfg, try cb.add(encoder, types), mask, null, batch, seq, dim, null, null);
}

/// Cached trunk keys and values per head layer, `[rows, dim]` each, and a
/// `[rows, dim]` zero block standing in for trunk queries.
pub const Prefix = struct { rows: usize, keys: []const CT, values: []const CT };
/// Each head layer's keys and values, as host copies or dense tensors owned
/// by the caller (see `modern_bert.Capture`).
pub const Capture = struct {
    keys: []const []f32 = &.{},
    values: []const []f32 = &.{},
    key_tensors: []?CT = &.{},
    value_tensors: []?CT = &.{},
};

/// Pre-norm TransformerEncoder layers. Takes ownership of `input`. With a
/// prefix, `input` holds only the rows after it and attention spans both.
/// With `segments`, attention is segment-masked (tree-packed rows) and
/// `seq` counts every key of the row; otherwise it is dense over `mask`.
fn layers(cb: *const CB, a: std.mem.Allocator, cfg: Config, input: CT, mask: []const i64, segments: ?modern.Packed, batch: usize, seq: usize, dim: usize, trunk: ?Prefix, capture: ?Capture) !CT {
    const prefix_rows: usize = if (trunk) |p| p.rows else 0;
    const rows = batch * seq - prefix_rows;
    var hidden = input;
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
        const qkv = try cb.linear(n1, qw, qb, rows, dim, dim * 3);
        defer cb.free(qkv);
        const q = try cb.sliceLastDim(qkv, 0, dim);
        defer cb.free(q);
        const k = try cb.sliceLastDim(qkv, dim, dim * 2);
        defer cb.free(k);
        const v = try cb.sliceLastDim(qkv, dim * 2, dim * 3);
        defer cb.free(v);
        if (capture) |c| try @import("modern_bert.zig").captureLayer(cb, a, c.keys, c.values, c.key_tensors, c.value_tensors, layer, k, v, rows, dim);
        var joined: [2]?CT = .{ null, null };
        defer for (joined) |tensor| if (tensor) |t| cb.free(t);
        if (trunk) |p| {
            joined[0] = try modern.joinRows(cb, a, p.keys[layer], p.rows, k, rows, dim);
            joined[1] = try modern.joinRows(cb, a, p.values[layer], p.rows, v, rows, dim);
        }
        const attn = if (segments) |row|
            try modern.packedAttention(cb, a, q, joined[0] orelse k, joined[1] orelse v, row, std.math.maxInt(u32), rows, seq, dim / 64, 64)
        else
            try cb.scaledDotProductAttention(q, k, v, mask, null, batch, seq, dim / 64, 64);
        defer cb.free(attn);
        const proj = try linear(cb, attn, try std.fmt.bufPrint(&buf, "{s}.self_attn.out_proj", .{prefix}), rows, dim, dim);
        defer cb.free(proj);
        const residual = try cb.add(hidden, proj);
        defer cb.free(residual);
        const n2 = try norm(cb, residual, try std.fmt.bufPrint(&buf, "{s}.norm2", .{prefix}), dim);
        defer cb.free(n2);
        const up = try linear(cb, n2, try std.fmt.bufPrint(&buf, "{s}.linear1", .{prefix}), rows, dim, dim * 4);
        defer cb.free(up);
        const relu = try cb.relu(up);
        defer cb.free(relu);
        const down = try linear(cb, relu, try std.fmt.bufPrint(&buf, "{s}.linear2", .{prefix}), rows, dim * 4, dim);
        defer cb.free(down);
        const next = try cb.add(residual, down);
        cb.free(hidden);
        hidden = next;
    }
    return hidden;
}
