// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! CPU DeBERTa training attention with replayed counter dropout and bounded
//! score tiles. This module is not an inference fallback. It implements the
//! dedicated v1 graph contract; graph/backend integration is separate.
//!
//! Packed inputs are Q,K,V [B*S,H] and Qr,Kr [R,H], with H=heads*D.
//! Control is physical i32: six seed/microbatch/replica low/high bit limbs,
//! B*S strict 0/1 token masks, then 2*S-1 bucket IDs. Both relative terms use
//! bucket[q-k+S-1]. No attention-sized probabilities or dropout masks persist.
//!
//! Source masking fills invalid query OR key scores with finite -maxF32.
//! Fully masked query rows therefore have uniform probability, zero score
//! derivatives, and potentially nonzero V derivatives. Dropout changes the
//! PV numerator only; its address is independent of the execution tiling.
const std = @import("std");
const native = @import("../backends/native.zig");
const ExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;

pub const Attrs = @import("ml").graph.DebertaTrainingAttentionAttrs;
const Allocator = std.mem.Allocator;
const mib = 1024 * 1024;
const gib = 1024 * mib;

pub const Limits = struct {
    query_tile: usize = 128,
    key_tile: usize = 128,
    max_scratch_bytes: usize = 8 * mib,
    max_tensor_bytes: usize = gib,
    /// Score elements across forward and the two backward replay sweeps.
    /// This is work admission, not allocation bytes or FLOPs.
    max_work_items: u64 = 4 * 1024 * 1024 * 1024,
};

pub const Options = struct {
    limits: Limits = .{},
    control: ?ExecutionControl = null,
    io: ?std.Io = null,

    fn check(self: Options) !void {
        if (self.control) |control| try control.check();
        if (self.io) |io| try io.checkCancel();
    }

    fn transB(self: Options, m: usize, n: usize, k: usize, a: []const f32, b: []const f32, out: []f32) !void {
        try self.check();
        if (self.io) |io| try native.sgemmTransB(io, m, n, k, 1, a, b, 0, out) else native.sgemmTransBSync(m, n, k, 1, a, b, 0, out);
        try self.check();
    }

    fn matmul(self: Options, m: usize, n: usize, k: usize, a: []const f32, b: []const f32, beta: f32, out: []f32) !void {
        try self.check();
        if (self.io) |io| try native.sgemm(io, m, n, k, 1, a, b, beta, out) else native.sgemmSync(m, n, k, 1, a, b, beta, out);
        try self.check();
    }

    fn transA(self: Options, m: usize, n: usize, k: usize, a: []const f32, b: []const f32, out: []f32) !void {
        try self.check();
        // The repository's transposed-A primitive has no Io overload. Each
        // dispatch is bounded by the admitted tile scratch and checked here.
        native.sgemmTransA(m, n, k, 1, a, b, 0, out);
        try self.check();
    }
};

pub const Plan = struct {
    batch_tokens: usize,
    hidden: usize,
    output_elements: usize,
    qkv_elements: usize,
    relative_elements: usize,
    control_elements: usize,
    gradient_elements: usize,
    output_bytes: usize,
    qkv_bytes: usize,
    relative_bytes: usize,
    control_bytes: usize,
    gradient_bytes: usize,
    input_bytes: usize,
    scratch_elements: usize,
    scratch_bytes: usize,
    forward_owned_bytes: usize,
    backward_owned_bytes: usize,
    forward_work_items: u64,
    backward_work_items: u64,
    query_tile: usize,
    key_tile: usize,
    relative_tile: usize,
};

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.InvalidDebertaTrainingAttentionShape;
}
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.InvalidDebertaTrainingAttentionShape;
}

pub fn plan(attrs: Attrs, limits: Limits) !Plan {
    const layout = try attrs.layout();
    if (limits.query_tile == 0 or limits.key_tile == 0 or limits.query_tile > 512 or limits.key_tile > 512)
        return error.InvalidDebertaTrainingAttentionTile;
    if (limits.max_scratch_bytes == 0 or limits.max_scratch_bytes > 128 * mib or
        limits.max_tensor_bytes == 0 or limits.max_tensor_bytes > gib or
        limits.max_work_items == 0 or limits.max_work_items > 1 << 40)
        return error.InvalidDebertaTrainingAttentionLimit;
    const batch_tokens = std.math.cast(usize, layout.batch_tokens) orelse return error.InvalidDebertaTrainingAttentionShape;
    const hidden = std.math.cast(usize, layout.hidden) orelse return error.InvalidDebertaTrainingAttentionShape;
    const output_elements = try mul(batch_tokens, hidden);
    const qkv_elements = try mul(3, output_elements);
    const relative_elements = try mul(try mul(2, attrs.relative_rows), hidden);
    const control_elements = std.math.cast(usize, layout.control_elements) orelse return error.InvalidDebertaTrainingAttentionShape;
    const gradient_elements = try add(qkv_elements, relative_elements);
    const output_bytes = try mul(output_elements, 4);
    const qkv_bytes = try mul(qkv_elements, 4);
    const relative_bytes = try mul(relative_elements, 4);
    const control_bytes = try mul(control_elements, 4);
    const gradient_bytes = try mul(gradient_elements, 4);
    for ([_]usize{ output_bytes, qkv_bytes, relative_bytes, control_bytes, gradient_bytes }) |bytes|
        if (bytes > limits.max_tensor_bytes) return error.DebertaTrainingAttentionTensorLimitExceeded;
    const forward_work = std.math.mul(u64, try std.math.mul(u64, attrs.batch, attrs.num_heads), try std.math.mul(u64, attrs.seq_len, attrs.seq_len)) catch return error.InvalidDebertaTrainingAttentionShape;
    const backward_work = std.math.mul(u64, forward_work, 2) catch return error.InvalidDebertaTrainingAttentionShape;
    const total_work = std.math.add(u64, forward_work, backward_work) catch return error.InvalidDebertaTrainingAttentionShape;
    if (total_work > limits.max_work_items) return error.DebertaTrainingAttentionWorkLimitExceeded;
    const query_tile = @min(attrs.seq_len, limits.query_tile);
    const key_tile = @min(attrs.seq_len, limits.key_tile);
    const relative_tile = try add(query_tile, key_tile) - 1;
    // Two Q*K arrays (score/probability and dO*V), Q*R and K*R arrays,
    // four Q*D packs, five K*D packs, two R*D packs and three row vectors.
    var scratch = try mul(2, try mul(query_tile, key_tile));
    scratch = try add(scratch, try mul(try add(query_tile, key_tile), relative_tile));
    scratch = try add(scratch, try mul(try add(try mul(4, query_tile), try mul(5, key_tile)), attrs.head_dim));
    scratch = try add(scratch, try mul(try mul(2, relative_tile), attrs.head_dim));
    scratch = try add(scratch, try mul(3, query_tile));
    const scratch_bytes = try mul(scratch, 4);
    if (scratch_bytes > limits.max_scratch_bytes) return error.DebertaTrainingAttentionScratchLimitExceeded;
    return .{
        .batch_tokens = batch_tokens,
        .hidden = hidden,
        .output_elements = output_elements,
        .qkv_elements = qkv_elements,
        .relative_elements = relative_elements,
        .control_elements = control_elements,
        .gradient_elements = gradient_elements,
        .output_bytes = output_bytes,
        .qkv_bytes = qkv_bytes,
        .relative_bytes = relative_bytes,
        .control_bytes = control_bytes,
        .gradient_bytes = gradient_bytes,
        .input_bytes = try add(try add(qkv_bytes, relative_bytes), control_bytes),
        .scratch_elements = scratch,
        .scratch_bytes = scratch_bytes,
        .forward_owned_bytes = try add(output_bytes, scratch_bytes),
        .backward_owned_bytes = try add(gradient_bytes, scratch_bytes),
        .forward_work_items = forward_work,
        .backward_work_items = backward_work,
        .query_tile = query_tile,
        .key_tile = key_tile,
        .relative_tile = relative_tile,
    };
}

pub const Replay = struct { seed: u64, micro_batch: u64, replica: u64 };

pub fn mix(value: u64) u64 {
    var x = value +% 0x9e3779b97f4a7c15;
    x = (x ^ (x >> 30)) *% 0xbf58476d1ce4e5b9;
    x = (x ^ (x >> 27)) *% 0x94d049bb133111eb;
    return x ^ (x >> 31);
}

pub const Dropout = struct {
    stream: u64,
    threshold: u64,
    scale: f32,

    pub fn init(attrs: Attrs, replay: Replay) !Dropout {
        _ = try attrs.layout();
        return .{
            .stream = mix(replay.seed) ^ mix(replay.micro_batch) ^ mix(replay.replica +% 0x7265706c696361) ^ mix(attrs.dropout_stream_id),
            .threshold = @intFromFloat(@as(f64, attrs.dropout_probability) * 4294967296.0),
            .scale = 1.0 / (1.0 - attrs.dropout_probability),
        };
    }

    pub fn value(self: Dropout, index: u64) f32 {
        return if ((mix(self.stream ^ mix(index)) >> 32) < self.threshold) 0 else self.scale;
    }
};

pub const ControlView = struct { token_valid: []align(1) const i32, buckets: []align(1) const i32, replay: Replay, dropout: Dropout };

fn decodeU64(words: []align(1) const i32) u64 {
    return @as(u64, @as(u32, @bitCast(words[0]))) | (@as(u64, @as(u32, @bitCast(words[1]))) << 32);
}

/// Allocation-free, strict decoder shared by CPU and resident metadata
/// admission. Returned slices borrow the immutable physical i32 input.
pub fn validateControl(attrs: Attrs, words: []align(1) const i32, options: Options) !ControlView {
    const layout = try attrs.layout();
    const count = std.math.cast(usize, layout.control_elements) orelse return error.InvalidDebertaTrainingAttentionControl;
    const tokens = std.math.cast(usize, layout.batch_tokens) orelse return error.InvalidDebertaTrainingAttentionControl;
    if (words.len != count) return error.InvalidDebertaTrainingAttentionControl;
    const token_valid = words[6..][0..tokens];
    const buckets = words[6 + tokens ..];
    for (token_valid, 0..) |value, i| {
        if (i % 4096 == 0) try options.check();
        if (value != 0 and value != 1) return error.InvalidDebertaTrainingAttentionControl;
    }
    for (buckets, 0..) |bucket, i| {
        if (i % 4096 == 0) try options.check();
        if (bucket < 0 or bucket >= attrs.relative_rows) return error.InvalidDebertaTrainingAttentionControl;
    }
    const replay = Replay{ .seed = decodeU64(words[0..2]), .micro_batch = decodeU64(words[2..4]), .replica = decodeU64(words[4..6]) };
    return .{ .token_valid = token_valid, .buckets = buckets, .replay = replay, .dropout = try Dropout.init(attrs, replay) };
}

fn finite(values: []const f32, options: Options) !void {
    for (values, 0..) |value, index| {
        if (index % 4096 == 0) try options.check();
        if (!std.math.isFinite(value)) return error.NonFiniteDebertaTrainingAttention;
    }
}

fn validateInputs(attrs: Attrs, p: Plan, qkv: []const f32, relative: []const f32, words: []align(1) const i32, options: Options) !ControlView {
    try options.check();
    if (qkv.len != p.qkv_elements or relative.len != p.relative_elements) return error.InvalidDebertaTrainingAttentionShape;
    const control = try validateControl(attrs, words, options);
    try finite(qkv, options);
    try finite(relative, options);
    return control;
}

fn overlaps(left: []const u8, right: []const u8) bool {
    if (left.len == 0 or right.len == 0) return false;
    const lp = @intFromPtr(left.ptr);
    const rp = @intFromPtr(right.ptr);
    return if (lp <= rp) rp - lp < left.len else lp - rp < right.len;
}

fn disjoint(output: []f32, storage: []f32, qkv: []const f32, relative: []const f32, words: []align(1) const i32, dout: ?[]const f32) !void {
    const output_bytes = std.mem.sliceAsBytes(output);
    const storage_bytes = std.mem.sliceAsBytes(storage);
    if (overlaps(output_bytes, storage_bytes)) return error.AliasedDebertaTrainingAttentionStorage;
    for ([_][]const u8{ std.mem.sliceAsBytes(qkv), std.mem.sliceAsBytes(relative), std.mem.sliceAsBytes(words), if (dout) |values| std.mem.sliceAsBytes(values) else &.{} }) |input| {
        if (overlaps(output_bytes, input) or overlaps(storage_bytes, input)) return error.AliasedDebertaTrainingAttentionStorage;
    }
}

const Scratch = struct {
    scores: []f32,
    dp: []f32,
    c2p: []f32,
    p2c: []f32,
    q: []f32,
    out: []f32,
    dout: []f32,
    gq: []f32,
    k: []f32,
    scaled_k: []f32,
    v: []f32,
    gk: []f32,
    gv: []f32,
    qr: []f32,
    kr: []f32,
    maxima: []f32,
    sums: []f32,
    deltas: []f32,

    fn init(storage: []f32, p: Plan, dim: usize) Scratch {
        const Take = struct {
            fn apply(all: []f32, cursor: *usize, count: usize) []f32 {
                const result = all[cursor.*..][0..count];
                cursor.* += count;
                return result;
            }
        };
        var offset: usize = 0;
        const result = Scratch{
            .scores = Take.apply(storage, &offset, p.query_tile * p.key_tile),
            .dp = Take.apply(storage, &offset, p.query_tile * p.key_tile),
            .c2p = Take.apply(storage, &offset, p.query_tile * p.relative_tile),
            .p2c = Take.apply(storage, &offset, p.key_tile * p.relative_tile),
            .q = Take.apply(storage, &offset, p.query_tile * dim),
            .out = Take.apply(storage, &offset, p.query_tile * dim),
            .dout = Take.apply(storage, &offset, p.query_tile * dim),
            .gq = Take.apply(storage, &offset, p.query_tile * dim),
            .k = Take.apply(storage, &offset, p.key_tile * dim),
            .scaled_k = Take.apply(storage, &offset, p.key_tile * dim),
            .v = Take.apply(storage, &offset, p.key_tile * dim),
            .gk = Take.apply(storage, &offset, p.key_tile * dim),
            .gv = Take.apply(storage, &offset, p.key_tile * dim),
            .qr = Take.apply(storage, &offset, p.relative_tile * dim),
            .kr = Take.apply(storage, &offset, p.relative_tile * dim),
            .maxima = Take.apply(storage, &offset, p.query_tile),
            .sums = Take.apply(storage, &offset, p.query_tile),
            .deltas = Take.apply(storage, &offset, p.query_tile),
        };
        std.debug.assert(offset == storage.len);
        return result;
    }
};

const Tile = struct {
    attrs: Attrs,
    plan: Plan,
    control: ControlView,
    scratch: Scratch,
    options: Options,
    batch: usize,
    head: usize,
    query_start: usize,
    queries: usize,
    key_start: usize = 0,
    keys: usize = 0,

    fn relativeCount(self: Tile) usize {
        return self.queries + self.keys - 1;
    }
    fn relativeStart(self: Tile) usize {
        return self.query_start + self.attrs.seq_len - (self.key_start + self.keys);
    }
    fn dataOffset(self: Tile, token: usize) usize {
        return (self.batch * self.attrs.seq_len + token) * self.plan.hidden + self.head * self.attrs.head_dim;
    }
    fn dropout(self: Tile, query: usize, key: usize) f32 {
        const index = ((self.batch * self.attrs.num_heads + self.head) * self.attrs.seq_len + self.query_start + query) * self.attrs.seq_len + self.key_start + key;
        return self.control.dropout.value(index);
    }
    fn valid(self: Tile, query: usize, key: usize) bool {
        const valid_tokens = self.control.token_valid[self.batch * self.attrs.seq_len ..][0..self.attrs.seq_len];
        return valid_tokens[self.query_start + query] != 0 and valid_tokens[self.key_start + key] != 0;
    }
    fn packQueries(self: Tile, qkv: []const f32, dout: ?[]const f32) !void {
        const d = self.attrs.head_dim;
        for (0..self.queries) |query| {
            try self.options.check();
            const offset = self.dataOffset(self.query_start + query);
            @memcpy(self.scratch.q[query * d ..][0..d], qkv[offset..][0..d]);
            if (dout) |values| @memcpy(self.scratch.dout[query * d ..][0..d], values[offset..][0..d]);
        }
    }
    fn scores(self: Tile, qkv: []const f32, relative: []const f32) !void {
        const s = self.scratch;
        const d = self.attrs.head_dim;
        const scale = @sqrt(@as(f32, @floatFromInt(d)) * 3.0);
        for (0..self.keys) |key| {
            try self.options.check();
            const offset = self.dataOffset(self.key_start + key);
            @memcpy(s.k[key * d ..][0..d], qkv[self.plan.output_elements + offset ..][0..d]);
            @memcpy(s.v[key * d ..][0..d], qkv[2 * self.plan.output_elements + offset ..][0..d]);
            for (s.scaled_k[key * d ..][0..d], s.k[key * d ..][0..d]) |*out, value| out.* = value / scale;
        }
        const r_count = self.relativeCount();
        for (0..r_count) |r| {
            try self.options.check();
            const bucket: usize = @intCast(self.control.buckets[self.relativeStart() + r]);
            const offset = bucket * self.plan.hidden + self.head * d;
            @memcpy(s.qr[r * d ..][0..d], relative[offset..][0..d]);
            @memcpy(s.kr[r * d ..][0..d], relative[self.plan.relative_elements / 2 + offset ..][0..d]);
        }
        // Follow the source evaluation order: scale K before C2C; divide
        // C2P and P2C individually, add them, then add the C2C score.
        try self.options.transB(self.queries, self.keys, d, s.q[0 .. self.queries * d], s.scaled_k[0 .. self.keys * d], s.scores[0 .. self.queries * self.keys]);
        try self.options.transB(self.queries, r_count, d, s.q[0 .. self.queries * d], s.kr[0 .. r_count * d], s.c2p[0 .. self.queries * r_count]);
        try self.options.transB(self.keys, r_count, d, s.k[0 .. self.keys * d], s.qr[0 .. r_count * d], s.p2c[0 .. self.keys * r_count]);
        for (0..self.queries) |query| {
            try self.options.check();
            for (0..self.keys) |key| {
                const out = &s.scores[query * self.keys + key];
                if (!self.valid(query, key)) {
                    out.* = -std.math.floatMax(f32);
                    continue;
                }
                const r = query + self.keys - 1 - key;
                out.* += s.c2p[query * r_count + r] / scale + s.p2c[key * r_count + r] / scale;
                if (!std.math.isFinite(out.*)) return error.NonFiniteDebertaTrainingAttention;
            }
        }
    }

    fn online(self: Tile, comptime is_backward: bool) !void {
        const s = self.scratch;
        const d = self.attrs.head_dim;
        if (is_backward) try self.options.transB(self.queries, self.keys, d, s.dout[0 .. self.queries * d], s.v[0 .. self.keys * d], s.dp[0 .. self.queries * self.keys]);
        for (0..self.queries) |query| {
            try self.options.check();
            const row = s.scores[query * self.keys ..][0..self.keys];
            var maximum = s.maxima[query];
            for (row) |score| maximum = @max(maximum, score);
            if (s.sums[query] != 0) {
                const factor = @exp(s.maxima[query] - maximum);
                s.sums[query] *= factor;
                if (is_backward) {
                    s.deltas[query] *= factor;
                } else {
                    for (s.out[query * d ..][0..d]) |*value| value.* *= factor;
                }
            }
            var sum: f32 = 0;
            for (row, 0..) |*score, key| {
                const probability_numerator = @exp(score.* - maximum);
                sum += probability_numerator;
                score.* = probability_numerator * self.dropout(query, key);
                if (is_backward) s.deltas[query] += score.* * s.dp[query * self.keys + key];
            }
            s.maxima[query] = maximum;
            s.sums[query] += sum;
        }
        if (!is_backward) try self.options.matmul(self.queries, d, self.keys, s.scores[0 .. self.queries * self.keys], s.v[0 .. self.keys * d], 1, s.out[0 .. self.queries * d]);
    }

    fn backward(self: Tile, gradient: []f32) !void {
        const s = self.scratch;
        const d = self.attrs.head_dim;
        const r_count = self.relativeCount();
        const scale = @sqrt(@as(f32, @floatFromInt(d)) * 3.0);
        try self.options.transB(self.queries, self.keys, d, s.dout[0 .. self.queries * d], s.v[0 .. self.keys * d], s.dp[0 .. self.queries * self.keys]);
        for (0..self.queries) |query| {
            try self.options.check();
            for (0..self.keys) |key| {
                const i = query * self.keys + key;
                // Save dScore in dp; scores becomes post-dropout P for dV.
                const probability = @exp(s.scores[i] - s.maxima[query]) / s.sums[query];
                const keep = self.dropout(query, key);
                s.scores[i] = probability * keep;
                s.dp[i] = if (self.valid(query, key)) probability * (keep * s.dp[i] - s.deltas[query]) else 0;
            }
        }
        try self.options.transA(self.keys, d, self.queries, s.scores[0 .. self.queries * self.keys], s.dout[0 .. self.queries * d], s.gv[0 .. self.keys * d]);
        // C2C: Q gradient uses already divided K; K's division VJP follows
        // the Q^T product. Relative bias gradients receive dScore/scale.
        try self.options.matmul(self.queries, d, self.keys, s.dp[0 .. self.queries * self.keys], s.scaled_k[0 .. self.keys * d], 0, s.gq[0 .. self.queries * d]);
        try self.options.transA(self.keys, d, self.queries, s.dp[0 .. self.queries * self.keys], s.q[0 .. self.queries * d], s.gk[0 .. self.keys * d]);
        for (s.gk[0 .. self.keys * d]) |*value| value.* /= scale;
        @memset(s.c2p[0 .. self.queries * r_count], 0);
        @memset(s.p2c[0 .. self.keys * r_count], 0);
        for (0..self.queries) |query| {
            try self.options.check();
            for (0..self.keys) |key| {
                const r = query + self.keys - 1 - key;
                const ds = s.dp[query * self.keys + key] / scale;
                // Each (query,relative-offset) / (key,relative-offset)
                // receives one entry. Repeated bucket IDs are reduced below.
                s.c2p[query * r_count + r] = ds;
                s.p2c[key * r_count + r] = ds;
            }
        }
        try self.options.matmul(self.queries, d, r_count, s.c2p[0 .. self.queries * r_count], s.kr[0 .. r_count * d], 1, s.gq[0 .. self.queries * d]);
        try self.options.matmul(self.keys, d, r_count, s.p2c[0 .. self.keys * r_count], s.qr[0 .. r_count * d], 1, s.gk[0 .. self.keys * d]);
        // Relative projection packs are no longer needed; use their storage
        // for deterministic per-relative-offset gradient partials.
        try self.options.transA(r_count, d, self.queries, s.c2p[0 .. self.queries * r_count], s.q[0 .. self.queries * d], s.kr[0 .. r_count * d]);
        try self.options.transA(r_count, d, self.keys, s.p2c[0 .. self.keys * r_count], s.k[0 .. self.keys * d], s.qr[0 .. r_count * d]);
        for (0..self.queries) |query| {
            try self.options.check();
            const offset = self.dataOffset(self.query_start + query);
            for (gradient[offset..][0..d], s.gq[query * d ..][0..d]) |*value, partial| value.* += partial;
        }
        for (0..self.keys) |key| {
            try self.options.check();
            const offset = self.dataOffset(self.key_start + key);
            for (gradient[self.plan.output_elements + offset ..][0..d], s.gk[key * d ..][0..d]) |*value, partial| value.* += partial;
            for (gradient[2 * self.plan.output_elements + offset ..][0..d], s.gv[key * d ..][0..d]) |*value, partial| value.* += partial;
        }
        for (0..r_count) |r| {
            try self.options.check();
            const bucket: usize = @intCast(self.control.buckets[self.relativeStart() + r]);
            const offset = self.plan.qkv_elements + bucket * self.plan.hidden + self.head * d;
            for (gradient[offset..][0..d], s.qr[r * d ..][0..d]) |*value, partial| value.* += partial;
            for (gradient[offset + self.plan.relative_elements / 2 ..][0..d], s.kr[r * d ..][0..d]) |*value, partial| value.* += partial;
        }
    }
};

fn compute(comptime is_backward: bool, attrs: Attrs, p: Plan, qkv: []const f32, relative: []const f32, control: ControlView, dout: ?[]const f32, output: []f32, storage: []f32, options: Options) !void {
    const scratch = Scratch.init(storage, p, attrs.head_dim);
    if (is_backward) @memset(output, 0);
    for (0..attrs.batch) |batch| {
        for (0..attrs.num_heads) |head| {
            var query_start: usize = 0;
            while (query_start < attrs.seq_len) {
                try options.check();
                const queries = @min(p.query_tile, attrs.seq_len - query_start);
                var tile = Tile{ .attrs = attrs, .plan = p, .control = control, .scratch = scratch, .options = options, .batch = batch, .head = head, .query_start = query_start, .queries = queries };
                try tile.packQueries(qkv, dout);
                @memset(scratch.maxima[0..queries], -std.math.inf(f32));
                @memset(scratch.sums[0..queries], 0);
                if (is_backward) @memset(scratch.deltas[0..queries], 0) else @memset(scratch.out[0 .. queries * attrs.head_dim], 0);
                while (tile.key_start < attrs.seq_len) {
                    tile.keys = @min(p.key_tile, attrs.seq_len - tile.key_start);
                    try tile.scores(qkv, relative);
                    try tile.online(is_backward);
                    tile.key_start += tile.keys;
                }
                if (is_backward) {
                    for (0..queries) |query| {
                        scratch.deltas[query] /= scratch.sums[query];
                        if (!std.math.isFinite(scratch.deltas[query])) return error.NonFiniteDebertaTrainingAttention;
                    }
                    tile.key_start = 0;
                    while (tile.key_start < attrs.seq_len) {
                        tile.keys = @min(p.key_tile, attrs.seq_len - tile.key_start);
                        try tile.scores(qkv, relative);
                        try tile.backward(output);
                        tile.key_start += tile.keys;
                    }
                } else {
                    for (0..queries) |query| {
                        try options.check();
                        const offset = tile.dataOffset(query_start + query);
                        for (output[offset..][0..attrs.head_dim], scratch.out[query * attrs.head_dim ..][0..attrs.head_dim]) |*value, accumulated| value.* = accumulated / scratch.sums[query];
                    }
                }
                query_start += queries;
            }
        }
    }
    try finite(output, options);
}

/// Caller-owned output and scratch must be disjoint from all inputs and each
/// other. On failure their contents are invalid; every input remains unchanged.
pub fn forwardInto(attrs: Attrs, qkv: []const f32, relative: []const f32, control: []align(1) const i32, output: []f32, scratch: []f32, options: Options) !void {
    const p = try plan(attrs, options.limits);
    if (output.len != p.output_elements or scratch.len != p.scratch_elements) return error.InvalidDebertaTrainingAttentionShape;
    try disjoint(output, scratch, qkv, relative, control, null);
    const view = try validateInputs(attrs, p, qkv, relative, control, options);
    try compute(false, attrs, p, qkv, relative, view, null, output, scratch, options);
}

/// Packed gradient order is dQ,dK,dV,dQr,dKr. Relative-table gradients include
/// every batch, query, key and repeated bucket contribution in a fixed order.
pub fn backwardInto(attrs: Attrs, qkv: []const f32, relative: []const f32, control: []align(1) const i32, dout: []const f32, gradient: []f32, scratch: []f32, options: Options) !void {
    const p = try plan(attrs, options.limits);
    if (dout.len != p.output_elements or gradient.len != p.gradient_elements or scratch.len != p.scratch_elements) return error.InvalidDebertaTrainingAttentionShape;
    try disjoint(gradient, scratch, qkv, relative, control, dout);
    const view = try validateInputs(attrs, p, qkv, relative, control, options);
    try finite(dout, options);
    try compute(true, attrs, p, qkv, relative, view, dout, gradient, scratch, options);
}

pub fn forward(a: Allocator, attrs: Attrs, qkv: []const f32, relative: []const f32, control: []align(1) const i32, options: Options) ![]f32 {
    const p = try plan(attrs, options.limits);
    const view = try validateInputs(attrs, p, qkv, relative, control, options);
    const output = try a.alloc(f32, p.output_elements);
    errdefer a.free(output);
    const scratch = try a.alloc(f32, p.scratch_elements);
    defer a.free(scratch);
    try compute(false, attrs, p, qkv, relative, view, null, output, scratch, options);
    return output;
}

pub fn backward(a: Allocator, attrs: Attrs, qkv: []const f32, relative: []const f32, control: []align(1) const i32, dout: []const f32, options: Options) ![]f32 {
    const p = try plan(attrs, options.limits);
    if (dout.len != p.output_elements) return error.InvalidDebertaTrainingAttentionShape;
    const view = try validateInputs(attrs, p, qkv, relative, control, options);
    try finite(dout, options);
    const gradient = try a.alloc(f32, p.gradient_elements);
    errdefer a.free(gradient);
    const scratch = try a.alloc(f32, p.scratch_elements);
    defer a.free(scratch);
    try compute(true, attrs, p, qkv, relative, view, dout, gradient, scratch, options);
    return gradient;
}
