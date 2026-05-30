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
const builtin = @import("builtin");
const primitives = @import("primitives.zig");
const gemm = @import("gemm.zig");
const pool = @import("pool.zig");

const vec_len = primitives.vec_len;
const F32xN = @Vector(vec_len, f32);
const BLOCK_Q: usize = 64;
const BLOCK_KV: usize = 256;

/// T5 relative position bucket computation.
/// Maps a relative position (key_pos - query_pos) to a bucket index.
pub fn t5RelativePositionBucket(relative_position: i64, num_buckets_: usize, max_distance_: usize, bidirectional: bool) usize {
    var num_buckets: i64 = @intCast(num_buckets_);
    const max_distance: f64 = @floatFromInt(max_distance_);
    var rel_pos = relative_position;

    var offset: i64 = 0;
    if (bidirectional) {
        num_buckets = @divTrunc(num_buckets, 2);
        if (rel_pos > 0) {
            offset = num_buckets;
        } else {
            rel_pos = -rel_pos;
        }
    } else {
        rel_pos = -@min(rel_pos, 0);
    }

    const max_exact = @divTrunc(num_buckets, 2);
    if (rel_pos < max_exact) {
        return @intCast(offset + rel_pos);
    }

    const rel_f: f64 = @floatFromInt(rel_pos);
    const max_exact_f: f64 = @floatFromInt(max_exact);
    const num_buckets_f: f64 = @floatFromInt(num_buckets);

    const log_ratio = @log(rel_f / max_exact_f) / @log(max_distance / max_exact_f);
    const bucket_f = max_exact_f + log_ratio * (num_buckets_f - max_exact_f);
    const bucket = @min(@as(i64, @intFromFloat(bucket_f)), num_buckets - 1);

    return @intCast(offset + bucket);
}

/// Shared RoPE rotation core. Rotates `output` in-place using one position
/// value per head-sized chunk. `positions[tok]` gives the absolute position
/// for token index `tok`. Tokens with position 0 are identity (cos 0 = 1,
/// sin 0 = 0) so padded slots are left unchanged.
pub fn ropeCore(output: []f32, positions: []const usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, consecutive_pairs: bool) void {
    const total_tokens = output.len / head_dim;
    const half = rope_dim / 2;
    const head_half = head_dim / 2;
    for (0..total_tokens) |tok| {
        const pos = positions[tok];
        const base = tok * head_dim;
        for (0..half) |j| {
            const freq = 1.0 / std.math.pow(f32, theta, @as(f32, @floatFromInt(2 * j)) / @as(f32, @floatFromInt(rope_dim)));
            const angle = @as(f32, @floatFromInt(pos)) * freq_scale * freq;
            const cos_val = @cos(angle);
            const sin_val = @sin(angle);
            const idx0 = if (consecutive_pairs) 2 * j else j;
            const idx1 = if (consecutive_pairs) 2 * j + 1 else j + head_half;
            const x0 = output[base + idx0];
            const x1 = output[base + idx1];
            output[base + idx0] = x0 * cos_val - x1 * sin_val;
            output[base + idx1] = x0 * sin_val + x1 * cos_val;
        }
    }
}

/// Tiled flash causal attention supporting both MHA and GQA.
pub fn flashCausalAttentionHost(
    allocator: std.mem.Allocator,
    Q: []const f32,
    K: []const f32,
    V: []const f32,
    bias: ?[]const f32,
    attn_or_mask: ?[]const u8,
    sliding_window: usize,
    batch: usize,
    q_seq_len: usize,
    kv_seq_len: usize,
    query_position_offset: usize,
    kv_position_offset: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) ![]f32 {
    if (num_kv_heads == 0 or num_heads == 0 or head_dim == 0) return error.InvalidAttentionShape;
    if (num_heads % num_kv_heads != 0) return error.InvalidAttentionShape;
    const H_q = std.math.mul(usize, num_heads, head_dim) catch return error.InvalidAttentionShape;
    const H_kv = std.math.mul(usize, num_kv_heads, head_dim) catch return error.InvalidAttentionShape;
    const heads_per_group = num_heads / num_kv_heads;
    const q_expected = std.math.mul(usize, std.math.mul(usize, batch, q_seq_len) catch return error.InvalidAttentionShape, H_q) catch return error.InvalidAttentionShape;
    const kv_expected = std.math.mul(usize, std.math.mul(usize, batch, kv_seq_len) catch return error.InvalidAttentionShape, H_kv) catch return error.InvalidAttentionShape;
    if (Q.len != q_expected or K.len != kv_expected or V.len != kv_expected) return error.InvalidAttentionShape;
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    const query_end = std.math.add(usize, query_position_offset, q_seq_len) catch return error.InvalidAttentionShape;
    const kv_total_end = std.math.add(usize, kv_position_offset, kv_seq_len) catch return error.InvalidAttentionShape;
    const mask_seq_len = @max(query_end, kv_total_end);

    const output_elems = std.math.mul(usize, batch, q_seq_len) catch return error.InvalidAttentionShape;
    const output = try allocator.alloc(f32, std.math.mul(usize, output_elems, H_q) catch return error.InvalidAttentionShape);
    errdefer allocator.free(output);
    @memset(output, 0.0);

    const bq = @min(BLOCK_Q, q_seq_len);
    const bkv = @min(BLOCK_KV, kv_seq_len);
    const score_tile = try allocator.alloc(f32, std.math.mul(usize, bq, bkv) catch return error.InvalidAttentionShape);
    defer allocator.free(score_tile);
    const row_max = try allocator.alloc(f32, bq);
    defer allocator.free(row_max);
    const row_sum = try allocator.alloc(f32, bq);
    defer allocator.free(row_sum);

    // Q/K/V are token-major here (each token packs all heads contiguously),
    // so the per-head rows are strided by H_q/H_kv.  The register-tiled SGEMM
    // expects contiguous matrices, so we pack each head's current Q-/K-/V-
    // block into scratch buffers, run the GEMM, then scatter the packed
    // output rows back to the strided `output` at the end of each Q-block.
    // Packing cost: cur_bq + 2*cur_bkv head_dim-sized memcpys per K-block,
    // which is < 2% of the GEMM work it enables.
    const q_pack = try allocator.alloc(f32, bq * head_dim);
    defer allocator.free(q_pack);
    const k_pack = try allocator.alloc(f32, bkv * head_dim);
    defer allocator.free(k_pack);
    const v_pack = try allocator.alloc(f32, bkv * head_dim);
    defer allocator.free(v_pack);
    const out_pack = try allocator.alloc(f32, bq * head_dim);
    defer allocator.free(out_pack);

    for (0..batch) |b| {
        for (0..num_heads) |h| {
            const kv_h = h / heads_per_group;
            var q_start: usize = 0;
            while (q_start < q_seq_len) {
                const q_end = @min(q_start + BLOCK_Q, q_seq_len);
                const cur_bq = q_end - q_start;

                for (0..cur_bq) |r| {
                    row_max[r] = -std.math.inf(f32);
                    row_sum[r] = 0.0;
                }

                // Pack Q for this head's Q-block into a contiguous tile.
                // Q is token-major so each Q row is at stride H_q in memory.
                for (0..cur_bq) |qi_local| {
                    const qi = q_start + qi_local;
                    const src = Q[(b * q_seq_len + qi) * H_q + h * head_dim ..][0..head_dim];
                    @memcpy(q_pack[qi_local * head_dim ..][0..head_dim], src);
                }
                // Packed output accumulator for this Q-block; scattered back
                // to `output` at the end.
                @memset(out_pack[0 .. cur_bq * head_dim], 0.0);

                var kv_start: usize = 0;
                while (kv_start < kv_seq_len) {
                    const kv_end = @min(kv_start + BLOCK_KV, kv_seq_len);
                    const cur_bkv = kv_end - kv_start;

                    // Pack K and V for this K-block (one head's slab) into
                    // contiguous tiles so the GEMM kernel's register tiling
                    // can reuse each K/V row across all Q rows in the block.
                    for (0..cur_bkv) |ki_local| {
                        const ki = kv_start + ki_local;
                        const k_src = K[(b * kv_seq_len + ki) * H_kv + kv_h * head_dim ..][0..head_dim];
                        const v_src = V[(b * kv_seq_len + ki) * H_kv + kv_h * head_dim ..][0..head_dim];
                        @memcpy(k_pack[ki_local * head_dim ..][0..head_dim], k_src);
                        @memcpy(v_pack[ki_local * head_dim ..][0..head_dim], v_src);
                    }

                    // QK^T as a single register-tiled GEMM with prescaled Q
                    // (alpha=scale folds the per-cell `* scale` into the
                    // matmul, dropping cur_bq*cur_bkv scalar muls).
                    gemm.sgemmTransBSequential(cur_bq, cur_bkv, head_dim, scale, q_pack[0 .. cur_bq * head_dim], k_pack[0 .. cur_bkv * head_dim], 0.0, score_tile[0 .. cur_bq * cur_bkv]);

                    if (bias) |bias_data| {
                        for (0..cur_bq) |qi_local| {
                            const qi = q_start + qi_local;
                            const query_pos = query_position_offset + qi;
                            for (0..cur_bkv) |ki_local| {
                                const ki = kv_start + ki_local;
                                const key_pos = kv_position_offset + ki;
                                if (causalBiasValue(bias_data, b, h, qi, query_pos, ki, key_pos, batch, q_seq_len, kv_seq_len, mask_seq_len, num_heads)) |bias_value| {
                                    score_tile[qi_local * cur_bkv + ki_local] += bias_value;
                                }
                            }
                        }
                    }

                    for (0..cur_bq) |qi_local| {
                        const query_pos = query_position_offset + q_start + qi_local;
                        for (0..cur_bkv) |ki_local| {
                            const key_pos = kv_position_offset + kv_start + ki_local;
                            if ((key_pos > query_pos and !allowsFutureAttention(attn_or_mask, mask_seq_len, query_pos, key_pos)) or
                                !allowsPastAttention(sliding_window, query_pos, key_pos))
                            {
                                score_tile[qi_local * cur_bkv + ki_local] = -std.math.inf(f32);
                            }
                        }
                    }

                    // Streaming softmax: per Q-row, find block max, rescale
                    // out_pack, then turn score_row into exp(score - new_max).
                    // V projection is deferred to a single batched GEMM after
                    // the loop (see below).
                    for (0..cur_bq) |qi_local| {
                        const out_row = out_pack[qi_local * head_dim ..][0..head_dim];

                        var block_max: f32 = -std.math.inf(f32);
                        for (0..cur_bkv) |ki_local| {
                            const s = score_tile[qi_local * cur_bkv + ki_local];
                            if (s > block_max) block_max = s;
                        }

                        const old_max = row_max[qi_local];
                        const new_max = @max(old_max, block_max);
                        if (new_max == -std.math.inf(f32)) {
                            @memset(score_tile[qi_local * cur_bkv ..][0..cur_bkv], 0.0);
                            continue;
                        }

                        if (row_sum[qi_local] != 0.0) {
                            const rescale = @exp(old_max - new_max);
                            if (rescale != 1.0) {
                                const r_splat: F32xN = @splat(rescale);
                                var d: usize = 0;
                                while (d + vec_len <= head_dim) : (d += vec_len) {
                                    const ov: F32xN = out_row[d..][0..vec_len].*;
                                    out_row[d..][0..vec_len].* = ov * r_splat;
                                }
                                while (d < head_dim) : (d += 1) {
                                    out_row[d] *= rescale;
                                }
                                row_sum[qi_local] *= rescale;
                            }
                        }

                        const score_row = score_tile[qi_local * cur_bkv ..][0..cur_bkv];
                        const block_sum = primitives.expSubtractAndSum(score_row, new_max);
                        row_max[qi_local] = new_max;
                        row_sum[qi_local] += block_sum;
                    }

                    // Batched V projection: out_pack[bq, hd] += score_tile[bq, bkv] @ V_pack[bkv, hd].
                    gemm.sgemmSequential(cur_bq, head_dim, cur_bkv, 1.0, score_tile[0 .. cur_bq * cur_bkv], v_pack[0 .. cur_bkv * head_dim], 1.0, out_pack[0 .. cur_bq * head_dim]);

                    kv_start = kv_end;
                }

                // Scatter packed output back to the strided `output`,
                // applying the final 1/row_sum normalization in the same
                // pass to avoid an extra memory sweep.
                for (0..cur_bq) |qi_local| {
                    const qi = q_start + qi_local;
                    const dst = output[(b * q_seq_len + qi) * H_q + h * head_dim ..][0..head_dim];
                    const src = out_pack[qi_local * head_dim ..][0..head_dim];
                    if (row_sum[qi_local] != 0.0) {
                        const inv_sum = 1.0 / row_sum[qi_local];
                        const inv_splat: F32xN = @splat(inv_sum);
                        var d: usize = 0;
                        while (d + vec_len <= head_dim) : (d += vec_len) {
                            const sv: F32xN = src[d..][0..vec_len].*;
                            dst[d..][0..vec_len].* = sv * inv_splat;
                        }
                        while (d < head_dim) : (d += 1) {
                            dst[d] = src[d] * inv_sum;
                        }
                    } else {
                        @memcpy(dst, src);
                    }
                }

                q_start = q_end;
            }
        }
    }

    return output;
}

fn mul3(a: usize, b: usize, c: usize) ?usize {
    const ab = std.math.mul(usize, a, b) catch return null;
    const abc = std.math.mul(usize, ab, c) catch return null;
    return abc;
}

fn mul4(a: usize, b: usize, c: usize, d: usize) ?usize {
    const abc = mul3(a, b, c) orelse return null;
    const abcd = std.math.mul(usize, abc, d) catch return null;
    return abcd;
}

fn causalBiasValue(
    bias: []const f32,
    batch_idx: usize,
    head_idx: usize,
    query_local: usize,
    query_pos: usize,
    key_local: usize,
    key_pos: usize,
    batch: usize,
    q_seq_len: usize,
    kv_seq_len: usize,
    total_seq_len: usize,
    num_heads: usize,
) ?f32 {
    if (mul4(batch, num_heads, q_seq_len, kv_seq_len)) |len| {
        if (bias.len == len) {
            const idx = ((batch_idx * num_heads + head_idx) * q_seq_len + query_local) * kv_seq_len + key_local;
            return if (idx < bias.len) bias[idx] else null;
        }
    }
    if (mul3(num_heads, q_seq_len, kv_seq_len)) |len| {
        if (bias.len == len) {
            const idx = (head_idx * q_seq_len + query_local) * kv_seq_len + key_local;
            return if (idx < bias.len) bias[idx] else null;
        }
    }
    if (mul4(batch, num_heads, q_seq_len, total_seq_len)) |len| {
        if (bias.len == len) {
            const idx = ((batch_idx * num_heads + head_idx) * q_seq_len + query_local) * total_seq_len + key_pos;
            return if (idx < bias.len) bias[idx] else null;
        }
    }
    if (mul3(num_heads, q_seq_len, total_seq_len)) |len| {
        if (bias.len == len) {
            const idx = (head_idx * q_seq_len + query_local) * total_seq_len + key_pos;
            return if (idx < bias.len) bias[idx] else null;
        }
    }
    if (mul4(batch, num_heads, total_seq_len, total_seq_len)) |len| {
        if (bias.len == len) {
            const idx = ((batch_idx * num_heads + head_idx) * total_seq_len + query_pos) * total_seq_len + key_pos;
            return if (idx < bias.len) bias[idx] else null;
        }
    }
    if (mul3(num_heads, total_seq_len, total_seq_len)) |len| {
        if (bias.len == len) {
            const idx = (head_idx * total_seq_len + query_pos) * total_seq_len + key_pos;
            return if (idx < bias.len) bias[idx] else null;
        }
    }
    return null;
}

/// Bidirectional (non-causal) flash attention for encoder layers.
///
/// Drop-in replacement for the materialized `softmax(Q @ K^T) @ V` path in
/// `sdpaOp`: tiles Q into BLOCK_Q-row blocks and K/V into BLOCK_KV-col
/// blocks, runs an online softmax per Q-tile, and never materializes the
/// `[seq, seq]` score matrix.  Memory bandwidth scales with `seq * head_dim`
/// instead of `seq^2`.
///
/// Q/K/V layout: `[batch, num_heads, seq_len, head_dim]` (head-major,
/// matching the production sdpaOp input -- the QKV linear's natural
/// `[batch, seq, num_heads*head_dim]` is transposed to head-major before
/// attention).  Output uses the same layout.
///
/// Optional `bias` is the additive position bias `[H?, seq, seq]` -- either
/// `[num_heads, seq, seq]` (shared across batch) or
/// `[batch * num_heads, seq, seq]`.  `mask` is `[batch, seq]` with `0` =
/// masked out, `1` = attend.
pub fn flashAttentionHost(
    allocator: std.mem.Allocator,
    Q: []const f32,
    K: []const f32,
    V: []const f32,
    bias: ?[]const f32,
    mask: []const i64,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
) ![]f32 {
    if (num_heads == 0 or head_dim == 0) return error.InvalidAttentionShape;
    const H = std.math.mul(usize, num_heads, head_dim) catch return error.InvalidAttentionShape;
    const expected = std.math.mul(usize, std.math.mul(usize, batch, seq_len) catch return error.InvalidAttentionShape, H) catch return error.InvalidAttentionShape;
    if (Q.len != expected or K.len != expected or V.len != expected) return error.InvalidAttentionShape;
    const mask_expected = std.math.mul(usize, batch, seq_len) catch return error.InvalidAttentionShape;
    if (mask.len < mask_expected) return error.InvalidAttentionShape;
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    const seq_sq = std.math.mul(usize, seq_len, seq_len) catch return error.InvalidAttentionShape;
    const bias_shared_len = std.math.mul(usize, num_heads, seq_sq) catch return error.InvalidAttentionShape;
    const bias_batched_len = std.math.mul(usize, batch, bias_shared_len) catch return error.InvalidAttentionShape;
    const output_elems = std.math.mul(usize, batch, seq_len) catch return error.InvalidAttentionShape;
    const output = try allocator.alloc(f32, std.math.mul(usize, output_elems, H) catch return error.InvalidAttentionShape);
    errdefer allocator.free(output);
    @memset(output, 0.0);

    const bq = @min(BLOCK_Q, seq_len);
    const bkv = @min(BLOCK_KV, seq_len);
    const score_tile = try allocator.alloc(f32, std.math.mul(usize, bq, bkv) catch return error.InvalidAttentionShape);
    defer allocator.free(score_tile);
    const row_max = try allocator.alloc(f32, bq);
    defer allocator.free(row_max);
    const row_sum = try allocator.alloc(f32, bq);
    defer allocator.free(row_sum);

    for (0..batch) |b| {
        const mask_base = b * seq_len;
        for (0..num_heads) |h| {
            // Head-major: each (b, h) occupies a contiguous [seq, head_dim]
            // slab in Q/K/V/output starting at (b * num_heads + h) * seq * head_dim.
            const bh_idx = b * num_heads + h;
            const bh_offset = bh_idx * seq_len * head_dim;

            // Position-bias offset for this (b, h) -- either head-shared
            // (num_heads * seq * seq) or per-(b, h) (batch * num_heads * seq * seq).
            const bias_offset: ?usize = if (bias) |bdata|
                if (bdata.len == bias_batched_len)
                    bh_idx * seq_sq
                else if (bdata.len == bias_shared_len)
                    h * seq_sq
                else
                    null
            else
                null;

            var q_start: usize = 0;
            while (q_start < seq_len) {
                const q_end = @min(q_start + BLOCK_Q, seq_len);
                const cur_bq = q_end - q_start;

                for (0..cur_bq) |r| {
                    row_max[r] = -std.math.inf(f32);
                    row_sum[r] = 0.0;
                }

                var kv_start: usize = 0;
                while (kv_start < seq_len) {
                    const kv_end = @min(kv_start + BLOCK_KV, seq_len);
                    const cur_bkv = kv_end - kv_start;

                    // Compute Q_i @ K_j^T * scale into score_tile via the
                    // register-tiled GEMM kernel.  The previous scalar dotPtrs
                    // loop reloaded each K row cur_bq times from L1/L2 (no
                    // reuse across Q rows); sgemmTransBSequential keeps an
                    // MR×NR vector accumulator tile resident across all K
                    // and reuses each K panel m/MR times.  Single-threaded
                    // because the outer (b, h) loop is already sequential
                    // and threading per-head matmul would oversubscribe.
                    // alpha=scale folds the per-cell post-multiply (#3) into
                    // the GEMM, so we drop a `* scale` per (qi, ki).
                    const q_block = Q[bh_offset + q_start * head_dim ..][0 .. cur_bq * head_dim];
                    const k_block = K[bh_offset + kv_start * head_dim ..][0 .. cur_bkv * head_dim];
                    gemm.sgemmTransBSequential(cur_bq, cur_bkv, head_dim, scale, q_block, k_block, 0.0, score_tile[0 .. cur_bq * cur_bkv]);

                    // Add position bias if provided.
                    if (bias) |bdata| if (bias_offset) |off| {
                        for (0..cur_bq) |qi_local| {
                            const qi = q_start + qi_local;
                            for (0..cur_bkv) |ki_local| {
                                const ki = kv_start + ki_local;
                                score_tile[qi_local * cur_bkv + ki_local] +=
                                    bdata[off + qi * seq_len + ki];
                            }
                        }
                    };

                    // Apply per-position mask (key side).  Bidirectional, so
                    // no positional masking; only the -inf masked-out keys.
                    for (0..cur_bkv) |ki_local| {
                        const ki = kv_start + ki_local;
                        if (mask[mask_base + ki] == 0) {
                            for (0..cur_bq) |qi_local| {
                                score_tile[qi_local * cur_bkv + ki_local] = -std.math.inf(f32);
                            }
                        }
                    }

                    // Streaming softmax: per-Q-row, find block max, rescale
                    // the running output (exp(old_max - new_max)) and turn
                    // score_row into exp(score - new_max).  Defers the V
                    // projection to a single batched GEMM after the loop —
                    // see below.
                    for (0..cur_bq) |qi_local| {
                        const qi = q_start + qi_local;
                        const out_ptr = output[bh_offset + qi * head_dim ..][0..head_dim];

                        var block_max: f32 = -std.math.inf(f32);
                        for (0..cur_bkv) |ki_local| {
                            const s = score_tile[qi_local * cur_bkv + ki_local];
                            if (s > block_max) block_max = s;
                        }

                        const old_max = row_max[qi_local];
                        const new_max = @max(old_max, block_max);
                        if (new_max == -std.math.inf(f32)) {
                            // All scores -inf for this row → zero its score_row
                            // so the deferred GEMM contributes nothing.
                            @memset(score_tile[qi_local * cur_bkv ..][0..cur_bkv], 0.0);
                            continue;
                        }

                        if (row_sum[qi_local] != 0.0) {
                            const rescale = @exp(old_max - new_max);
                            if (rescale != 1.0) {
                                const r_splat: F32xN = @splat(rescale);
                                var d: usize = 0;
                                while (d + vec_len <= head_dim) : (d += vec_len) {
                                    const ov: F32xN = out_ptr[d..][0..vec_len].*;
                                    out_ptr[d..][0..vec_len].* = ov * r_splat;
                                }
                                while (d < head_dim) : (d += 1) {
                                    out_ptr[d] *= rescale;
                                }
                                row_sum[qi_local] *= rescale;
                            }
                        }

                        const score_row = score_tile[qi_local * cur_bkv ..][0..cur_bkv];
                        const block_sum = primitives.expSubtractAndSum(score_row, new_max);
                        row_max[qi_local] = new_max;
                        row_sum[qi_local] += block_sum;
                    }

                    // V projection: out[bq, hd] += score_tile[bq, bkv] @
                    // V_block[bkv, hd].  Replaces the per-Q-row axpy loop
                    // (which reloaded every V row cur_bq times from L1/L2)
                    // with a register-tiled GEMM that reuses each V panel
                    // across all Q rows in the block.  beta=1.0 because the
                    // per-row rescale above has already prepared `out`.
                    const out_block = output[bh_offset + q_start * head_dim ..][0 .. cur_bq * head_dim];
                    const v_block = V[bh_offset + kv_start * head_dim ..][0 .. cur_bkv * head_dim];
                    gemm.sgemmSequential(cur_bq, head_dim, cur_bkv, 1.0, score_tile[0 .. cur_bq * cur_bkv], v_block, 1.0, out_block);

                    kv_start = kv_end;
                }

                // Final normalization.
                for (0..cur_bq) |qi_local| {
                    const qi = q_start + qi_local;
                    if (row_sum[qi_local] != 0.0) {
                        const inv_sum = 1.0 / row_sum[qi_local];
                        const out_ptr = output[bh_offset + qi * head_dim ..][0..head_dim];
                        const inv_splat: F32xN = @splat(inv_sum);
                        var d: usize = 0;
                        while (d + vec_len <= head_dim) : (d += vec_len) {
                            const ov: F32xN = out_ptr[d..][0..vec_len].*;
                            out_ptr[d..][0..vec_len].* = ov * inv_splat;
                        }
                        while (d < head_dim) : (d += 1) {
                            out_ptr[d] *= inv_sum;
                        }
                    }
                }

                q_start = q_end;
            }
        }
    }

    return output;
}

pub fn crossAttentionHost(
    allocator: std.mem.Allocator,
    Q: []const f32,
    K: []const f32,
    V: []const f32,
    enc_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
    num_heads: usize,
    head_dim: usize,
) ![]f32 {
    const H = std.math.mul(usize, num_heads, head_dim) catch return error.InvalidAttentionShape;
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));

    const output = try allocator.alloc(f32, std.math.mul(usize, batch * dec_seq, H) catch return error.InvalidAttentionShape);
    @memset(output, 0.0);

    const scores = try allocator.alloc(f32, dec_seq * enc_seq);
    defer allocator.free(scores);

    for (0..batch) |b| {
        for (0..num_heads) |h| {
            for (0..dec_seq) |qi| {
                const q_ptr = Q[(b * dec_seq + qi) * H + h * head_dim ..].ptr;
                const row = scores[qi * enc_seq ..][0..enc_seq];
                for (0..enc_seq) |ki| {
                    const k_ptr = K[(b * enc_seq + ki) * H + h * head_dim ..].ptr;
                    row[ki] = if (enc_mask[b * enc_seq + ki] == 0)
                        -std.math.inf(f32)
                    else
                        primitives.dotPtrs(q_ptr, k_ptr, head_dim) * scale;
                }
                softmaxInPlace(row);
            }

            for (0..dec_seq) |qi| {
                const out_ptr = output[(b * dec_seq + qi) * H + h * head_dim ..].ptr;
                const row = scores[qi * enc_seq ..][0..enc_seq];
                for (0..enc_seq) |vi| {
                    const w = row[vi];
                    if (w == 0.0) continue;
                    const v_ptr = V[(b * enc_seq + vi) * H + h * head_dim ..].ptr;
                    primitives.axpyPtrs(w, v_ptr, out_ptr, head_dim);
                }
            }
        }
    }

    return output;
}

// Per-(batch, head) work unit for the disentangled attention.  A single job
// fully computes the output slice `output[(b * seq_len + qi) * H + head_off ..
// + head_dim]` for all qi in [0, seq_len).
const DebertaJobCtx = struct {
    Q: []const f32,
    K: []const f32,
    V: []const f32,
    Q_r: []const f32,
    K_r: []const f32,
    mask: []const i64,
    output: []f32,
    score_tile: []f32, // per-job scratch [BLOCK_Q * BLOCK_KV]
    q_pack: []f32,
    k_pack: []f32,
    v_pack: []f32,
    out_pack: []f32,
    row_max: []f32,
    row_sum: []f32,
    seq_len: usize,
    H: usize,
    head_dim: usize,
    scale: f32,
    b: usize,
    head_off: usize,
    err: ?anyerror = null,

    fn run(raw: *anyopaque) void {
        const ctx: *DebertaJobCtx = @ptrCast(@alignCast(raw));
        debertaDisentangledHead(ctx);
    }
};

fn debertaDisentangledHead(ctx: *DebertaJobCtx) void {
    const seq_len = ctx.seq_len;
    const H = ctx.H;
    const head_dim = ctx.head_dim;
    const head_off = ctx.head_off;
    const b = ctx.b;
    const scale = ctx.scale;

    const bq_max = @min(BLOCK_Q, seq_len);
    const bkv_max = @min(BLOCK_KV, seq_len);
    const mask_base = b * seq_len;
    var all_keys_valid = true;
    for (ctx.mask[mask_base..][0..seq_len]) |m| {
        if (m == 0) {
            all_keys_valid = false;
            break;
        }
    }

    for (0..seq_len) |ki| {
        const k_src = ctx.K[(b * seq_len + ki) * H + head_off ..][0..head_dim];
        const v_src = ctx.V[(b * seq_len + ki) * H + head_off ..][0..head_dim];
        @memcpy(ctx.k_pack[ki * head_dim ..][0..head_dim], k_src);
        @memcpy(ctx.v_pack[ki * head_dim ..][0..head_dim], v_src);
    }

    var q_start: usize = 0;
    while (q_start < seq_len) {
        const q_end = @min(q_start + bq_max, seq_len);
        const cur_bq = q_end - q_start;

        for (0..cur_bq) |qi_local| {
            const qi = q_start + qi_local;
            ctx.row_max[qi_local] = -std.math.inf(f32);
            ctx.row_sum[qi_local] = 0.0;

            const src = ctx.Q[(b * seq_len + qi) * H + head_off ..][0..head_dim];
            @memcpy(ctx.q_pack[qi_local * head_dim ..][0..head_dim], src);
        }
        @memset(ctx.out_pack[0 .. cur_bq * head_dim], 0.0);

        var kv_start: usize = 0;
        while (kv_start < seq_len) {
            const kv_end = @min(kv_start + bkv_max, seq_len);
            const cur_bkv = kv_end - kv_start;
            const k_block = ctx.k_pack[kv_start * head_dim ..][0 .. cur_bkv * head_dim];
            const v_block = ctx.v_pack[kv_start * head_dim ..][0 .. cur_bkv * head_dim];

            // Content-to-content scores: Q @ K^T.  The relative DeBERTa
            // terms are index-shifted by (qi, ki), so they stay in the exact
            // scalar loop below while the dense term uses the SGEMM kernel.
            gemm.sgemmTransBSequential(
                cur_bq,
                cur_bkv,
                head_dim,
                scale,
                ctx.q_pack[0 .. cur_bq * head_dim],
                k_block,
                0.0,
                ctx.score_tile[0 .. cur_bq * cur_bkv],
            );

            for (0..cur_bq) |qi_local| {
                const qi = q_start + qi_local;
                const rel_base = qi + seq_len - 1;
                const q_ptr = ctx.q_pack[qi_local * head_dim ..].ptr;
                const score_row = ctx.score_tile[qi_local * cur_bkv ..][0..cur_bkv];
                for (0..cur_bkv) |ki_local| {
                    const ki = kv_start + ki_local;
                    if (!all_keys_valid and ctx.mask[mask_base + ki] == 0) {
                        score_row[ki_local] = -std.math.inf(f32);
                        continue;
                    }

                    const rel_idx = rel_base - ki;
                    const k_ptr = k_block[ki_local * head_dim ..].ptr;
                    const q_r_ptr = ctx.Q_r[rel_idx * H + head_off ..].ptr;
                    const k_r_ptr = ctx.K_r[rel_idx * H + head_off ..].ptr;
                    const rel_score = primitives.dotPtrs(q_ptr, k_r_ptr, head_dim) +
                        primitives.dotPtrs(q_r_ptr, k_ptr, head_dim);
                    score_row[ki_local] += rel_score * scale;
                }
            }

            for (0..cur_bq) |qi_local| {
                const out_row = ctx.out_pack[qi_local * head_dim ..][0..head_dim];
                const score_row = ctx.score_tile[qi_local * cur_bkv ..][0..cur_bkv];

                var block_max: f32 = -std.math.inf(f32);
                for (0..cur_bkv) |ki_local| {
                    const score = score_row[ki_local];
                    if (score > block_max) block_max = score;
                }

                const old_max = ctx.row_max[qi_local];
                const new_max = @max(old_max, block_max);
                if (new_max == -std.math.inf(f32)) {
                    @memset(score_row, 0.0);
                    continue;
                }

                if (ctx.row_sum[qi_local] != 0.0) {
                    const rescale = @exp(old_max - new_max);
                    if (rescale != 1.0) {
                        const r_splat: F32xN = @splat(rescale);
                        var d: usize = 0;
                        while (d + vec_len <= head_dim) : (d += vec_len) {
                            const ov: F32xN = out_row[d..][0..vec_len].*;
                            out_row[d..][0..vec_len].* = ov * r_splat;
                        }
                        while (d < head_dim) : (d += 1) out_row[d] *= rescale;
                        ctx.row_sum[qi_local] *= rescale;
                    }
                }

                const block_sum = primitives.expSubtractAndSum(score_row, new_max);
                ctx.row_max[qi_local] = new_max;
                ctx.row_sum[qi_local] += block_sum;
            }

            gemm.sgemmSequential(
                cur_bq,
                head_dim,
                cur_bkv,
                1.0,
                ctx.score_tile[0 .. cur_bq * cur_bkv],
                v_block,
                1.0,
                ctx.out_pack[0 .. cur_bq * head_dim],
            );

            kv_start = kv_end;
        }

        for (0..cur_bq) |qi_local| {
            const qi = q_start + qi_local;
            const dst = ctx.output[(b * seq_len + qi) * H + head_off ..][0..head_dim];
            const src = ctx.out_pack[qi_local * head_dim ..][0..head_dim];
            if (ctx.row_sum[qi_local] != 0.0) {
                const inv_sum = 1.0 / ctx.row_sum[qi_local];
                const inv_splat: F32xN = @splat(inv_sum);
                var d: usize = 0;
                while (d + vec_len <= head_dim) : (d += vec_len) {
                    const sv: F32xN = src[d..][0..vec_len].*;
                    dst[d..][0..vec_len].* = sv * inv_splat;
                }
                while (d < head_dim) : (d += 1) dst[d] = src[d] * inv_sum;
            } else {
                @memset(dst, 0.0);
            }
        }

        q_start = q_end;
    }
}

pub fn debertaDisentangledAttentionHost(
    allocator: std.mem.Allocator,
    Q: []const f32,
    K: []const f32,
    V: []const f32,
    Q_r: []const f32,
    K_r: []const f32,
    mask: []const i64,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
) ![]f32 {
    if (batch == 0 or seq_len == 0) {
        return allocator.alloc(f32, 0);
    }
    if (num_heads == 0 or head_dim == 0) return error.InvalidAttentionShape;

    const H = std.math.mul(usize, num_heads, head_dim) catch return error.InvalidAttentionShape;
    const tokens = std.math.mul(usize, batch, seq_len) catch return error.InvalidAttentionShape;
    const output_len = std.math.mul(usize, tokens, H) catch return error.InvalidAttentionShape;
    if (Q.len < output_len or K.len < output_len or V.len < output_len) return error.InvalidAttentionShape;

    const rel_len = std.math.sub(usize, std.math.mul(usize, seq_len, 2) catch return error.InvalidAttentionShape, 1) catch return error.InvalidAttentionShape;
    const rel_expected = std.math.mul(usize, rel_len, H) catch return error.InvalidAttentionShape;
    if (Q_r.len < rel_expected or K_r.len < rel_expected) return error.InvalidAttentionShape;

    const mask_expected = std.math.mul(usize, batch, seq_len) catch return error.InvalidAttentionShape;
    if (mask.len < mask_expected) return error.InvalidAttentionShape;

    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)) * 3.0);

    const output = try allocator.alloc(f32, output_len);
    errdefer allocator.free(output);
    @memset(output, 0.0);

    const total_jobs = std.math.mul(usize, batch, num_heads) catch return error.InvalidAttentionShape;

    // Pick a worker count.  Each (b, head) job does
    //   3 * seq_len^2 * head_dim FMAs + seq_len^2 V mixing FMAs
    // so threading only pays for itself once total work clears the same
    // ~4M-FMA floor sgemm uses.
    const seq_len_u64: u64 = @intCast(seq_len);
    const head_dim_u64: u64 = @intCast(head_dim);
    const jobs_u64: u64 = @intCast(total_jobs);
    const seq_sq = std.math.mul(u64, seq_len_u64, seq_len_u64) catch return error.InvalidAttentionShape;
    const seq_sq_head = std.math.mul(u64, seq_sq, head_dim_u64) catch return error.InvalidAttentionShape;
    const work_per_job = std.math.mul(u64, 3, seq_sq_head) catch return error.InvalidAttentionShape;
    const total_work = std.math.mul(u64, work_per_job, jobs_u64) catch return error.InvalidAttentionShape;
    const workers = pickAttentionWorkers(total_jobs, total_work);
    const bq_max = @min(BLOCK_Q, seq_len);
    const bkv_max = @min(BLOCK_KV, seq_len);
    const score_tile_len = std.math.mul(usize, bq_max, bkv_max) catch return error.InvalidAttentionShape;
    const q_pack_len = std.math.mul(usize, bq_max, head_dim) catch return error.InvalidAttentionShape;
    const kv_pack_len = std.math.mul(usize, seq_len, head_dim) catch return error.InvalidAttentionShape;
    const out_pack_len = std.math.mul(usize, bq_max, head_dim) catch return error.InvalidAttentionShape;
    const row_len = bq_max;
    var scratch_per_worker = score_tile_len;
    scratch_per_worker = std.math.add(usize, scratch_per_worker, q_pack_len) catch return error.InvalidAttentionShape;
    scratch_per_worker = std.math.add(usize, scratch_per_worker, kv_pack_len) catch return error.InvalidAttentionShape;
    scratch_per_worker = std.math.add(usize, scratch_per_worker, kv_pack_len) catch return error.InvalidAttentionShape;
    scratch_per_worker = std.math.add(usize, scratch_per_worker, out_pack_len) catch return error.InvalidAttentionShape;
    scratch_per_worker = std.math.add(usize, scratch_per_worker, row_len) catch return error.InvalidAttentionShape;
    scratch_per_worker = std.math.add(usize, scratch_per_worker, row_len) catch return error.InvalidAttentionShape;

    if (workers <= 1) {
        // Single-threaded fast path: one scratch region, sequential walk.
        const scratch = try allocator.alloc(f32, scratch_per_worker);
        defer allocator.free(scratch);

        var off: usize = 0;
        const score_tile = scratch[off..][0..score_tile_len];
        off += score_tile_len;
        const q_pack = scratch[off..][0..q_pack_len];
        off += q_pack_len;
        const k_pack = scratch[off..][0..kv_pack_len];
        off += kv_pack_len;
        const v_pack = scratch[off..][0..kv_pack_len];
        off += kv_pack_len;
        const out_pack = scratch[off..][0..out_pack_len];
        off += out_pack_len;
        const row_max = scratch[off..][0..row_len];
        off += row_len;
        const row_sum = scratch[off..][0..row_len];

        var ctx = DebertaJobCtx{
            .Q = Q,
            .K = K,
            .V = V,
            .Q_r = Q_r,
            .K_r = K_r,
            .mask = mask,
            .output = output,
            .score_tile = score_tile,
            .q_pack = q_pack,
            .k_pack = k_pack,
            .v_pack = v_pack,
            .out_pack = out_pack,
            .row_max = row_max,
            .row_sum = row_sum,
            .seq_len = seq_len,
            .H = H,
            .head_dim = head_dim,
            .scale = scale,
            .b = 0,
            .head_off = 0,
        };
        for (0..batch) |b| {
            for (0..num_heads) |h| {
                ctx.b = b;
                ctx.head_off = h * head_dim;
                debertaDisentangledHead(&ctx);
            }
        }
        return output;
    }

    // Threaded path: one scratch buffer per worker, contiguous (b, head) chunks
    // assigned per worker.  Each worker walks its chunk sequentially, reusing
    // its scratch across the chunk so we don't pay per-job allocation cost.
    const scratch_total = try std.math.mul(usize, workers, scratch_per_worker);
    const scratch = try allocator.alloc(f32, scratch_total);
    defer allocator.free(scratch);

    const ChunkCtx = struct {
        outer: *const DebertaJobCtx,
        b_start: usize,
        b_end: usize, // (b, head) range expressed as flat indices into [0, total_jobs)
        scratch: []f32,
        num_heads: usize,
        head_dim: usize,

        fn run(raw: *anyopaque) void {
            const cc: *@This() = @ptrCast(@alignCast(raw));
            var local = cc.outer.*;
            var off: usize = 0;
            local.score_tile = cc.scratch[off..][0..local.score_tile.len];
            off += local.score_tile.len;
            local.q_pack = cc.scratch[off..][0..local.q_pack.len];
            off += local.q_pack.len;
            local.k_pack = cc.scratch[off..][0..local.k_pack.len];
            off += local.k_pack.len;
            local.v_pack = cc.scratch[off..][0..local.v_pack.len];
            off += local.v_pack.len;
            local.out_pack = cc.scratch[off..][0..local.out_pack.len];
            off += local.out_pack.len;
            local.row_max = cc.scratch[off..][0..local.row_max.len];
            off += local.row_max.len;
            local.row_sum = cc.scratch[off..][0..local.row_sum.len];
            var idx = cc.b_start;
            while (idx < cc.b_end) : (idx += 1) {
                local.b = idx / cc.num_heads;
                local.head_off = (idx % cc.num_heads) * cc.head_dim;
                debertaDisentangledHead(&local);
            }
        }
    };

    var contexts: [pool.max_workers]ChunkCtx = undefined;
    var jobs: [pool.max_workers]pool.Job = undefined;

    const outer_ctx = DebertaJobCtx{
        .Q = Q,
        .K = K,
        .V = V,
        .Q_r = Q_r,
        .K_r = K_r,
        .mask = mask,
        .output = output,
        .score_tile = scratch[0..score_tile_len], // slice lengths reused by worker split.
        .q_pack = scratch[0..q_pack_len],
        .k_pack = scratch[0..kv_pack_len],
        .v_pack = scratch[0..kv_pack_len],
        .out_pack = scratch[0..out_pack_len],
        .row_max = scratch[0..row_len],
        .row_sum = scratch[0..row_len],
        .seq_len = seq_len,
        .H = H,
        .head_dim = head_dim,
        .scale = scale,
        .b = 0,
        .head_off = 0,
    };

    var num_jobs: usize = 0;
    for (0..workers) |w| {
        const start = (total_jobs * w) / workers;
        const end = if (w + 1 == workers) total_jobs else (total_jobs * (w + 1)) / workers;
        if (start >= end) continue;
        contexts[num_jobs] = .{
            .outer = &outer_ctx,
            .b_start = start,
            .b_end = end,
            .scratch = scratch[w * scratch_per_worker ..][0..scratch_per_worker],
            .num_heads = num_heads,
            .head_dim = head_dim,
        };
        jobs[num_jobs] = .{ .fn_ptr = ChunkCtx.run, .ctx = @ptrCast(&contexts[num_jobs]) };
        num_jobs += 1;
    }
    pool.dispatchJobs(jobs[0..num_jobs]);

    return output;
}

/// Threading threshold for disentangled attention.  Same 4M-FMA floor sgemm
/// uses, but `min_jobs_per_worker` is 1 since each (b, head) job is already
/// substantial work (`3 * S^2 * D` FMAs).
fn pickAttentionWorkers(total_jobs: usize, total_work: u64) usize {
    const flops_floor: u64 = 4_000_000;
    if (total_work < flops_floor) return 1;
    if (builtin.single_threaded) return 1;
    const cpu = pool.cachedCpuCount();
    return @max(1, @min(@min(pool.max_workers, cpu), total_jobs));
}

pub fn channelAttention(
    allocator: std.mem.Allocator,
    out: []f32,
    qkv: []const f32,
    batch: usize,
    seq_len: usize,
    dim: usize,
    groups: usize,
) !void {
    const channels_per_group = dim / groups;
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(seq_len)));
    const scores_buf = try allocator.alloc(f32, channels_per_group * channels_per_group);
    defer allocator.free(scores_buf);
    const scale_splat: F32xN = @splat(scale);

    for (0..batch) |b| {
        for (0..groups) |g| {
            const group_offset = g * channels_per_group;
            const score_slice = scores_buf[0 .. channels_per_group * channels_per_group];
            @memset(score_slice, 0.0);

            for (0..seq_len) |n| {
                const base = ((b * seq_len + n) * dim * 3) + group_offset;
                const q_row = qkv[base..][0..channels_per_group];
                const k_row = qkv[base + dim ..][0..channels_per_group];

                for (0..channels_per_group) |qc| {
                    const q_splat: F32xN = @splat(q_row[qc]);
                    const score_row = score_slice[qc * channels_per_group ..][0..channels_per_group];
                    var kc: usize = 0;
                    while (kc + vec_len <= channels_per_group) : (kc += vec_len) {
                        const kv: F32xN = k_row[kc..][0..vec_len].*;
                        const sv: F32xN = score_row[kc..][0..vec_len].*;
                        score_row[kc..][0..vec_len].* = sv + q_splat * kv;
                    }
                    while (kc < channels_per_group) : (kc += 1) {
                        score_row[kc] += q_row[qc] * k_row[kc];
                    }
                }
            }

            for (0..channels_per_group) |qc| {
                const score_row = score_slice[qc * channels_per_group ..][0..channels_per_group];
                var kc: usize = 0;
                while (kc + vec_len <= channels_per_group) : (kc += vec_len) {
                    const sv: F32xN = score_row[kc..][0..vec_len].*;
                    score_row[kc..][0..vec_len].* = sv * scale_splat;
                }
                while (kc < channels_per_group) : (kc += 1) {
                    score_row[kc] *= scale;
                }

                var max_val = score_row[0];
                for (score_row[1..]) |value| {
                    if (value > max_val) max_val = value;
                }
                var sum: f32 = 0.0;
                for (0..channels_per_group) |col| {
                    score_row[col] = @exp(score_row[col] - max_val);
                    sum += score_row[col];
                }
                if (sum != 0.0) {
                    const inv = 1.0 / sum;
                    const inv_splat: F32xN = @splat(inv);
                    kc = 0;
                    while (kc + vec_len <= channels_per_group) : (kc += vec_len) {
                        const sv: F32xN = score_row[kc..][0..vec_len].*;
                        score_row[kc..][0..vec_len].* = sv * inv_splat;
                    }
                    while (kc < channels_per_group) : (kc += 1) {
                        score_row[kc] *= inv;
                    }
                }
            }

            for (0..seq_len) |n| {
                const value_base = ((b * seq_len + n) * dim * 3) + 2 * dim + group_offset;
                const value_row = qkv[value_base..][0..channels_per_group];
                const dst = (b * seq_len + n) * dim + group_offset;
                for (0..channels_per_group) |qc| {
                    const score_row = score_slice[qc * channels_per_group ..][0..channels_per_group];
                    out[dst + qc] = primitives.dot(score_row, value_row);
                }
            }
        }
    }
}

fn softmaxInPlace(row: []f32) void {
    primitives.softmaxRow(row);
}

fn allowsFutureAttention(attn_or_mask: ?[]const u8, total_sequence_len: usize, query_pos: usize, key_pos: usize) bool {
    const mask = attn_or_mask orelse return false;
    if (query_pos >= total_sequence_len or key_pos >= total_sequence_len) return false;
    const idx = query_pos * total_sequence_len + key_pos;
    if (idx >= mask.len) return false;
    return mask[idx] != 0;
}

fn allowsPastAttention(sliding_window: usize, query_pos: usize, key_pos: usize) bool {
    if (key_pos > query_pos) return false;
    if (sliding_window == 0) return true;
    return query_pos - key_pos < sliding_window;
}

fn channelAttentionReference(
    out: []f32,
    qkv: []const f32,
    batch: usize,
    seq_len: usize,
    dim: usize,
    groups: usize,
) void {
    const channels_per_group = dim / groups;
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(seq_len)));

    for (0..batch) |b| {
        for (0..groups) |g| {
            const group_offset = g * channels_per_group;
            for (0..channels_per_group) |qc| {
                for (0..seq_len) |n| {
                    const dst = (b * seq_len + n) * dim + group_offset + qc;
                    out[dst] = 0.0;
                }
            }

            for (0..channels_per_group) |qc| {
                var scores: [32]f32 = undefined;
                std.debug.assert(channels_per_group <= scores.len);
                for (0..channels_per_group) |kc| {
                    var acc: f32 = 0.0;
                    for (0..seq_len) |n| {
                        const base = ((b * seq_len + n) * dim * 3) + group_offset;
                        acc += qkv[base + qc] * qkv[base + dim + kc];
                    }
                    scores[kc] = acc * scale;
                }

                var max_val = scores[0];
                for (scores[1..channels_per_group]) |value| {
                    if (value > max_val) max_val = value;
                }
                var sum: f32 = 0.0;
                for (0..channels_per_group) |kc| {
                    scores[kc] = @exp(scores[kc] - max_val);
                    sum += scores[kc];
                }
                const inv = if (sum == 0.0) 0.0 else 1.0 / sum;
                for (0..channels_per_group) |kc| {
                    scores[kc] *= inv;
                }

                for (0..seq_len) |n| {
                    const base = ((b * seq_len + n) * dim * 3) + 2 * dim + group_offset;
                    var acc: f32 = 0.0;
                    for (0..channels_per_group) |vc| {
                        acc += scores[vc] * qkv[base + vc];
                    }
                    out[(b * seq_len + n) * dim + group_offset + qc] = acc;
                }
            }
        }
    }
}

test "t5RelativePositionBucket matches small exact buckets" {
    try std.testing.expectEqual(@as(usize, 0), t5RelativePositionBucket(0, 32, 128, false));
    try std.testing.expectEqual(@as(usize, 1), t5RelativePositionBucket(-1, 32, 128, false));
    try std.testing.expectEqual(@as(usize, 17), t5RelativePositionBucket(1, 32, 128, true));
}

test "ropeCore applies expected rotation" {
    var data = [_]f32{ 1.0, 0.0, 0.0, 1.0 };
    const positions = [_]usize{1};
    ropeCore(&data, &positions, 4, 4, 10000.0, 1.0, false);
    try std.testing.expectApproxEqAbs(@cos(@as(f32, 1.0)), data[0], 1e-5);
    try std.testing.expectApproxEqAbs(@sin(@as(f32, 1.0)), data[2], 1e-5);
}

test "channelAttention preserves identical channels" {
    const allocator = std.testing.allocator;
    const qkv = [_]f32{
        1, 1, 1, 1, 2, 2,
        1, 1, 1, 1, 2, 2,
    };
    var out = [_]f32{ 0, 0, 0, 0 };
    try channelAttention(allocator, &out, &qkv, 1, 2, 2, 1);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), out[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), out[1], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), out[2], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), out[3], 1e-5);
}

test "channelAttention matches scalar reference" {
    const allocator = std.testing.allocator;
    const qkv = [_]f32{
        0.1, 0.4, 0.7, 0.2, 0.3, 0.8, 0.6, 0.5, 0.9, 0.0, 0.2, 0.1,
        0.5, 0.2, 0.1, 0.6, 0.4, 0.3, 0.7, 0.8, 0.9, 0.2, 0.5, 0.4,
        0.9, 0.3, 0.2, 0.7, 0.5, 0.1, 0.4, 0.6, 0.3, 0.8, 0.7, 0.2,
    };
    var actual = @as([12]f32, @splat(0));
    var expected = @as([12]f32, @splat(0));
    try channelAttention(allocator, &actual, &qkv, 1, 3, 4, 2);
    channelAttentionReference(&expected, &qkv, 1, 3, 4, 2);
    for (actual, expected) |got, want| {
        try std.testing.expectApproxEqAbs(want, got, 1e-5);
    }
}

test "crossAttentionHost matches masked hand calculation" {
    const allocator = std.testing.allocator;
    const q = [_]f32{ 1, 0, 0, 1 };
    const k = [_]f32{ 1, 0, 0, 1 };
    const v = [_]f32{ 2, 4, 6, 8 };
    const mask = [_]i64{ 1, 0 };
    const out = try crossAttentionHost(allocator, &q, &k, &v, &mask, 1, 2, 2, 1, 2);
    defer allocator.free(out);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 2, 4, 2, 4 }, out);
}

test "flashCausalAttentionHost matches reference for CLIP-text shape" {
    const allocator = std.testing.allocator;
    const batch: usize = 1;
    const seq_len: usize = 77;
    const num_heads: usize = 12;
    const head_dim: usize = 64;
    const H = num_heads * head_dim;

    const q = try allocator.alloc(f32, batch * seq_len * H);
    defer allocator.free(q);
    const k = try allocator.alloc(f32, batch * seq_len * H);
    defer allocator.free(k);
    const v = try allocator.alloc(f32, batch * seq_len * H);
    defer allocator.free(v);
    var prng = std.Random.DefaultPrng.init(0x77AA_CC11);
    for (q) |*x| x.* = (prng.random().float(f32) - 0.5) * 0.5;
    for (k) |*x| x.* = (prng.random().float(f32) - 0.5) * 0.5;
    for (v) |*x| x.* = (prng.random().float(f32) - 0.5);

    const out = try flashCausalAttentionHost(allocator, q, k, v, null, null, 0, batch, seq_len, seq_len, 0, 0, num_heads, num_heads, head_dim);
    defer allocator.free(out);

    // Spot-check via a reference implementation that materializes
    // softmax(Q@K^T/sqrt(d) + causal_mask) @ V per (b, h, qi).
    const ref = try allocator.alloc(f32, batch * seq_len * H);
    defer allocator.free(ref);
    @memset(ref, 0);
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    for (0..batch) |b| {
        for (0..num_heads) |h| {
            for (0..seq_len) |qi| {
                // Build full row of scores.
                var scores: [128]f32 = undefined;
                std.debug.assert(seq_len <= scores.len);
                var max_s: f32 = -std.math.inf(f32);
                for (0..seq_len) |ki| {
                    if (ki > qi) {
                        scores[ki] = -std.math.inf(f32);
                    } else {
                        var dot: f32 = 0;
                        for (0..head_dim) |d| dot += q[(b * seq_len + qi) * H + h * head_dim + d] * k[(b * seq_len + ki) * H + h * head_dim + d];
                        scores[ki] = dot * scale;
                        if (scores[ki] > max_s) max_s = scores[ki];
                    }
                }
                var sum: f32 = 0;
                for (0..seq_len) |ki| {
                    if (scores[ki] == -std.math.inf(f32)) {
                        scores[ki] = 0;
                    } else {
                        scores[ki] = @exp(scores[ki] - max_s);
                        sum += scores[ki];
                    }
                }
                const inv = if (sum > 0) 1.0 / sum else 0.0;
                for (0..head_dim) |d| {
                    var acc: f32 = 0;
                    for (0..seq_len) |ki| acc += scores[ki] * inv * v[(b * seq_len + ki) * H + h * head_dim + d];
                    ref[(b * seq_len + qi) * H + h * head_dim + d] = acc;
                }
            }
        }
    }
    for (out, ref) |x, y| try std.testing.expect(@abs(x - y) < 5e-4);
}

test "flashAttentionHost returns zero rows when every key is masked" {
    const allocator = std.testing.allocator;
    const q = [_]f32{ 1, 0, 0, 1 };
    const k = [_]f32{ 1, 0, 0, 1 };
    const v = [_]f32{ 2, 4, 6, 8 };
    const mask = [_]i64{ 0, 0 };
    const out = try flashAttentionHost(allocator, &q, &k, &v, null, &mask, 1, 2, 1, 2);
    defer allocator.free(out);
    for (out) |value| {
        try std.testing.expect(!std.math.isNan(value));
        try std.testing.expectEqual(@as(f32, 0.0), value);
    }
}

test "flashCausalAttentionHost returns zero row when offsets mask all keys" {
    const allocator = std.testing.allocator;
    const q = [_]f32{1};
    const k = [_]f32{1};
    const v = [_]f32{7};
    const out = try flashCausalAttentionHost(allocator, &q, &k, &v, null, null, 0, 1, 1, 1, 0, 1, 1, 1, 1);
    defer allocator.free(out);
    try std.testing.expect(!std.math.isNan(out[0]));
    try std.testing.expectEqual(@as(f32, 0.0), out[0]);
}

test "flashCausalAttentionHost uses local bias columns with kv offset" {
    const allocator = std.testing.allocator;
    const q = [_]f32{0};
    const k = [_]f32{ 0, 0 };
    const v = [_]f32{ 10, 20 };
    const bias = [_]f32{ 0, 1 };
    const out = try flashCausalAttentionHost(allocator, &q, &k, &v, &bias, null, 0, 1, 1, 2, 5, 4, 1, 1, 1);
    defer allocator.free(out);
    const e0: f32 = 1.0;
    const e1: f32 = @exp(@as(f32, 1.0));
    const expected = (e0 * 10.0 + e1 * 20.0) / (e0 + e1);
    try std.testing.expectApproxEqAbs(expected, out[0], 1e-5);
}

// Reference materialized softmax(Q @ K^T / sqrt(d)) @ V with optional bias
// and per-position mask, mirroring the legacy sdpaOp body in
// pkg/inference/src/ops/native_compute.zig (head-major Q/K/V layout:
// `[batch, num_heads, seq, head_dim]`).  Used to verify flashAttentionHost.
fn referenceSdpaForTest(
    allocator: std.mem.Allocator,
    Q: []const f32,
    K: []const f32,
    V: []const f32,
    bias: ?[]const f32,
    mask: []const i64,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
) ![]f32 {
    const H = num_heads * head_dim;
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    const out = try allocator.alloc(f32, batch * seq_len * H);
    @memset(out, 0.0);
    const scores = try allocator.alloc(f32, seq_len * seq_len);
    defer allocator.free(scores);
    for (0..batch) |b| {
        for (0..num_heads) |h| {
            const bh_idx = b * num_heads + h;
            const bh_offset = bh_idx * seq_len * head_dim;
            for (0..seq_len) |qi| {
                const q_ptr = Q[bh_offset + qi * head_dim ..].ptr;
                for (0..seq_len) |ki| {
                    const k_ptr = K[bh_offset + ki * head_dim ..].ptr;
                    scores[qi * seq_len + ki] = primitives.dotPtrs(q_ptr, k_ptr, head_dim) * scale;
                }
            }
            if (bias) |bdata| {
                const off = if (bdata.len == batch * num_heads * seq_len * seq_len)
                    bh_idx * seq_len * seq_len
                else
                    h * seq_len * seq_len;
                for (0..seq_len * seq_len) |i| scores[i] += bdata[off + i];
            }
            for (0..seq_len) |qi| {
                for (0..seq_len) |ki| {
                    if (mask[b * seq_len + ki] == 0) {
                        scores[qi * seq_len + ki] = -std.math.inf(f32);
                    }
                }
            }
            // Numerically-stable row softmax in place.
            for (0..seq_len) |qi| {
                const row = scores[qi * seq_len ..][0..seq_len];
                var m: f32 = -std.math.inf(f32);
                for (row) |s| if (s > m) {
                    m = s;
                };
                var sum: f32 = 0.0;
                for (row) |*s| {
                    s.* = if (m == -std.math.inf(f32)) 0.0 else @exp(s.* - m);
                    sum += s.*;
                }
                if (sum > 0) for (row) |*s| {
                    s.* /= sum;
                };
            }
            for (0..seq_len) |qi| {
                const out_ptr = out[bh_offset + qi * head_dim ..].ptr;
                for (0..seq_len) |vi| {
                    const w = scores[qi * seq_len + vi];
                    if (w == 0.0) continue;
                    const v_ptr = V[bh_offset + vi * head_dim ..].ptr;
                    primitives.axpyPtrs(w, v_ptr, out_ptr, head_dim);
                }
            }
        }
    }
    return out;
}

test "flashAttentionHost matches materialized softmax (small, no bias)" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0xfeed);
    const rand = prng.random();

    const batch: usize = 2;
    const seq: usize = 70;
    const num_heads: usize = 4;
    const head_dim: usize = 32;
    const total_elems = batch * seq * num_heads * head_dim;

    const Q = try allocator.alloc(f32, total_elems);
    defer allocator.free(Q);
    const K = try allocator.alloc(f32, total_elems);
    defer allocator.free(K);
    const V = try allocator.alloc(f32, total_elems);
    defer allocator.free(V);
    for (Q) |*x| x.* = rand.float(f32) * 2 - 1;
    for (K) |*x| x.* = rand.float(f32) * 2 - 1;
    for (V) |*x| x.* = rand.float(f32) * 2 - 1;

    const mask = try allocator.alloc(i64, batch * seq);
    defer allocator.free(mask);
    for (mask, 0..) |*m, i| m.* = if (i % 13 == 0) 0 else 1; // sparse padding

    const ref = try referenceSdpaForTest(allocator, Q, K, V, null, mask, batch, seq, num_heads, head_dim);
    defer allocator.free(ref);
    const flash = try flashAttentionHost(allocator, Q, K, V, null, mask, batch, seq, num_heads, head_dim);
    defer allocator.free(flash);

    for (ref, flash) |a, b| try std.testing.expect(@abs(a - b) < 1e-4);
}

test "flashAttentionHost matches materialized softmax (large, with bias)" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0xfade);
    const rand = prng.random();

    // Spans multiple BLOCK_Q (Q tiles) and BLOCK_KV (KV tiles) so the
    // streaming-softmax rescale path is exercised.
    const batch: usize = 1;
    const seq: usize = 130;
    const num_heads: usize = 2;
    const head_dim: usize = 16;
    const total_elems = batch * seq * num_heads * head_dim;

    const Q = try allocator.alloc(f32, total_elems);
    defer allocator.free(Q);
    const K = try allocator.alloc(f32, total_elems);
    defer allocator.free(K);
    const V = try allocator.alloc(f32, total_elems);
    defer allocator.free(V);
    for (Q) |*x| x.* = rand.float(f32) * 2 - 1;
    for (K) |*x| x.* = rand.float(f32) * 2 - 1;
    for (V) |*x| x.* = rand.float(f32) * 2 - 1;

    const bias = try allocator.alloc(f32, num_heads * seq * seq);
    defer allocator.free(bias);
    for (bias) |*x| x.* = rand.float(f32) * 0.5;

    const mask = try allocator.alloc(i64, batch * seq);
    defer allocator.free(mask);
    for (mask) |*m| m.* = 1;

    const ref = try referenceSdpaForTest(allocator, Q, K, V, bias, mask, batch, seq, num_heads, head_dim);
    defer allocator.free(ref);
    const flash = try flashAttentionHost(allocator, Q, K, V, bias, mask, batch, seq, num_heads, head_dim);
    defer allocator.free(flash);

    for (ref, flash) |a, b| try std.testing.expect(@abs(a - b) < 5e-4);
}

test "debertaDisentangledAttentionHost matches scalar sanity case" {
    const allocator = std.testing.allocator;
    const q = [_]f32{
        1, 0,
        0, 1,
    };
    const k = [_]f32{
        1, 0,
        0, 1,
    };
    const v = [_]f32{
        2, 4,
        6, 8,
    };
    const q_r = [_]f32{
        1, 0,
        1, 0,
        1, 0,
    };
    const k_r = [_]f32{
        0, 1,
        0, 1,
        0, 1,
    };
    const mask = [_]i64{ 1, 1 };
    const out = try debertaDisentangledAttentionHost(allocator, &q, &k, &v, &q_r, &k_r, &mask, 1, 2, 1, 2);
    defer allocator.free(out);

    // Outputs should be convex combinations of V rows.
    try std.testing.expect(out[0] >= 2.0 and out[0] <= 6.0);
    try std.testing.expect(out[1] >= 4.0 and out[1] <= 8.0);
    try std.testing.expect(out[2] >= 2.0 and out[2] <= 6.0);
    try std.testing.expect(out[3] >= 4.0 and out[3] <= 8.0);
}

// Scalar reference for the threaded path test below.  Mirrors the original
// (pre-thread) loop body so any drift in the production path is caught.
fn debertaDisentangledReference(
    allocator: std.mem.Allocator,
    Q: []const f32,
    K: []const f32,
    V: []const f32,
    Q_r: []const f32,
    K_r: []const f32,
    mask: []const i64,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
) ![]f32 {
    const H = num_heads * head_dim;
    const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)) * 3.0);
    const output = try allocator.alloc(f32, batch * seq_len * H);
    @memset(output, 0.0);
    const scores = try allocator.alloc(f32, seq_len * seq_len);
    defer allocator.free(scores);
    for (0..batch) |b| {
        for (0..num_heads) |h| {
            const head_off = h * head_dim;
            for (0..seq_len) |qi| {
                const q_ptr = Q[(b * seq_len + qi) * H + head_off ..].ptr;
                const row = scores[qi * seq_len ..][0..seq_len];
                for (0..seq_len) |ki| {
                    const rel_idx: usize = @intCast(@as(i64, @intCast(qi)) - @as(i64, @intCast(ki)) + @as(i64, @intCast(seq_len - 1)));
                    const k_ptr = K[(b * seq_len + ki) * H + head_off ..].ptr;
                    const q_r_ptr = Q_r[rel_idx * H + head_off ..].ptr;
                    const k_r_ptr = K_r[rel_idx * H + head_off ..].ptr;
                    var score = primitives.dotPtrs(q_ptr, k_ptr, head_dim);
                    score += primitives.dotPtrs(q_ptr, k_r_ptr, head_dim);
                    score += primitives.dotPtrs(q_r_ptr, k_ptr, head_dim);
                    row[ki] = if (mask[b * seq_len + ki] == 0)
                        -std.math.inf(f32)
                    else
                        score * scale;
                }
                softmaxInPlace(row);
            }
            for (0..seq_len) |qi| {
                const out_ptr = output[(b * seq_len + qi) * H + head_off ..].ptr;
                const row = scores[qi * seq_len ..][0..seq_len];
                for (0..seq_len) |vi| {
                    const w = row[vi];
                    if (w == 0.0) continue;
                    const v_ptr = V[(b * seq_len + vi) * H + head_off ..].ptr;
                    primitives.axpyPtrs(w, v_ptr, out_ptr, head_dim);
                }
            }
        }
    }
    return output;
}

test "debertaDisentangledAttentionHost threaded path matches scalar reference" {
    const allocator = std.testing.allocator;

    // Shape large enough that pickAttentionWorkers selects > 1 worker on
    // multi-core hosts.  3 * S^2 * D * (B*H) = 3*64*64*16*32 ~ 6.3M FMAs > 4M floor.
    const batch: usize = 2;
    const num_heads: usize = 4;
    const seq_len: usize = 16;
    const head_dim: usize = 8;
    const H = num_heads * head_dim;
    const num_rel = 2 * seq_len - 1;

    var prng = std.Random.DefaultPrng.init(0xDEBE17A);
    const rand = prng.random();

    const Q = try allocator.alloc(f32, batch * seq_len * H);
    defer allocator.free(Q);
    const K = try allocator.alloc(f32, batch * seq_len * H);
    defer allocator.free(K);
    const V = try allocator.alloc(f32, batch * seq_len * H);
    defer allocator.free(V);
    const Q_r = try allocator.alloc(f32, num_rel * H);
    defer allocator.free(Q_r);
    const K_r = try allocator.alloc(f32, num_rel * H);
    defer allocator.free(K_r);
    for (Q) |*x| x.* = rand.float(f32) - 0.5;
    for (K) |*x| x.* = rand.float(f32) - 0.5;
    for (V) |*x| x.* = rand.float(f32) - 0.5;
    for (Q_r) |*x| x.* = rand.float(f32) - 0.5;
    for (K_r) |*x| x.* = rand.float(f32) - 0.5;

    const mask = try allocator.alloc(i64, batch * seq_len);
    defer allocator.free(mask);
    for (mask, 0..) |*m, i| m.* = if (i % 5 == 4) 0 else 1;

    const ref = try debertaDisentangledReference(allocator, Q, K, V, Q_r, K_r, mask, batch, seq_len, num_heads, head_dim);
    defer allocator.free(ref);
    const got = try debertaDisentangledAttentionHost(allocator, Q, K, V, Q_r, K_r, mask, batch, seq_len, num_heads, head_dim);
    defer allocator.free(got);

    for (ref, got) |a, b| try std.testing.expect(@abs(a - b) < 1e-5);
}
