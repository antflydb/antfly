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

// Fused Chunker-Embedder model module.
//
// Layers on top of a ModernBERT encoder:
//   1. Boundary head  – 2-layer MLP → [batch*seq_len, 2] logits
//   2. Embedding head – optional linear projection → [batch*seq_len, embedding_dim]
//   3. Late-chunking pool – CPU mean-pool per chunk span → [batch*max_chunks, embedding_dim]
//   4. Decode boundaries – CPU post-processing of boundary logits → []ChunkSpan

const std = @import("std");
const modern_bert = @import("../architectures/modern_bert.zig");
const ops_mod = @import("../ops/ops.zig");
const ComputeBackend = ops_mod.ComputeBackend;
const CT = ops_mod.CT;
const fused_chunker_splade = @import("fused_chunker_splade.zig");
pub const SpladeConfig = fused_chunker_splade.SpladeConfig;

pub const artifact_family_version = "fused_chunker_embedder/v1alpha1";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

pub const Config = struct {
    // ModernBERT base
    vocab_size: u32 = 50368,
    hidden_size: u32 = 768,
    num_hidden_layers: u32 = 22,
    num_attention_heads: u32 = 12,
    intermediate_size: u32 = 1152,
    max_position_embeddings: u32 = 8192,
    global_rope_theta: f32 = 160000.0,
    local_rope_theta: f32 = 10000.0,
    global_attn_every_n_layers: u32 = 3,
    local_attention_window: u32 = 128,
    layer_norm_eps: f32 = 1e-5,

    // Boundary head
    boundary_mlp_dim: u32 = 256,
    num_boundary_labels: u32 = 2,

    // Embedding head
    embedding_dim: u32 = 768, // equal to hidden_size → no projection matrix used
    normalize_output: bool = true,

    // Inference knobs
    min_chunk_tokens: u32 = 32,
    max_chunk_tokens: u32 = 512,
    boundary_threshold: f32 = 0.5,

    // SPLADE sparse embedding head (optional)
    enable_splade: bool = false,
    splade_config: SpladeConfig = .{},

    pub fn modernBertConfig(self: Config) modern_bert.Config {
        return .{
            .vocab_size = self.vocab_size,
            .hidden_size = self.hidden_size,
            .num_hidden_layers = self.num_hidden_layers,
            .num_attention_heads = self.num_attention_heads,
            .intermediate_size = self.intermediate_size,
            .max_position_embeddings = self.max_position_embeddings,
            .global_rope_theta = self.global_rope_theta,
            .local_rope_theta = self.local_rope_theta,
            .global_attn_every_n_layers = self.global_attn_every_n_layers,
            .local_attention_window = self.local_attention_window,
            .layer_norm_eps = self.layer_norm_eps,
        };
    }
};

// ---------------------------------------------------------------------------
// ForwardResult
// ---------------------------------------------------------------------------

pub const ForwardResult = struct {
    boundary_logits: []f32,
    token_embeddings: []f32,

    pub fn deinit(self: *ForwardResult, allocator: std.mem.Allocator) void {
        allocator.free(self.boundary_logits);
        allocator.free(self.token_embeddings);
        self.* = undefined;
    }
};

// ---------------------------------------------------------------------------
// Public forward entry points
// ---------------------------------------------------------------------------

/// Full forward pass: runs the encoder, boundary head, and embedding head.
///
/// boundary_logits  — owned [batch * seq_len * num_boundary_labels] f32
/// token_embeddings — owned [batch * seq_len * embedding_dim] f32
pub fn forward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
) !ForwardResult {
    const total = batch * seq_len;
    const H: usize = @intCast(config.hidden_size);

    // 1. ModernBERT encoder — hidden CT [total, H]
    const hidden_ct = try modern_bert.forwardCT(
        cb,
        allocator,
        config.modernBertConfig(),
        input_ids,
        attention_mask,
        batch,
        seq_len,
    );
    defer cb.free(hidden_ct);

    // 2. Boundary head — CT [total, num_boundary_labels]
    const bl_ct = try boundaryHeadCT(cb, config, hidden_ct, total, H);
    defer cb.free(bl_ct);

    // 3. Embedding head — CT [total, embedding_dim].
    //    embeddingHeadCT always returns a freshly-owned CT (even in the
    //    identity path) so both defers are safe.
    const emb_ct = try embeddingHeadCT(cb, allocator, config, hidden_ct, total, H);
    defer cb.free(emb_ct);

    // 4. Materialise to f32.
    const boundary_logits = try cb.toFloat32(bl_ct, allocator);
    errdefer allocator.free(boundary_logits);

    const token_embeddings = try cb.toFloat32(emb_ct, allocator);
    errdefer allocator.free(token_embeddings);

    return .{
        .boundary_logits = boundary_logits,
        .token_embeddings = token_embeddings,
    };
}

/// Boundary-only forward (skips embedding head — faster when only chunk
/// segmentation is needed, not retrieval embeddings).
///
/// Returns owned [batch * seq_len * num_boundary_labels] f32.
pub fn forwardBoundaryOnly(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    const total = batch * seq_len;
    const H: usize = @intCast(config.hidden_size);

    const hidden_ct = try modern_bert.forwardCT(
        cb,
        allocator,
        config.modernBertConfig(),
        input_ids,
        attention_mask,
        batch,
        seq_len,
    );
    defer cb.free(hidden_ct);

    const bl_ct = try boundaryHeadCT(cb, config, hidden_ct, total, H);
    defer cb.free(bl_ct);

    return cb.toFloat32(bl_ct, allocator);
}

// ---------------------------------------------------------------------------
// Boundary head (ComputeBackend)
// ---------------------------------------------------------------------------
//
// Weight naming follows gopeft / HF safetensors convention:
//   fused_chunker_embedder/boundary_head/mlp_dense1/weight  [mlp_dim, H]
//   fused_chunker_embedder/boundary_head/mlp_dense1/bias    [mlp_dim]
//   fused_chunker_embedder/boundary_head/mlp_dense2/weight  [num_labels, mlp_dim]
//   fused_chunker_embedder/boundary_head/mlp_dense2/bias    [num_labels]
//
// hidden [total, H] → linear(H→mlp_dim) → gelu → linear(mlp_dim→num_labels)
//                  → logits [total, num_labels]

fn boundaryHeadCT(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    total: usize,
    hidden_dim: usize,
) !CT {
    const mlp_dim: usize = @intCast(config.boundary_mlp_dim);
    const num_labels: usize = @intCast(config.num_boundary_labels);

    const w1 = try cb.getWeight("fused_chunker_embedder/boundary_head/mlp_dense1/weight");
    defer cb.free(w1);
    const b1 = try cb.getWeight("fused_chunker_embedder/boundary_head/mlp_dense1/bias");
    defer cb.free(b1);

    const dense1 = try cb.linear(hidden, w1, b1, total, hidden_dim, mlp_dim);
    defer cb.free(dense1);

    const act = try cb.gelu(dense1);
    defer cb.free(act);

    const w2 = try cb.getWeight("fused_chunker_embedder/boundary_head/mlp_dense2/weight");
    defer cb.free(w2);
    const b2 = try cb.getWeight("fused_chunker_embedder/boundary_head/mlp_dense2/bias");
    defer cb.free(b2);

    return cb.linear(act, w2, b2, total, mlp_dim, num_labels);
}

// ---------------------------------------------------------------------------
// Embedding head (ComputeBackend)
// ---------------------------------------------------------------------------
//
// When embedding_dim == hidden_size no projection is needed.  To avoid
// aliasing hidden_ct (which the caller already defers-free), we perform a
// round-trip through f32 in that path so the returned CT is always freshly
// owned and independently free-able.
//
// Weight names (projection path only):
//   fused_chunker_embedder/embedding_head/proj/weight  [embedding_dim, H]
//   fused_chunker_embedder/embedding_head/proj/bias    [embedding_dim]

fn embeddingHeadCT(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    total: usize,
    hidden_dim: usize,
) !CT {
    const emb_dim: usize = @intCast(config.embedding_dim);

    if (emb_dim == hidden_dim) {
        // Identity path: copy through f32 to produce a distinct CT so the
        // caller can safely defer-free both hidden_ct and emb_ct.
        const data = try cb.toFloat32(hidden, allocator);
        defer allocator.free(data);
        return cb.fromFloat32(data);
    }

    const w = try cb.getWeight("fused_chunker_embedder/embedding_head/proj/weight");
    defer cb.free(w);
    const b = try cb.getWeight("fused_chunker_embedder/embedding_head/proj/bias");
    defer cb.free(b);

    return cb.linear(hidden, w, b, total, hidden_dim, emb_dim);
}

// ---------------------------------------------------------------------------
// Late-chunking pool (pure Zig, no ML backend)
// ---------------------------------------------------------------------------

/// Mean-pool token embeddings per chunk span, with optional L2 normalisation.
///
/// token_embeddings : [batch * seq_len * embed_dim] f32  (row-major)
/// chunk_starts     : [batch * max_chunks] u32
/// chunk_ends       : [batch * max_chunks] u32  (exclusive)
/// chunk_mask       : [batch * max_chunks] f32  (1.0 = valid, 0.0 = padding)
///
/// Returns owned [batch * max_chunks * embed_dim] f32.
/// Masked-out (padding) slots are left as zero.
pub fn lateChunkingPool(
    allocator: std.mem.Allocator,
    config: Config,
    token_embeddings: []const f32,
    chunk_starts: []const u32,
    chunk_ends: []const u32,
    chunk_mask: []const f32,
    batch: usize,
    seq_len: usize,
    max_chunks: usize,
) ![]f32 {
    const embed_dim: usize = @intCast(config.embedding_dim);

    const out = try allocator.alloc(f32, batch * max_chunks * embed_dim);
    @memset(out, 0);

    for (0..batch) |b| {
        for (0..max_chunks) |c| {
            const chunk_idx = b * max_chunks + c;
            if (chunk_mask[chunk_idx] == 0.0) continue;

            const start: usize = @intCast(chunk_starts[chunk_idx]);
            const end: usize = @intCast(chunk_ends[chunk_idx]);
            if (end <= start) continue;

            const count: f32 = @floatFromInt(end - start);
            const out_base = chunk_idx * embed_dim;

            // Accumulate token embeddings over the span [start, end).
            for (start..end) |tok| {
                const emb_base = (b * seq_len + tok) * embed_dim;
                for (0..embed_dim) |d| {
                    out[out_base + d] += token_embeddings[emb_base + d];
                }
            }

            // Mean.
            for (0..embed_dim) |d| {
                out[out_base + d] /= count;
            }

            // Optional L2 normalisation.
            if (config.normalize_output) {
                var sq_sum: f32 = 0.0;
                for (0..embed_dim) |d| {
                    sq_sum += out[out_base + d] * out[out_base + d];
                }
                const norm = @sqrt(sq_sum + 1e-12);
                if (norm > 1e-12) {
                    for (0..embed_dim) |d| {
                        out[out_base + d] /= norm;
                    }
                }
            }
        }
    }

    return out;
}

// ---------------------------------------------------------------------------
// Decode boundaries (pure Zig, no ML backend)
// ---------------------------------------------------------------------------

pub const ChunkSpan = struct {
    start_token: u32,
    end_token: u32, // exclusive
};

/// Convert 2-class boundary logits for a single sequence into chunk spans.
///
/// boundary_logits : [seq_len * 2] f32  (class 0 = non-boundary, class 1 = boundary)
/// attention_mask  : [seq_len] i32      (1 = real token, 0 = padding)
///
/// Returns an owned []ChunkSpan; caller is responsible for freeing.
pub fn decodeBoundaries(
    allocator: std.mem.Allocator,
    config: Config,
    boundary_logits: []const f32,
    attention_mask: []const i32,
    seq_len: usize,
) ![]ChunkSpan {
    const threshold = config.boundary_threshold;
    const min_tok: usize = @intCast(config.min_chunk_tokens);
    const max_tok: usize = @intCast(config.max_chunk_tokens);

    // Effective sequence length: last index where mask == 1, plus one.
    var valid_len: usize = 0;
    for (0..seq_len) |i| {
        if (attention_mask[i] > 0) valid_len = i + 1;
    }

    // Collect boundary positions via numerically-stable 2-class softmax.
    var boundary_positions = std.ArrayListUnmanaged(usize).empty;
    defer boundary_positions.deinit(allocator);

    for (0..valid_len) |i| {
        const logit0 = boundary_logits[i * 2 + 0];
        const logit1 = boundary_logits[i * 2 + 1];
        const max_logit = @max(logit0, logit1);
        const e0 = @exp(logit0 - max_logit);
        const e1 = @exp(logit1 - max_logit);
        const p = e1 / (e0 + e1);
        if (p > threshold) {
            try boundary_positions.append(allocator, i);
        }
    }

    // Convert boundary positions to chunk spans, enforcing min/max lengths.
    var spans = std.ArrayListUnmanaged(ChunkSpan).empty;
    errdefer spans.deinit(allocator);

    var chunk_start: usize = 0;

    for (boundary_positions.items) |bp| {
        const chunk_end = bp;
        if (chunk_end > chunk_start) {
            const chunk_len = chunk_end - chunk_start;
            if (chunk_len >= min_tok) {
                if (chunk_len > max_tok) {
                    try appendSplitSpans(allocator, &spans, chunk_start, chunk_end, max_tok);
                } else {
                    try spans.append(allocator, .{
                        .start_token = @intCast(chunk_start),
                        .end_token = @intCast(chunk_end),
                    });
                }
            }
        }
        chunk_start = bp;
    }

    // Final trailing chunk.
    if (valid_len > chunk_start) {
        const final_len = valid_len - chunk_start;
        if (final_len >= min_tok) {
            if (final_len > max_tok) {
                try appendSplitSpans(allocator, &spans, chunk_start, valid_len, max_tok);
            } else {
                try spans.append(allocator, .{
                    .start_token = @intCast(chunk_start),
                    .end_token = @intCast(valid_len),
                });
            }
        }
    }

    return spans.toOwnedSlice(allocator);
}

/// Split [region_start, region_end) into max_tok-width spans and append them.
fn appendSplitSpans(
    allocator: std.mem.Allocator,
    spans: *std.ArrayListUnmanaged(ChunkSpan),
    region_start: usize,
    region_end: usize,
    max_tok: usize,
) !void {
    var pos = region_start;
    while (pos + max_tok < region_end) {
        try spans.append(allocator, .{
            .start_token = @intCast(pos),
            .end_token = @intCast(pos + max_tok),
        });
        pos += max_tok;
    }
    if (pos < region_end) {
        try spans.append(allocator, .{
            .start_token = @intCast(pos),
            .end_token = @intCast(region_end),
        });
    }
}

// ---------------------------------------------------------------------------
// computeChunkSpladeVectors
// ---------------------------------------------------------------------------

/// Compute SPLADE sparse vectors for every valid chunk.
///
/// For each chunk c where chunk_mask[c] > 0, this function:
///   1. Extracts the token range [chunk_starts[c], chunk_ends[c]) from `hidden`.
///      Tokens are in batch-major order: token t of batch b is at row b*seq_len+t.
///   2. Calls computeSpladeActivation over those rows.
///   3. Appends the resulting [vocab_size] vector to the output.
///
/// Parameters:
///   hidden        : [batch * seq_len * hidden_size] f32 (output of ModernBERT encoder)
///   splade_weight : [vocab_size * hidden_size] f32 (SPLADE projection weight)
///   chunk_starts  : [batch * num_chunks] i32 — token-level start (inclusive)
///   chunk_ends    : [batch * num_chunks] i32 — token-level end (exclusive)
///   chunk_mask    : [batch * num_chunks] f32 — 1.0 = valid chunk
///
/// Returns owned [num_valid_chunks * vocab_size] f32 (caller frees).
/// Chunks are visited in row-major order over batch × num_chunks.
pub fn computeChunkSpladeVectors(
    allocator: std.mem.Allocator,
    config: Config,
    hidden: []const f32,
    splade_weight: []const f32,
    chunk_starts: []const i32,
    chunk_ends: []const i32,
    chunk_mask: []const f32,
    batch_size: usize,
    seq_len: usize,
    num_chunks: usize,
) ![]f32 {
    const H: usize = @intCast(config.hidden_size);
    const V: usize = @intCast(config.splade_config.vocab_size);
    const pooling = config.splade_config.pooling;

    // Count valid chunks first so we can allocate exactly.
    var num_valid: usize = 0;
    for (0..batch_size * num_chunks) |idx| {
        if (chunk_mask[idx] > 0.5) num_valid += 1;
    }

    const out = try allocator.alloc(f32, num_valid * V);
    errdefer allocator.free(out);
    @memset(out, 0);

    // Scratch buffer for a single chunk's SPLADE vector.
    const splade_buf = try allocator.alloc(f32, V);
    defer allocator.free(splade_buf);

    // Pre-allocate a scratch buffer large enough for any chunk's hidden states.
    // Upper bound: seq_len tokens × hidden_size floats.
    const max_chunk_tokens = seq_len;
    const chunk_hidden_scratch = try allocator.alloc(f32, max_chunk_tokens * H);
    defer allocator.free(chunk_hidden_scratch);

    var out_row: usize = 0;
    for (0..batch_size) |b| {
        for (0..num_chunks) |c| {
            const chunk_idx = b * num_chunks + c;
            if (chunk_mask[chunk_idx] <= 0.5) continue;

            const tok_start: usize = @intCast(@max(0, chunk_starts[chunk_idx]));
            const tok_end = @min(@as(usize, @intCast(@max(0, chunk_ends[chunk_idx]))), seq_len);
            std.debug.assert(tok_end <= seq_len);
            if (tok_end <= tok_start) {
                // Empty span — leave the output row as zeros and advance.
                out_row += 1;
                continue;
            }

            // Build a contiguous view of just this chunk's hidden states using
            // the pre-allocated scratch buffer (avoids per-chunk heap allocation).
            const total_chunk_tokens = tok_end - tok_start;
            const chunk_hidden = chunk_hidden_scratch[0 .. total_chunk_tokens * H];

            for (0..total_chunk_tokens) |t| {
                const src_row = b * seq_len + tok_start + t;
                const src_base = src_row * H;
                const dst_base = t * H;
                @memcpy(chunk_hidden[dst_base .. dst_base + H], hidden[src_base .. src_base + H]);
            }

            @memset(splade_buf, 0);
            try fused_chunker_splade.computeSpladeActivation(
                allocator,
                chunk_hidden,
                splade_weight,
                splade_buf,
                total_chunk_tokens,
                H,
                config.splade_config.vocab_size,
                pooling,
            );

            const dst_base = out_row * V;
            @memcpy(out[dst_base .. dst_base + V], splade_buf);
            out_row += 1;
        }
    }

    return out;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "decode boundaries smoke test" {
    const allocator = std.testing.allocator;

    const cfg = Config{
        .boundary_threshold = 0.5,
        .min_chunk_tokens = 2,
        .max_chunk_tokens = 10,
    };

    // 8-token sequence; position 3 is a strong boundary.
    const seq_len = 8;
    const attention_mask = [seq_len]i32{ 1, 1, 1, 1, 1, 1, 1, 1 };

    // logits layout: [logit0_pos0, logit1_pos0, logit0_pos1, logit1_pos1, ...]
    const logits = [seq_len * 2]f32{
        1.0,  -1.0, // pos 0: not boundary
        1.0,  -1.0, // pos 1: not boundary
        1.0,  -1.0, // pos 2: not boundary
        -5.0, 5.0,  // pos 3: strong boundary
        1.0,  -1.0, // pos 4: not boundary
        1.0,  -1.0, // pos 5: not boundary
        1.0,  -1.0, // pos 6: not boundary
        1.0,  -1.0, // pos 7: not boundary
    };

    const spans = try decodeBoundaries(allocator, cfg, &logits, &attention_mask, seq_len);
    defer allocator.free(spans);

    // [0,3) len=3 >= min=2 → kept; [3,8) len=5 >= min=2 → kept.
    try std.testing.expectEqual(@as(usize, 2), spans.len);
    try std.testing.expectEqual(@as(u32, 0), spans[0].start_token);
    try std.testing.expectEqual(@as(u32, 3), spans[0].end_token);
    try std.testing.expectEqual(@as(u32, 3), spans[1].start_token);
    try std.testing.expectEqual(@as(u32, 8), spans[1].end_token);
}

test "decode boundaries — short chunks are dropped" {
    const allocator = std.testing.allocator;

    const cfg = Config{
        .boundary_threshold = 0.5,
        .min_chunk_tokens = 4,
        .max_chunk_tokens = 512,
    };

    // Boundary at position 2 → [0,2) len=2 < min_chunk_tokens=4 → dropped.
    const seq_len = 6;
    const attention_mask = [seq_len]i32{ 1, 1, 1, 1, 1, 1 };

    const logits = [seq_len * 2]f32{
        1.0,  -1.0, // 0: not boundary
        1.0,  -1.0, // 1: not boundary
        -5.0, 5.0,  // 2: boundary
        1.0,  -1.0, // 3: not boundary
        1.0,  -1.0, // 4: not boundary
        1.0,  -1.0, // 5: not boundary
    };

    const spans = try decodeBoundaries(allocator, cfg, &logits, &attention_mask, seq_len);
    defer allocator.free(spans);

    // [0,2) dropped; [2,6) len=4 == min → kept.
    try std.testing.expectEqual(@as(usize, 1), spans.len);
    try std.testing.expectEqual(@as(u32, 2), spans[0].start_token);
    try std.testing.expectEqual(@as(u32, 6), spans[0].end_token);
}

test "decode boundaries — max_chunk splitting" {
    const allocator = std.testing.allocator;

    const cfg = Config{
        .boundary_threshold = 0.5,
        .min_chunk_tokens = 1,
        .max_chunk_tokens = 3,
    };

    // 9-token sequence, no boundaries → single 9-token chunk splits into 3×3.
    const seq_len = 9;
    const attention_mask = [seq_len]i32{ 1, 1, 1, 1, 1, 1, 1, 1, 1 };

    var logits: [seq_len * 2]f32 = undefined;
    for (0..seq_len) |i| {
        logits[i * 2 + 0] = 2.0;
        logits[i * 2 + 1] = -2.0;
    }

    const spans = try decodeBoundaries(allocator, cfg, &logits, &attention_mask, seq_len);
    defer allocator.free(spans);

    // [0,3), [3,6), [6,9)
    try std.testing.expectEqual(@as(usize, 3), spans.len);
    try std.testing.expectEqual(@as(u32, 0), spans[0].start_token);
    try std.testing.expectEqual(@as(u32, 3), spans[0].end_token);
    try std.testing.expectEqual(@as(u32, 3), spans[1].start_token);
    try std.testing.expectEqual(@as(u32, 6), spans[1].end_token);
    try std.testing.expectEqual(@as(u32, 6), spans[2].start_token);
    try std.testing.expectEqual(@as(u32, 9), spans[2].end_token);
}

test "late chunking pool — mean and L2 norm" {
    const allocator = std.testing.allocator;

    const cfg = Config{
        .embedding_dim = 2,
        .normalize_output = true,
    };

    // batch=1, seq_len=4, embed_dim=2, max_chunks=2
    // token_embeddings [4, 2]: tok0=[1,0], tok1=[3,0], tok2=[0,2], tok3=[0,4]
    const emb = [8]f32{ 1, 0, 3, 0, 0, 2, 0, 4 };

    const starts = [2]u32{ 0, 2 };
    const ends = [2]u32{ 2, 4 };
    const mask = [2]f32{ 1.0, 1.0 };

    const out = try lateChunkingPool(allocator, cfg, &emb, &starts, &ends, &mask, 1, 4, 2);
    defer allocator.free(out);

    // chunk0 mean = [2,0] → L2 norm → [1,0]
    // chunk1 mean = [0,3] → L2 norm → [0,1]
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), out[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), out[1], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), out[2], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), out[3], 1e-5);
}

test "late chunking pool — masked chunks are zero" {
    const allocator = std.testing.allocator;

    const cfg = Config{
        .embedding_dim = 2,
        .normalize_output = false,
    };

    // batch=1, seq_len=2, embed_dim=2, max_chunks=2
    // token_embeddings: tok0=[1,2], tok1=[3,4]
    const emb = [4]f32{ 1, 2, 3, 4 };
    const starts = [2]u32{ 0, 0 };
    const ends = [2]u32{ 2, 2 };
    const mask = [2]f32{ 1.0, 0.0 }; // second chunk masked out

    const out = try lateChunkingPool(allocator, cfg, &emb, &starts, &ends, &mask, 1, 2, 2);
    defer allocator.free(out);

    // chunk0: mean([1,2],[3,4]) = [2,3]; chunk1 masked → [0,0]
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), out[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), out[1], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), out[2], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), out[3], 1e-5);
}
