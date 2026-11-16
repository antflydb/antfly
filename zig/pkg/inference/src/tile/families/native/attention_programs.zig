// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
pub const source_0 = @import("antfly_tile").native_attention.cudaOnlineRow64();

const program = @import("../../native_program.zig");
pub const page_body = (@import("antfly_tile").layout.PageTable{ .unit = .token_bases }).native(.{ .wide = false, .page_size = "p.page_size", .block_count = "p.block_count" });
pub const metal_page = struct {
    pub const source_0 = program.metal("inline uint termite_attention_page_token(device const uint *block_table, constant termite_metal_paged_attention_params &p, uint logical_token) {\n" ++ page_body ++ "}\n");
};

const tile = @import("antfly_tile");
pub const cuda_relative = struct {
    pub const source_0 = program.render(
        \\__device__ __forceinline__ bool antfly_relative_attention_key_valid_f32(
        \\    const long long* mask,
        \\    const float* attn_bias,
        \\    unsigned int use_bias,
        \\    unsigned int b,
        \\    unsigned int head,
        \\    unsigned int qi,
        \\    unsigned int ki,
        \\    unsigned int seq_len,
        \\    unsigned int num_heads
        \\) {
        \\    if (use_bias != 0u) {
        \\        unsigned int bias_idx = ((b * num_heads + head) * seq_len + qi) * seq_len + ki;
        \\        return attn_bias[bias_idx] >= -1.0e8f;
        \\    }
        \\    return mask[b * seq_len + ki] != 0ll;
        \\}
        \\
        \\__device__ __forceinline__ float antfly_relative_attention_score_f32(
        \\    const float* q,
        \\    const float* k,
        \\    const float* q_r,
        \\    const float* k_r,
        \\    unsigned int b,
        \\    unsigned int qi,
        \\    unsigned int ki,
        \\    unsigned int head,
        \\    unsigned int seq_len,
        \\    unsigned int num_heads,
        \\    unsigned int head_dim
        \\) {
        \\    unsigned int hidden = num_heads * head_dim;
        \\    unsigned int head_off = head * head_dim;
        \\    unsigned int q_base = (b * seq_len + qi) * hidden + head_off;
        \\    unsigned int k_base = (b * seq_len + ki) * hidden + head_off;
        \\    unsigned int rel_base = (qi + seq_len - 1u - ki) * hidden + head_off;
        \\    float score = 0.0f;
        \\    for (unsigned int d = 0; d < head_dim; ++d) {
        \\        float qd = q[q_base + d];
        \\        float kd = k[k_base + d];
        \\{{relative_products}}
        \\    }
        \\    return score * rsqrtf((float)head_dim * 3.0f);
        \\}
        \\
    , .{
        .relative_products = tile.native_projection.accumulate(&.{ .{ .left = "qd", .right = "kd" }, .{ .left = "qd", .right = "k_r[rel_base + d]" }, .{ .left = "q_r[rel_base + d]", .right = "kd" } }, .combined, "score"),
    });
    pub const source_1 = program.render(
        \\__device__ __forceinline__ float antfly_attention_probability_vjp_dot_f32(
        \\    const float* v,
        \\    const float* d_out,
        \\    unsigned int b,
        \\    unsigned int qi,
        \\    unsigned int ki,
        \\    unsigned int head,
        \\    unsigned int seq_len,
        \\    unsigned int num_heads,
        \\    unsigned int head_dim
        \\) {
        \\    unsigned int hidden = num_heads * head_dim;
        \\    unsigned int head_off = head * head_dim;
        \\    unsigned int out_base = (b * seq_len + qi) * hidden + head_off;
        \\    unsigned int v_base = (b * seq_len + ki) * hidden + head_off;
        \\    float value = 0.0f;
        \\{{probability_dot}}
        \\    return value;
        \\}
        \\
    , .{
        .probability_dot = (tile.native_projection.Dot{ .integer = "unsigned int", .index = "d", .end = "head_dim", .left = "d_out[out_base + d]", .right = "v[v_base + d]", .accumulator = "value" }).native(),
    });
};

/// Relative and vision MMA schedules share online-softmax semantics; warp
/// widths and probability publication remain in their native schedules.
pub const cuda_tensor_online = struct {
    const online = tile.native_attention.Online;
    pub const source_0 =
        "const float alpha = old_max > kNegInf * 0.5f ? " ++ online.correction.renderNative(.{ .exp = "expf" }, .{ .old_max = "old_max", .new_max = "next_max" }, null) ++ " : 0.0f;\n" ++
        "const float beta = score > kNegInf * 0.5f ? " ++ online.probability.renderNative(.{ .exp = "expf" }, .{ .score = "score", .new_max = "next_max" }, null) ++ " : 0.0f;\n";
    pub const source_1 = "running_sum[m] = " ++ online.mass.renderNative(.{}, .{ .old_sum = "running_sum[m]", .correction = "alpha", .tile_sum = "tile_sum" }, null) ++ ";\n";
};
