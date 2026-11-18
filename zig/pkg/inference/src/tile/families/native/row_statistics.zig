// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Compatibility bindings for the existing CUDA/Metal shared reduction trees.
const Tree = @import("antfly_tile").native_statistics.Tree;
pub const metal = .{
    .rms_tree = Tree.native(.{ .integer = "uint", .lane = "tid", .threads = "tpg", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .planes = &.{.{ .partial = "shmem", .value = "mean_square" }} }),
    .moments_tree = Tree.native(.{ .integer = "uint", .lane = "tid", .threads = "width", .capacity = 256, .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .planes = &.{ .{ .partial = "sums", .value = "sum" }, .{ .partial = "sqs", .value = "sq" } } }),
};
pub const source_0 =
    "template<unsigned Planes>\n" ++
    "__device__ __forceinline__ void antfly_row_tree_statistics(\n" ++
    "    float (&values)[Planes], float* const (&partials)[Planes], unsigned threads = blockDim.x) {\n" ++
    "    const unsigned tid = threadIdx.x;\n" ++
    Tree.native(.{ .integer = "unsigned", .lane = "tid", .threads = "threads", .barrier = "__syncthreads();", .plane_count = "Planes", .planes = &.{.{ .partial = "partials[p]", .value = "values[p]" }} }) ++
    "}\n";

pub const projection = struct {
    pub const source_0 = Tree.native(.{
        .integer = "unsigned int",
        .lane = "tid",
        .threads = "256u",
        .barrier = "__syncthreads();",
        .initialize = false,
        .axes = &.{ .{ .name = "r", .extent = "ROWS_PER_BLOCK" }, .{ .name = "c", .extent = "COLS" } },
        .planes = &.{.{ .partial = "partial[r][c]", .value = "acc[r][c]" }},
    });
};

const metal_barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);";
fn metalTree(comptime lane: []const u8, comptime threads: []const u8, comptime planes: []const Tree.Plane, comptime operation: Tree.Operation) []const u8 {
    return Tree.native(.{ .integer = "uint", .lane = lane, .threads = threads, .barrier = metal_barrier, .planes = planes, .operation = operation });
}
pub const metal_direct = .{
    .pool_count = Tree.broadcast("shmem[0]", "count", metal_barrier),
    .pool_squares = metalTree("tid", "threads", &.{.{ .partial = "shmem", .value = "local_sq" }}, .sum),
    .embedding_peak = metalTree("t", "n", &.{.{ .partial = "scratch", .value = "peak" }}, .maximum),
    .embedding_sum = metalTree("t", "n", &.{.{ .partial = "scratch", .value = "sum" }}, .sum),
};
pub const metal_pair = .{ .rows_tree = native_stages.projection_rows_bf16, .pair_tree = metalTree("lane", "32u", &.{ .{ .partial = "partials_a", .value = "acc_a" }, .{ .partial = "partials_b", .value = "acc_b" } }, .sum) };
pub const metal_scalar = .{ .scalar_tree = metalTree("lane", "32u", &.{.{ .partial = "partials", .value = "acc" }}, .sum) };
pub const metal_storage = .{ .head_maximum = metalTree("tid.x", "tg_size.x", &.{.{ .partial = "tg_max", .value = "abs_val" }}, .maximum) };

fn cudaIncludeTree(comptime partial: []const u8, comptime threads: []const u8, comptime columns: ?[]const u8, comptime lane: []const u8, comptime maximum: bool) []const u8 {
    return Tree.native(.{ .integer = "unsigned int", .lane = lane, .threads = threads, .barrier = "__syncthreads();", .initialize = false, .operation = if (maximum) .maximum else .sum, .maximum = "fmaxf", .axes = if (columns) |count| &.{.{ .name = "c", .extent = count }} else &.{}, .planes = &.{.{ .partial = partial, .value = "" }} });
}
pub const cuda_include_column4 = struct {
    pub const source_0 = cudaIncludeTree("partial[c]", "256u", "4u", "tid", false);
};
pub const cuda_include_argmax = struct {
    pub const source_0 = cudaIncludeTree("partial[c]", "256u", "TERMITE_QUANT_ARGMAX_COLUMNS", "tid", false);
};
pub const cuda_include_scalar256 = struct {
    pub const source_0 = cudaIncludeTree("partial", "256u", null, "tid", false);
};
pub const cuda_include_scalar_block = struct {
    pub const source_0 = cudaIncludeTree("partial", "blockDim.x", null, "tid", false);
};
pub const cuda_include_block_attention = struct {
    pub const source_0 = cudaIncludeTree("scratch", "128u", null, "tid", false);
    pub const source_1 = @This().source_0;
};
pub const cuda_include_score_softmax = struct {
    pub const source_0 = cudaIncludeTree("reduce", "blockDim.x", null, "lane", true);
    pub const source_1 = cudaIncludeTree("reduce", "blockDim.x", null, "lane", false);
};

// Shared tree stages for native phases with caller-owned publication.
pub const native_stages = .{
    .projection_rows_bf16 = Tree.native(.{ .integer = "uint", .lane = "lane", .threads = "32u", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .initialize = false, .publication_barrier = false, .axes = &.{.{ .name = "r", .extent = "p.rows", .unroll = false }}, .planes = &.{.{ .partial = "partials", .offset = "r * 32u", .value = "" }} }),
    .gqa_shared_dot = Tree.native(.{ .integer = "uint", .lane = "tid", .threads = "NT", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .initialize = false, .publication_barrier = false, .planes = &.{.{ .partial = "partials", .value = "" }} }),
    .attention_sum_separate = Tree.native(.{ .integer = "unsigned int", .lane = "tid", .threads = "128u", .barrier = "__syncthreads();", .add = "__fadd_rn", .initialize = false, .publication_barrier = false, .planes = &.{.{ .partial = "scratch", .value = "" }} }),
    .selection_score_sum = Tree.native(.{ .integer = "unsigned int", .lane = "tid", .threads = "blockDim.x", .barrier = "__syncthreads();", .initialize = false, .publication_barrier = false, .planes = &.{.{ .partial = "partial", .value = "" }} }),

    .training_tensor_sum = Tree.native(.{ .integer = "unsigned int", .lane = "threadIdx.x", .threads = "blockDim.x", .barrier = "__syncthreads();", .initialize = false, .publication_barrier = false, .operation = .sum, .planes = &.{.{ .partial = "partial", .value = "" }} }),
    .projection_tile_sum = Tree.native(.{ .integer = "unsigned int", .lane = "tid", .threads = "256u", .barrier = "__syncthreads();", .initialize = false, .publication_barrier = false, .operation = .sum, .planes = &.{.{ .partial = "partial", .value = "" }} }),
    .relative_probability_vjp_sum = Tree.native(.{ .integer = "unsigned int", .lane = "tid", .threads = "blockDim.x", .barrier = "__syncthreads();", .initialize = false, .publication_barrier = false, .operation = .sum, .planes = &.{.{ .partial = "reduction", .value = "" }} }),
    .attention_row_maximum_stages = Tree.native(.{ .integer = "uint", .lane = "lid", .threads = "width", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .initialize = false, .publication_barrier = false, .operation = .maximum, .planes = &.{.{ .partial = "partials", .value = "" }} }),
    .relative_four_row_maximum = Tree.native(.{ .integer = "uint", .lane = "lid", .threads = "width", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .initialize = false, .publication_barrier = false, .operation = .maximum, .planes = &.{ .{ .partial = "partials", .value = "" }, .{ .partial = "partials", .value = "", .offset = "width" }, .{ .partial = "partials", .value = "", .offset = "2u * width" }, .{ .partial = "partials", .value = "", .offset = "3u * width" } } }),
    .attention_tile_maximum = Tree.native(.{ .integer = "uint", .lane = "tid", .threads = "256u", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .initialize = false, .publication_barrier = false, .operation = .maximum, .planes = &.{.{ .partial = "partial", .value = "" }} }),
    .attention_row_sum_stages = Tree.native(.{ .integer = "uint", .lane = "lid", .threads = "width", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .initialize = false, .publication_barrier = false, .operation = .sum, .planes = &.{.{ .partial = "partials", .value = "" }} }),
    .relative_four_row_sum = Tree.native(.{ .integer = "uint", .lane = "lid", .threads = "width", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .initialize = false, .publication_barrier = false, .operation = .sum, .planes = &.{ .{ .partial = "partials", .value = "" }, .{ .partial = "partials", .value = "", .offset = "width" }, .{ .partial = "partials", .value = "", .offset = "2u * width" }, .{ .partial = "partials", .value = "", .offset = "3u * width" } } }),
    .attention_tile_sum = Tree.native(.{ .integer = "uint", .lane = "tid", .threads = "256u", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .initialize = false, .publication_barrier = false, .operation = .sum, .planes = &.{.{ .partial = "partial", .value = "" }} }),
    .masked_loss_sum = Tree.native(.{ .integer = "uint", .lane = "tid", .threads = "threads", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .initialize = false, .publication_barrier = false, .operation = .sum, .planes = &.{.{ .partial = "shmem", .value = "" }} }),
    .masked_loss_moments = Tree.native(.{ .integer = "uint", .lane = "tid", .threads = "threads", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .initialize = false, .publication_barrier = false, .operation = .sum, .planes = &.{ .{ .partial = "shmem", .value = "" }, .{ .partial = "shmem", .value = "", .offset = "threads" } } }),
    .bounded_backward_moments = Tree.native(.{ .integer = "uint", .lane = "tid", .threads = "width", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .initialize = false, .publication_barrier = false, .operation = .sum, .capacity = 256, .planes = &.{ .{ .partial = "a", .value = "" }, .{ .partial = "b", .value = "" } } }),
    .channel_row_maximum = Tree.native(.{ .integer = "unsigned int", .lane = "tid", .threads = "256u", .barrier = "__syncthreads();", .initialize = false, .publication_barrier = false, .operation = .maximum, .maximum = "fmaxf", .planes = &.{.{ .partial = "scratch", .value = "" }} }),
    .cross_row_maximum = Tree.native(.{ .integer = "unsigned int", .lane = "tid", .threads = "blockDim.x", .barrier = "__syncthreads();", .initialize = false, .publication_barrier = false, .operation = .maximum, .maximum = "fmaxf", .planes = &.{.{ .partial = "scratch", .value = "" }} }),
    .channel_row_sum = Tree.native(.{ .integer = "unsigned int", .lane = "tid", .threads = "256u", .barrier = "__syncthreads();", .initialize = false, .publication_barrier = false, .operation = .sum, .planes = &.{.{ .partial = "scratch", .value = "" }} }),
    .cross_row_sum = Tree.native(.{ .integer = "unsigned int", .lane = "tid", .threads = "blockDim.x", .barrier = "__syncthreads();", .initialize = false, .publication_barrier = false, .operation = .sum, .planes = &.{.{ .partial = "scratch", .value = "" }} }),
};

pub const cuda_selection = .{
    .selection_score_sum = native_stages.selection_score_sum,
    .selection_index_maximum = Tree.native(.{
        .integer = "unsigned int",
        .lane = "tid",
        .threads = "blockDim.x",
        .barrier = "__syncthreads();",
        .initialize = false,
        .publication_barrier = false,
        .operation = .maximum,
        .planes = &.{.{ .partial = "block_values", .value = "" }},
        .indexed = .{ .indices = "block_indices", .rhs_value = "rhs_score", .rhs_index = "rhs_token", .lhs_value = "lhs_score", .lhs_index = "lhs_token" },
    }),
};
pub const cuda_indexed_maximum = struct {
    pub const source_0 = Tree.native(.{
        .integer = "unsigned int",
        .lane = "tid",
        .threads = "blockDim.x",
        .barrier = "__syncthreads();",
        .initialize = false,
        .publication_barrier = false,
        .operation = .maximum,
        .planes = &.{.{ .partial = "best_values", .value = "" }},
        .indexed = .{ .indices = "best_indices", .rhs_value = "other_value", .rhs_index = "other_index" },
    });
};
pub const metal_indexed_maximum = struct {
    pub const source_0 = @import("../../native_program.zig").metal(Tree.native(.{
        .integer = "uint",
        .lane = "tid",
        .threads = "uint(tg_size)",
        .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);",
        .initialize = false,
        .publication_barrier = false,
        .operation = .maximum,
        .planes = &.{.{ .partial = "sh_vals", .value = "" }},
        .indexed = .{ .indices = "sh_ids", .rhs_value = "other", .rhs_index = "other_idx", .guard_lane = "uint(tid)", .neighbor_lane = "uint(tid)", .initial_shift = "1u", .compact_update = true },
    }));
};

pub const bf16_narrow_exports = struct {
    pub const source_0 =
        \\extern "C" __global__ void termite_embedding_gemma2_attention_normalized_softmax_bf16(
        \\    __nv_bfloat16* probs, const float* scores, const long long* mask,
        \\    unsigned int sequence, unsigned int query_base, unsigned int rows,
        \\    unsigned int batch_index, unsigned int tile_capacity) {
        \\    antfly_score_softmax_bf16<AntflyScoreMask::rows_and_keys, AntflyProbabilityPublication::normalized_bf16>(
        \\        probs, scores, mask, sequence, query_base, rows, batch_index, tile_capacity, sequence);
        \\}
        \\extern "C" __global__ void antfly_score_softmax_rows_keys_legacy_bf16_warp256(
        \\    __nv_bfloat16* probs, const float* scores, const long long* mask,
        \\    unsigned sequence, unsigned query_base, unsigned rows,
        \\    unsigned batch_index, unsigned tile_capacity) {
        \\    antfly_score_softmax_bf16<AntflyScoreMask::rows_and_keys, AntflyProbabilityPublication::legacy_beta_bf16, true>(
        \\        probs, scores, mask, sequence, query_base, rows, batch_index, tile_capacity, sequence);
        \\}
        \\extern "C" __global__ void antfly_score_softmax_prefix_bf16_warp256(
        \\    __nv_bfloat16* probs, const float* scores, unsigned sequence,
        \\    unsigned valid_keys, unsigned rows, unsigned tile_capacity) {
        \\    antfly_score_softmax_bf16<AntflyScoreMask::prefix_keys, AntflyProbabilityPublication::normalized_bf16, true>(
        \\        probs, scores, nullptr, sequence, 0u, rows, 0u, tile_capacity, valid_keys);
        \\}
        \\
    ;
};

const CudaShuffle = @import("antfly_tile").native_statistics.CudaShuffle;
pub const cuda_relative_shuffle = struct {
    pub const source_0 = (CudaShuffle{ .value = "maximum", .loop = "for(int offset=16;offset>0;offset>>=1)", .operation = .maximum }).native();
    pub const source_1 = (CudaShuffle{ .value = "denominator", .loop = "for(int offset=16;offset>0;offset>>=1)", .operation = .sum, .add = "__fadd_rn" }).native();
};
pub const cuda_ragged_shuffle = struct {
    pub const source_0 = (CudaShuffle{ .value = "v", .offset = "d", .loop = "for (int d=16; d; d>>=1) ", .operation = .sum }).native();
    pub const source_1 = (CudaShuffle{ .value = "v", .offset = "d", .loop = "for (int d=16; d; d>>=1) ", .operation = .maximum, .maximum = "max" }).native();
};
