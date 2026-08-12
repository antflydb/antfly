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

//! Typed renderer for generated CUDA quant kernels.
//!
//! A `RenderPlan` owns the route identity and the complete launch contract.
//! Rendering composes common metadata, include directives, helper fragments,
//! and one kernel body. Production-bundle validation consumes the same helper
//! and body fragments, so standalone candidates and promoted kernels cannot
//! silently diverge in support code.

const std = @import("std");
const quant_matmul = @import("quant_matmul.zig");
const quant_kernel_op = @import("quant_kernel_op.zig");

pub const EpilogueKind = quant_kernel_op.Epilogue;

pub const KernelKind = enum {
    q4_k_small_batch_bias_gelu,
    q4_k_mmv,
    q4_0_mmv,
    q4_0_mm,
    q4_0_pair_mmv,
    q4_0_pair_activation_q8_1,
    q4_0_pair_activation_q8_1_e2b_6144,
    q4_0_pair_activation_q8_1_e2b_12288,
    // llama.cpp CUDA block_q8_1 activation layout: half2(d, raw f32 sum)
    // plus 32 int8 values. This is an operational CUDA matmul boundary, not
    // the serialized CPU-reference invariant d * sum(qs).
    // These remain SM89 diagnostic candidates and are intentionally distinct
    // from the historical zero-sum 36-byte activation layout above.
    q4_0_pair_activation_ggml_q8_1_e2b_6144,
    q4_0_pair_activation_ggml_q8_1_e2b_12288,
    q4_0_down_q8_1,
    q4_0_down_q8_1_e2b_6144,
    q4_0_down_q8_1_e2b_12288,
    q4_0_down_ggml_q8_1_e2b_6144,
    q4_0_down_ggml_q8_1_e2b_12288,
    // Exact F32 E2B FFN candidates. These deliberately retain the
    // handwritten F32 activation boundary and warp reduction order.
    q4_0_pair_activation_f32_e2b_6144_exact,
    q4_0_pair_activation_f32_e2b_12288_exact,
    q4_0_down_f32_e2b_6144_exact,
    q4_0_down_f32_e2b_12288_exact,
    q4_0_q8_1_argmax_e2b_tile8,
    q6_k_q8_1_argmax_k2560_tile8,
    q6_k_q8_1_argmax_k3840_tile8,
};

pub const GridMapping = enum {
    /// grid.x selects an output-column tile; grid.y selects an input row tile.
    output_cols_by_rows,
    /// grid.x selects an output-column tile. The route owns one input row.
    output_cols,
    /// grid.x flattens `(input row, output-column block)`.
    flattened_rows_by_output_blocks,
    /// grid.x flattens `(batch, query row, attention head)`.
    batch_query_heads,
    /// grid.x flattens `(batch, KV head, split-KV partition)`.
    batch_kv_heads_by_splits,
    /// grid.x flattens `(batch, query head, split-KV partition)`.
    batch_query_heads_by_splits,
    /// grid.x selects `(batch, query head)` and grid.y selects a contiguous
    /// output-dimension tile within that head.
    batch_query_heads_by_output_tiles,
    /// grid.x selects one query head and grid.y selects a contiguous query-row
    /// tile. The qualified Gemma 4 Flash prefill contract fixes batch to one.
    query_heads_by_query_tiles,
};

pub const DimensionConstraint = struct {
    min: u32 = 0,
    max: u32 = 0,
    multiple_of: u32 = 1,
    fixed: u32 = 0,

    pub fn validate(self: DimensionConstraint) !void {
        if (self.multiple_of == 0) return error.InvalidDimensionMultiple;
        if (self.max != 0 and self.min > self.max) return error.InvalidDimensionBounds;
        if (self.fixed != 0) {
            if (self.fixed < self.min) return error.FixedDimensionBelowMinimum;
            if (self.max != 0 and self.fixed > self.max) return error.FixedDimensionAboveMaximum;
            if (self.fixed % self.multiple_of != 0) return error.FixedDimensionMisaligned;
        }
    }
};

/// Exact launch ABI for a generated CUDA kernel. Unlike the generic compiler
/// schedule, this records specialized blocks such as the 640-thread fused FFN
/// route and its 32-output tile.
pub const LaunchMetadata = struct {
    grid: GridMapping,
    threads_per_block: u16,
    output_rows_per_block: u16,
    output_cols_per_block: u16,
    static_shared_memory_bytes: u32,
    /// All current generated CUDA families use static `__shared__` arrays.
    /// Future attention/tiling plans must record their launch-time allocation.
    dynamic_shared_memory_bytes: u32 = 0,
    rows: DimensionConstraint,
    input_dim: DimensionConstraint,
    output_dim: DimensionConstraint,

    pub fn validate(self: LaunchMetadata) !void {
        if (self.threads_per_block == 0 or self.threads_per_block > 1024 or self.threads_per_block % 32 != 0) {
            return error.InvalidCudaThreadCount;
        }
        if (self.output_rows_per_block == 0 or self.output_cols_per_block == 0) {
            return error.InvalidCudaOutputTile;
        }
        try self.rows.validate();
        try self.input_dim.validate();
        try self.output_dim.validate();
    }
};

pub const Route = struct {
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: EpilogueKind,
    dispatch: quant_matmul.DispatchKind,
};

pub const QuantDecodePrimitive = enum {
    q4_k_scale_min,
    q4_0_f16_nibbles,
    /// Historical Antfly 36-byte activation block. Bytes 2..3 are zero and
    /// Q4 nibbles are centered before DP4A.
    q4_0_q8_zp_dp4a,
    /// GGML block_q8_1: half2(d, sum) followed by 32 signed int8 values.
    q4_0_q8_1_dp4a,
    q6_k_q8_1_dp4a,
};

pub const ReductionPrimitive = enum {
    shared_tree,
    warp_then_shared,
    warp_sum_and_max,
    warp_sum_then_argmax_pairs,
};

pub const Q4KSmallBatchLowering = struct {
    decode: QuantDecodePrimitive = .q4_k_scale_min,
    reduction: ReductionPrimitive = .shared_tree,
    block_values: u16 = 256,
};

/// Parameterized lowering shared by q4_0 MMV, 8-row MM, and paired MMV.
/// `row_tile` and `projection_count` drive every accumulator/shared-memory
/// dimension and output write in the generated body.
pub const Q4ProjectionLowering = struct {
    decode: QuantDecodePrimitive = .q4_0_f16_nibbles,
    reduction: ReductionPrimitive = .warp_then_shared,
    row_tile: u8,
    projection_count: u8,
    columns_per_block: u8 = 4,
    warps_per_block: u8 = 8,

    pub fn validate(self: Q4ProjectionLowering) !void {
        if (self.decode != .q4_0_f16_nibbles) return error.InvalidQ4ProjectionDecode;
        if (self.reduction != .warp_then_shared) return error.InvalidQ4ProjectionReduction;
        if (self.row_tile != 1 and self.row_tile != 8) return error.InvalidQ4ProjectionRowTile;
        if (self.projection_count != 1 and self.projection_count != 2) return error.InvalidQ4ProjectionCount;
        if (self.row_tile > 1 and self.projection_count != 1) return error.UnsupportedQ4ProjectionShape;
        if (self.columns_per_block != 4 or self.warps_per_block != 8) return error.InvalidQ4ProjectionTopology;
    }
};

pub const PairActivationQ8Lowering = struct {
    decode: QuantDecodePrimitive = .q4_0_q8_zp_dp4a,
    reduction: ReductionPrimitive = .warp_sum_and_max,
    input_blocks: u16,
    output_blocks: u16,
    output_values_per_block: u8,
    warps_per_projection_group: u8,
};

pub const DownQ8Lowering = struct {
    decode: QuantDecodePrimitive = .q4_0_q8_zp_dp4a,
    reduction: ReductionPrimitive = .warp_then_shared,
    input_blocks: u16,
    columns_per_block: u8,
};

/// F32 pair-activation lowering that intentionally mirrors
/// `termite_linear_q4_0_pair_activation_f32_tile4_w4`. The generated route
/// must not move the activation boundary into a Q8_1 buffer.
pub const ExactFfnPairF32Lowering = struct {
    decode: QuantDecodePrimitive = .q4_0_f16_nibbles,
    reduction: ReductionPrimitive = .warp_then_shared,
    input_blocks: u16,
    columns_per_block: u8 = 4,
    warps_per_block: u8 = 4,
};

/// F32 down-projection lowering that intentionally mirrors
/// `termite_linear_q4_0_f32_tile4` after the normal activation-multiply
/// buffer has been materialized.
pub const ExactFfnDownF32Lowering = struct {
    decode: QuantDecodePrimitive = .q4_0_f16_nibbles,
    reduction: ReductionPrimitive = .warp_then_shared,
    input_blocks: u16,
    columns_per_block: u8 = 4,
    warps_per_block: u8 = 8,
};

pub const Q4Q8ArgmaxLowering = struct {
    decode: QuantDecodePrimitive = .q4_0_q8_zp_dp4a,
    reduction: ReductionPrimitive = .warp_sum_then_argmax_pairs,
    input_blocks: u16,
    columns_per_block: u8,
    warps_per_block: u8,
};

pub const Q6KQ8ArgmaxLowering = struct {
    decode: QuantDecodePrimitive = .q6_k_q8_1_dp4a,
    reduction: ReductionPrimitive = .warp_sum_then_argmax_pairs,
    input_blocks: u16,
    columns_per_block: u8,
    warps_per_block: u8,
};

pub const KernelLowering = union(enum) {
    q4_k_small_batch: Q4KSmallBatchLowering,
    q4_k_projection: Q4KSmallBatchLowering,
    q4_0_projection: Q4ProjectionLowering,
    q4_0_pair_activation_q8_1: PairActivationQ8Lowering,
    q4_0_down_q8_1: DownQ8Lowering,
    q4_0_pair_activation_f32_exact: ExactFfnPairF32Lowering,
    q4_0_down_f32_exact: ExactFfnDownF32Lowering,
    q4_0_q8_1_argmax: Q4Q8ArgmaxLowering,
    q6_k_q8_1_argmax: Q6KQ8ArgmaxLowering,

    pub fn validate(self: KernelLowering) !void {
        switch (self) {
            .q4_k_small_batch => |lowering| {
                if (lowering.decode != .q4_k_scale_min or lowering.reduction != .shared_tree or lowering.block_values != 256) {
                    return error.InvalidQ4KLowering;
                }
            },
            .q4_k_projection => |lowering| {
                if (lowering.decode != .q4_k_scale_min or lowering.reduction != .shared_tree or lowering.block_values != 256) {
                    return error.InvalidQ4KLowering;
                }
            },
            .q4_0_projection => |lowering| try lowering.validate(),
            .q4_0_pair_activation_q8_1 => |lowering| {
                if ((lowering.decode != .q4_0_q8_zp_dp4a and lowering.decode != .q4_0_q8_1_dp4a) or
                    lowering.reduction != .warp_sum_and_max or
                    lowering.input_blocks == 0 or lowering.output_blocks == 0 or
                    lowering.output_values_per_block != 32 or lowering.warps_per_projection_group == 0)
                {
                    return error.InvalidPairActivationQ8Lowering;
                }
            },
            .q4_0_down_q8_1 => |lowering| {
                if ((lowering.decode != .q4_0_q8_zp_dp4a and lowering.decode != .q4_0_q8_1_dp4a) or
                    lowering.reduction != .warp_then_shared or
                    lowering.input_blocks == 0 or
                    (lowering.columns_per_block != 1 and lowering.columns_per_block != 4))
                {
                    return error.InvalidDownQ8Lowering;
                }
            },
            .q4_0_pair_activation_f32_exact => |lowering| {
                if (lowering.decode != .q4_0_f16_nibbles or lowering.reduction != .warp_then_shared or
                    lowering.input_blocks == 0 or lowering.columns_per_block != 4 or lowering.warps_per_block != 4)
                {
                    return error.InvalidExactFfnPairF32Lowering;
                }
            },
            .q4_0_down_f32_exact => |lowering| {
                if (lowering.decode != .q4_0_f16_nibbles or lowering.reduction != .warp_then_shared or
                    lowering.input_blocks == 0 or lowering.columns_per_block != 4 or lowering.warps_per_block != 8)
                {
                    return error.InvalidExactFfnDownF32Lowering;
                }
            },
            .q4_0_q8_1_argmax => |lowering| {
                if (lowering.decode != .q4_0_q8_zp_dp4a or
                    lowering.reduction != .warp_sum_then_argmax_pairs or
                    lowering.input_blocks == 0 or lowering.columns_per_block != 8 or
                    lowering.warps_per_block == 0)
                {
                    return error.InvalidQ4Q8ArgmaxLowering;
                }
            },
            .q6_k_q8_1_argmax => |lowering| {
                if (lowering.decode != .q6_k_q8_1_dp4a or
                    lowering.reduction != .warp_sum_then_argmax_pairs or
                    lowering.input_blocks == 0 or lowering.columns_per_block != 8 or
                    lowering.warps_per_block == 0)
                {
                    return error.InvalidQ6KQ8ArgmaxLowering;
                }
            },
        }
    }
};

pub const RenderPlan = struct {
    kind: KernelKind,
    route: Route,
    kernel_id: []const u8,
    production_baseline: []const u8,
    production_enabled: bool,
    launch: LaunchMetadata,
    lowering: KernelLowering,

    pub fn validate(self: RenderPlan) !void {
        try self.launch.validate();
        try self.lowering.validate();
        try validatePlanOwnedLowering(self.launch, self.lowering);
        const expected = planFor(self.kind);
        if (!std.meta.eql(self.route, expected.route)) return error.CudaRouteDoesNotMatchKind;
        if (!std.mem.eql(u8, self.kernel_id, expected.kernel_id)) return error.CudaKernelIdDoesNotMatchKind;
        if (!std.mem.eql(u8, self.production_baseline, expected.production_baseline)) return error.CudaBaselineDoesNotMatchKind;
        if (self.production_enabled != expected.production_enabled) return error.CudaPromotionStateDoesNotMatchKind;
        if (!std.meta.eql(self.launch, expected.launch)) return error.CudaLaunchDoesNotMatchKind;
        if (!std.meta.eql(self.lowering, expected.lowering)) return error.CudaLoweringDoesNotMatchKind;
    }
};

fn validatePlanOwnedLowering(launch: LaunchMetadata, lowering: KernelLowering) !void {
    switch (lowering) {
        .q4_0_pair_activation_q8_1 => |pair| {
            if (launch.rows.fixed != 1 or launch.rows.min > 1 or
                (launch.rows.max != 0 and launch.rows.max != 1) or
                launch.input_dim.fixed == 0 or launch.output_dim.fixed == 0 or
                launch.input_dim.fixed % 32 != 0 or launch.output_dim.fixed % 32 != 0 or
                launch.input_dim.fixed / 32 != pair.input_blocks or
                launch.output_dim.fixed / 32 != pair.output_blocks or
                launch.output_cols_per_block != pair.output_values_per_block)
            {
                return error.PairActivationQ8ShapeDoesNotMatchLowering;
            }
            const expected_threads = @as(u32, pair.warps_per_projection_group) * 4 * 32;
            const expected_shared = @as(u32, pair.warps_per_projection_group) * 4 * 4 * 2 * @sizeOf(f32) +
                @as(u32, pair.output_values_per_block) * @sizeOf(f32);
            if (launch.threads_per_block != expected_threads or
                launch.static_shared_memory_bytes != expected_shared)
            {
                return error.PairActivationQ8TopologyDoesNotMatchLowering;
            }
        },
        .q4_0_down_q8_1 => |down| {
            if (launch.rows.fixed != 1 or launch.rows.min > 1 or
                (launch.rows.max != 0 and launch.rows.max != 1) or
                launch.input_dim.fixed == 0 or launch.output_dim.fixed == 0 or
                launch.input_dim.fixed % 32 != 0 or
                launch.input_dim.fixed / 32 != down.input_blocks or
                launch.output_cols_per_block != down.columns_per_block)
            {
                return error.DownQ8ShapeDoesNotMatchLowering;
            }
            const warps = launch.threads_per_block / 32;
            const expected_shared = @as(u32, down.columns_per_block) * warps * @sizeOf(f32);
            if (launch.threads_per_block < 32 or launch.threads_per_block % 32 != 0 or
                launch.static_shared_memory_bytes != expected_shared)
            {
                return error.DownQ8TopologyDoesNotMatchLowering;
            }
        },
        .q4_0_pair_activation_f32_exact => |pair| {
            if (launch.rows.fixed != 1 or launch.rows.min > 1 or
                (launch.rows.max != 0 and launch.rows.max != 1) or
                launch.input_dim.fixed == 0 or launch.output_dim.fixed == 0 or
                launch.input_dim.fixed % 32 != 0 or
                launch.input_dim.fixed / 32 != pair.input_blocks or
                launch.output_cols_per_block != pair.columns_per_block)
            {
                return error.ExactFfnPairF32ShapeDoesNotMatchLowering;
            }
            const expected_threads = @as(u32, pair.warps_per_block) * 32;
            const expected_shared = @as(u32, 2) *
                @as(u32, pair.columns_per_block) *
                @as(u32, pair.warps_per_block) *
                @sizeOf(f32);
            if (launch.threads_per_block != expected_threads or launch.static_shared_memory_bytes != expected_shared) {
                return error.ExactFfnPairF32TopologyDoesNotMatchLowering;
            }
        },
        .q4_0_down_f32_exact => |down| {
            if (launch.rows.fixed != 1 or launch.rows.min > 1 or
                (launch.rows.max != 0 and launch.rows.max != 1) or
                launch.input_dim.fixed == 0 or launch.output_dim.fixed == 0 or
                launch.input_dim.fixed % 32 != 0 or
                launch.input_dim.fixed / 32 != down.input_blocks or
                launch.output_cols_per_block != down.columns_per_block)
            {
                return error.ExactFfnDownF32ShapeDoesNotMatchLowering;
            }
            const expected_threads = @as(u32, down.warps_per_block) * 32;
            const expected_shared = @as(u32, down.columns_per_block) *
                @as(u32, down.warps_per_block) *
                @sizeOf(f32);
            if (launch.threads_per_block != expected_threads or launch.static_shared_memory_bytes != expected_shared) {
                return error.ExactFfnDownF32TopologyDoesNotMatchLowering;
            }
        },
        .q4_0_q8_1_argmax => |argmax| {
            if (launch.rows.fixed != 1 or launch.rows.min > 1 or
                (launch.rows.max != 0 and launch.rows.max != 1) or
                launch.input_dim.fixed == 0 or launch.output_dim.fixed == 0 or
                launch.input_dim.fixed % 32 != 0 or
                launch.input_dim.fixed / 32 != argmax.input_blocks or
                launch.output_cols_per_block != argmax.columns_per_block)
            {
                return error.Q4Q8ArgmaxShapeDoesNotMatchLowering;
            }
            const expected_threads = @as(u32, argmax.warps_per_block) * 32;
            const expected_shared = @as(u32, argmax.columns_per_block) * argmax.warps_per_block * @sizeOf(f32);
            if (launch.threads_per_block != expected_threads or
                launch.static_shared_memory_bytes != expected_shared)
            {
                return error.Q4Q8ArgmaxTopologyDoesNotMatchLowering;
            }
        },
        .q6_k_q8_1_argmax => |argmax| {
            if (launch.rows.fixed != 1 or launch.rows.min > 1 or
                (launch.rows.max != 0 and launch.rows.max != 1) or
                launch.input_dim.fixed == 0 or launch.output_dim.fixed == 0 or
                launch.input_dim.fixed % 256 != 0 or
                launch.input_dim.fixed / 256 != argmax.input_blocks or
                launch.output_cols_per_block != argmax.columns_per_block)
            {
                return error.Q6KQ8ArgmaxShapeDoesNotMatchLowering;
            }
            const expected_threads = @as(u32, argmax.warps_per_block) * 32;
            const task_threads = @as(u32, argmax.input_blocks) * 16;
            const expected_shared = @as(u32, argmax.columns_per_block) * argmax.warps_per_block * @sizeOf(f32);
            if (task_threads > expected_threads or expected_threads - task_threads >= 32 or
                launch.threads_per_block != expected_threads or
                launch.static_shared_memory_bytes != expected_shared)
            {
                return error.Q6KQ8ArgmaxTopologyDoesNotMatchLowering;
            }
        },
        else => {},
    }
}

pub const AttentionKernelKind = enum {
    /// Legacy split-8 candidates. Keep these names and their entry-point IDs
    /// stable because the CUDA runtime currently loads them directly.
    gqa_decode_split_kv_hd256_f32,
    gqa_decode_split_kv_hd512_f32,
    /// Standalone schedule candidates. They deliberately are not part of the
    /// runtime-owned CUDA bundle until a dispatcher selects and promotes one.
    gqa_decode_split2_kv_hd256_f32,
    gqa_decode_split2_kv_hd512_f32,
    gqa_decode_split4_kv_hd256_f32,
    gqa_decode_split4_kv_hd512_f32,
    /// Exact decode candidate: score each key in parallel, then retain the
    /// chronological online softmax/value recurrence used by the fast baseline.
    gqa_decode_score_prework_hd256_f32,
    gqa_decode_score_prework_hd512_f32,
};

pub const AttentionReduction = enum {
    split_kv_two_stage,
    parallel_scores_then_serial,
};

pub const AttentionSoftmax = enum {
    online_partials_stable_merge,
    online_chronological,
};

/// Storage is part of the attention semantic plan. Paged compressed keys use
/// a distinct value because their pointer and page-table ABI is not F32.
pub const AttentionStorage = enum {
    f32,
    f16,
    bf16,
    /// Page-table-addressed raw F16 rows with an exact runtime format tag.
    paged_f16,
    paged_f16_or_polar4,
    /// Paged values consumed through the runtime format tag. The exact score
    /// prework route keeps F32 as a control and accepts the production F16 KV
    /// cache without changing the chronological F32 recurrence/output ABI.
    paged_f16_or_f32,
};

/// A split-KV partition count is an owned schedule parameter, not a hidden
/// renderer constant. The generated variants are intentionally small and
/// power-of-two so the workspace and merge contract remain easy to validate.
pub const AttentionSplitVariant = enum(u8) {
    /// This is not a split-KV schedule. It occupies a distinct typed value so
    /// the score-prework workspace/launch contract cannot alias a merge plan.
    score_prework = 128,
    split2 = 2,
    split4 = 4,
    split8 = 8,

    pub fn kvSplits(self: AttentionSplitVariant) u8 {
        return @intFromEnum(self);
    }

    pub fn sourceTag(self: AttentionSplitVariant) []const u8 {
        return switch (self) {
            .score_prework => "score_prework",
            .split2 => "split2",
            .split4 => "split4",
            .split8 => "split8",
        };
    }
};

pub const generated_attention_split_variants = [_]AttentionSplitVariant{
    .split2,
    .split4,
    .split8,
};

/// Compatibility alias for the existing runtime-owned split-8 route. New code
/// should read the split count from `AttentionLowering.split_variant`.
pub const generated_attention_kv_splits: u8 = AttentionSplitVariant.split8.kvSplits();

pub fn attentionSplitVariantFor(kv_splits: u8) ?AttentionSplitVariant {
    return switch (kv_splits) {
        2 => .split2,
        4 => .split4,
        8 => .split8,
        128 => .score_prework,
        else => null,
    };
}

/// Maximum supported GQA ratio. The generated split kernel maps one query head
/// to each block, so every positive divisor up to this value is supported.
pub const generated_attention_query_heads_per_kv_head: u8 = 16;
pub const generated_attention_workspace_max_batch: u8 = 1;
pub const generated_attention_workspace_max_query_heads: u8 = 32;
pub const generated_attention_workspace_max_head_dim: u16 = 512;
pub const generated_attention_split_kv_min_tokens_default: u16 = 512;
pub const generated_attention_score_prework_chunks: u8 = 128;
/// The score-prework route overlays the existing largest split-KV workspace.
/// Keeping this power-of-two capacity fixed preserves CUDA graph launch
/// topology while leaving room for the normal split candidate when selected.
pub const generated_attention_score_prework_max_kv_tokens: u16 = 4096;
pub const generated_attention_score_prework_tiled64_tile_size: u16 = 64;
pub const generated_attention_score_prework_tiled64_hd256_max_kv_tokens: u16 = 512;
pub const generated_attention_score_prework_tiled64_hd512_max_kv_tokens: u16 = 4096;
/// The exact consumer materializes the chronological online-softmax
/// coefficients once per head, then lets every value lane replay the same
/// recurrence without a block-wide barrier for every key. Two coefficient
/// arrays plus the final maximum/denominator fit comfortably below the CUDA
/// per-block shared-memory limit on the qualified sm_89 route.
pub const generated_attention_score_prework_recurrence_shared_bytes: u32 =
    (@as(u32, 2) * generated_attention_score_prework_max_kv_tokens + 2) * @sizeOf(f32);

pub fn generatedAttentionScorePreworkTiled64MaxKvTokens(head_dim: u16) ?u16 {
    return switch (head_dim) {
        256 => generated_attention_score_prework_tiled64_hd256_max_kv_tokens,
        512 => generated_attention_score_prework_tiled64_hd512_max_kv_tokens,
        else => null,
    };
}

pub fn generatedAttentionScorePreworkTiled64SharedBytes(head_dim: u16) ?u32 {
    const max_kv_tokens = generatedAttentionScorePreworkTiled64MaxKvTokens(head_dim) orelse return null;
    return (@as(u32, 2) * max_kv_tokens + 2) * @sizeOf(f32);
}

/// Legacy split-8 IDs. Their values are part of the runtime module ABI.
pub const generated_attention_hd256_serial_kernel_id = "antfly_gqa_attention_decode_scalars_hd256_f32_v1";
pub const generated_attention_hd256_stage1_kernel_id = "antfly_gqa_attention_decode_split_kv_hd256_f32_stage1_v1";
pub const generated_attention_hd256_stage2_kernel_id = "antfly_gqa_attention_decode_split_kv_hd256_f32_stage2_v1";
pub const generated_attention_hd512_serial_kernel_id = "antfly_gqa_attention_decode_scalars_hd512_f32_v1";
pub const generated_attention_hd512_stage1_kernel_id = "antfly_gqa_attention_decode_split_kv_hd512_f32_stage1_v1";
pub const generated_attention_hd512_stage2_kernel_id = "antfly_gqa_attention_decode_split_kv_hd512_f32_stage2_v1";

pub const generated_attention_hd256_split8_serial_kernel_id = generated_attention_hd256_serial_kernel_id;
pub const generated_attention_hd256_split8_stage1_kernel_id = generated_attention_hd256_stage1_kernel_id;
pub const generated_attention_hd256_split8_stage2_kernel_id = generated_attention_hd256_stage2_kernel_id;
pub const generated_attention_hd512_split8_serial_kernel_id = generated_attention_hd512_serial_kernel_id;
pub const generated_attention_hd512_split8_stage1_kernel_id = generated_attention_hd512_stage1_kernel_id;
pub const generated_attention_hd512_split8_stage2_kernel_id = generated_attention_hd512_stage2_kernel_id;

pub const generated_attention_hd256_split2_serial_kernel_id = "antfly_gqa_attention_decode_scalars_split2_hd256_f32_v1";
pub const generated_attention_hd256_split2_stage1_kernel_id = "antfly_gqa_attention_decode_split2_kv_hd256_f32_stage1_v1";
pub const generated_attention_hd256_split2_stage2_kernel_id = "antfly_gqa_attention_decode_split2_kv_hd256_f32_stage2_v1";
pub const generated_attention_hd512_split2_serial_kernel_id = "antfly_gqa_attention_decode_scalars_split2_hd512_f32_v1";
pub const generated_attention_hd512_split2_stage1_kernel_id = "antfly_gqa_attention_decode_split2_kv_hd512_f32_stage1_v1";
pub const generated_attention_hd512_split2_stage2_kernel_id = "antfly_gqa_attention_decode_split2_kv_hd512_f32_stage2_v1";

pub const generated_attention_hd256_split4_serial_kernel_id = "antfly_gqa_attention_decode_scalars_split4_hd256_f32_v1";
pub const generated_attention_hd256_split4_stage1_kernel_id = "antfly_gqa_attention_decode_split4_kv_hd256_f32_stage1_v1";
pub const generated_attention_hd256_split4_stage2_kernel_id = "antfly_gqa_attention_decode_split4_kv_hd256_f32_stage2_v1";
pub const generated_attention_hd512_split4_serial_kernel_id = "antfly_gqa_attention_decode_scalars_split4_hd512_f32_v1";
pub const generated_attention_hd512_split4_stage1_kernel_id = "antfly_gqa_attention_decode_split4_kv_hd512_f32_stage1_v1";
pub const generated_attention_hd512_split4_stage2_kernel_id = "antfly_gqa_attention_decode_split4_kv_hd512_f32_stage2_v1";

pub const generated_attention_hd256_score_prework_kernel_id = "antfly_gqa_attention_decode_turboquant_score_prework_hd256_f32_v1";
pub const generated_attention_hd256_score_prework_serial_kernel_id = "antfly_gqa_attention_decode_turboquant_score_prework_serial_hd256_f32_v1";
pub const generated_attention_hd256_score_prework_tiled64_kernel_id = "antfly_gqa_attention_decode_turboquant_score_prework_tiled64_hd256_f32_v1";
pub const generated_attention_hd512_score_prework_kernel_id = "antfly_gqa_attention_decode_turboquant_score_prework_hd512_f32_v1";
pub const generated_attention_hd512_score_prework_serial_kernel_id = "antfly_gqa_attention_decode_turboquant_score_prework_serial_hd512_f32_v1";
pub const generated_attention_hd512_score_prework_tiled64_kernel_id = "antfly_gqa_attention_decode_turboquant_score_prework_tiled64_hd512_f32_v1";

pub const AttentionWorkspaceLayout = struct {
    partial_values_offset: usize,
    partial_max_offset: usize,
    partial_denom_offset: usize,
    total_bytes: usize,
};

/// Returns the exact workspace contract for one split count. The runtime keeps
/// using the compatible split-8 wrapper below; candidates expose their smaller
/// contract through their manifest without affecting that allocation.
pub fn generatedAttentionWorkspaceLayoutFor(kv_splits: u8) ?AttentionWorkspaceLayout {
    const variant = attentionSplitVariantFor(kv_splits) orelse return null;
    if (variant == .score_prework) {
        const score_bytes = @as(usize, generated_attention_workspace_max_batch) *
            @as(usize, generated_attention_workspace_max_query_heads) *
            @as(usize, generated_attention_score_prework_max_kv_tokens) *
            @sizeOf(f32);
        return .{
            .partial_values_offset = 0,
            .partial_max_offset = score_bytes,
            .partial_denom_offset = score_bytes,
            .total_bytes = score_bytes,
        };
    }
    const partial_count = @as(usize, generated_attention_workspace_max_batch) *
        @as(usize, generated_attention_workspace_max_query_heads) *
        @as(usize, kv_splits);
    const values_bytes = partial_count * @as(usize, generated_attention_workspace_max_head_dim) * @sizeOf(f32);
    const stats_bytes = partial_count * @sizeOf(f32);
    return .{
        .partial_values_offset = 0,
        .partial_max_offset = values_bytes,
        .partial_denom_offset = values_bytes + stats_bytes,
        .total_bytes = values_bytes + 2 * stats_bytes,
    };
}

pub fn generatedAttentionWorkspaceLayout() AttentionWorkspaceLayout {
    return generatedAttentionWorkspaceLayoutFor(generated_attention_kv_splits) orelse unreachable;
}

pub const AttentionLowering = struct {
    kind: quant_kernel_op.AttentionKind,
    reduction: AttentionReduction,
    softmax: AttentionSoftmax,
    head_dim: u16,
    device_decode_scalars: bool,
    split_variant: AttentionSplitVariant,
    kv_splits: u8,
    query_heads_per_kv_head: u8,
    query_storage: AttentionStorage,
    key_storage: AttentionStorage,
    value_storage: AttentionStorage,
    output_storage: AttentionStorage,
    split_kv_min_tokens_default: u16,
    max_kv_tokens: u16,

    pub fn validate(self: AttentionLowering) !void {
        if (self.kind != .decode_1x or
            (self.head_dim != 256 and self.head_dim != 512) or
            !self.device_decode_scalars or
            self.kv_splits != self.split_variant.kvSplits() or
            self.query_heads_per_kv_head == 0 or
            self.query_heads_per_kv_head > generated_attention_query_heads_per_kv_head or
            self.query_storage != .f32 or self.output_storage != .f32)
        {
            return error.InvalidCudaAttentionLowering;
        }
        switch (self.reduction) {
            .split_kv_two_stage => {
                if (self.softmax != .online_partials_stable_merge or self.split_variant == .score_prework or
                    self.key_storage != .f32 or self.value_storage != .f32 or
                    self.split_kv_min_tokens_default == 0 or
                    self.max_kv_tokens != 0)
                {
                    return error.InvalidCudaAttentionLowering;
                }
            },
            .parallel_scores_then_serial => {
                if (self.softmax != .online_chronological or self.split_variant != .score_prework or
                    self.kv_splits != generated_attention_score_prework_chunks or
                    self.key_storage != .paged_f16_or_polar4 or
                    self.value_storage != .paged_f16_or_f32 or
                    self.split_kv_min_tokens_default != 0 or
                    self.max_kv_tokens != generated_attention_score_prework_max_kv_tokens)
                {
                    return error.InvalidCudaAttentionLowering;
                }
            },
        }
    }
};

/// Typed plan for generated CUDA attention. It is intentionally separate from
/// the quant-matmul `RenderPlan`: attention has no quant format, row bucket, or
/// epilogue, and must not acquire placeholder values for those fields.
pub const AttentionRenderPlan = struct {
    kind: AttentionKernelKind,
    /// Stable source-level identity. Unlike the legacy split-8 entry point
    /// names, this always carries the schedule split count.
    source_id: []const u8,
    kernel_id: []const u8,
    production_baseline: []const u8,
    production_enabled: bool,
    serial_kernel_id: []const u8,
    serial_launch: LaunchMetadata,
    launch: LaunchMetadata,
    reduction_kernel_id: []const u8,
    reduction_launch: LaunchMetadata,
    /// Optional exact consumer that repeats the chronological scalar
    /// recurrence per output tile. It shares the serial consumer ABI and score
    /// workspace, so choosing it changes launch topology but not graph data.
    tiled64_kernel_id: ?[]const u8 = null,
    tiled64_launch: ?LaunchMetadata = null,
    lowering: AttentionLowering,

    pub fn validate(self: AttentionRenderPlan) !void {
        try self.serial_launch.validate();
        try self.launch.validate();
        try self.reduction_launch.validate();
        if (self.tiled64_launch) |launch| try launch.validate();
        try self.lowering.validate();
        if (self.source_id.len == 0 or
            !std.mem.containsAtLeast(u8, self.source_id, 1, self.lowering.split_variant.sourceTag()))
        {
            return error.CudaAttentionSourceIdDoesNotMatchSplitCount;
        }
        const expected = attentionPlanFor(self.kind);
        if (!std.mem.eql(u8, self.source_id, expected.source_id)) return error.CudaAttentionSourceIdDoesNotMatchKind;
        if (!std.mem.eql(u8, self.kernel_id, expected.kernel_id)) return error.CudaKernelIdDoesNotMatchKind;
        if (!std.mem.eql(u8, self.production_baseline, expected.production_baseline)) return error.CudaBaselineDoesNotMatchKind;
        if (self.production_enabled != expected.production_enabled) return error.CudaPromotionStateDoesNotMatchKind;
        if (!std.mem.eql(u8, self.serial_kernel_id, expected.serial_kernel_id)) return error.CudaKernelIdDoesNotMatchKind;
        if (!std.meta.eql(self.serial_launch, expected.serial_launch)) return error.CudaLaunchDoesNotMatchKind;
        if (!std.meta.eql(self.launch, expected.launch)) return error.CudaLaunchDoesNotMatchKind;
        if (!std.mem.eql(u8, self.reduction_kernel_id, expected.reduction_kernel_id)) return error.CudaKernelIdDoesNotMatchKind;
        if (!std.meta.eql(self.reduction_launch, expected.reduction_launch)) return error.CudaLaunchDoesNotMatchKind;
        if ((self.tiled64_kernel_id == null) != (expected.tiled64_kernel_id == null)) return error.CudaKernelIdDoesNotMatchKind;
        if (self.tiled64_kernel_id) |kernel_id| {
            if (!std.mem.eql(u8, kernel_id, expected.tiled64_kernel_id.?)) return error.CudaKernelIdDoesNotMatchKind;
        }
        if (!std.meta.eql(self.tiled64_launch, expected.tiled64_launch)) return error.CudaLaunchDoesNotMatchKind;
        if (!std.meta.eql(self.lowering, expected.lowering)) return error.CudaLoweringDoesNotMatchKind;
    }
};

/// Dedicated single-kernel Flash-prefill family. It intentionally does not use
/// `AttentionKernelKind`: decode plans own serial/producer/consumer/reduction
/// fields that have no meaning for this one-launch production ABI.
pub const FlashPrefillKernelKind = enum {
    gqa_prefill_flash_sm89_hd256_swa512_f32,
    gqa_prefill_flash_sm89_hd512_global_f32,
};

pub const generated_flash_prefill_hd256_kernel_id =
    "antfly_gqa_attention_prefill_flash_sm89_hd256_swa512_f32_v1";
pub const generated_flash_prefill_hd512_kernel_id =
    "antfly_gqa_attention_prefill_flash_sm89_hd512_global_f32_v1";
pub const generated_flash_prefill_query_tile: u16 = 16;
pub const generated_flash_prefill_key_tile: u16 = 16;
pub const generated_flash_prefill_page_size_tokens: u16 = 16;
pub const generated_flash_prefill_threads: u16 = 256;
pub const generated_flash_prefill_format_f16: u32 = 2;

pub const FlashPrefillQueryLengthPolicy = enum {
    /// Qualified production buckets: q512 at prefixes 0/512/1024/1536 and the
    /// final q3 tail at prefix 2048. q1 always remains on the decode route.
    gemma4_q512_or_q3_v1,

    pub fn accepts(self: FlashPrefillQueryLengthPolicy, q_len: usize, query_position_offset: usize) bool {
        return switch (self) {
            .gemma4_q512_or_q3_v1 => (q_len == 512 and
                (query_position_offset == 0 or query_position_offset == 512 or
                    query_position_offset == 1024 or query_position_offset == 1536)) or
                (q_len == 3 and query_position_offset == 2048),
        };
    }
};

pub fn generatedFlashPrefillDynamicSharedBytes(head_dim: u16) ?u32 {
    if (head_dim != 256 and head_dim != 512) return null;
    return 2 * generated_flash_prefill_query_tile * @as(u32, head_dim) * @sizeOf(f16) +
        generated_flash_prefill_query_tile * generated_flash_prefill_key_tile * @sizeOf(f32) +
        generated_flash_prefill_query_tile * generated_flash_prefill_key_tile * @sizeOf(f16) +
        8 * generated_flash_prefill_query_tile * 16 * @sizeOf(f32) +
        3 * generated_flash_prefill_query_tile * @sizeOf(u32) +
        4 * generated_flash_prefill_query_tile * @sizeOf(f32) +
        @sizeOf(u32);
}

pub const FlashPrefillLowering = struct {
    kind: quant_kernel_op.AttentionKind,
    head_dim: u16,
    query_heads: u8,
    kv_heads: u8,
    query_tile: u16,
    key_tile: u16,
    page_size_tokens: u16,
    sliding_window: u16,
    query_length_policy: FlashPrefillQueryLengthPolicy,
    query_storage: AttentionStorage,
    key_storage: AttentionStorage,
    value_storage: AttentionStorage,
    output_storage: AttentionStorage,
    format: u32,
    value_format: u32,
    required_compute_major: u8,
    required_compute_minor: u8,

    pub fn validate(self: FlashPrefillLowering) !void {
        if (self.kind != .prefill_flash or
            (self.head_dim != 256 and self.head_dim != 512) or
            self.query_heads != 8 or self.kv_heads != 1 or
            self.query_tile != generated_flash_prefill_query_tile or
            self.key_tile != generated_flash_prefill_key_tile or
            self.page_size_tokens != generated_flash_prefill_page_size_tokens or
            self.sliding_window != @as(u16, if (self.head_dim == 256) 512 else 0) or
            self.query_storage != .f32 or self.key_storage != .paged_f16 or
            self.value_storage != .paged_f16 or self.output_storage != .f32 or
            self.format != generated_flash_prefill_format_f16 or
            self.value_format != generated_flash_prefill_format_f16 or
            self.required_compute_major != 8 or self.required_compute_minor != 9)
        {
            return error.InvalidCudaFlashPrefillLowering;
        }
    }
};

pub const FlashPrefillRenderPlan = struct {
    kind: FlashPrefillKernelKind,
    source_id: []const u8,
    namespace_id: []const u8,
    kernel_id: []const u8,
    production_baseline: []const u8,
    production_enabled: bool,
    runtime_default_enabled: bool,
    /// The HMMA path intentionally rounds F32 Q to F16. Exact parity remains a
    /// hard requirement for any runtime default; the paged-prefill differential
    /// passed every guard/page-table/adversarial/determinism case
    /// bitwise-identical, which qualified this route as the automatic default.
    exact_token_parity_required_for_default: bool,
    exact_token_parity_qualified: bool,
    launch: LaunchMetadata,
    lowering: FlashPrefillLowering,

    pub fn validate(self: FlashPrefillRenderPlan) !void {
        try self.launch.validate();
        try self.lowering.validate();
        if (self.runtime_default_enabled and
            (!self.production_enabled or !self.exact_token_parity_required_for_default or
                !self.exact_token_parity_qualified))
        {
            return error.CudaFlashPrefillDefaultRequiresExactTokenParity;
        }
        const expected = flashPrefillPlanFor(self.kind);
        if (!std.meta.eql(self, expected)) return error.CudaFlashPrefillPlanDoesNotMatchKind;
    }
};

/// Dedicated one-launch split-K online-softmax decode family. Like Flash
/// prefill, this does not reuse `AttentionRenderPlan`: its persistent partial
/// workspace and last-CTA completion protocol are a distinct ABI/topology.
pub const SplitkOnlineDecodeKernelKind = enum {
    gqa_decode_splitk_online_sm89_hd256_swa512_f16_f32,
    gqa_decode_splitk_online_sm89_hd512_global_f16_f32,
};

pub const generated_splitk_online_decode_hd256_kernel_id =
    "antfly_gqa_attention_decode_splitk_online_sm89_hd256_swa512_f16_f32_v1";
pub const generated_splitk_online_decode_hd512_kernel_id =
    "antfly_gqa_attention_decode_splitk_online_sm89_hd512_global_f16_f32_v1";
pub const generated_splitk_online_decode_threads: u16 = 128;
pub const generated_splitk_online_decode_splits: u16 = 64;
pub const generated_splitk_online_decode_query_heads: u16 = 8;
pub const generated_splitk_online_decode_kv_heads: u16 = 1;
pub const generated_splitk_online_decode_page_size_tokens: u16 = 16;
pub const generated_splitk_online_decode_format_f16: u32 = 2;
/// CUDA 13.2 reports 576 bytes of statically allocated shared memory for both
/// qualified specializations. Keep this explicit so catalog resource
/// accounting agrees with the packaged SM89 cubin.
pub const generated_splitk_online_decode_static_shared_memory_bytes: u32 = 576;
pub const generated_splitk_online_decode_hd256_max_visible_tokens: u16 = 512;
pub const generated_splitk_online_decode_hd512_max_visible_tokens: u16 = 4096;
/// The typed replay profile is qualified through this absolute sequence
/// boundary. Local layers may see only 512 tokens, but share graph/device
/// scalars and paged storage with the global layers.
pub const generated_splitk_online_decode_max_sequence_tokens: u16 = 4096;

pub const SplitkOnlineDecodeWorkspaceLayout = struct {
    completion_counters_offset: usize,
    partial_values_offset: usize,
    partial_max_offset: usize,
    partial_denom_offset: usize,
    total_bytes: usize,
};

/// One layout covers both qualified head dimensions. Keeping offsets fixed is
/// important for CUDA graph replay: the launch parameter identity does not
/// change when execution crosses a local/global Gemma layer.
pub fn generatedSplitkOnlineDecodeWorkspaceLayout() SplitkOnlineDecodeWorkspaceLayout {
    const alignment: usize = 256;
    const counters_bytes: usize = generated_splitk_online_decode_query_heads * @sizeOf(u32);
    const partial_count: usize = @as(usize, generated_splitk_online_decode_query_heads) *
        generated_splitk_online_decode_splits;
    const partial_values_bytes = partial_count * 512 * @sizeOf(f32);
    const scalar_partials_bytes = partial_count * @sizeOf(f32);
    const partial_values_offset = std.mem.alignForward(usize, counters_bytes, alignment);
    const partial_max_offset = std.mem.alignForward(usize, partial_values_offset + partial_values_bytes, alignment);
    const partial_denom_offset = std.mem.alignForward(usize, partial_max_offset + scalar_partials_bytes, alignment);
    return .{
        .completion_counters_offset = 0,
        .partial_values_offset = partial_values_offset,
        .partial_max_offset = partial_max_offset,
        .partial_denom_offset = partial_denom_offset,
        .total_bytes = std.mem.alignForward(usize, partial_denom_offset + scalar_partials_bytes, alignment),
    };
}

pub const SplitkOnlineDecodeLowering = struct {
    kind: quant_kernel_op.AttentionKind,
    head_dim: u16,
    query_heads: u16,
    kv_heads: u16,
    kv_splits: u16,
    page_size_tokens: u16,
    sliding_window: u16,
    max_visible_tokens: u16,
    query_storage: AttentionStorage,
    key_storage: AttentionStorage,
    value_storage: AttentionStorage,
    output_storage: AttentionStorage,
    format: u32,
    value_format: u32,
    required_compute_major: u8,
    required_compute_minor: u8,

    pub fn validate(self: SplitkOnlineDecodeLowering) !void {
        if (self.kind != .decode_1x or
            (self.head_dim != 256 and self.head_dim != 512) or
            self.query_heads != generated_splitk_online_decode_query_heads or
            self.kv_heads != generated_splitk_online_decode_kv_heads or
            self.kv_splits != generated_splitk_online_decode_splits or
            self.page_size_tokens != generated_splitk_online_decode_page_size_tokens or
            self.sliding_window != @as(u16, if (self.head_dim == 256) 512 else 0) or
            self.max_visible_tokens != @as(u16, if (self.head_dim == 256)
                generated_splitk_online_decode_hd256_max_visible_tokens
            else
                generated_splitk_online_decode_hd512_max_visible_tokens) or
            self.query_storage != .f32 or self.key_storage != .paged_f16 or
            self.value_storage != .paged_f16 or self.output_storage != .f32 or
            self.format != generated_splitk_online_decode_format_f16 or
            self.value_format != generated_splitk_online_decode_format_f16 or
            self.required_compute_major != 8 or self.required_compute_minor != 9)
        {
            return error.InvalidCudaSplitkOnlineDecodeLowering;
        }
    }
};

pub const SplitkOnlineDecodeRenderPlan = struct {
    kind: SplitkOnlineDecodeKernelKind,
    source_id: []const u8,
    namespace_id: []const u8,
    kernel_id: []const u8,
    production_baseline: []const u8,
    production_enabled: bool,
    runtime_default_enabled: bool,
    exact_token_parity_required_for_default: bool,
    exact_token_parity_qualified: bool,
    launch: LaunchMetadata,
    workspace: SplitkOnlineDecodeWorkspaceLayout,
    lowering: SplitkOnlineDecodeLowering,

    pub fn validate(self: SplitkOnlineDecodeRenderPlan) !void {
        try self.launch.validate();
        try self.lowering.validate();
        if (self.runtime_default_enabled and
            (!self.production_enabled or !self.exact_token_parity_required_for_default or
                !self.exact_token_parity_qualified))
        {
            return error.CudaSplitkOnlineDecodeDefaultRequiresExactTokenParity;
        }
        const expected = splitkOnlineDecodePlanFor(self.kind);
        if (!std.meta.eql(self, expected)) return error.CudaSplitkOnlineDecodePlanDoesNotMatchKind;
    }
};

pub const SourceFragment = struct {
    name: []const u8,
    source: []const u8,
};

pub const KernelSupport = struct {
    includes: []const []const u8,
    helpers: []const SourceFragment,
};

const license_header =
    \\// Copyright 2026 Antfly, Inc.
    \\//
    \\// Licensed under the Apache License, Version 2.0 (the "License");
    \\// you may not use this file except in compliance with the License.
    \\// You may obtain a copy of the License at
    \\//
    \\//     http://www.apache.org/licenses/LICENSE-2.0
    \\//
    \\// Unless required by applicable law or agreed to in writing, software
    \\// distributed under the License is distributed on an "AS IS" BASIS,
    \\// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    \\// See the License for the specific language governing permissions and
    \\// limitations under the License.
;

const includes_cuda_fp16_stdint = [_][]const u8{ "<cuda_fp16.h>", "<stdint.h>" };
const includes_cuda_fp16_math_stdint = [_][]const u8{ "<cuda_fp16.h>", "<math.h>", "<stdint.h>" };
const includes_cuda_fp16 = [_][]const u8{"<cuda_fp16.h>"};
const includes_math = [_][]const u8{"<math.h>"};

pub fn planFor(kind: KernelKind) RenderPlan {
    return switch (kind) {
        .q4_k_small_batch_bias_gelu => .{
            .kind = kind,
            .route = .{ .format = .q4_k, .row_bucket = .rows_2_8, .epilogue = .bias_gelu, .dispatch = .small_batch },
            .kernel_id = "antfly_q4_k_small_batch_bias_gelu_f32_v1",
            .production_baseline = "termite_linear_q4_k_bias_gelu_f32_tile4_r2",
            .production_enabled = false,
            .launch = .{
                .grid = .output_cols_by_rows,
                .threads_per_block = 128,
                .output_rows_per_block = 1,
                .output_cols_per_block = 1,
                .static_shared_memory_bytes = 512,
                .rows = .{ .min = 2, .max = 8 },
                .input_dim = .{ .multiple_of = 256 },
                .output_dim = .{},
            },
            .lowering = .{ .q4_k_small_batch = .{} },
        },
        .q4_k_mmv => .{
            .kind = kind,
            .route = .{ .format = .q4_k, .row_bucket = .rows_1, .epilogue = .none, .dispatch = .mmv },
            .kernel_id = "antfly_q4_k_mmv_f32_v1",
            .production_baseline = "termite_linear_q4_k_f32_tile4",
            .production_enabled = false,
            .launch = .{
                .grid = .output_cols,
                .threads_per_block = 256,
                .output_rows_per_block = 1,
                .output_cols_per_block = 1,
                .static_shared_memory_bytes = 1024,
                .rows = .{ .min = 1, .max = 1, .fixed = 1 },
                .input_dim = .{ .multiple_of = 256 },
                .output_dim = .{},
            },
            .lowering = .{ .q4_k_projection = .{} },
        },
        .q4_0_mmv => .{
            .kind = kind,
            .route = .{ .format = .q4_0, .row_bucket = .rows_1, .epilogue = .none, .dispatch = .mmv },
            .kernel_id = "antfly_q4_0_mmv_f32_v1",
            .production_baseline = "termite_linear_q4_0_f32_tile4",
            .production_enabled = true,
            .launch = .{
                .grid = .output_cols,
                .threads_per_block = 256,
                .output_rows_per_block = 1,
                .output_cols_per_block = 4,
                .static_shared_memory_bytes = 128,
                .rows = .{ .min = 1, .max = 1, .fixed = 1 },
                .input_dim = .{ .multiple_of = 32 },
                .output_dim = .{},
            },
            .lowering = .{ .q4_0_projection = .{ .row_tile = 1, .projection_count = 1 } },
        },
        .q4_0_mm => .{
            .kind = kind,
            .route = .{ .format = .q4_0, .row_bucket = .rows_9_64, .epilogue = .none, .dispatch = .mm },
            .kernel_id = "antfly_q4_0_mm_f32_v1",
            .production_baseline = "termite_linear_q4_0_f32",
            .production_enabled = true,
            .launch = .{
                .grid = .output_cols_by_rows,
                .threads_per_block = 256,
                .output_rows_per_block = 8,
                .output_cols_per_block = 4,
                .static_shared_memory_bytes = 1024,
                .rows = .{ .min = 9, .max = 64 },
                .input_dim = .{ .multiple_of = 32 },
                .output_dim = .{},
            },
            .lowering = .{ .q4_0_projection = .{ .row_tile = 8, .projection_count = 1 } },
        },
        .q4_0_pair_mmv => .{
            .kind = kind,
            .route = .{ .format = .q4_0, .row_bucket = .rows_1, .epilogue = .pair, .dispatch = .mmv },
            .kernel_id = "antfly_q4_0_pair_mmv_f32_v1",
            .production_baseline = "termite_linear_q4_0_pair_nobias_f32_tile4_w4",
            .production_enabled = true,
            .launch = .{
                .grid = .output_cols,
                .threads_per_block = 256,
                .output_rows_per_block = 1,
                .output_cols_per_block = 4,
                .static_shared_memory_bytes = 256,
                .rows = .{ .min = 1, .max = 1, .fixed = 1 },
                .input_dim = .{ .multiple_of = 32 },
                .output_dim = .{},
            },
            .lowering = .{ .q4_0_projection = .{ .row_tile = 1, .projection_count = 2 } },
        },
        .q4_0_pair_activation_q8_1 => pairActivationQ8Plan(
            kind,
            "antfly_q4_0_pair_activation_q8_1_mmv_v1",
            2560,
            10240,
            5,
            .q4_0_q8_zp_dp4a,
            true,
        ),
        .q4_0_pair_activation_q8_1_e2b_6144 => pairActivationQ8Plan(
            kind,
            "antfly_q4_0_pair_activation_q8_1_e2b_6144_mmv_v1",
            1536,
            6144,
            3,
            .q4_0_q8_zp_dp4a,
            false,
        ),
        .q4_0_pair_activation_q8_1_e2b_12288 => pairActivationQ8Plan(
            kind,
            "antfly_q4_0_pair_activation_q8_1_e2b_12288_mmv_v1",
            1536,
            12288,
            3,
            .q4_0_q8_zp_dp4a,
            false,
        ),
        .q4_0_pair_activation_ggml_q8_1_e2b_6144 => pairActivationQ8Plan(
            kind,
            "antfly_q4_0_pair_activation_ggml_q8_1_e2b_6144_mmv_v1",
            1536,
            6144,
            3,
            .q4_0_q8_1_dp4a,
            false,
        ),
        .q4_0_pair_activation_ggml_q8_1_e2b_12288 => pairActivationQ8Plan(
            kind,
            "antfly_q4_0_pair_activation_ggml_q8_1_e2b_12288_mmv_v1",
            1536,
            12288,
            3,
            .q4_0_q8_1_dp4a,
            false,
        ),
        .q4_0_down_q8_1 => downQ8Plan(
            kind,
            "antfly_q4_0_down_q8_1_mmv_v1",
            10240,
            2560,
            256,
            .q4_0_q8_zp_dp4a,
            true,
        ),
        .q4_0_down_q8_1_e2b_6144 => downQ8Plan(
            kind,
            "antfly_q4_0_down_q8_1_e2b_6144_mmv_v1",
            6144,
            1536,
            128,
            .q4_0_q8_zp_dp4a,
            false,
        ),
        .q4_0_down_q8_1_e2b_12288 => downQ8Plan(
            kind,
            "antfly_q4_0_down_q8_1_e2b_12288_mmv_v1",
            12288,
            1536,
            256,
            .q4_0_q8_zp_dp4a,
            false,
        ),
        .q4_0_down_ggml_q8_1_e2b_6144 => downGgmlQ8Plan(
            kind,
            "antfly_q4_0_down_ggml_q8_1_e2b_6144_mmv_v1",
            6144,
            1536,
        ),
        .q4_0_down_ggml_q8_1_e2b_12288 => downGgmlQ8Plan(
            kind,
            "antfly_q4_0_down_ggml_q8_1_e2b_12288_mmv_v1",
            12288,
            1536,
        ),
        .q4_0_pair_activation_f32_e2b_6144_exact => exactFfnPairF32Plan(
            kind,
            "antfly_q4_0_pair_activation_f32_e2b_6144_exact_v1",
            1536,
            6144,
        ),
        .q4_0_pair_activation_f32_e2b_12288_exact => exactFfnPairF32Plan(
            kind,
            "antfly_q4_0_pair_activation_f32_e2b_12288_exact_v1",
            1536,
            12288,
        ),
        .q4_0_down_f32_e2b_6144_exact => exactFfnDownF32Plan(
            kind,
            "antfly_q4_0_down_f32_e2b_6144_exact_v1",
            6144,
            1536,
        ),
        .q4_0_down_f32_e2b_12288_exact => exactFfnDownF32Plan(
            kind,
            "antfly_q4_0_down_f32_e2b_12288_exact_v1",
            12288,
            1536,
        ),
        .q4_0_q8_1_argmax_e2b_tile8 => .{
            .kind = kind,
            .route = .{ .format = .q4_0, .row_bucket = .rows_1, .epilogue = .argmax, .dispatch = .mmv },
            .kernel_id = "antfly_q4_0_q8_1_argmax_rows_stage1_tile8_v1",
            .production_baseline = "termite_linear_q4_0_q8_1_f32_tile4+termite_argmax_last_row_f32",
            .production_enabled = false,
            .launch = .{
                .grid = .flattened_rows_by_output_blocks,
                .threads_per_block = 96,
                .output_rows_per_block = 1,
                .output_cols_per_block = 8,
                .static_shared_memory_bytes = 96,
                .rows = .{ .min = 1, .max = 1, .fixed = 1 },
                .input_dim = .{ .multiple_of = 32, .fixed = 1536 },
                .output_dim = .{ .multiple_of = 8, .fixed = 262144 },
            },
            .lowering = .{ .q4_0_q8_1_argmax = .{
                .input_blocks = 48,
                .columns_per_block = 8,
                .warps_per_block = 3,
            } },
        },
        .q6_k_q8_1_argmax_k2560_tile8 => .{
            .kind = kind,
            .route = .{ .format = .q6_k, .row_bucket = .rows_1, .epilogue = .argmax, .dispatch = .mmv },
            .kernel_id = "antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1",
            .production_baseline = "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b+termite_argmax_reduce_rows_pairs_f32_w16",
            .production_enabled = false,
            .launch = .{
                .grid = .flattened_rows_by_output_blocks,
                .threads_per_block = 160,
                .output_rows_per_block = 1,
                .output_cols_per_block = 8,
                .static_shared_memory_bytes = 160,
                .rows = .{ .min = 1, .max = 1, .fixed = 1 },
                .input_dim = .{ .multiple_of = 256, .fixed = 2560 },
                .output_dim = .{ .multiple_of = 8, .fixed = 262144 },
            },
            .lowering = .{ .q6_k_q8_1_argmax = .{
                .input_blocks = 10,
                .columns_per_block = 8,
                .warps_per_block = 5,
            } },
        },
        .q6_k_q8_1_argmax_k3840_tile8 => .{
            .kind = kind,
            .route = .{ .format = .q6_k, .row_bucket = .rows_1, .epilogue = .argmax, .dispatch = .mmv },
            .kernel_id = "antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1",
            .production_baseline = "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8+termite_argmax_reduce_rows_pairs_f32_w16",
            .production_enabled = false,
            .launch = .{
                .grid = .flattened_rows_by_output_blocks,
                .threads_per_block = 256,
                .output_rows_per_block = 1,
                .output_cols_per_block = 8,
                .static_shared_memory_bytes = 256,
                .rows = .{ .min = 1, .max = 1, .fixed = 1 },
                .input_dim = .{ .multiple_of = 256, .fixed = 3840 },
                .output_dim = .{ .multiple_of = 8, .fixed = 262144 },
            },
            .lowering = .{ .q6_k_q8_1_argmax = .{
                .input_blocks = 15,
                .columns_per_block = 8,
                .warps_per_block = 8,
            } },
        },
    };
}

fn pairActivationQ8Plan(
    kind: KernelKind,
    kernel_id: []const u8,
    input_dim: u16,
    output_dim: u16,
    warps_per_projection_group: u8,
    decode: QuantDecodePrimitive,
    production_enabled: bool,
) RenderPlan {
    const input_blocks = input_dim / 32;
    const output_blocks = output_dim / 32;
    const threads = @as(u16, warps_per_projection_group) * 4 * 32;
    const shared_bytes = @as(u32, warps_per_projection_group) * 4 * 4 * 2 * @sizeOf(f32) + 32 * @sizeOf(f32);
    return .{
        .kind = kind,
        .route = .{ .format = .q4_0, .row_bucket = .rows_1, .epilogue = .pair_activation, .dispatch = .mmv },
        .kernel_id = kernel_id,
        .production_baseline = "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn",
        .production_enabled = production_enabled,
        .launch = .{
            .grid = .flattened_rows_by_output_blocks,
            .threads_per_block = threads,
            .output_rows_per_block = 1,
            .output_cols_per_block = 32,
            .static_shared_memory_bytes = shared_bytes,
            .rows = .{ .min = 1, .max = 1, .fixed = 1 },
            .input_dim = .{ .multiple_of = 32, .fixed = input_dim },
            .output_dim = .{ .multiple_of = 32, .fixed = output_dim },
        },
        .lowering = .{ .q4_0_pair_activation_q8_1 = .{
            .decode = decode,
            .input_blocks = input_blocks,
            .output_blocks = output_blocks,
            .output_values_per_block = 32,
            .warps_per_projection_group = warps_per_projection_group,
        } },
    };
}

fn downQ8Plan(
    kind: KernelKind,
    kernel_id: []const u8,
    input_dim: u16,
    output_dim: u16,
    threads_per_block: u16,
    decode: QuantDecodePrimitive,
    production_enabled: bool,
) RenderPlan {
    const input_blocks = input_dim / 32;
    const warps = threads_per_block / 32;
    return .{
        .kind = kind,
        .route = .{ .format = .q4_0, .row_bucket = .rows_1, .epilogue = .gated_down, .dispatch = .mmv },
        .kernel_id = kernel_id,
        .production_baseline = "termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down",
        .production_enabled = production_enabled,
        .launch = .{
            .grid = .output_cols,
            .threads_per_block = threads_per_block,
            .output_rows_per_block = 1,
            .output_cols_per_block = 4,
            .static_shared_memory_bytes = @as(u32, 4) * warps * @sizeOf(f32),
            .rows = .{ .min = 1, .max = 1, .fixed = 1 },
            .input_dim = .{ .multiple_of = 32, .fixed = input_dim },
            .output_dim = .{ .fixed = output_dim },
        },
        .lowering = .{ .q4_0_down_q8_1 = .{
            .decode = decode,
            .input_blocks = input_blocks,
            .columns_per_block = 4,
        } },
    };
}

/// Llama.cpp-style MMVQ topology for one output row per CTA on Ada: four
/// warps (128 threads), grid.x = rows * out_dim. Pair activation keeps its
/// 32-output CTA because the destination Q8_1 scale/sum requires a complete
/// 32-value block; the down projection can use the canonical one-row MMVQ.
fn downGgmlQ8Plan(
    kind: KernelKind,
    kernel_id: []const u8,
    input_dim: u16,
    output_dim: u16,
) RenderPlan {
    const threads_per_block: u16 = 128;
    return .{
        .kind = kind,
        .route = .{ .format = .q4_0, .row_bucket = .rows_1, .epilogue = .gated_down, .dispatch = .mmv },
        .kernel_id = kernel_id,
        .production_baseline = "termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down",
        .production_enabled = false,
        .launch = .{
            .grid = .output_cols,
            .threads_per_block = threads_per_block,
            .output_rows_per_block = 1,
            .output_cols_per_block = 1,
            .static_shared_memory_bytes = 4 * @sizeOf(f32),
            .rows = .{ .min = 1, .max = 1, .fixed = 1 },
            .input_dim = .{ .multiple_of = 32, .fixed = input_dim },
            .output_dim = .{ .fixed = output_dim },
        },
        .lowering = .{ .q4_0_down_q8_1 = .{
            .decode = .q4_0_q8_1_dp4a,
            .input_blocks = input_dim / 32,
            .columns_per_block = 1,
        } },
    };
}

fn exactFfnPairF32Plan(
    kind: KernelKind,
    kernel_id: []const u8,
    input_dim: u16,
    output_dim: u16,
) RenderPlan {
    const input_blocks = input_dim / 32;
    const warps: u8 = 4;
    return .{
        .kind = kind,
        .route = .{ .format = .q4_0, .row_bucket = .rows_1, .epilogue = .pair_activation, .dispatch = .mmv },
        .kernel_id = kernel_id,
        .production_baseline = "termite_linear_q4_0_pair_activation_f32_tile4_w4",
        .production_enabled = false,
        .launch = .{
            .grid = .output_cols,
            .threads_per_block = @as(u16, warps) * 32,
            .output_rows_per_block = 1,
            .output_cols_per_block = 4,
            .static_shared_memory_bytes = @as(u32, 2) * 4 * @as(u32, warps) * @sizeOf(f32),
            .rows = .{ .min = 1, .max = 1, .fixed = 1 },
            .input_dim = .{ .multiple_of = 32, .fixed = input_dim },
            .output_dim = .{ .fixed = output_dim },
        },
        .lowering = .{ .q4_0_pair_activation_f32_exact = .{
            .input_blocks = input_blocks,
            .warps_per_block = warps,
        } },
    };
}

fn exactFfnDownF32Plan(
    kind: KernelKind,
    kernel_id: []const u8,
    input_dim: u16,
    output_dim: u16,
) RenderPlan {
    const input_blocks = input_dim / 32;
    const warps: u8 = 8;
    return .{
        .kind = kind,
        .route = .{ .format = .q4_0, .row_bucket = .rows_1, .epilogue = .gated_down, .dispatch = .mmv },
        .kernel_id = kernel_id,
        .production_baseline = "termite_linear_q4_0_f32_tile4",
        .production_enabled = false,
        .launch = .{
            .grid = .output_cols,
            .threads_per_block = @as(u16, warps) * 32,
            .output_rows_per_block = 1,
            .output_cols_per_block = 4,
            .static_shared_memory_bytes = @as(u32, 4) * @as(u32, warps) * @sizeOf(f32),
            .rows = .{ .min = 1, .max = 1, .fixed = 1 },
            .input_dim = .{ .multiple_of = 32, .fixed = input_dim },
            .output_dim = .{ .fixed = output_dim },
        },
        .lowering = .{ .q4_0_down_f32_exact = .{
            .input_blocks = input_blocks,
            .warps_per_block = warps,
        } },
    };
}

pub fn attentionPlanFor(kind: AttentionKernelKind) AttentionRenderPlan {
    return switch (kind) {
        .gqa_decode_split_kv_hd256_f32 => attentionPlan(
            kind,
            .split8,
            256,
            "antfly_gqa_attention_decode_split8_kv_hd256_f32_v1",
            generated_attention_hd256_split8_serial_kernel_id,
            generated_attention_hd256_split8_stage1_kernel_id,
            generated_attention_hd256_split8_stage2_kernel_id,
        ),
        .gqa_decode_split_kv_hd512_f32 => attentionPlan(
            kind,
            .split8,
            512,
            "antfly_gqa_attention_decode_split8_kv_hd512_f32_v1",
            generated_attention_hd512_split8_serial_kernel_id,
            generated_attention_hd512_split8_stage1_kernel_id,
            generated_attention_hd512_split8_stage2_kernel_id,
        ),
        .gqa_decode_split2_kv_hd256_f32 => attentionPlan(
            kind,
            .split2,
            256,
            "antfly_gqa_attention_decode_split2_kv_hd256_f32_v1",
            generated_attention_hd256_split2_serial_kernel_id,
            generated_attention_hd256_split2_stage1_kernel_id,
            generated_attention_hd256_split2_stage2_kernel_id,
        ),
        .gqa_decode_split2_kv_hd512_f32 => attentionPlan(
            kind,
            .split2,
            512,
            "antfly_gqa_attention_decode_split2_kv_hd512_f32_v1",
            generated_attention_hd512_split2_serial_kernel_id,
            generated_attention_hd512_split2_stage1_kernel_id,
            generated_attention_hd512_split2_stage2_kernel_id,
        ),
        .gqa_decode_split4_kv_hd256_f32 => attentionPlan(
            kind,
            .split4,
            256,
            "antfly_gqa_attention_decode_split4_kv_hd256_f32_v1",
            generated_attention_hd256_split4_serial_kernel_id,
            generated_attention_hd256_split4_stage1_kernel_id,
            generated_attention_hd256_split4_stage2_kernel_id,
        ),
        .gqa_decode_split4_kv_hd512_f32 => attentionPlan(
            kind,
            .split4,
            512,
            "antfly_gqa_attention_decode_split4_kv_hd512_f32_v1",
            generated_attention_hd512_split4_serial_kernel_id,
            generated_attention_hd512_split4_stage1_kernel_id,
            generated_attention_hd512_split4_stage2_kernel_id,
        ),
        .gqa_decode_score_prework_hd256_f32 => scorePreworkAttentionPlan(
            kind,
            256,
            "antfly_gqa_attention_decode_turboquant_score_prework_hd256_f32_v2",
            generated_attention_hd256_score_prework_kernel_id,
            generated_attention_hd256_score_prework_serial_kernel_id,
            generated_attention_hd256_score_prework_tiled64_kernel_id,
        ),
        .gqa_decode_score_prework_hd512_f32 => scorePreworkAttentionPlan(
            kind,
            512,
            "antfly_gqa_attention_decode_turboquant_score_prework_hd512_f32_v2",
            generated_attention_hd512_score_prework_kernel_id,
            generated_attention_hd512_score_prework_serial_kernel_id,
            generated_attention_hd512_score_prework_tiled64_kernel_id,
        ),
    };
}

fn attentionPlan(
    kind: AttentionKernelKind,
    split_variant: AttentionSplitVariant,
    head_dim: u16,
    source_id: []const u8,
    serial_kernel_id: []const u8,
    stage1_kernel_id: []const u8,
    stage2_kernel_id: []const u8,
) AttentionRenderPlan {
    return .{
        .kind = kind,
        .source_id = source_id,
        .kernel_id = stage1_kernel_id,
        .production_baseline = "termite_gqa_attention_decode_scalars_fast_f32",
        .production_enabled = false,
        .serial_kernel_id = serial_kernel_id,
        .serial_launch = .{
            .grid = .batch_query_heads,
            .threads_per_block = if (head_dim == 256) 256 else 512,
            .output_rows_per_block = 1,
            .output_cols_per_block = head_dim,
            .static_shared_memory_bytes = 20 * @sizeOf(f32),
            .rows = .{ .min = 1, .max = 1, .fixed = 1 },
            .input_dim = .{ .min = head_dim, .max = head_dim, .fixed = head_dim },
            .output_dim = .{},
        },
        .launch = .{
            .grid = .batch_query_heads_by_splits,
            .threads_per_block = head_dim,
            .output_rows_per_block = 1,
            .output_cols_per_block = head_dim,
            .static_shared_memory_bytes = if (head_dim == 256) 384 else 640,
            .rows = .{ .min = 1, .max = 1, .fixed = 1 },
            .input_dim = .{ .min = head_dim, .max = head_dim, .fixed = head_dim },
            .output_dim = .{},
        },
        .reduction_kernel_id = stage2_kernel_id,
        .reduction_launch = .{
            .grid = .batch_query_heads,
            .threads_per_block = head_dim,
            .output_rows_per_block = 1,
            .output_cols_per_block = head_dim,
            // One running denominator plus an ordered alpha/beta pair for
            // every contiguous KV partial summary.
            .static_shared_memory_bytes = @as(u32, 1 + 2 * @as(u32, split_variant.kvSplits())) * @sizeOf(f32),
            .rows = .{ .min = 1, .max = 1, .fixed = 1 },
            .input_dim = .{ .min = head_dim, .max = head_dim, .fixed = head_dim },
            .output_dim = .{},
        },
        .lowering = .{
            .kind = .decode_1x,
            .reduction = .split_kv_two_stage,
            .softmax = .online_partials_stable_merge,
            .head_dim = head_dim,
            .device_decode_scalars = true,
            .split_variant = split_variant,
            .kv_splits = split_variant.kvSplits(),
            .query_heads_per_kv_head = generated_attention_query_heads_per_kv_head,
            .query_storage = .f32,
            .key_storage = .f32,
            .value_storage = .f32,
            .output_storage = .f32,
            .split_kv_min_tokens_default = generated_attention_split_kv_min_tokens_default,
            .max_kv_tokens = 0,
        },
    };
}

fn scorePreworkAttentionPlan(
    kind: AttentionKernelKind,
    head_dim: u16,
    source_id: []const u8,
    score_kernel_id: []const u8,
    serial_kernel_id: []const u8,
    tiled64_kernel_id: []const u8,
) AttentionRenderPlan {
    const threads: u16 = if (head_dim == 256) 256 else 512;
    const tiled64_shared_bytes = generatedAttentionScorePreworkTiled64SharedBytes(head_dim) orelse unreachable;
    return .{
        .kind = kind,
        .source_id = source_id,
        .kernel_id = score_kernel_id,
        .production_baseline = "termite_gqa_attention_decode_turboquant_fast_f32",
        // Promoted composite: the runtime automatic selector engages this
        // route by default for the qualified SM89 Gemma 4 F16 geometry at the
        // 512-token KV crossover.
        .production_enabled = true,
        // The reduction field names predate this two-kernel lowering. Both
        // refer to the one chronological recurrence consumer; emitting it once
        // keeps the ABI explicit without inventing a fake merge kernel.
        .serial_kernel_id = serial_kernel_id,
        .serial_launch = .{
            .grid = .batch_query_heads,
            .threads_per_block = threads,
            .output_rows_per_block = 1,
            .output_cols_per_block = head_dim,
            .static_shared_memory_bytes = generated_attention_score_prework_recurrence_shared_bytes,
            .rows = .{ .min = 1, .max = 1, .fixed = 1 },
            .input_dim = .{ .min = head_dim, .max = head_dim, .fixed = head_dim },
            .output_dim = .{},
        },
        .launch = .{
            .grid = .batch_query_heads_by_splits,
            .threads_per_block = threads,
            .output_rows_per_block = 1,
            .output_cols_per_block = 1,
            .static_shared_memory_bytes = @as(u32, threads / 32) * @sizeOf(f32),
            .rows = .{ .min = 1, .max = 1, .fixed = 1 },
            .input_dim = .{ .min = head_dim, .max = head_dim, .fixed = head_dim },
            .output_dim = .{},
        },
        .reduction_kernel_id = serial_kernel_id,
        .reduction_launch = .{
            .grid = .batch_query_heads,
            .threads_per_block = threads,
            .output_rows_per_block = 1,
            .output_cols_per_block = head_dim,
            .static_shared_memory_bytes = generated_attention_score_prework_recurrence_shared_bytes,
            .rows = .{ .min = 1, .max = 1, .fixed = 1 },
            .input_dim = .{ .min = head_dim, .max = head_dim, .fixed = head_dim },
            .output_dim = .{},
        },
        .tiled64_kernel_id = tiled64_kernel_id,
        .tiled64_launch = .{
            .grid = .batch_query_heads_by_output_tiles,
            .threads_per_block = generated_attention_score_prework_tiled64_tile_size,
            .output_rows_per_block = 1,
            .output_cols_per_block = generated_attention_score_prework_tiled64_tile_size,
            .static_shared_memory_bytes = tiled64_shared_bytes,
            .rows = .{ .min = 1, .max = 1, .fixed = 1 },
            .input_dim = .{ .min = head_dim, .max = head_dim, .fixed = head_dim },
            .output_dim = .{},
        },
        .lowering = .{
            .kind = .decode_1x,
            .reduction = .parallel_scores_then_serial,
            .softmax = .online_chronological,
            .head_dim = head_dim,
            .device_decode_scalars = true,
            .split_variant = .score_prework,
            .kv_splits = generated_attention_score_prework_chunks,
            .query_heads_per_kv_head = generated_attention_query_heads_per_kv_head,
            .query_storage = .f32,
            .key_storage = .paged_f16_or_polar4,
            .value_storage = .paged_f16_or_f32,
            .output_storage = .f32,
            .split_kv_min_tokens_default = 0,
            .max_kv_tokens = generated_attention_score_prework_max_kv_tokens,
        },
    };
}

pub fn flashPrefillPlanFor(kind: FlashPrefillKernelKind) FlashPrefillRenderPlan {
    return switch (kind) {
        .gqa_prefill_flash_sm89_hd256_swa512_f32 => flashPrefillPlan(
            kind,
            256,
            512,
            "antfly_gqa_attention_prefill_flash_sm89_hd256_swa512_f32_v1",
            "antfly_flash_prefill_sm89_hd256_swa512_f32_v1",
            generated_flash_prefill_hd256_kernel_id,
        ),
        .gqa_prefill_flash_sm89_hd512_global_f32 => flashPrefillPlan(
            kind,
            512,
            0,
            "antfly_gqa_attention_prefill_flash_sm89_hd512_global_f32_v1",
            "antfly_flash_prefill_sm89_hd512_global_f32_v1",
            generated_flash_prefill_hd512_kernel_id,
        ),
    };
}

fn flashPrefillPlan(
    kind: FlashPrefillKernelKind,
    head_dim: u16,
    sliding_window: u16,
    source_id: []const u8,
    namespace_id: []const u8,
    kernel_id: []const u8,
) FlashPrefillRenderPlan {
    return .{
        .kind = kind,
        .source_id = source_id,
        .namespace_id = namespace_id,
        .kernel_id = kernel_id,
        .production_baseline = "termite_gqa_attention_prefill_tiled_f16_warp_f32",
        .production_enabled = true,
        .runtime_default_enabled = true,
        .exact_token_parity_required_for_default = true,
        .exact_token_parity_qualified = true,
        .launch = .{
            .grid = .query_heads_by_query_tiles,
            .threads_per_block = generated_flash_prefill_threads,
            .output_rows_per_block = generated_flash_prefill_query_tile,
            .output_cols_per_block = head_dim,
            .static_shared_memory_bytes = 0,
            .dynamic_shared_memory_bytes = generatedFlashPrefillDynamicSharedBytes(head_dim).?,
            .rows = .{ .min = 3, .max = 512 },
            .input_dim = .{ .min = head_dim, .max = head_dim, .fixed = head_dim },
            .output_dim = .{},
        },
        .lowering = .{
            .kind = .prefill_flash,
            .head_dim = head_dim,
            .query_heads = 8,
            .kv_heads = 1,
            .query_tile = generated_flash_prefill_query_tile,
            .key_tile = generated_flash_prefill_key_tile,
            .page_size_tokens = generated_flash_prefill_page_size_tokens,
            .sliding_window = sliding_window,
            .query_length_policy = .gemma4_q512_or_q3_v1,
            .query_storage = .f32,
            .key_storage = .paged_f16,
            .value_storage = .paged_f16,
            .output_storage = .f32,
            .format = generated_flash_prefill_format_f16,
            .value_format = generated_flash_prefill_format_f16,
            .required_compute_major = 8,
            .required_compute_minor = 9,
        },
    };
}

pub fn splitkOnlineDecodePlanFor(kind: SplitkOnlineDecodeKernelKind) SplitkOnlineDecodeRenderPlan {
    return switch (kind) {
        .gqa_decode_splitk_online_sm89_hd256_swa512_f16_f32 => splitkOnlineDecodePlan(
            kind,
            256,
            512,
            generated_splitk_online_decode_hd256_max_visible_tokens,
            "antfly_gqa_attention_decode_splitk_online_sm89_hd256_swa512_f16_f32_v1",
            "antfly_splitk_online_decode_sm89_hd256_swa512_f16_f32_v1",
            generated_splitk_online_decode_hd256_kernel_id,
        ),
        .gqa_decode_splitk_online_sm89_hd512_global_f16_f32 => splitkOnlineDecodePlan(
            kind,
            512,
            0,
            generated_splitk_online_decode_hd512_max_visible_tokens,
            "antfly_gqa_attention_decode_splitk_online_sm89_hd512_global_f16_f32_v1",
            "antfly_splitk_online_decode_sm89_hd512_global_f16_f32_v1",
            generated_splitk_online_decode_hd512_kernel_id,
        ),
    };
}

fn splitkOnlineDecodePlan(
    kind: SplitkOnlineDecodeKernelKind,
    head_dim: u16,
    sliding_window: u16,
    max_visible_tokens: u16,
    source_id: []const u8,
    namespace_id: []const u8,
    kernel_id: []const u8,
) SplitkOnlineDecodeRenderPlan {
    return .{
        .kind = kind,
        .source_id = source_id,
        .namespace_id = namespace_id,
        .kernel_id = kernel_id,
        .production_baseline = "termite_gqa_attention_decode_turboquant_fast_f32",
        .production_enabled = false,
        .runtime_default_enabled = false,
        .exact_token_parity_required_for_default = true,
        .exact_token_parity_qualified = false,
        .launch = .{
            .grid = .batch_query_heads_by_splits,
            .threads_per_block = generated_splitk_online_decode_threads,
            .output_rows_per_block = 1,
            .output_cols_per_block = head_dim,
            .static_shared_memory_bytes = generated_splitk_online_decode_static_shared_memory_bytes,
            .rows = .{ .min = 1, .max = 1, .fixed = 1 },
            .input_dim = .{ .min = head_dim, .max = head_dim, .fixed = head_dim },
            .output_dim = .{},
        },
        .workspace = generatedSplitkOnlineDecodeWorkspaceLayout(),
        .lowering = .{
            .kind = .decode_1x,
            .head_dim = head_dim,
            .query_heads = generated_splitk_online_decode_query_heads,
            .kv_heads = generated_splitk_online_decode_kv_heads,
            .kv_splits = generated_splitk_online_decode_splits,
            .page_size_tokens = generated_splitk_online_decode_page_size_tokens,
            .sliding_window = sliding_window,
            .max_visible_tokens = max_visible_tokens,
            .query_storage = .f32,
            .key_storage = .paged_f16,
            .value_storage = .paged_f16,
            .output_storage = .f32,
            .format = generated_splitk_online_decode_format_f16,
            .value_format = generated_splitk_online_decode_format_f16,
            .required_compute_major = 8,
            .required_compute_minor = 9,
        },
    };
}

fn sourceKind(plan: RenderPlan) []const u8 {
    return if (plan.production_enabled)
        "Promoted generated kernel"
    else
        "Dev-only generated kernel candidate";
}

fn promotionComment(kind: KernelKind) []const u8 {
    return switch (kind) {
        .q4_k_small_batch_bias_gelu =>
        \\// Not compiled into production artifacts until correctness and benchmark gates
        \\// beat the handwritten CUDA baseline.
        ,
        .q4_k_mmv =>
        \\// Decode candidate for Q4_K-backed Gemma 4 projections. It remains dev-only
        \\// until target-specific microbench and full-model promotion gates pass.
        ,
        .q4_0_mmv =>
        \\// Qualified on sequential benchmark evidence vs the handwritten CUDA baseline;
        \\// runtime dispatch requires ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MMV=1.
        ,
        .q4_0_mm =>
        \\// Qualified on sequential benchmark evidence vs the handwritten CUDA baseline;
        \\// runtime dispatch requires ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MM=1.
        ,
        .q4_0_pair_mmv =>
        \\// Qualified on sequential benchmark evidence vs the handwritten CUDA baseline;
        \\// runtime dispatch requires ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_PAIR=1.
        ,
        .q4_0_pair_activation_q8_1 =>
        \\// Qualified on sequential benchmark evidence vs the handwritten CUDA baseline;
        \\// runtime dispatch requires ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_PAIR_Q8=1.
        ,
        .q4_0_pair_activation_q8_1_e2b_6144,
        .q4_0_pair_activation_q8_1_e2b_12288,
        .q4_0_down_q8_1_e2b_6144,
        .q4_0_down_q8_1_e2b_12288,
        =>
        \\// Shape-specific E2B candidate; not bundled or wired until correctness and
        \\// sequential benchmark evidence clear the promotion gate.
        ,
        .q4_0_pair_activation_ggml_q8_1_e2b_6144,
        .q4_0_pair_activation_ggml_q8_1_e2b_12288,
        .q4_0_down_ggml_q8_1_e2b_6144,
        .q4_0_down_ggml_q8_1_e2b_12288,
        =>
        \\// SM89-only diagnostic candidates using llama.cpp CUDA's
        \\// block_q8_1 half2(d,raw_sum) activation contract.
        \\// Q8_1 contract. Production is default-off pending differential parity
        \\// and rotating-buffer end-to-end throughput evidence.
        ,
        .q4_0_pair_activation_f32_e2b_6144_exact,
        .q4_0_pair_activation_f32_e2b_12288_exact,
        .q4_0_down_f32_e2b_6144_exact,
        .q4_0_down_f32_e2b_12288_exact,
        =>
        \\// Runtime-wired exact F32 E2B FFN candidate. It preserves the handwritten
        \\// activation buffer and reduction order; promotion still requires GPU parity
        \\// and full-model speed evidence.
        ,
        .q4_0_q8_1_argmax_e2b_tile8 =>
        \\// Runtime-wired E2B candidate behind ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_Q8_1_ARGMAX;
        \\// production stays disabled until exact-token and full-chain benchmark gates pass.
        ,
        .q6_k_q8_1_argmax_k2560_tile8,
        .q6_k_q8_1_argmax_k3840_tile8,
        =>
        \\// Standalone shape candidates. The handwritten Q6_K x Q8_1 LM-head route
        \\// remains authoritative until exact-token and target-specific speed gates pass.
        ,
        .q4_0_down_q8_1 =>
        \\// Qualified on sequential benchmark evidence vs the handwritten CUDA baseline;
        \\// runtime dispatch requires ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_DOWN_Q8=1.
        ,
    };
}

pub fn planId(plan: RenderPlan, allocator: std.mem.Allocator) ![]u8 {
    return std.fmt.allocPrint(allocator, "cuda/{s}/{s}/{s}/{s}", .{
        @tagName(plan.route.format),
        @tagName(plan.route.row_bucket),
        @tagName(plan.route.epilogue),
        @tagName(plan.route.dispatch),
    });
}

pub fn renderKernel(allocator: std.mem.Allocator, plan: RenderPlan) ![]u8 {
    try plan.validate();
    const support = supportFor(plan.lowering);
    const route_id = try planId(plan, allocator);
    defer allocator.free(route_id);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, license_header);
    try out.appendSlice(allocator, "\n\n// ");
    try out.appendSlice(allocator, sourceKind(plan));
    try out.appendSlice(allocator, " from graph/quant_kernel_compiler.zig.\n// plan_id=");
    try out.appendSlice(allocator, route_id);
    try out.appendSlice(allocator, "\n// kernel_id=");
    try out.appendSlice(allocator, plan.kernel_id);
    try out.appendSlice(allocator, "\n// production_baseline=");
    try out.appendSlice(allocator, plan.production_baseline);
    try out.appendSlice(allocator, if (plan.production_enabled) "\n// production_enabled=true\n" else "\n// production_enabled=false\n");
    try out.appendSlice(allocator, promotionComment(plan.kind));
    try out.appendSlice(allocator, "\n\n");
    for (support.includes) |include| {
        try out.appendSlice(allocator, "#include ");
        try out.appendSlice(allocator, include);
        try out.append(allocator, '\n');
    }
    try out.append(allocator, '\n');
    for (support.helpers) |helper| {
        try out.appendSlice(allocator, helper.source);
        try out.appendSlice(allocator, "\n\n");
    }
    try renderBody(allocator, &out, plan);
    if (out.items.len == 0 or out.items[out.items.len - 1] != '\n') try out.append(allocator, '\n');
    return out.toOwnedSlice(allocator);
}

pub fn attentionPlanId(plan: AttentionRenderPlan, allocator: std.mem.Allocator) ![]u8 {
    return switch (plan.lowering.reduction) {
        .split_kv_two_stage => std.fmt.allocPrint(allocator, "cuda/attention/{s}/hd{d}/gqa{d}/split{d}/min{d}/{s}/device_scalars", .{
            @tagName(plan.lowering.kind),
            plan.lowering.head_dim,
            plan.lowering.query_heads_per_kv_head,
            plan.lowering.kv_splits,
            plan.lowering.split_kv_min_tokens_default,
            @tagName(plan.lowering.query_storage),
        }),
        .parallel_scores_then_serial => std.fmt.allocPrint(allocator, "cuda/attention/{s}/hd{d}/gqa{d}/score-prework/max{d}/consumers-serial+tiled{d}-max{d}/{s}/device_scalars", .{
            @tagName(plan.lowering.kind),
            plan.lowering.head_dim,
            plan.lowering.query_heads_per_kv_head,
            plan.lowering.max_kv_tokens,
            generated_attention_score_prework_tiled64_tile_size,
            generatedAttentionScorePreworkTiled64MaxKvTokens(plan.lowering.head_dim) orelse 0,
            @tagName(plan.lowering.query_storage),
        }),
    };
}

pub fn renderAttentionKernel(allocator: std.mem.Allocator, plan: AttentionRenderPlan) ![]u8 {
    try plan.validate();
    const route_id = try attentionPlanId(plan, allocator);
    defer allocator.free(route_id);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, license_header);
    try out.appendSlice(allocator, if (plan.production_enabled)
        "\n\n// Production generated attention kernel from graph/quant_kernel_compiler.zig.\n// plan_id="
    else
        "\n\n// Dev-only generated attention kernel from graph/quant_kernel_compiler.zig.\n// plan_id=");
    try out.appendSlice(allocator, route_id);
    try out.appendSlice(allocator, "\n// source_id=");
    try out.appendSlice(allocator, plan.source_id);
    try out.appendSlice(allocator, "\n// kernel_id=");
    try out.appendSlice(allocator, plan.kernel_id);
    try out.appendSlice(allocator, "\n// serial_kernel_id=");
    try out.appendSlice(allocator, plan.serial_kernel_id);
    if (plan.tiled64_kernel_id) |kernel_id| {
        try out.appendSlice(allocator, "\n// tiled64_kernel_id=");
        try out.appendSlice(allocator, kernel_id);
    }
    try out.appendSlice(allocator, "\n// reduction_kernel_id=");
    try out.appendSlice(allocator, plan.reduction_kernel_id);
    try out.appendSlice(allocator, "\n// production_baseline=");
    try out.appendSlice(allocator, plan.production_baseline);
    try out.appendSlice(allocator, if (plan.production_enabled) "\n// production_enabled=true\n" else "\n// production_enabled=false\n");
    try out.appendSlice(
        allocator,
        if (plan.lowering.reduction == .parallel_scores_then_serial)
            "// Default-on automatic SM89 selection; rollback: ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK=0.\n" ++
                "// Tiled consumer opt-in: ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK_CONSUMER=tiled64.\n\n"
        else
            "// Runtime opt-in: ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE=1.\n\n",
    );
    for (includes_math) |include| {
        try out.appendSlice(allocator, "#include ");
        try out.appendSlice(allocator, include);
        try out.append(allocator, '\n');
    }
    if (plan.lowering.reduction == .parallel_scores_then_serial) {
        try out.appendSlice(allocator, "#include <cuda_fp16.h>\n");
    }
    try out.append(allocator, '\n');
    for (attention_runtime_external_helpers) |helper| {
        try out.appendSlice(allocator, helper.source);
        try out.appendSlice(allocator, "\n\n");
    }
    if (plan.lowering.reduction == .parallel_scores_then_serial) {
        for (attention_score_prework_runtime_external_helpers) |helper| {
            try out.appendSlice(allocator, helper.source);
            try out.appendSlice(allocator, "\n\n");
        }
    }
    try renderAttentionBody(allocator, &out, plan);
    if (out.items.len == 0 or out.items[out.items.len - 1] != '\n') try out.append(allocator, '\n');
    return out.toOwnedSlice(allocator);
}

const flash_prefill_f16_sm89_template =
    @embedFile("templates/cuda_gqa_flash_prefill_f16_sm89.cuh");

pub fn flashPrefillPlanId(plan: FlashPrefillRenderPlan, allocator: std.mem.Allocator) ![]u8 {
    try plan.validate();
    return std.fmt.allocPrint(
        allocator,
        "cuda/attention/prefill_flash/sm89/hd{d}/gqa8/page16/q512-or-q3/swa{d}/f32q-f16kv-hmma",
        .{ plan.lowering.head_dim, plan.lowering.sliding_window },
    );
}

pub fn renderFlashPrefillBodyAlloc(
    allocator: std.mem.Allocator,
    plan: FlashPrefillRenderPlan,
) ![]u8 {
    try plan.validate();
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try appendFmt(allocator, &out,
        \\#define ANTFLY_FLASH_NAMESPACE {s}
        \\#define ANTFLY_FLASH_KERNEL {s}
        \\#define ANTFLY_FLASH_HEAD_DIM {d}
        \\#define ANTFLY_FLASH_SLIDING_WINDOW {d}
        \\
    , .{
        plan.namespace_id,
        plan.kernel_id,
        plan.lowering.head_dim,
        plan.lowering.sliding_window,
    });
    try out.appendSlice(allocator, flash_prefill_f16_sm89_template);
    if (out.items.len == 0 or out.items[out.items.len - 1] != '\n') try out.append(allocator, '\n');
    try out.appendSlice(allocator,
        \\#undef ANTFLY_FLASH_SLIDING_WINDOW
        \\#undef ANTFLY_FLASH_HEAD_DIM
        \\#undef ANTFLY_FLASH_KERNEL
        \\#undef ANTFLY_FLASH_NAMESPACE
        \\
    );
    return out.toOwnedSlice(allocator);
}

pub fn renderFlashPrefillKernel(
    allocator: std.mem.Allocator,
    plan: FlashPrefillRenderPlan,
) ![]u8 {
    try plan.validate();
    const route_id = try flashPrefillPlanId(plan, allocator);
    defer allocator.free(route_id);
    const body = try renderFlashPrefillBodyAlloc(allocator, plan);
    defer allocator.free(body);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try appendFmt(allocator, &out,
        \\// {s} generated Flash prefill from graph/quant_kernel_compiler.zig.
        \\// plan_id={s}
        \\// source_id={s}
        \\// kernel_id={s}
        \\// production_baseline={s}
        \\// runtime_default_enabled={s}
        \\// exact_token_parity_qualified={s}
        \\
    , .{
        if (plan.production_enabled) "Production" else "Default-off",
        route_id,
        plan.source_id,
        plan.kernel_id,
        plan.production_baseline,
        if (plan.runtime_default_enabled) "true" else "false",
        if (plan.exact_token_parity_qualified) "true" else "false",
    });
    if (plan.runtime_default_enabled) {
        try out.appendSlice(
            allocator,
            "// Default-on automatic SM89 selection; rollback: ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=off.\n",
        );
    }
    try out.appendSlice(allocator, body);
    return out.toOwnedSlice(allocator);
}

const splitk_online_decode_sm89_template =
    @embedFile("templates/cuda_gqa_decode_splitk_online_sm89.cuh");

pub fn splitkOnlineDecodePlanId(
    plan: SplitkOnlineDecodeRenderPlan,
    allocator: std.mem.Allocator,
) ![]u8 {
    try plan.validate();
    return std.fmt.allocPrint(
        allocator,
        "cuda/attention/decode_1x/splitk-online/sm89/hd{d}/gqa8/split64/t128/page16/swa{d}/f32q-f16kv-f32o",
        .{ plan.lowering.head_dim, plan.lowering.sliding_window },
    );
}

pub fn renderSplitkOnlineDecodeBodyAlloc(
    allocator: std.mem.Allocator,
    plan: SplitkOnlineDecodeRenderPlan,
) ![]u8 {
    try plan.validate();
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try appendFmt(allocator, &out,
        \\#define ANTFLY_SPLITK_ONLINE_NAMESPACE {s}
        \\#define ANTFLY_SPLITK_ONLINE_KERNEL {s}
        \\#define ANTFLY_SPLITK_ONLINE_HEAD_DIM {d}
        \\#define ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW {d}
        \\#define ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS {d}
        \\
    , .{
        plan.namespace_id,
        plan.kernel_id,
        plan.lowering.head_dim,
        plan.lowering.sliding_window,
        plan.lowering.max_visible_tokens,
    });
    try out.appendSlice(allocator, splitk_online_decode_sm89_template);
    if (out.items.len == 0 or out.items[out.items.len - 1] != '\n') try out.append(allocator, '\n');
    try out.appendSlice(allocator,
        \\#undef ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS
        \\#undef ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW
        \\#undef ANTFLY_SPLITK_ONLINE_HEAD_DIM
        \\#undef ANTFLY_SPLITK_ONLINE_KERNEL
        \\#undef ANTFLY_SPLITK_ONLINE_NAMESPACE
        \\
    );
    return out.toOwnedSlice(allocator);
}

pub fn renderSplitkOnlineDecodeKernel(
    allocator: std.mem.Allocator,
    plan: SplitkOnlineDecodeRenderPlan,
) ![]u8 {
    try plan.validate();
    const route_id = try splitkOnlineDecodePlanId(plan, allocator);
    defer allocator.free(route_id);
    const body = try renderSplitkOnlineDecodeBodyAlloc(allocator, plan);
    defer allocator.free(body);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try appendFmt(allocator, &out,
        \\// Default-off generated split-K online decode from graph/quant_kernel_compiler.zig.
        \\// plan_id={s}
        \\// source_id={s}
        \\// kernel_id={s}
        \\// production_baseline={s}
        \\// runtime_default_enabled=false
        \\// exact_token_parity_qualified=false
        \\
    , .{
        route_id,
        plan.source_id,
        plan.kernel_id,
        plan.production_baseline,
    });
    try out.appendSlice(allocator, body);
    return out.toOwnedSlice(allocator);
}

const helper_half_le_to_float = SourceFragment{
    .name = "antfly_half_le_to_float",
    .source =
    \\static __device__ __forceinline__ float antfly_half_le_to_float(const uint8_t *p) {
    \\    const uint16_t bits = (uint16_t)p[0] | ((uint16_t)p[1] << 8);
    \\    return __half2float(__ushort_as_half(bits));
    \\}
    ,
};

const helper_warp_reduce_sum = SourceFragment{
    .name = "antfly_warp_reduce_sum",
    .source =
    \\static __device__ __forceinline__ float antfly_warp_reduce_sum(float value) {
    \\    value += __shfl_down_sync(0xffffffffu, value, 16);
    \\    value += __shfl_down_sync(0xffffffffu, value, 8);
    \\    value += __shfl_down_sync(0xffffffffu, value, 4);
    \\    value += __shfl_down_sync(0xffffffffu, value, 2);
    \\    value += __shfl_down_sync(0xffffffffu, value, 1);
    \\    return value;
    \\}
    ,
};

const helper_q4_k_block_view = SourceFragment{
    .name = "antfly_q4_k_block_view",
    .source =
    \\struct antfly_q4_k_block_view {
    \\    const uint8_t *d;
    \\    const uint8_t *dmin;
    \\    const uint8_t *scales;
    \\    const uint8_t *qs;
    \\};
    ,
};

const helper_gelu = SourceFragment{
    .name = "antfly_gelu",
    .source =
    \\static __device__ __forceinline__ float antfly_gelu(float x) {
    \\    if (!isfinite(x)) return 0.0f;
    \\    const float inner = 0.7978845608028654f * (x + 0.044715f * x * x * x);
    \\    if (inner > 10.0f) return x;
    \\    if (inner < -10.0f) return 0.0f;
    \\    const float y = 0.5f * x * (1.0f + tanhf(inner));
    \\    return isfinite(y) ? y : 0.0f;
    \\}
    ,
};

const helper_q4_k_unpack_scale_min = SourceFragment{
    .name = "antfly_q4_k_unpack_scale_min",
    .source =
    \\static __device__ __forceinline__ void antfly_q4_k_unpack_scale_min(
    \\    const uint8_t *scales,
    \\    int sub,
    \\    float *scale,
    \\    float *min_v
    \\) {
    \\    if (sub < 4) {
    \\        *scale = (float)(scales[sub] & 63u);
    \\        *min_v = (float)(scales[sub + 4] & 63u);
    \\        return;
    \\    }
    \\
    \\    *scale = (float)((scales[sub + 4] & 0x0fu) | ((scales[sub - 4] >> 6) << 4));
    \\    *min_v = (float)((scales[sub + 4] >> 4) | ((scales[sub] >> 6) << 4));
    \\}
    ,
};

const helper_q4_k_dequant_lane = SourceFragment{
    .name = "antfly_q4_k_dequant_lane",
    .source =
    \\static __device__ __forceinline__ float antfly_q4_k_dequant_lane(const uint8_t *block, int lane) {
    \\    antfly_q4_k_block_view view = {
    \\        block,
    \\        block + 2,
    \\        block + 4,
    \\        block + 16,
    \\    };
    \\    const int sub = lane >> 5;
    \\    const int q_index = (sub >> 1) * 32 + (lane & 31);
    \\    const uint8_t packed = view.qs[q_index];
    \\    const uint8_t q = (sub & 1) == 0 ? (packed & 0x0fu) : (packed >> 4);
    \\    const float d = antfly_half_le_to_float(view.d);
    \\    const float dmin = antfly_half_le_to_float(view.dmin);
    \\    float raw_scale = 0.0f;
    \\    float raw_min = 0.0f;
    \\    antfly_q4_k_unpack_scale_min(view.scales, sub, &raw_scale, &raw_min);
    \\    return d * raw_scale * (float)q - dmin * raw_min;
    \\}
    ,
};

const q4_k_helpers = [_]SourceFragment{
    helper_q4_k_block_view,
    helper_half_le_to_float,
    helper_gelu,
    helper_q4_k_unpack_scale_min,
    helper_q4_k_dequant_lane,
};

const q4_k_projection_helpers = [_]SourceFragment{
    helper_q4_k_block_view,
    helper_half_le_to_float,
    helper_q4_k_unpack_scale_min,
    helper_q4_k_dequant_lane,
};

const q4_0_helpers = [_]SourceFragment{ helper_half_le_to_float, helper_warp_reduce_sum };

// These fragments intentionally retain the handwritten helper names and
// arithmetic. The standalone sources duplicate them; the runtime-owned region
// reuses the single copies that already precede its marker in the CUDA bundle.
const helper_termite_half_to_float = SourceFragment{
    .name = "termite_half_to_float",
    .source =
    \\__device__ __forceinline__ float termite_half_to_float(unsigned short h) {
    \\    return __half2float(__ushort_as_half(h));
    \\}
    ,
};

const helper_termite_warp_reduce_sum = SourceFragment{
    .name = "termite_warp_reduce_sum",
    .source =
    \\__device__ __forceinline__ float termite_warp_reduce_sum(float v) {
    \\    v += __shfl_down_sync(0xffffffffu, v, 16);
    \\    v += __shfl_down_sync(0xffffffffu, v, 8);
    \\    v += __shfl_down_sync(0xffffffffu, v, 4);
    \\    v += __shfl_down_sync(0xffffffffu, v, 2);
    \\    v += __shfl_down_sync(0xffffffffu, v, 1);
    \\    return v;
    \\}
    ,
};

const helper_termite_q4_0_value_nibble = SourceFragment{
    .name = "termite_q4_0_value_nibble",
    .source =
    \\__device__ __forceinline__ float termite_q4_0_value_nibble(
    \\    const unsigned char* bp,
    \\    unsigned int q_offset,
    \\    unsigned int high_nibble
    \\) {
    \\    unsigned short h = (unsigned short)bp[0] | ((unsigned short)bp[1] << 8);
    \\    float d = termite_half_to_float(h);
    \\    unsigned char packed = bp[q_offset];
    \\    int q = high_nibble != 0u ? (int)(packed >> 4) : (int)(packed & 0x0fu);
    \\    return (float)(q - 8) * d;
    \\}
    ,
};

const helper_termite_decoder_activation_f32 = SourceFragment{
    .name = "termite_decoder_activation_f32",
    .source =
    \\__device__ __forceinline__ float termite_decoder_activation_f32(float x, unsigned int activation) {
    \\    if (activation == 0u) {
    \\        return 0.5f * x * (1.0f + tanhf(0.7978845608028654f * (x + 0.044715f * x * x * x)));
    \\    } else if (activation == 1u) {
    \\        return 0.5f * x * (1.0f + tanhf(0.7978845608028654f * (x + 0.044715f * x * x * x)));
    \\    } else if (activation == 2u) {
    \\        return x / (1.0f + expf(-x));
    \\    } else if (activation == 3u) {
    \\        return fmaxf(x, 0.0f);
    \\    } else if (activation == 4u) {
    \\        return x / (1.0f + expf(-1.702f * x));
    \\    }
    \\    float r = fmaxf(x, 0.0f);
    \\    return r * r;
    \\}
    ,
};

const q4_0_exact_ffn_f32_helpers = [_]SourceFragment{
    helper_termite_half_to_float,
    helper_termite_warp_reduce_sum,
    helper_termite_q4_0_value_nibble,
    helper_termite_decoder_activation_f32,
};

const helper_half_bits_to_float = SourceFragment{
    .name = "antfly_half_bits_to_float",
    .source =
    \\static __device__ __forceinline__ float antfly_half_bits_to_float(unsigned short bits) {
    \\    return __half2float(__ushort_as_half(bits));
    \\}
    ,
};

const helper_warp_reduce_sum_f32 = SourceFragment{
    .name = "antfly_warp_reduce_sum_f32",
    .source =
    \\static __device__ __forceinline__ float antfly_warp_reduce_sum_f32(float value) {
    \\    value += __shfl_down_sync(0xffffffffu, value, 16);
    \\    value += __shfl_down_sync(0xffffffffu, value, 8);
    \\    value += __shfl_down_sync(0xffffffffu, value, 4);
    \\    value += __shfl_down_sync(0xffffffffu, value, 2);
    \\    value += __shfl_down_sync(0xffffffffu, value, 1);
    \\    return value;
    \\}
    ,
};

// The runtime bundle already owns byte-for-byte compatible helpers under the
// `termite_` names. Standalone generated attention artifacts duplicate them so
// they compile independently; `renderAttentionBodyAlloc` intentionally emits
// only the bodies and therefore reuses the bundle definitions.
const helper_termite_warp_reduce_sum_f32 = SourceFragment{
    .name = "termite_warp_reduce_sum_f32",
    .source =
    \\__device__ __forceinline__ float termite_warp_reduce_sum_f32(float value) {
    \\    value += __shfl_down_sync(0xffffffffu, value, 16);
    \\    value += __shfl_down_sync(0xffffffffu, value, 8);
    \\    value += __shfl_down_sync(0xffffffffu, value, 4);
    \\    value += __shfl_down_sync(0xffffffffu, value, 2);
    \\    value += __shfl_down_sync(0xffffffffu, value, 1);
    \\    return value;
    \\}
    ,
};

const helper_termite_block_reduce_sum_f32 = SourceFragment{
    .name = "termite_block_reduce_sum_f32",
    .source =
    \\__device__ __forceinline__ float termite_block_reduce_sum_f32(float value, float* warp_sums) {
    \\    unsigned int lane = threadIdx.x & 31u;
    \\    unsigned int warp = threadIdx.x >> 5;
    \\    unsigned int warp_count = (blockDim.x + 31u) >> 5;
    \\    value = termite_warp_reduce_sum_f32(value);
    \\    if (lane == 0u) warp_sums[warp] = value;
    \\    __syncthreads();
    \\    value = (warp == 0u && lane < warp_count) ? warp_sums[lane] : 0.0f;
    \\    if (warp == 0u) value = termite_warp_reduce_sum_f32(value);
    \\    return value;
    \\}
    ,
};

pub const attention_runtime_external_helpers = [_]SourceFragment{
    helper_termite_warp_reduce_sum_f32,
    helper_termite_block_reduce_sum_f32,
};

// The paged F32-KV score-prework candidate consumes the same block-table and
// key encoding helpers as the production TurboQuant fast path. Keep these
// fragments text-compatible with inference_cuda_kernels.cu: standalone
// generated sources need definitions, whereas the runtime region reuses the
// handwritten copies before its codegen marker.
const helper_termite_tq_decode_polar4_scalar = SourceFragment{
    .name = "termite_tq_decode_polar4_scalar",
    .source =
    \\__device__ __forceinline__ float termite_tq_decode_polar4_scalar(unsigned char code) {
    \\    return ((float)(code & 0x0fu) / 7.5f) - 1.0f;
    \\}
    ,
};

const helper_termite_tq_decode_polar4_at = SourceFragment{
    .name = "termite_tq_decode_polar4_at",
    .source =
    \\__device__ __forceinline__ float termite_tq_decode_polar4_at(const unsigned char* encoded, unsigned int value_index) {
    \\    unsigned char packed = encoded[value_index >> 1];
    \\    unsigned char code = (value_index & 1u) == 0u ? (packed & 0x0fu) : ((packed >> 4) & 0x0fu);
    \\    return termite_tq_decode_polar4_scalar(code);
    \\}
    ,
};

const helper_termite_tq_physical_token = SourceFragment{
    .name = "termite_tq_physical_token",
    .source =
    \\__device__ __forceinline__ unsigned int termite_tq_physical_token(
    \\    unsigned int logical_token,
    \\    const unsigned int* block_table,
    \\    unsigned int block_count,
    \\    unsigned int page_size_tokens,
    \\    unsigned int physical_token_capacity
    \\) {
    \\    unsigned long long physical = (unsigned long long)logical_token;
    \\    if (block_table != 0 && block_count != 0u && page_size_tokens != 0u) {
    \\        unsigned int block_index = logical_token / page_size_tokens;
    \\        if (block_index >= block_count) return 0xffffffffu;
    \\        unsigned int token_offset = logical_token - block_index * page_size_tokens;
    \\        const unsigned long long physical_block = (unsigned long long)block_table[block_index];
    \\        physical = physical_block * (unsigned long long)page_size_tokens +
    \\            (unsigned long long)token_offset;
    \\    }
    \\    if (physical > 0xffffffffull ||
    \\        physical >= (unsigned long long)physical_token_capacity) return 0xffffffffu;
    \\    return (unsigned int)physical;
    \\}
    ,
};

const helper_termite_tq_f16_value = SourceFragment{
    .name = "termite_tq_f16_value",
    .source =
    \\__device__ __forceinline__ float termite_tq_f16_value(const unsigned char* row, unsigned int value_index) {
    \\    const unsigned char* src = row + value_index * 2u;
    \\    unsigned short raw = (unsigned short)src[0] | ((unsigned short)src[1] << 8);
    \\    return __half2float(__ushort_as_half(raw));
    \\}
    ,
};

pub const attention_score_prework_runtime_external_helpers = [_]SourceFragment{
    helper_termite_tq_decode_polar4_scalar,
    helper_termite_tq_decode_polar4_at,
    helper_termite_tq_physical_token,
    helper_termite_tq_f16_value,
};

pub const attention_score_prework_runtime_external_declarations = [_]SourceFragment{
    .{
        .name = "termite_tq_decode_polar4_scalar_declaration",
        .source = "__device__ __forceinline__ float termite_tq_decode_polar4_scalar(unsigned char code);",
    },
    .{
        .name = "termite_tq_decode_polar4_at_declaration",
        .source = "__device__ __forceinline__ float termite_tq_decode_polar4_at(const unsigned char* encoded, unsigned int value_index);",
    },
    .{
        .name = "termite_tq_physical_token_declaration",
        .source =
        \\__device__ __forceinline__ unsigned int termite_tq_physical_token(
        \\    unsigned int logical_token,
        \\    const unsigned int* block_table,
        \\    unsigned int block_count,
        \\    unsigned int page_size_tokens,
        \\    unsigned int physical_token_capacity
        \\);
        ,
    },
    .{
        .name = "termite_tq_f16_value_declaration",
        .source = "__device__ __forceinline__ float termite_tq_f16_value(const unsigned char* row, unsigned int value_index);",
    },
};

const helper_warp_reduce_max_f32 = SourceFragment{
    .name = "antfly_warp_reduce_max_f32",
    .source =
    \\static __device__ __forceinline__ float antfly_warp_reduce_max_f32(float value) {
    \\    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 16));
    \\    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 8));
    \\    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 4));
    \\    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 2));
    \\    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 1));
    \\    return __shfl_sync(0xffffffffu, value, 0);
    \\}
    ,
};

const helper_decoder_activation_f32 = SourceFragment{
    .name = "antfly_decoder_activation_f32",
    .source =
    \\static __device__ __forceinline__ float antfly_decoder_activation_f32(float x, unsigned int activation) {
    \\    if (activation <= 1u) {
    \\        const float inner = 0.7978845608028654f * (x + 0.044715f * x * x * x);
    \\        return 0.5f * x * (1.0f + tanhf(inner));
    \\    }
    \\    if (activation == 2u) return x / (1.0f + __expf(-x));
    \\    if (activation == 3u) return fmaxf(x, 0.0f);
    \\    if (activation == 4u) return x / (1.0f + __expf(-1.702f * x));
    \\    const float r = fmaxf(x, 0.0f);
    \\    return r * r;
    \\}
    ,
};

const helper_q4_0_word_u16 = SourceFragment{
    .name = "antfly_q4_0_word_u16",
    .source =
    \\// q4_0 payload bytes live at bp+2; bp is always 2-byte aligned (18-byte
    \\// blocks), so every 4-byte word can be assembled from two aligned u16 loads.
    \\static __device__ __forceinline__ unsigned int antfly_q4_0_word_u16(const unsigned char *payload) {
    \\    const unsigned short *halves = (const unsigned short *)payload;
    \\    return (unsigned int)halves[0] | ((unsigned int)halves[1] << 16);
    \\}
    ,
};

const helper_q4_0_word_u16_no_comment = SourceFragment{
    .name = "antfly_q4_0_word_u16",
    .source =
    \\static __device__ __forceinline__ unsigned int antfly_q4_0_word_u16(const unsigned char *payload) {
    \\    const unsigned short *halves = (const unsigned short *)payload;
    \\    return (unsigned int)halves[0] | ((unsigned int)halves[1] << 16);
    \\}
    ,
};

const helper_q4_0_q8_dot16 = SourceFragment{
    .name = "antfly_q4_0_q8_dot16",
    .source =
    \\static __device__ __forceinline__ float antfly_q4_0_q8_dot16(
    \\    const unsigned char *q4_bp,
    \\    float q8_d,
    \\    unsigned int iqs,
    \\    int q8_low0,
    \\    int q8_high0,
    \\    int q8_low1,
    \\    int q8_high1
    \\) {
    \\    const float q4_d = antfly_half_bits_to_float(((const unsigned short *)q4_bp)[0]);
    \\    const unsigned int base0 = iqs * 4u;
    \\    const unsigned int word0 = antfly_q4_0_word_u16(q4_bp + 2u + base0);
    \\    const unsigned int word1 = antfly_q4_0_word_u16(q4_bp + 2u + base0 + 4u);
    \\    const unsigned int low0 = __vadd4(word0 & 0x0f0f0f0fu, 0xf8f8f8f8u);
    \\    const unsigned int high0 = __vadd4((word0 >> 4) & 0x0f0f0f0fu, 0xf8f8f8f8u);
    \\    const unsigned int low1 = __vadd4(word1 & 0x0f0f0f0fu, 0xf8f8f8f8u);
    \\    const unsigned int high1 = __vadd4((word1 >> 4) & 0x0f0f0f0fu, 0xf8f8f8f8u);
    \\    int sumi = __dp4a((int)low0, q8_low0, 0);
    \\    sumi = __dp4a((int)high0, q8_high0, sumi);
    \\    sumi = __dp4a((int)low1, q8_low1, sumi);
    \\    sumi = __dp4a((int)high1, q8_high1, sumi);
    \\    return q4_d * q8_d * (float)sumi;
    \\}
    ,
};

pub const helper_q4_0_ggml_q8_1_dot16 = SourceFragment{
    .name = "antfly_q4_0_ggml_q8_1_dot16",
    .source =
    \\// One of two 16-value contributions for a Q4_0/Q8_1 block. Keeping Q4
    \\// nibbles unsigned removes four packed-byte centering instructions; each
    \\// contribution applies half of the block-wide -8*sum correction.
    \\static __device__ __forceinline__ float antfly_q4_0_ggml_q8_1_dot16(
    \\    const unsigned char *q4_bp,
    \\    float q8_d,
    \\    float q8_sum,
    \\    unsigned int iqs,
    \\    int q8_low0,
    \\    int q8_high0,
    \\    int q8_low1,
    \\    int q8_high1
    \\) {
    \\    const float q4_d = antfly_half_bits_to_float(((const unsigned short *)q4_bp)[0]);
    \\    const unsigned int base0 = iqs * 4u;
    \\    const unsigned int word0 = antfly_q4_0_word_u16(q4_bp + 2u + base0);
    \\    const unsigned int word1 = antfly_q4_0_word_u16(q4_bp + 2u + base0 + 4u);
    \\    const unsigned int low0 = word0 & 0x0f0f0f0fu;
    \\    const unsigned int high0 = (word0 >> 4) & 0x0f0f0f0fu;
    \\    const unsigned int low1 = word1 & 0x0f0f0f0fu;
    \\    const unsigned int high1 = (word1 >> 4) & 0x0f0f0f0fu;
    \\    int sumi = __dp4a((int)low0, q8_low0, 0);
    \\    sumi = __dp4a((int)high0, q8_high0, sumi);
    \\    sumi = __dp4a((int)low1, q8_low1, sumi);
    \\    sumi = __dp4a((int)high1, q8_high1, sumi);
    \\    return q4_d * (q8_d * (float)sumi - 4.0f * q8_sum);
    \\}
    ,
};

pub const ggml_q8_1_runtime_owned_helpers = [_]SourceFragment{
    helper_q4_0_ggml_q8_1_dot16,
};

const q4_0_q8_helpers = [_]SourceFragment{
    helper_half_bits_to_float,
    helper_warp_reduce_sum_f32,
    helper_warp_reduce_max_f32,
    helper_decoder_activation_f32,
    helper_q4_0_word_u16,
    helper_q4_0_q8_dot16,
};

const q4_0_down_q8_helpers = [_]SourceFragment{
    helper_half_bits_to_float,
    helper_warp_reduce_sum_f32,
    helper_q4_0_word_u16_no_comment,
    helper_q4_0_q8_dot16,
};

const q4_0_ggml_q8_1_helpers = [_]SourceFragment{
    helper_half_bits_to_float,
    helper_warp_reduce_sum_f32,
    helper_warp_reduce_max_f32,
    helper_decoder_activation_f32,
    helper_q4_0_word_u16,
    helper_q4_0_ggml_q8_1_dot16,
};

const q4_0_down_ggml_q8_1_helpers = [_]SourceFragment{
    helper_half_bits_to_float,
    helper_warp_reduce_sum_f32,
    helper_q4_0_word_u16_no_comment,
    helper_q4_0_ggml_q8_1_dot16,
};

const helper_q6_k_sub_scale_f32 = SourceFragment{
    .name = "antfly_q6_k_sub_scale_f32",
    .source =
    \\static __device__ __forceinline__ float antfly_q6_k_sub_scale_f32(
    \\    const unsigned char *block,
    \\    unsigned int sub
    \\) {
    \\    const signed char *scales = (const signed char *)(block + 192u);
    \\    const unsigned short d_bits = (unsigned short)block[208] | ((unsigned short)block[209] << 8);
    \\    return antfly_half_bits_to_float(d_bits) * (float)scales[sub];
    \\}
    ,
};

const q6_k_q8_helpers = [_]SourceFragment{
    helper_half_bits_to_float,
    helper_warp_reduce_sum_f32,
    helper_q6_k_sub_scale_f32,
};

pub fn renderAttentionBodyAlloc(allocator: std.mem.Allocator, plan: AttentionRenderPlan) ![]u8 {
    try plan.validate();
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try renderAttentionBody(allocator, &out, plan);
    return out.toOwnedSlice(allocator);
}

fn renderAttentionBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    plan: AttentionRenderPlan,
) !void {
    if (plan.lowering.reduction == .parallel_scores_then_serial) {
        return renderAttentionScorePreworkBody(allocator, out, plan);
    }
    try renderAttentionSerialBody(allocator, out, plan);
    try out.appendSlice(allocator, "\n\n");
    const head_dim = plan.lowering.head_dim;
    const warps = plan.launch.threads_per_block / 32;
    const values_per_thread = head_dim / plan.launch.threads_per_block;
    try appendFmt(allocator, out, "extern \"C\" __global__ void {s}(\n", .{plan.kernel_id});
    try out.appendSlice(allocator,
        \\    float* partial_values,
        \\    float* partial_max,
        \\    float* partial_denom,
        \\    const float* q,
        \\    const float* k,
        \\    const float* v,
        \\    const unsigned char* attn_or_mask,
        \\    const float* bias,
        \\    unsigned int batch,
        \\    unsigned int q_seq_len,
        \\    unsigned int kv_seq_len,
        \\    unsigned int num_heads,
        \\    unsigned int num_kv_heads,
        \\    unsigned int head_dim,
        \\    unsigned int query_position_offset,
        \\    unsigned int kv_position_offset,
        \\    unsigned int sliding_window,
        \\    unsigned int total_sequence_len,
        \\    unsigned int mask_len,
        \\    unsigned int bias_mode,
        \\    unsigned int split_kv_min_tokens,
        \\    const unsigned int* decode_scalars
        \\) {
        \\    if (decode_scalars != 0) {
        \\        kv_position_offset = decode_scalars[4];
        \\        query_position_offset = decode_scalars[1];
        \\        kv_seq_len = decode_scalars[2];
        \\        total_sequence_len = decode_scalars[3];
        \\    }
        \\    if (kv_seq_len < split_kv_min_tokens) return;
        \\
    );
    try appendFmt(
        allocator,
        out,
        "    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||\n" ++
            "        head_dim != {d}u || blockDim.x != {d}u ||\n" ++
            "        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||\n" ++
            "        (num_heads / num_kv_heads) > {d}u || num_heads > {d}u) return;\n",
        .{ head_dim, plan.launch.threads_per_block, plan.lowering.query_heads_per_kv_head, generated_attention_workspace_max_query_heads },
    );
    try appendFmt(
        allocator,
        out,
        "    const unsigned int splits = {d}u;\n" ++
            "    const unsigned int split = blockIdx.x % splits;\n" ++
            "    const unsigned int head_block = blockIdx.x / splits;\n" ++
            "    const unsigned int head = head_block % num_heads;\n" ++
            "    const unsigned int b = head_block / num_heads;\n" ++
            "    if (b >= batch) return;\n" ++
            "    const unsigned int heads_per_kv = num_heads / num_kv_heads;\n" ++
            "    const unsigned int kv_head = head / heads_per_kv;\n" ++
            "    const unsigned int query_pos = query_position_offset;\n" ++
            "    unsigned int key_start = 0u;\n" ++
            "    unsigned int key_end = 0u;\n" ++
            "    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {{\n" ++
            "        const unsigned int visible = query_pos - kv_position_offset + 1u;\n" ++
            "        key_end = visible < kv_seq_len ? visible : kv_seq_len;\n" ++
            "        if (sliding_window != 0u) {{\n" ++
            "            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;\n" ++
            "            if (window_start_abs > kv_position_offset) {{\n" ++
            "                key_start = window_start_abs - kv_position_offset;\n" ++
            "                if (key_start > key_end) key_start = key_end;\n" ++
            "            }}\n" ++
            "        }}\n" ++
            "    }}\n" ++
            "    unsigned int split_begin = (kv_seq_len * split) / splits;\n" ++
            "    unsigned int split_end = (kv_seq_len * (split + 1u)) / splits;\n" ++
            "    if (split_begin < key_start) split_begin = key_start;\n" ++
            "    if (split_end > key_end) split_end = key_end;\n" ++
            "    if (split_begin > split_end) split_begin = split_end;\n" ++
            "    const unsigned int tid = threadIdx.x;\n" ++
            "    const unsigned int lane = tid & 31u;\n" ++
            "    const unsigned int warp = tid >> 5u;\n" ++
            "    const unsigned int q_hidden = num_heads * head_dim;\n" ++
            "    const unsigned int kv_hidden = num_kv_heads * head_dim;\n" ++
            "    const unsigned int q_base = (b * q_seq_len) * q_hidden + head * head_dim;\n" ++
            "    const float scale_input = (float)head_dim;\n" ++
            "    float scale;\n" ++
            "    asm volatile (\"rsqrt.approx.f32 %0, %1;\" : \"=f\"(scale) : \"f\"(scale_input));\n\n",
        .{plan.lowering.kv_splits},
    );
    try appendFmt(
        allocator,
        out,
        "    __shared__ float warp_sums[{d}];\n" ++
            "    __shared__ float head_max;\n" ++
            "    __shared__ float head_denom;\n" ++
            "    __shared__ float head_alpha;\n" ++
            "    __shared__ float head_beta;\n" ++
            "    float q_values[{d}];\n" ++
            "    float acc[{d}];\n",
        .{ warps, values_per_thread, values_per_thread },
    );
    try out.appendSlice(allocator,
        \\#pragma unroll
        \\    for (unsigned int item = 0u; item <
    );
    try appendFmt(allocator, out, " {d}u; ++item) {{\n", .{values_per_thread});
    try out.appendSlice(allocator,
        \\        const unsigned int d = tid + item * blockDim.x;
        \\        q_values[item] = q[q_base + d];
        \\        acc[item] = 0.0f;
        \\    }
        \\    if (tid == 0u) {
        \\        head_max = -3.402823466e+38f;
        \\        head_denom = 0.0f;
        \\        head_alpha = 0.0f;
        \\        head_beta = 0.0f;
        \\    }
        \\    __syncthreads();
        \\
        \\    for (unsigned int ki = split_begin; ki < split_end; ++ki) {
        \\        const unsigned int key_pos = kv_position_offset + ki;
        \\        const unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        \\        const bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        \\        const bool future_blocked = key_pos > query_pos && !future_allowed;
        \\        const bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        \\        const bool valid = !(future_blocked || past_blocked);
        \\        const unsigned int kv_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;
        \\
        \\        float dot = 0.0f;
        \\#pragma unroll
        \\        for (unsigned int item = 0u; item <
    );
    try appendFmt(allocator, out, " {d}u; ++item) {{\n", .{values_per_thread});
    try out.appendSlice(allocator,
        \\            const unsigned int d = tid + item * blockDim.x;
        \\            const float key_value = valid ? k[kv_base + d] : 0.0f;
        \\            dot += q_values[item] * key_value;
        \\        }
        \\        for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
        \\            dot += __shfl_down_sync(0xffffffffu, dot, offset);
        \\        }
        \\        if (lane == 0u) warp_sums[warp] = dot;
        \\        __syncthreads();
        \\
        \\        float block_dot = (warp == 0u && lane <
    );
    try appendFmt(allocator, out, " {d}u) ? warp_sums[lane] : 0.0f;\n", .{warps});
    try out.appendSlice(allocator,
        \\        if (warp == 0u) {
        \\            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
        \\                block_dot += __shfl_down_sync(0xffffffffu, block_dot, offset);
        \\            }
        \\            if (lane == 0u) {
        \\                float score = valid ? block_dot * scale : -3.402823466e+38f;
        \\                if (valid && bias_mode == 1u) score += bias[head * kv_seq_len + ki];
        \\                if (valid && bias_mode == 2u) score += bias[(b * num_heads + head) * kv_seq_len + ki];
        \\                const float next_max = fmaxf(head_max, score);
        \\                const float alpha = head_denom > 0.0f ? expf(head_max - next_max) : 0.0f;
        \\                const float beta = valid ? expf(score - next_max) : 0.0f;
        \\                head_denom = head_denom * alpha + beta;
        \\                head_max = next_max;
        \\                head_alpha = alpha;
        \\                head_beta = beta;
        \\            }
        \\        }
        \\        __syncthreads();
        \\
        \\#pragma unroll
        \\        for (unsigned int item = 0u; item <
    );
    try appendFmt(allocator, out, " {d}u; ++item) {{\n", .{values_per_thread});
    try out.appendSlice(allocator,
        \\            const unsigned int d = tid + item * blockDim.x;
        \\            const float value = valid ? v[kv_base + d] : 0.0f;
        \\            acc[item] = acc[item] * head_alpha + head_beta * value;
        \\        }
        \\    }
        \\
        \\    const unsigned int partial = (b * num_heads + head) * splits + split;
        \\#pragma unroll
        \\    for (unsigned int item = 0u; item <
    );
    try appendFmt(allocator, out, " {d}u; ++item) {{\n", .{values_per_thread});
    try out.appendSlice(allocator,
        \\        const unsigned int d = tid + item * blockDim.x;
        \\        partial_values[(size_t)partial * head_dim + d] = acc[item];
        \\    }
        \\    if (tid == 0u) {
        \\        partial_max[partial] = head_max;
        \\        partial_denom[partial] = head_denom;
        \\    }
        \\}
        \\
        \\
    );

    try appendFmt(allocator, out, "extern \"C\" __global__ void {s}(\n", .{plan.reduction_kernel_id});
    try out.appendSlice(allocator,
        \\    float* dst,
        \\    const float* partial_values,
        \\    const float* partial_max,
        \\    const float* partial_denom,
        \\    unsigned int batch,
        \\    unsigned int num_heads,
        \\    unsigned int head_dim,
        \\    unsigned int kv_seq_len,
        \\    unsigned int split_kv_min_tokens,
        \\    const unsigned int* decode_scalars
        \\) {
        \\    if (decode_scalars != 0) kv_seq_len = decode_scalars[2];
        \\    if (kv_seq_len < split_kv_min_tokens) return;
    );
    try appendFmt(
        allocator,
        out,
        "\n    if (batch != 1u || head_dim != {d}u || blockDim.x != {d}u || num_heads > {d}u) return;\n" ++
            "    const unsigned int head_block = blockIdx.x;\n" ++
            "    if (head_block >= batch * num_heads) return;\n" ++
            "    const unsigned int splits = {d}u;\n" ++
            "    __shared__ float merged_denom;\n" ++
            "    __shared__ float merge_alpha[{d}];\n" ++
            "    __shared__ float merge_beta[{d}];\n" ++
            "    if (threadIdx.x == 0u) {{\n" ++
            "        float merged_max = -3.402823466e+38f;\n" ++
            "        float denom = 0.0f;\n" ++
            "        for (unsigned int split = 0u; split < splits; ++split) {{\n" ++
            "            const unsigned int partial = head_block * splits + split;\n" ++
            "            const float local_denom = partial_denom[partial];\n" ++
            "            if (local_denom > 0.0f) {{\n" ++
            "                const float next_max = fmaxf(merged_max, partial_max[partial]);\n" ++
            "                const float alpha = denom > 0.0f ? expf(merged_max - next_max) : 0.0f;\n" ++
            "                const float beta = expf(partial_max[partial] - next_max);\n" ++
            "                denom = denom * alpha + local_denom * beta;\n" ++
            "                merged_max = next_max;\n" ++
            "                merge_alpha[split] = alpha;\n" ++
            "                merge_beta[split] = beta;\n" ++
            "            }} else {{\n" ++
            "                merge_alpha[split] = 1.0f;\n" ++
            "                merge_beta[split] = 0.0f;\n" ++
            "            }}\n" ++
            "        }}\n" ++
            "        merged_denom = denom;\n" ++
            "    }}\n" ++
            "    __syncthreads();\n",
        .{ head_dim, plan.reduction_launch.threads_per_block, generated_attention_workspace_max_query_heads, plan.lowering.kv_splits, plan.lowering.kv_splits, plan.lowering.kv_splits },
    );
    try appendFmt(
        allocator,
        out,
        "#pragma unroll\n" ++
            "    for (unsigned int item = 0u; item < {d}u; ++item) {{\n" ++
            "        const unsigned int d = threadIdx.x + item * blockDim.x;\n" ++
            "        float numerator = 0.0f;\n" ++
            "#pragma unroll\n" ++
            "        for (unsigned int split = 0u; split < splits; ++split) {{\n" ++
            "            const unsigned int partial = head_block * splits + split;\n" ++
            "            numerator = numerator * merge_alpha[split] + partial_values[(size_t)partial * head_dim + d] * merge_beta[split];\n" ++
            "        }}\n" ++
            "        dst[(size_t)head_block * head_dim + d] = merged_denom > 0.0f ? numerator / merged_denom : 0.0f;\n" ++
            "    }}\n" ++
            "}}\n",
        .{values_per_thread},
    );
}

fn renderAttentionSerialBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    plan: AttentionRenderPlan,
) !void {
    const head_dim = plan.lowering.head_dim;
    const threads = plan.serial_launch.threads_per_block;
    try appendFmt(allocator, out, "extern \"C\" __global__ void {s}(\n", .{plan.serial_kernel_id});
    try out.appendSlice(allocator,
        \\    float* dst,
        \\    const float* q,
        \\    const float* k,
        \\    const float* v,
        \\    const unsigned char* attn_or_mask,
        \\    const float* bias,
        \\    unsigned int batch,
        \\    unsigned int q_seq_len,
        \\    unsigned int kv_seq_len,
        \\    unsigned int num_heads,
        \\    unsigned int num_kv_heads,
        \\    unsigned int head_dim,
        \\    unsigned int query_position_offset,
        \\    unsigned int kv_position_offset,
        \\    unsigned int sliding_window,
        \\    unsigned int total_sequence_len,
        \\    unsigned int mask_len,
        \\    unsigned int bias_mode,
        \\    unsigned int split_kv_min_tokens,
        \\    const unsigned int* decode_scalars
        \\) {
        \\    if (decode_scalars != 0) {
        \\        kv_position_offset = decode_scalars[4];
        \\        query_position_offset = decode_scalars[1];
        \\        kv_seq_len = decode_scalars[2];
        \\    }
        \\    if (kv_seq_len >= split_kv_min_tokens) return;
        \\
    );
    try appendFmt(
        allocator,
        out,
        "    const unsigned int block = blockIdx.x;\n" ++
            "    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||\n" ++
            "        block >= num_heads || head_dim != {d}u || blockDim.x != {d}u ||\n" ++
            "        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||\n" ++
            "        (num_heads / num_kv_heads) > {d}u || num_heads > {d}u) return;\n",
        .{ head_dim, threads, plan.lowering.query_heads_per_kv_head, generated_attention_workspace_max_query_heads },
    );
    try out.appendSlice(allocator,
        \\    __shared__ float warp_sums[16];
        \\    __shared__ float shared_max_score;
        \\    __shared__ float shared_denom;
        \\    __shared__ float shared_alpha;
        \\    __shared__ float shared_beta;
        \\    const unsigned int lane = threadIdx.x;
        \\    const unsigned int head = block;
        \\    const unsigned int query_pos = query_position_offset;
        \\    unsigned int key_start = 0u;
        \\    unsigned int key_end = 0u;
        \\    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        \\        const unsigned int visible = query_pos - kv_position_offset + 1u;
        \\        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        \\        if (sliding_window != 0u) {
        \\            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
        \\            if (window_start_abs > kv_position_offset) {
        \\                key_start = window_start_abs - kv_position_offset;
        \\                if (key_start > key_end) key_start = key_end;
        \\            }
        \\        }
        \\    }
        \\    const unsigned int heads_per_group = num_heads / num_kv_heads;
        \\    const unsigned int kv_head = head / heads_per_group;
        \\    const unsigned int kv_hidden = num_kv_heads * head_dim;
        \\    const unsigned int q_base = head * head_dim;
        \\    const float scale_input = (float)head_dim;
        \\    float scale;
        \\    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));
        \\    if (lane == 0u) {
        \\        shared_max_score = -3.402823466e+38f;
        \\        shared_denom = 0.0f;
        \\    }
        \\    __syncthreads();
        \\
        \\    float acc = 0.0f;
        \\    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        \\        float partial = 0.0f;
        \\        if (lane < head_dim) {
        \\            const unsigned int k_base = ki * kv_hidden + kv_head * head_dim;
        \\            partial = q[q_base + lane] * k[k_base + lane];
        \\        }
        \\        const float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        \\        if (lane == 0u) {
        \\            const float score = dot * scale;
        \\            const float next_max = fmaxf(shared_max_score, score);
        \\            shared_alpha = expf(shared_max_score - next_max);
        \\            shared_beta = expf(score - next_max);
        \\            shared_denom = shared_denom * shared_alpha + shared_beta;
        \\            shared_max_score = next_max;
        \\        }
        \\        __syncthreads();
        \\        if (lane < head_dim) {
        \\            const unsigned int v_idx = ki * kv_hidden + kv_head * head_dim + lane;
        \\            acc = acc * shared_alpha + shared_beta * v[v_idx];
        \\        }
        \\        __syncthreads();
        \\    }
        \\
        \\    if (lane < head_dim) {
        \\        const unsigned int out_idx = head * head_dim + lane;
        \\        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
        \\    }
        \\
        \\    (void)attn_or_mask;
        \\    (void)bias;
        \\    (void)total_sequence_len;
        \\}
    );
}

/// Emits an exact two-kernel candidate for decode attention. The score stage
/// uses the bundle's same block reduction as `termite_gqa_attention_decode_*
/// fast_f32`; the consumer deliberately keeps the chronological online
/// softmax/value recurrence instead of merging independently normalized
/// partials. This is the important numerical boundary: parallel dot products
/// are safe, a reordered softmax merge is not bitwise compatible.
fn renderAttentionScorePreworkBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    plan: AttentionRenderPlan,
) !void {
    if (plan.lowering.reduction != .parallel_scores_then_serial) return error.InvalidCudaAttentionLowering;
    const head_dim = plan.lowering.head_dim;
    const threads = plan.launch.threads_per_block;
    const warps = threads / 32;

    try appendFmt(allocator, out, "extern \"C\" __global__ void {s}(\n", .{plan.kernel_id});
    try out.appendSlice(allocator,
        \\    float* scores,
        \\    const float* q,
        \\    const unsigned char* k,
        \\    const unsigned int* block_table,
        \\    unsigned int batch,
        \\    unsigned int q_seq_len,
        \\    unsigned int kv_seq_len,
        \\    unsigned int num_heads,
        \\    unsigned int num_kv_heads,
        \\    unsigned int head_dim,
        \\    unsigned int query_position_offset,
        \\    unsigned int kv_position_offset,
        \\    unsigned int sliding_window,
        \\    unsigned int total_sequence_len,
        \\    unsigned int key_row_bytes,
        \\    unsigned int base_key_row_bytes,
        \\    unsigned int block_count,
        \\    unsigned int page_size_tokens,
        \\    unsigned int format,
        \\    unsigned int physical_token_capacity,
        \\    unsigned int score_capacity,
        \\    unsigned int chunk_size,
        \\    unsigned int chunk_count,
        \\    const unsigned int* decode_scalars
        \\) {
        \\    if (decode_scalars != 0) {
        \\        kv_position_offset = decode_scalars[4];
        \\        query_position_offset = decode_scalars[1];
        \\        kv_seq_len = decode_scalars[2];
        \\        total_sequence_len = decode_scalars[3];
        \\    }
        \\
    );
    try appendFmt(
        allocator,
        out,
        "    if (batch != 1u || q_seq_len != 1u || head_dim != {d}u || blockDim.x != {d}u ||\n" ++
            "        score_capacity == 0u || score_capacity > {d}u || chunk_size == 0u ||\n" ++
            "        chunk_count != {d}u || key_row_bytes == 0u || base_key_row_bytes != key_row_bytes ||\n" ++
            "        (format != 0u && format != 2u) || num_kv_heads == 0u || num_heads == 0u ||\n" ++
            "        (num_heads % num_kv_heads) != 0u || (num_heads / num_kv_heads) > {d}u ||\n" ++
            "        num_heads > {d}u) return;\n" ++
            "    const unsigned int block = blockIdx.x;\n" ++
            "    const unsigned int head = block / chunk_count;\n" ++
            "    const unsigned int chunk = block - head * chunk_count;\n" ++
            "    if (head >= num_heads) return;\n",
        .{ head_dim, threads, generated_attention_score_prework_max_kv_tokens, generated_attention_score_prework_chunks, plan.lowering.query_heads_per_kv_head, generated_attention_workspace_max_query_heads },
    );
    try appendFmt(
        allocator,
        out,
        "    __shared__ float warp_sums[{d}];\n" ++
            "    const unsigned int lane = threadIdx.x;\n" ++
            "    const unsigned int query_pos = query_position_offset;\n" ++
            "    unsigned int key_start = 0u;\n" ++
            "    unsigned int key_end = 0u;\n" ++
            "    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {{\n" ++
            "        const unsigned int visible = query_pos - kv_position_offset + 1u;\n" ++
            "        key_end = visible < kv_seq_len ? visible : kv_seq_len;\n" ++
            "        if (sliding_window != 0u) {{\n" ++
            "            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;\n" ++
            "            if (window_start_abs > kv_position_offset) {{\n" ++
            "                key_start = window_start_abs - kv_position_offset;\n" ++
            "                if (key_start > key_end) key_start = key_end;\n" ++
            "            }}\n" ++
            "        }}\n" ++
            "    }}\n" ++
            "    const unsigned int visible_count = key_end - key_start;\n" ++
            "    if (visible_count > score_capacity) return;\n" ++
            "    unsigned int chunk_begin = key_start + chunk * chunk_size;\n" ++
            "    unsigned int chunk_end = chunk_begin + chunk_size;\n" ++
            "    if (chunk_end > key_end) chunk_end = key_end;\n" ++
            "    if (chunk_begin >= chunk_end) return;\n" ++
            "    const unsigned int heads_per_group = num_heads / num_kv_heads;\n" ++
            "    const unsigned int kv_head = head / heads_per_group;\n" ++
            "    const unsigned int q_base = head * head_dim;\n" ++
            "    const float scale_input = (float)head_dim;\n" ++
            "    float scale;\n" ++
            "    asm volatile (\"rsqrt.approx.f32 %0, %1;\" : \"=f\"(scale) : \"f\"(scale_input));\n" ++
            "    for (unsigned int ki = chunk_begin; ki < chunk_end; ++ki) {{\n" ++
            "        const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);\n" ++
            "        const bool valid = physical_token != 0xffffffffu;\n" ++
            "        const unsigned char* k_row = valid ? k + (size_t)physical_token * key_row_bytes : k;\n" ++
            "        float partial = 0.0f;\n" ++
            "        if (valid && lane < head_dim) {{\n" ++
            "            const unsigned int value_index = kv_head * head_dim + lane;\n" ++
            "            const float key_value = format == 0u\n" ++
            "                ? termite_tq_decode_polar4_at(k_row, value_index)\n" ++
            "                : termite_tq_f16_value(k_row, value_index);\n" ++
            "            partial = q[q_base + lane] * key_value;\n" ++
            "        }}\n" ++
            "        const float dot = termite_block_reduce_sum_f32(partial, warp_sums);\n" ++
            "        if (lane == 0u && valid) scores[(size_t)head * score_capacity + (ki - key_start)] = dot * scale;\n" ++
            "    }}\n" ++
            "    (void)total_sequence_len;\n" ++
            "}}\n",
        .{warps},
    );

    try out.appendSlice(allocator, "\n\n");
    try appendFmt(allocator, out, "extern \"C\" __global__ void {s}(\n", .{plan.reduction_kernel_id});
    try out.appendSlice(allocator,
        \\    float* dst,
        \\    const float* scores,
        \\    const unsigned char* v,
        \\    const unsigned int* block_table,
        \\    unsigned int batch,
        \\    unsigned int q_seq_len,
        \\    unsigned int kv_seq_len,
        \\    unsigned int num_heads,
        \\    unsigned int num_kv_heads,
        \\    unsigned int head_dim,
        \\    unsigned int query_position_offset,
        \\    unsigned int kv_position_offset,
        \\    unsigned int sliding_window,
        \\    unsigned int total_sequence_len,
        \\    unsigned int value_row_bytes,
        \\    unsigned int block_count,
        \\    unsigned int page_size_tokens,
        \\    unsigned int value_format,
        \\    unsigned int physical_token_capacity,
        \\    unsigned int score_capacity,
        \\    const unsigned int* decode_scalars
        \\) {
        \\    if (decode_scalars != 0) {
        \\        kv_position_offset = decode_scalars[4];
        \\        query_position_offset = decode_scalars[1];
        \\        kv_seq_len = decode_scalars[2];
        \\        total_sequence_len = decode_scalars[3];
        \\    }
        \\
    );
    try appendFmt(
        allocator,
        out,
        "    const unsigned int head = blockIdx.x;\n" ++
            "    if (batch != 1u || q_seq_len != 1u || head >= num_heads ||\n" ++
            "        head_dim != {d}u || blockDim.x != {d}u || value_row_bytes == 0u ||\n" ++
            "        (value_format != 0u && value_format != 2u) || score_capacity == 0u || score_capacity > {d}u || num_kv_heads == 0u ||\n" ++
            "        num_heads == 0u || (num_heads % num_kv_heads) != 0u ||\n" ++
            "        (num_heads / num_kv_heads) > {d}u || num_heads > {d}u) return;\n" ++
            "    __shared__ float shared_max_score;\n" ++
            "    __shared__ float shared_denom;\n" ++
            "    __shared__ float shared_alpha[{d}];\n" ++
            "    __shared__ float shared_beta[{d}];\n" ++
            "    const unsigned int lane = threadIdx.x;\n" ++
            "    const unsigned int query_pos = query_position_offset;\n" ++
            "    unsigned int key_start = 0u;\n" ++
            "    unsigned int key_end = 0u;\n" ++
            "    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {{\n" ++
            "        const unsigned int visible = query_pos - kv_position_offset + 1u;\n" ++
            "        key_end = visible < kv_seq_len ? visible : kv_seq_len;\n" ++
            "        if (sliding_window != 0u) {{\n" ++
            "            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;\n" ++
            "            if (window_start_abs > kv_position_offset) {{\n" ++
            "                key_start = window_start_abs - kv_position_offset;\n" ++
            "                if (key_start > key_end) key_start = key_end;\n" ++
            "            }}\n" ++
            "        }}\n" ++
            "    }}\n" ++
            "    if (key_start > key_end) key_start = key_end;\n" ++
            "    if (key_end - key_start > score_capacity) return;\n" ++
            "    const unsigned int heads_per_group = num_heads / num_kv_heads;\n" ++
            "    const unsigned int kv_head = head / heads_per_group;\n" ++
            "    if (lane == 0u) {{\n" ++
            "        shared_max_score = -3.402823466e+38f;\n" ++
            "        shared_denom = 0.0f;\n" ++
            "        for (unsigned int ki = key_start; ki < key_end; ++ki) {{\n" ++
            "            const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);\n" ++
            "            const bool valid = physical_token != 0xffffffffu;\n" ++
            "            const unsigned int recurrence_index = ki - key_start;\n" ++
            "            if (valid) {{\n" ++
            "                const float score = scores[(size_t)head * score_capacity + (ki - key_start)];\n" ++
            "                const float next_max = fmaxf(shared_max_score, score);\n" ++
            "                shared_alpha[recurrence_index] = expf(shared_max_score - next_max);\n" ++
            "                shared_beta[recurrence_index] = expf(score - next_max);\n" ++
            "                shared_denom = shared_denom * shared_alpha[recurrence_index] + shared_beta[recurrence_index];\n" ++
            "                shared_max_score = next_max;\n" ++
            "            }} else {{\n" ++
            "                shared_alpha[recurrence_index] = 1.0f;\n" ++
            "                shared_beta[recurrence_index] = 0.0f;\n" ++
            "            }}\n" ++
            "        }}\n" ++
            "    }}\n" ++
            "    __syncthreads();\n" ++
            "    float acc = 0.0f;\n" ++
            "    if (lane < head_dim) {{\n" ++
            "        for (unsigned int ki = key_start; ki < key_end; ++ki) {{\n" ++
            "            const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);\n" ++
            "            const bool valid = physical_token != 0xffffffffu;\n" ++
            "            const unsigned int recurrence_index = ki - key_start;\n" ++
            "            acc *= shared_alpha[recurrence_index];\n" ++
            "            if (valid) {{\n" ++
            "                const unsigned char* v_row = v + (size_t)physical_token * value_row_bytes;\n" ++
            "                const unsigned int value_index = kv_head * head_dim + lane;\n" ++
            "                const float value = value_format == 0u\n" ++
            "                    ? reinterpret_cast<const float*>(v_row)[value_index]\n" ++
            "                    : termite_tq_f16_value(v_row, value_index);\n" ++
            "                acc += shared_beta[recurrence_index] * value;\n" ++
            "            }}\n" ++
            "        }}\n" ++
            "    }}\n" ++
            "    if (lane < head_dim) dst[head * head_dim + lane] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;\n" ++
            "    (void)total_sequence_len;\n" ++
            "}}\n",
        .{
            head_dim,
            threads,
            generated_attention_score_prework_max_kv_tokens,
            plan.lowering.query_heads_per_kv_head,
            generated_attention_workspace_max_query_heads,
            generated_attention_score_prework_max_kv_tokens,
            generated_attention_score_prework_max_kv_tokens,
        },
    );

    try renderAttentionScorePreworkTiled64Consumer(allocator, out, plan);
}

/// Emits the default-off output-dimension tiled consumer. Every CTA repeats
/// the lane-zero chronological coefficient recurrence in exactly the same
/// operation order as the serial consumer above, then replays those
/// coefficients for one disjoint 64-value output tile. There is no inter-CTA
/// reduction, extra workspace, or third launch.
fn renderAttentionScorePreworkTiled64Consumer(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    plan: AttentionRenderPlan,
) !void {
    const kernel_id = plan.tiled64_kernel_id orelse return error.InvalidCudaAttentionLowering;
    const launch = plan.tiled64_launch orelse return error.InvalidCudaAttentionLowering;
    const head_dim = plan.lowering.head_dim;
    const max_kv_tokens = generatedAttentionScorePreworkTiled64MaxKvTokens(head_dim) orelse
        return error.InvalidCudaAttentionLowering;

    try out.appendSlice(allocator, "\n\n");
    try appendFmt(allocator, out, "extern \"C\" __global__ void {s}(\n", .{kernel_id});
    try out.appendSlice(allocator,
        \\    float* dst,
        \\    const float* scores,
        \\    const unsigned char* v,
        \\    const unsigned int* block_table,
        \\    unsigned int batch,
        \\    unsigned int q_seq_len,
        \\    unsigned int kv_seq_len,
        \\    unsigned int num_heads,
        \\    unsigned int num_kv_heads,
        \\    unsigned int head_dim,
        \\    unsigned int query_position_offset,
        \\    unsigned int kv_position_offset,
        \\    unsigned int sliding_window,
        \\    unsigned int total_sequence_len,
        \\    unsigned int value_row_bytes,
        \\    unsigned int block_count,
        \\    unsigned int page_size_tokens,
        \\    unsigned int value_format,
        \\    unsigned int physical_token_capacity,
        \\    unsigned int score_capacity,
        \\    const unsigned int* decode_scalars
        \\) {
        \\    if (decode_scalars != 0) {
        \\        kv_position_offset = decode_scalars[4];
        \\        query_position_offset = decode_scalars[1];
        \\        kv_seq_len = decode_scalars[2];
        \\        total_sequence_len = decode_scalars[3];
        \\    }
        \\
    );
    try appendFmt(
        allocator,
        out,
        "    const unsigned int head = blockIdx.x;\n" ++
            "    const unsigned int output_tile = blockIdx.y;\n" ++
            "    if (batch != 1u || q_seq_len != 1u || head >= num_heads ||\n" ++
            "        head_dim != {d}u || blockDim.x != {d}u || output_tile >= head_dim / {d}u ||\n" ++
            "        value_row_bytes == 0u || (value_format != 0u && value_format != 2u) ||\n" ++
            "        score_capacity == 0u || score_capacity > {d}u || num_kv_heads == 0u ||\n" ++
            "        num_heads == 0u || (num_heads % num_kv_heads) != 0u ||\n" ++
            "        (num_heads / num_kv_heads) > {d}u || num_heads > {d}u) return;\n" ++
            "    __shared__ float shared_max_score;\n" ++
            "    __shared__ float shared_denom;\n" ++
            "    __shared__ float shared_alpha[{d}];\n" ++
            "    __shared__ float shared_beta[{d}];\n" ++
            "    const unsigned int tile_lane = threadIdx.x;\n" ++
            "    const unsigned int lane = output_tile * {d}u + tile_lane;\n" ++
            "    const unsigned int query_pos = query_position_offset;\n" ++
            "    unsigned int key_start = 0u;\n" ++
            "    unsigned int key_end = 0u;\n" ++
            "    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {{\n" ++
            "        const unsigned int visible = query_pos - kv_position_offset + 1u;\n" ++
            "        key_end = visible < kv_seq_len ? visible : kv_seq_len;\n" ++
            "        if (sliding_window != 0u) {{\n" ++
            "            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;\n" ++
            "            if (window_start_abs > kv_position_offset) {{\n" ++
            "                key_start = window_start_abs - kv_position_offset;\n" ++
            "                if (key_start > key_end) key_start = key_end;\n" ++
            "            }}\n" ++
            "        }}\n" ++
            "    }}\n" ++
            "    if (key_start > key_end) key_start = key_end;\n" ++
            "    if (key_end - key_start > score_capacity) return;\n" ++
            "    const unsigned int heads_per_group = num_heads / num_kv_heads;\n" ++
            "    const unsigned int kv_head = head / heads_per_group;\n" ++
            "    if (tile_lane == 0u) {{\n" ++
            "        shared_max_score = -3.402823466e+38f;\n" ++
            "        shared_denom = 0.0f;\n" ++
            "        for (unsigned int ki = key_start; ki < key_end; ++ki) {{\n" ++
            "            const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);\n" ++
            "            const bool valid = physical_token != 0xffffffffu;\n" ++
            "            const unsigned int recurrence_index = ki - key_start;\n" ++
            "            if (valid) {{\n" ++
            "                const float score = scores[(size_t)head * score_capacity + (ki - key_start)];\n" ++
            "                const float next_max = fmaxf(shared_max_score, score);\n" ++
            "                shared_alpha[recurrence_index] = expf(shared_max_score - next_max);\n" ++
            "                shared_beta[recurrence_index] = expf(score - next_max);\n" ++
            "                shared_denom = shared_denom * shared_alpha[recurrence_index] + shared_beta[recurrence_index];\n" ++
            "                shared_max_score = next_max;\n" ++
            "            }} else {{\n" ++
            "                shared_alpha[recurrence_index] = 1.0f;\n" ++
            "                shared_beta[recurrence_index] = 0.0f;\n" ++
            "            }}\n" ++
            "        }}\n" ++
            "    }}\n" ++
            "    __syncthreads();\n" ++
            "    float acc = 0.0f;\n" ++
            "    if (lane < head_dim) {{\n" ++
            "        for (unsigned int ki = key_start; ki < key_end; ++ki) {{\n" ++
            "            const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);\n" ++
            "            const bool valid = physical_token != 0xffffffffu;\n" ++
            "            const unsigned int recurrence_index = ki - key_start;\n" ++
            "            acc *= shared_alpha[recurrence_index];\n" ++
            "            if (valid) {{\n" ++
            "                const unsigned char* v_row = v + (size_t)physical_token * value_row_bytes;\n" ++
            "                const unsigned int value_index = kv_head * head_dim + lane;\n" ++
            "                const float value = value_format == 0u\n" ++
            "                    ? reinterpret_cast<const float*>(v_row)[value_index]\n" ++
            "                    : termite_tq_f16_value(v_row, value_index);\n" ++
            "                acc += shared_beta[recurrence_index] * value;\n" ++
            "            }}\n" ++
            "        }}\n" ++
            "    }}\n" ++
            "    if (lane < head_dim) dst[head * head_dim + lane] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;\n" ++
            "    (void)total_sequence_len;\n" ++
            "}}\n",
        .{
            head_dim,
            launch.threads_per_block,
            generated_attention_score_prework_tiled64_tile_size,
            max_kv_tokens,
            plan.lowering.query_heads_per_kv_head,
            generated_attention_workspace_max_query_heads,
            max_kv_tokens,
            max_kv_tokens,
            generated_attention_score_prework_tiled64_tile_size,
        },
    );
}

const body_q4_k_small_batch_bias_gelu = SourceFragment{
    .name = "antfly_q4_k_small_batch_bias_gelu_f32_v1",
    .source =
    \\extern "C" __global__ void antfly_q4_k_small_batch_bias_gelu_f32_v1(
    \\    const float *input,
    \\    const uint8_t *weight_q4_k,
    \\    const float *bias,
    \\    float *output,
    \\    int rows,
    \\    int in_dim,
    \\    int out_dim
    \\) {
    \\    const int row = blockIdx.y;
    \\    const int col = blockIdx.x;
    \\    if (row >= rows || rows < 2 || rows > 8 || col >= out_dim) return;
    \\    if (blockDim.x != 128) return;
    \\    if ((in_dim & 255) != 0) return;
    \\
    \\    float acc = 0.0f;
    \\    const int block_count = in_dim >> 8;
    \\    for (int block_idx = 0; block_idx < block_count; ++block_idx) {
    \\        const uint8_t *block = weight_q4_k + ((col * block_count + block_idx) * 144);
    \\        const int base = block_idx << 8;
    \\        for (int lane = threadIdx.x; lane < 256; lane += blockDim.x) {
    \\            acc += input[row * in_dim + base + lane] * antfly_q4_k_dequant_lane(block, lane);
    \\        }
    \\    }
    \\
    \\    __shared__ float partial[128];
    \\    partial[threadIdx.x] = acc;
    \\    __syncthreads();
    \\    for (int stride = blockDim.x >> 1; stride > 0; stride >>= 1) {
    \\        if (threadIdx.x < stride) partial[threadIdx.x] += partial[threadIdx.x + stride];
    \\        __syncthreads();
    \\    }
    \\    if (threadIdx.x == 0) output[row * out_dim + col] = antfly_gelu(partial[0] + bias[col]);
    \\}
    ,
};

const body_q4_k_mmv = SourceFragment{
    .name = "antfly_q4_k_mmv_f32_v1",
    .source =
    \\extern "C" __global__ void antfly_q4_k_mmv_f32_v1(
    \\    float *output,
    \\    const float *input,
    \\    const uint8_t *weight_q4_k,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    const unsigned int col = blockIdx.x;
    \\    const unsigned int tid = threadIdx.x;
    \\    if (rows != 1u || col >= out_dim || blockDim.x != 256u || (in_dim & 255u) != 0u) return;
    \\
    \\    float acc = 0.0f;
    \\    const unsigned int block_count = in_dim >> 8u;
    \\    for (unsigned int block_idx = 0u; block_idx < block_count; ++block_idx) {
    \\        const uint8_t *block = weight_q4_k + ((size_t)col * block_count + block_idx) * 144u;
    \\        const unsigned int input_index = (block_idx << 8u) + tid;
    \\        acc += input[input_index] * antfly_q4_k_dequant_lane(block, tid);
    \\    }
    \\
    \\    __shared__ float partial[256];
    \\    partial[tid] = acc;
    \\    __syncthreads();
    \\    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
    \\        if (tid < stride) partial[tid] += partial[tid + stride];
    \\        __syncthreads();
    \\    }
    \\    if (tid == 0u) output[col] = partial[0];
    \\}
    ,
};

const body_q4_0_pair_activation_q8_1 = SourceFragment{
    .name = "antfly_q4_0_pair_activation_q8_1_mmv_v1",
    .source =
    \\extern "C" __global__ void antfly_q4_0_pair_activation_q8_1_mmv_v1(
    \\    unsigned char *dst_q8,
    \\    const unsigned char *q8_input,
    \\    const unsigned char *weight_gate,
    \\    const unsigned char *weight_up,
    \\    unsigned int activation,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    if (rows == 0u || (in_dim & 31u) != 0u || (out_dim & 31u) != 0u) return;
    \\    const unsigned int row_blocks = in_dim >> 5;
    \\    const unsigned int out_row_blocks = out_dim >> 5;
    \\    const unsigned int group_cols = 4u;
    \\    const unsigned int groups_per_wave = 4u;
    \\    const unsigned int waves = 2u;
    \\
    \\    const unsigned int out_block = blockIdx.x % out_row_blocks;
    \\    const unsigned int row = blockIdx.x / out_row_blocks;
    \\    const unsigned int col_block = out_block * 32u;
    \\    const unsigned int tid = threadIdx.x;
    \\    const unsigned int lane = tid & 31u;
    \\    const unsigned int warp = tid >> 5u;
    \\    const unsigned int group = warp / 5u;
    \\    const unsigned int group_warp = warp - group * 5u;
    \\    if (blockDim.x != 640u || row >= rows) return;
    \\
    \\    __shared__ float gate_partial[4][4][5];
    \\    __shared__ float up_partial[4][4][5];
    \\    __shared__ float activated[32];
    \\
    \\    #pragma unroll
    \\    for (unsigned int wave = 0u; wave < waves; ++wave) {
    \\        if (group < groups_per_wave) {
    \\            const unsigned int local_tid = group_warp * 32u + lane;
    \\            const unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;
    \\            float gate_acc[4];
    \\            float up_acc[4];
    \\            #pragma unroll
    \\            for (unsigned int c = 0u; c < group_cols; ++c) {
    \\                gate_acc[c] = 0.0f;
    \\                up_acc[c] = 0.0f;
    \\            }
    \\
    \\            const unsigned int iqs = (local_tid & 1u) * 2u;
    \\            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += 80u) {
    \\                const unsigned char *q8_bp = q8_input + (row * row_blocks + block) * 36u;
    \\                const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
    \\                const signed char *q8_values = (const signed char *)(q8_bp + 4u);
    \\                const unsigned int q8_base0 = iqs * 4u;
    \\                const unsigned int q8_base1 = q8_base0 + 4u;
    \\                const int q8_low0 = *(const int *)(q8_values + q8_base0);
    \\                const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
    \\                const int q8_low1 = *(const int *)(q8_values + q8_base1);
    \\                const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);
    \\
    \\                #pragma unroll
    \\                for (unsigned int c = 0u; c < group_cols; ++c) {
    \\                    const unsigned int col = col_tile + c;
    \\                    const unsigned char *gate_bp = weight_gate + ((size_t)col * row_blocks + block) * 18u;
    \\                    const unsigned char *up_bp = weight_up + ((size_t)col * row_blocks + block) * 18u;
    \\                    gate_acc[c] += antfly_q4_0_q8_dot16(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
    \\                    up_acc[c] += antfly_q4_0_q8_dot16(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
    \\                }
    \\            }
    \\
    \\            #pragma unroll
    \\            for (unsigned int c = 0u; c < group_cols; ++c) {
    \\                const float gate_sum = antfly_warp_reduce_sum_f32(gate_acc[c]);
    \\                const float up_sum = antfly_warp_reduce_sum_f32(up_acc[c]);
    \\                if (lane == 0u) {
    \\                    gate_partial[group][c][group_warp] = gate_sum;
    \\                    up_partial[group][c][group_warp] = up_sum;
    \\                }
    \\            }
    \\        }
    \\        __syncthreads();
    \\        if (tid < 16u) {
    \\            const unsigned int out_group = tid >> 2u;
    \\            const unsigned int c = tid & 3u;
    \\            float gate_y = 0.0f;
    \\            float up_y = 0.0f;
    \\            #pragma unroll
    \\            for (unsigned int w = 0u; w < 5u; ++w) {
    \\                gate_y += gate_partial[out_group][c][w];
    \\                up_y += up_partial[out_group][c][w];
    \\            }
    \\            activated[wave * 16u + out_group * group_cols + c] = antfly_decoder_activation_f32(gate_y, activation) * up_y;
    \\        }
    \\        __syncthreads();
    \\    }
    \\
    \\    if (warp == 0u) {
    \\        const float x = activated[lane];
    \\        const float amax = antfly_warp_reduce_max_f32(fabsf(x));
    \\        const float d = amax > 0.0f ? amax / 127.0f : 0.0f;
    \\        int q = 0;
    \\        if (d > 0.0f) {
    \\            q = __float2int_rn(x / d);
    \\            q = max(-127, min(127, q));
    \\        }
    \\        unsigned char *bp = dst_q8 + ((size_t)row * out_row_blocks + out_block) * 36u;
    \\        bp[4u + lane] = (unsigned char)(signed char)q;
    \\        if (lane == 0u) {
    \\            const unsigned short d_bits = __half_as_ushort(__float2half(d));
    \\            bp[0] = (unsigned char)(d_bits & 0xffu);
    \\            bp[1] = (unsigned char)(d_bits >> 8);
    \\            bp[2] = 0u;
    \\            bp[3] = 0u;
    \\        }
    \\    }
    \\}
    ,
};

const body_q4_0_down_q8_1 = SourceFragment{
    .name = "antfly_q4_0_down_q8_1_mmv_v1",
    .source =
    \\extern "C" __global__ void antfly_q4_0_down_q8_1_mmv_v1(
    \\    float *dst,
    \\    const unsigned char *q8_input,
    \\    const unsigned char *weight,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    const unsigned int cols = 4u;
    \\    if (rows == 0u || (in_dim & 31u) != 0u || out_dim == 0u) return;
    \\    const unsigned int row_blocks = in_dim >> 5;
    \\    const unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    \\    const unsigned int row = blockIdx.x / col_tiles;
    \\    const unsigned int col_tile = (blockIdx.x % col_tiles) * cols;
    \\    const unsigned int tid = threadIdx.x;
    \\    const unsigned int lane = tid & 31u;
    \\    const unsigned int warp = tid >> 5u;
    \\    if (blockDim.x != 256u || row >= rows) return;
    \\
    \\    __shared__ float warp_partial[4][8];
    \\    float acc[4];
    \\    #pragma unroll
    \\    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;
    \\
    \\    const unsigned int iqs = (tid & 1u) * 2u;
    \\    for (unsigned int block = tid >> 1u; block < row_blocks; block += 128u) {
    \\        const unsigned char *q8_bp = q8_input + ((size_t)row * row_blocks + block) * 36u;
    \\        const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
    \\        const signed char *q8_values = (const signed char *)(q8_bp + 4u);
    \\        const unsigned int q8_base0 = iqs * 4u;
    \\        const unsigned int q8_base1 = q8_base0 + 4u;
    \\        const int q8_low0 = *(const int *)(q8_values + q8_base0);
    \\        const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
    \\        const int q8_low1 = *(const int *)(q8_values + q8_base1);
    \\        const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);
    \\
    \\        #pragma unroll
    \\        for (unsigned int c = 0u; c < cols; ++c) {
    \\            const unsigned int col = col_tile + c;
    \\            if (col < out_dim) {
    \\                const unsigned char *bp = weight + ((size_t)col * row_blocks + block) * 18u;
    \\                acc[c] += antfly_q4_0_q8_dot16(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
    \\            }
    \\        }
    \\    }
    \\
    \\    #pragma unroll
    \\    for (unsigned int c = 0u; c < cols; ++c) {
    \\        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
    \\        if (lane == 0u) warp_partial[c][warp] = sum;
    \\    }
    \\    __syncthreads();
    \\    if (tid < 4u) {
    \\        float y = 0.0f;
    \\        #pragma unroll
    \\        for (unsigned int w = 0u; w < 8u; ++w) y += warp_partial[tid][w];
    \\        const unsigned int col = col_tile + tid;
    \\        if (col < out_dim) dst[(size_t)row * out_dim + col] = y;
    \\    }
    \\}
    ,
};

fn appendFmt(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime fmt: []const u8,
    args: anytype,
) !void {
    const chunk = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(chunk);
    try out.appendSlice(allocator, chunk);
}

pub fn supportFor(lowering: KernelLowering) KernelSupport {
    return switch (lowering) {
        .q4_k_small_batch => .{ .includes = &includes_cuda_fp16_math_stdint, .helpers = &q4_k_helpers },
        .q4_k_projection => .{ .includes = &includes_cuda_fp16_stdint, .helpers = &q4_k_projection_helpers },
        .q4_0_projection => .{ .includes = &includes_cuda_fp16_stdint, .helpers = &q4_0_helpers },
        .q4_0_pair_activation_q8_1 => |pair| if (pair.decode == .q4_0_q8_1_dp4a)
            .{ .includes = &includes_cuda_fp16, .helpers = &q4_0_ggml_q8_1_helpers }
        else
            .{ .includes = &includes_cuda_fp16, .helpers = &q4_0_q8_helpers },
        .q4_0_down_q8_1 => |down| if (down.decode == .q4_0_q8_1_dp4a)
            .{ .includes = &includes_cuda_fp16, .helpers = &q4_0_down_ggml_q8_1_helpers }
        else
            .{ .includes = &includes_cuda_fp16, .helpers = &q4_0_down_q8_helpers },
        .q4_0_pair_activation_f32_exact,
        .q4_0_down_f32_exact,
        => .{ .includes = &includes_cuda_fp16, .helpers = &q4_0_exact_ffn_f32_helpers },
        .q4_0_q8_1_argmax => .{ .includes = &includes_cuda_fp16, .helpers = &q4_0_down_q8_helpers },
        .q6_k_q8_1_argmax => .{ .includes = &includes_cuda_fp16, .helpers = &q6_k_q8_helpers },
    };
}

pub fn renderBodyAlloc(allocator: std.mem.Allocator, plan: RenderPlan) ![]u8 {
    try plan.validate();
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try renderBody(allocator, &out, plan);
    return out.toOwnedSlice(allocator);
}

pub const ggml_q8_1_quantize_rows_kernel_id = "antfly_quantize_f32_ggml_q8_1_rows_v1";

/// Common activation-boundary kernel for llama.cpp CUDA's block_q8_1 layout:
/// half2(d, raw f32 input sum) followed by 32 signed values.
/// It is emitted once into the runtime dev-matmul region by the repository
/// code generator, rather than hand-maintained in the canonical CUDA source.
pub fn renderGgmlQ8_1QuantizeRowsBodyAlloc(allocator: std.mem.Allocator) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try appendFmt(
        allocator,
        &out,
        \\extern "C" __global__ void {s}(
        \\    unsigned char *dst_q8,
        \\    const float *input,
        \\    unsigned int rows,
        \\    unsigned int dim
        \\) {{
        \\    if (blockDim.x != 32u || rows == 0u || dim == 0u || (dim & 31u) != 0u) return;
        \\    const unsigned int blocks_per_row = dim >> 5;
        \\    const unsigned int block = blockIdx.x;
        \\    const unsigned int row = block / blocks_per_row;
        \\    const unsigned int value_block = block - row * blocks_per_row;
        \\    if (row >= rows) return;
        \\    const unsigned int lane = threadIdx.x;
        \\    const float x = input[(size_t)row * dim + value_block * 32u + lane];
        \\    const float amax = antfly_warp_reduce_max_f32(fabsf(x));
        \\    const float sum = antfly_warp_reduce_sum_f32(x);
        \\    const float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        \\    int q = d > 0.0f ? __float2int_rn(x / d) : 0;
        \\    q = max(-127, min(127, q));
        \\    unsigned char *bp = dst_q8 + (size_t)block * 36u;
        \\    bp[4u + lane] = (unsigned char)(signed char)q;
        \\    if (lane == 0u) {{
        \\        const unsigned short d_bits = __half_as_ushort(__float2half(d));
        \\        const unsigned short sum_bits = __half_as_ushort(__float2half(sum));
        \\        bp[0] = (unsigned char)(d_bits & 0xffu);
        \\        bp[1] = (unsigned char)(d_bits >> 8);
        \\        bp[2] = (unsigned char)(sum_bits & 0xffu);
        \\        bp[3] = (unsigned char)(sum_bits >> 8);
        \\    }}
        \\}}
    ,
        .{ggml_q8_1_quantize_rows_kernel_id},
    );
    return out.toOwnedSlice(allocator);
}

fn renderBody(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), plan: RenderPlan) !void {
    switch (plan.lowering) {
        .q4_k_small_batch => try out.appendSlice(allocator, body_q4_k_small_batch_bias_gelu.source),
        .q4_k_projection => try out.appendSlice(allocator, body_q4_k_mmv.source),
        .q4_0_projection => |lowering| try renderQ4ProjectionBody(allocator, out, plan.kernel_id, lowering),
        .q4_0_pair_activation_q8_1 => try renderPairActivationQ8Body(allocator, out, plan),
        .q4_0_down_q8_1 => try renderDownQ8Body(allocator, out, plan),
        .q4_0_pair_activation_f32_exact => try renderExactFfnPairF32Body(allocator, out, plan),
        .q4_0_down_f32_exact => try renderExactFfnDownF32Body(allocator, out, plan),
        .q4_0_q8_1_argmax => try renderQ4Q8ArgmaxBody(allocator, out, plan),
        .q6_k_q8_1_argmax => try renderQ6KQ8ArgmaxBody(allocator, out, plan),
    }
}

fn renderQ4ProjectionBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    kernel_id: []const u8,
    lowering: Q4ProjectionLowering,
) !void {
    try lowering.validate();
    const rows = lowering.row_tile;
    const projections = lowering.projection_count;
    const cols = lowering.columns_per_block;
    const warps = lowering.warps_per_block;

    try appendFmt(allocator, out, "extern \"C\" __global__ void {s}(\n", .{kernel_id});
    try out.appendSlice(allocator, "    const float *input,\n");
    if (projections == 1) {
        try out.appendSlice(allocator, "    const uint8_t *weight_q4_0,\n    float *output,\n");
    } else {
        try out.appendSlice(
            allocator,
            "    const uint8_t *weight_a_q4_0,\n" ++
                "    const uint8_t *weight_b_q4_0,\n" ++
                "    float *output_a,\n" ++
                "    float *output_b,\n",
        );
    }
    try out.appendSlice(allocator, "    int rows,\n    int in_dim,\n    int out_dim\n) {\n");
    try appendFmt(allocator, out, "    const int col0 = blockIdx.x << {d};\n", .{@as(u8, 2)});
    if (rows == 1) {
        try out.appendSlice(allocator, "    if (rows != 1 || col0 >= out_dim) return;\n");
    } else {
        try appendFmt(
            allocator,
            out,
            "    const int row0 = blockIdx.y << {d};\n" ++
                "    if (rows < 9 || rows > 64) return;\n" ++
                "    if (col0 >= out_dim || row0 >= rows) return;\n",
            .{@as(u8, 3)},
        );
    }
    try appendFmt(
        allocator,
        out,
        "    if (blockDim.x != {d}) return;\n" ++
            "    if ((in_dim & 31) != 0) return;\n\n" ++
            "    const int row_blocks = in_dim >> 5;\n" ++
            "    const int half_bytes = in_dim >> 1;\n",
        .{@as(u16, warps) * 32},
    );

    if (rows == 1 and projections == 1) {
        try appendFmt(allocator, out, "    float acc[{d}] = {{0.0f, 0.0f, 0.0f, 0.0f}};\n", .{cols});
    } else if (rows == 1) {
        try appendFmt(
            allocator,
            out,
            "    float acc[{d}][{d}];\n" ++
                "#pragma unroll\n" ++
                "    for (int w = 0; w < {d}; ++w) {{\n" ++
                "#pragma unroll\n" ++
                "        for (int c = 0; c < {d}; ++c) acc[w][c] = 0.0f;\n" ++
                "    }}\n",
            .{ projections, cols, projections, cols },
        );
    } else {
        try appendFmt(
            allocator,
            out,
            "    float acc[{d}][{d}];\n" ++
                "#pragma unroll\n" ++
                "    for (int c = 0; c < {d}; ++c) {{\n" ++
                "#pragma unroll\n" ++
                "        for (int r = 0; r < {d}; ++r) acc[c][r] = 0.0f;\n" ++
                "    }}\n",
            .{ cols, rows, cols, rows },
        );
    }

    if (rows != 1 or projections != 1) try out.append(allocator, '\n');
    try appendFmt(
        allocator,
        out,
        "    for (int byte_idx = threadIdx.x; byte_idx < half_bytes; byte_idx += {d}) {{\n" ++
            "        const int block_idx = byte_idx >> 4;\n" ++
            "        const int offset = byte_idx & 15;\n" ++
            "        const int base = block_idx << 5;\n",
        .{@as(u16, warps) * 32},
    );
    if (rows == 1) {
        try out.appendSlice(
            allocator,
            "        const float x_lo = input[base + offset];\n" ++
                "        const float x_hi = input[base + offset + 16];\n",
        );
    } else {
        try appendFmt(
            allocator,
            out,
            "        float x_lo[{d}];\n" ++
                "        float x_hi[{d}];\n" ++
                "#pragma unroll\n" ++
                "        for (int r = 0; r < {d}; ++r) {{\n" ++
                "            const int row = row0 + r;\n" ++
                "            x_lo[r] = row < rows ? input[(size_t)row * in_dim + base + offset] : 0.0f;\n" ++
                "            x_hi[r] = row < rows ? input[(size_t)row * in_dim + base + offset + 16] : 0.0f;\n" ++
                "        }}\n",
            .{ rows, rows, rows },
        );
    }
    if (projections == 2) {
        try appendFmt(
            allocator,
            out,
            "#pragma unroll\n" ++
                "        for (int w = 0; w < {d}; ++w) {{\n" ++
                "            const uint8_t *weight = w == 0 ? weight_a_q4_0 : weight_b_q4_0;\n",
            .{projections},
        );
    }
    const projection_loop_indent = if (projections == 2) "            " else "        ";
    const projection_body_indent = if (projections == 2) "                " else "            ";
    try appendFmt(
        allocator,
        out,
        "#pragma unroll\n" ++
            "{s}for (int c = 0; c < {d}; ++c) {{\n" ++
            "{s}if (col0 + c >= out_dim) continue;\n",
        .{ projection_loop_indent, cols, projection_body_indent },
    );
    const weight_name = if (projections == 2) "weight" else "weight_q4_0";
    try appendFmt(
        allocator,
        out,
        "{s}const uint8_t *block = {s} + ((size_t)(col0 + c) * row_blocks + block_idx) * 18;\n" ++
            "{s}const float d = antfly_half_le_to_float(block);\n" ++
            "{s}const int packed = (int)block[2 + offset];\n",
        .{ projection_body_indent, weight_name, projection_body_indent, projection_body_indent },
    );
    if (rows == 1) {
        if (projections == 1) {
            try out.appendSlice(allocator, "            acc[c] += d * (x_lo * (float)((packed & 15) - 8) + x_hi * (float)((packed >> 4) - 8));\n");
        } else {
            try out.appendSlice(allocator, "                acc[w][c] += d * (x_lo * (float)((packed & 15) - 8) + x_hi * (float)((packed >> 4) - 8));\n");
        }
    } else {
        try out.appendSlice(
            allocator,
            "            const float w_lo = d * (float)((packed & 15) - 8);\n" ++
                "            const float w_hi = d * (float)((packed >> 4) - 8);\n" ++
                "#pragma unroll\n",
        );
        try appendFmt(allocator, out, "            for (int r = 0; r < {d}; ++r) acc[c][r] += w_lo * x_lo[r] + w_hi * x_hi[r];\n", .{rows});
    }
    try out.appendSlice(allocator, if (projections == 2) "            }\n        }\n    }\n" else "        }\n    }\n");

    if (rows == 1 and projections == 1) {
        try appendFmt(allocator, out, "\n    __shared__ float partial[{d}][{d}];\n", .{ cols, warps });
    } else if (rows == 1) {
        try appendFmt(allocator, out, "\n    __shared__ float partial[{d}][{d}][{d}];\n", .{ projections, cols, warps });
    } else {
        try appendFmt(allocator, out, "\n    __shared__ float partial[{d}][{d}][{d}];\n", .{ cols, rows, warps });
    }
    try out.appendSlice(
        allocator,
        "    const int lane = threadIdx.x & 31;\n" ++
            "    const int warp = threadIdx.x >> 5;\n",
    );
    if (projections == 2) {
        try appendFmt(allocator, out, "#pragma unroll\n    for (int w = 0; w < {d}; ++w) {{\n", .{projections});
    }
    const reduction_loop_indent = if (projections == 2) "        " else "    ";
    try appendFmt(allocator, out, "#pragma unroll\n{s}for (int c = 0; c < {d}; ++c) {{\n", .{ reduction_loop_indent, cols });
    if (rows > 1) {
        try appendFmt(allocator, out, "#pragma unroll\n        for (int r = 0; r < {d}; ++r) {{\n", .{rows});
    }
    const acc_expr = if (rows > 1) "acc[c][r]" else if (projections == 2) "acc[w][c]" else "acc[c]";
    const partial_expr = if (rows > 1) "partial[c][r][warp]" else if (projections == 2) "partial[w][c][warp]" else "partial[c][warp]";
    const indent = if (rows > 1) "            " else if (projections == 2) "            " else "        ";
    try appendFmt(
        allocator,
        out,
        "{s}const float total = antfly_warp_reduce_sum({s});\n" ++
            "{s}if (lane == 0) {s} = total;\n",
        .{ indent, acc_expr, indent, partial_expr },
    );
    if (rows > 1) try out.appendSlice(allocator, "        }\n");
    try out.appendSlice(allocator, if (projections == 2) "        }\n" else "    }\n");
    if (projections == 2) try out.appendSlice(allocator, "    }\n");
    try out.appendSlice(allocator, "    __syncthreads();\n");

    const final_threads: u16 = @as(u16, projections) * @as(u16, cols) * @as(u16, rows);
    try appendFmt(allocator, out, "    if (threadIdx.x < {d}) {{\n", .{final_threads});
    if (projections == 2) {
        try appendFmt(
            allocator,
            out,
            "        const int w = threadIdx.x >> 2;\n" ++
                "        const int c = threadIdx.x & 3;\n" ++
                "        float total = 0.0f;\n" ++
                "#pragma unroll\n" ++
                "        for (int i = 0; i < {d}; ++i) total += partial[w][c][i];\n" ++
                "        if (col0 + c < out_dim) {{\n" ++
                "            float *output = w == 0 ? output_a : output_b;\n" ++
                "            output[col0 + c] = total;\n" ++
                "        }}\n",
            .{warps},
        );
    } else if (rows == 1) {
        try appendFmt(
            allocator,
            out,
            "        float total = 0.0f;\n" ++
                "#pragma unroll\n" ++
                "        for (int w = 0; w < {d}; ++w) total += partial[threadIdx.x][w];\n" ++
                "        if (col0 + threadIdx.x < out_dim) output[col0 + threadIdx.x] = total;\n",
            .{warps},
        );
    } else {
        try appendFmt(
            allocator,
            out,
            "        const int c = threadIdx.x >> 3;\n" ++
                "        const int r = threadIdx.x & 7;\n" ++
                "        float total = 0.0f;\n" ++
                "#pragma unroll\n" ++
                "        for (int w = 0; w < {d}; ++w) total += partial[c][r][w];\n" ++
                "        const int col = col0 + c;\n" ++
                "        const int row = row0 + r;\n" ++
                "        if (col < out_dim && row < rows) output[(size_t)row * out_dim + col] = total;\n",
            .{warps},
        );
    }
    try out.appendSlice(allocator, "    }\n}\n");
}

fn renderPairActivationQ8Body(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    plan: RenderPlan,
) !void {
    const lowering = switch (plan.lowering) {
        .q4_0_pair_activation_q8_1 => |value| value,
        else => return error.InvalidPairActivationQ8Lowering,
    };
    const warps = lowering.warps_per_projection_group;
    const ggml_q8_1 = lowering.decode == .q4_0_q8_1_dp4a;
    const dot_function = if (ggml_q8_1) "antfly_q4_0_ggml_q8_1_dot16" else "antfly_q4_0_q8_dot16";
    const dot_sum_argument = if (ggml_q8_1) ", q8_sum" else "";
    try appendFmt(allocator, out, "extern \"C\" __global__ void {s}(\n", .{plan.kernel_id});
    try out.appendSlice(allocator,
        \\    unsigned char *dst_q8,
        \\    const unsigned char *q8_input,
        \\    const unsigned char *weight_gate,
        \\    const unsigned char *weight_up,
        \\    unsigned int activation,
        \\    unsigned int rows,
        \\    unsigned int in_dim,
        \\    unsigned int out_dim
        \\) {
        \\    if (rows == 0u || (in_dim & 31u) != 0u || (out_dim & 31u) != 0u) return;
        \\    const unsigned int row_blocks = in_dim >> 5;
        \\    const unsigned int out_row_blocks = out_dim >> 5;
        \\    const unsigned int group_cols = 4u;
        \\    const unsigned int groups_per_wave = 4u;
        \\    const unsigned int waves = 2u;
        \\
        \\    const unsigned int out_block = blockIdx.x % out_row_blocks;
        \\    const unsigned int row = blockIdx.x / out_row_blocks;
        \\    const unsigned int col_block = out_block * 32u;
        \\    const unsigned int tid = threadIdx.x;
        \\    const unsigned int lane = tid & 31u;
        \\    const unsigned int warp = tid >> 5u;
        \\    const unsigned int group = warp /
    );
    try appendFmt(allocator, out, " {d}u;\n    const unsigned int group_warp = warp - group * {d}u;\n", .{ warps, warps });
    try appendFmt(allocator, out, "    if (blockDim.x != {d}u || row >= rows) return;\n\n", .{plan.launch.threads_per_block});
    try appendFmt(allocator, out, "    __shared__ float gate_partial[4][4][{d}];\n", .{warps});
    try appendFmt(allocator, out, "    __shared__ float up_partial[4][4][{d}];\n", .{warps});
    try out.appendSlice(allocator,
        \\    __shared__ float activated[32];
        \\
        \\    #pragma unroll
        \\    for (unsigned int wave = 0u; wave < waves; ++wave) {
        \\        if (group < groups_per_wave) {
        \\            const unsigned int local_tid = group_warp * 32u + lane;
        \\            const unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;
        \\            float gate_acc[4];
        \\            float up_acc[4];
        \\            #pragma unroll
        \\            for (unsigned int c = 0u; c < group_cols; ++c) {
        \\                gate_acc[c] = 0.0f;
        \\                up_acc[c] = 0.0f;
        \\            }
        \\
        \\            const unsigned int iqs = (local_tid & 1u) * 2u;
        \\            for (unsigned int block = local_tid >> 1u; block < row_blocks; block +=
    );
    try appendFmt(allocator, out, " {d}u) {{\n", .{lowering.input_blocks});
    try out.appendSlice(allocator,
        \\                const unsigned char *q8_bp = q8_input + (row * row_blocks + block) * 36u;
        \\                const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
    );
    try out.appendSlice(allocator, "\n");
    if (ggml_q8_1) try out.appendSlice(
        allocator,
        "                const float q8_sum = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[1]);\n",
    );
    try out.appendSlice(allocator,
        \\                const signed char *q8_values = (const signed char *)(q8_bp + 4u);
        \\                const unsigned int q8_base0 = iqs * 4u;
        \\                const unsigned int q8_base1 = q8_base0 + 4u;
        \\                const int q8_low0 = *(const int *)(q8_values + q8_base0);
        \\                const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
        \\                const int q8_low1 = *(const int *)(q8_values + q8_base1);
        \\                const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);
        \\
        \\                #pragma unroll
        \\                for (unsigned int c = 0u; c < group_cols; ++c) {
        \\                    const unsigned int col = col_tile + c;
        \\                    const unsigned char *gate_bp = weight_gate + ((size_t)col * row_blocks + block) * 18u;
        \\                    const unsigned char *up_bp = weight_up + ((size_t)col * row_blocks + block) * 18u;
    );
    try out.appendSlice(allocator, "\n");
    try appendFmt(
        allocator,
        out,
        "                    gate_acc[c] += {s}(gate_bp, q8_d{s}, iqs, q8_low0, q8_high0, q8_low1, q8_high1);\n" ++
            "                    up_acc[c] += {s}(up_bp, q8_d{s}, iqs, q8_low0, q8_high0, q8_low1, q8_high1);\n",
        .{ dot_function, dot_sum_argument, dot_function, dot_sum_argument },
    );
    try out.appendSlice(allocator,
        \\                }
        \\            }
        \\
        \\            #pragma unroll
        \\            for (unsigned int c = 0u; c < group_cols; ++c) {
        \\                const float gate_sum = antfly_warp_reduce_sum_f32(gate_acc[c]);
        \\                const float up_sum = antfly_warp_reduce_sum_f32(up_acc[c]);
        \\                if (lane == 0u) {
        \\                    gate_partial[group][c][group_warp] = gate_sum;
        \\                    up_partial[group][c][group_warp] = up_sum;
        \\                }
        \\            }
        \\        }
        \\        __syncthreads();
        \\        if (tid < 16u) {
        \\            const unsigned int out_group = tid >> 2u;
        \\            const unsigned int c = tid & 3u;
        \\            float gate_y = 0.0f;
        \\            float up_y = 0.0f;
        \\            #pragma unroll
        \\            for (unsigned int w = 0u; w <
    );
    try appendFmt(allocator, out, " {d}u; ++w) {{\n", .{warps});
    try out.appendSlice(allocator,
        \\                gate_y += gate_partial[out_group][c][w];
        \\                up_y += up_partial[out_group][c][w];
        \\            }
        \\            activated[wave * 16u + out_group * group_cols + c] = antfly_decoder_activation_f32(gate_y, activation) * up_y;
        \\        }
        \\        __syncthreads();
        \\    }
        \\
        \\    if (warp == 0u) {
        \\        const float x = activated[lane];
    );
    try out.appendSlice(allocator, "\n");
    if (ggml_q8_1) try out.appendSlice(
        allocator,
        "        const float sum = antfly_warp_reduce_sum_f32(x);\n",
    );
    try out.appendSlice(allocator,
        \\        const float amax = antfly_warp_reduce_max_f32(fabsf(x));
        \\        const float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        \\        int q = 0;
        \\        if (d > 0.0f) {
        \\            q = __float2int_rn(x / d);
        \\            q = max(-127, min(127, q));
        \\        }
        \\        unsigned char *bp = dst_q8 + ((size_t)row * out_row_blocks + out_block) * 36u;
        \\        bp[4u + lane] = (unsigned char)(signed char)q;
        \\        if (lane == 0u) {
        \\            const unsigned short d_bits = __half_as_ushort(__float2half(d));
        \\            bp[0] = (unsigned char)(d_bits & 0xffu);
        \\            bp[1] = (unsigned char)(d_bits >> 8);
    );
    try out.appendSlice(allocator, "\n");
    if (ggml_q8_1) {
        try out.appendSlice(allocator,
            \\            const unsigned short sum_bits = __half_as_ushort(__float2half(sum));
            \\            bp[2] = (unsigned char)(sum_bits & 0xffu);
            \\            bp[3] = (unsigned char)(sum_bits >> 8);
        );
    } else {
        try out.appendSlice(allocator,
            \\            bp[2] = 0u;
            \\            bp[3] = 0u;
        );
    }
    try out.appendSlice(allocator, "\n");
    try out.appendSlice(allocator,
        \\        }
        \\    }
        \\}
    );
}

fn renderDownQ8Body(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    plan: RenderPlan,
) !void {
    const lowering = switch (plan.lowering) {
        .q4_0_down_q8_1 => |value| value,
        else => return error.InvalidDownQ8Lowering,
    };
    const warps = plan.launch.threads_per_block / 32;
    const block_stride = plan.launch.threads_per_block / 2;
    const cols = lowering.columns_per_block;
    const ggml_q8_1 = lowering.decode == .q4_0_q8_1_dp4a;
    const dot_function = if (ggml_q8_1) "antfly_q4_0_ggml_q8_1_dot16" else "antfly_q4_0_q8_dot16";
    const dot_sum_argument = if (ggml_q8_1) ", q8_sum" else "";
    try appendFmt(allocator, out, "extern \"C\" __global__ void {s}(\n", .{plan.kernel_id});
    try out.appendSlice(allocator,
        \\    float *dst,
        \\    const unsigned char *q8_input,
        \\    const unsigned char *weight,
        \\    unsigned int rows,
        \\    unsigned int in_dim,
        \\    unsigned int out_dim
        \\) {
    );
    try out.appendSlice(allocator, "\n");
    try appendFmt(allocator, out, "    const unsigned int cols = {d}u;\n", .{cols});
    try out.appendSlice(allocator,
        \\    if (rows == 0u || (in_dim & 31u) != 0u || out_dim == 0u) return;
        \\    const unsigned int row_blocks = in_dim >> 5;
        \\    const unsigned int col_tiles = (out_dim + cols - 1u) / cols;
        \\    const unsigned int row = blockIdx.x / col_tiles;
        \\    const unsigned int col_tile = (blockIdx.x % col_tiles) * cols;
        \\    const unsigned int tid = threadIdx.x;
        \\    const unsigned int lane = tid & 31u;
        \\    const unsigned int warp = tid >> 5u;
    );
    try appendFmt(allocator, out, "\n    if (blockDim.x != {d}u || row >= rows) return;\n\n", .{plan.launch.threads_per_block});
    try appendFmt(allocator, out, "    __shared__ float warp_partial[{d}][{d}];\n", .{ cols, warps });
    try appendFmt(allocator, out, "    float acc[{d}];\n", .{cols});
    try out.appendSlice(allocator,
        \\    #pragma unroll
        \\    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;
        \\
        \\    const unsigned int iqs = (tid & 1u) * 2u;
        \\    for (unsigned int block = tid >> 1u; block < row_blocks; block +=
    );
    try appendFmt(allocator, out, " {d}u) {{\n", .{block_stride});
    try out.appendSlice(allocator,
        \\        const unsigned char *q8_bp = q8_input + ((size_t)row * row_blocks + block) * 36u;
        \\        const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
    );
    try out.appendSlice(allocator, "\n");
    if (ggml_q8_1) try out.appendSlice(
        allocator,
        "        const float q8_sum = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[1]);\n",
    );
    try out.appendSlice(allocator,
        \\        const signed char *q8_values = (const signed char *)(q8_bp + 4u);
        \\        const unsigned int q8_base0 = iqs * 4u;
        \\        const unsigned int q8_base1 = q8_base0 + 4u;
        \\        const int q8_low0 = *(const int *)(q8_values + q8_base0);
        \\        const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
        \\        const int q8_low1 = *(const int *)(q8_values + q8_base1);
        \\        const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);
        \\
        \\        #pragma unroll
        \\        for (unsigned int c = 0u; c < cols; ++c) {
        \\            const unsigned int col = col_tile + c;
        \\            if (col < out_dim) {
        \\                const unsigned char *bp = weight + ((size_t)col * row_blocks + block) * 18u;
    );
    try out.appendSlice(allocator, "\n");
    try appendFmt(
        allocator,
        out,
        "                acc[c] += {s}(bp, q8_d{s}, iqs, q8_low0, q8_high0, q8_low1, q8_high1);\n",
        .{ dot_function, dot_sum_argument },
    );
    try out.appendSlice(allocator,
        \\            }
        \\        }
        \\    }
        \\
        \\    #pragma unroll
        \\    for (unsigned int c = 0u; c < cols; ++c) {
        \\        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        \\        if (lane == 0u) warp_partial[c][warp] = sum;
        \\    }
        \\    __syncthreads();
    );
    try out.appendSlice(allocator, "\n");
    try appendFmt(allocator, out, "    if (tid < {d}u) {{\n", .{cols});
    try out.appendSlice(allocator,
        \\        float y = 0.0f;
        \\        #pragma unroll
        \\        for (unsigned int w = 0u; w <
    );
    try appendFmt(allocator, out, " {d}u; ++w) y += warp_partial[tid][w];\n", .{warps});
    try out.appendSlice(allocator,
        \\        const unsigned int col = col_tile + tid;
        \\        if (col < out_dim) dst[(size_t)row * out_dim + col] = y;
        \\    }
        \\}
    );
}

fn renderExactFfnPairF32Body(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    plan: RenderPlan,
) !void {
    const lowering = switch (plan.lowering) {
        .q4_0_pair_activation_f32_exact => |value| value,
        else => return error.InvalidExactFfnPairF32Lowering,
    };
    const input_dim = plan.launch.input_dim.fixed;
    const output_dim = plan.launch.output_dim.fixed;
    const threads = plan.launch.threads_per_block;
    try appendFmt(allocator, out, "extern \"C\" __global__ void {s}(\n", .{plan.kernel_id});
    try out.appendSlice(allocator,
        \\    float* dst,
        \\    const float* input,
        \\    const unsigned char* weight_gate,
        \\    const unsigned char* weight_up,
        \\    unsigned int rows,
        \\    unsigned int in_dim,
        \\    unsigned int out_dim,
        \\    unsigned int activation
        \\) {
        \\    const unsigned int cols = 4u;
        \\    const unsigned int tiles = (out_dim + cols - 1u) / cols;
        \\    const unsigned int row = blockIdx.x / tiles;
        \\    const unsigned int col_tile = (blockIdx.x - row * tiles) * cols;
        \\    const unsigned int tid = threadIdx.x;
        \\    const unsigned int lane = tid & 31u;
        \\    const unsigned int warp = tid >> 5u;
        \\    const unsigned int row_blocks = in_dim / 32u;
    );
    try appendFmt(
        allocator,
        out,
        "\n    if (rows != 1u || in_dim != {d}u || out_dim != {d}u || blockDim.x != {d}u || row >= rows) return;\n\n",
        .{ input_dim, output_dim, threads },
    );
    try appendFmt(allocator, out, "    __shared__ float gate_partial[4][{d}];\n", .{lowering.warps_per_block});
    try appendFmt(allocator, out, "    __shared__ float up_partial[4][{d}];\n", .{lowering.warps_per_block});
    try out.appendSlice(allocator,
        \\    float gate_acc[4];
        \\    float up_acc[4];
        \\    #pragma unroll
        \\    for (unsigned int c = 0u; c < 4u; ++c) {
        \\        gate_acc[c] = 0.0f;
        \\        up_acc[c] = 0.0f;
        \\    }
        \\
        \\    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        \\        const float x = input[row * in_dim + i];
        \\        const unsigned int block = i / 32u;
        \\        const unsigned int value_lane = i - block * 32u;
        \\        const unsigned int q_offset = 2u + (value_lane & 15u);
        \\        const unsigned int high_nibble = value_lane >> 4u;
        \\        #pragma unroll
        \\        for (unsigned int c = 0u; c < 4u; ++c) {
        \\            const unsigned int col = col_tile + c;
        \\            if (col < out_dim) {
        \\                const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
        \\                const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
        \\                gate_acc[c] += x * termite_q4_0_value_nibble(gate_bp, q_offset, high_nibble);
        \\                up_acc[c] += x * termite_q4_0_value_nibble(up_bp, q_offset, high_nibble);
        \\            }
        \\        }
        \\    }
        \\
        \\    #pragma unroll
        \\    for (unsigned int c = 0u; c < 4u; ++c) {
        \\        const float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
        \\        const float up_sum = termite_warp_reduce_sum(up_acc[c]);
        \\        if (lane == 0u && warp < 4u) {
        \\            gate_partial[c][warp] = gate_sum;
        \\            up_partial[c][warp] = up_sum;
        \\        }
        \\    }
        \\    __syncthreads();
        \\    if (tid == 0u) {
        \\        #pragma unroll
        \\        for (unsigned int c = 0u; c < 4u; ++c) {
        \\            const unsigned int col = col_tile + c;
        \\            if (col < out_dim) {
        \\                float gate_y = 0.0f;
        \\                float up_y = 0.0f;
        \\                #pragma unroll
        \\                for (unsigned int w = 0u; w < 4u; ++w) {
        \\                    gate_y += gate_partial[c][w];
        \\                    up_y += up_partial[c][w];
        \\                }
        \\                dst[row * out_dim + col] = termite_decoder_activation_f32(gate_y, activation) * up_y;
        \\            }
        \\        }
        \\    }
        \\}
    );
}

fn renderExactFfnDownF32Body(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    plan: RenderPlan,
) !void {
    const lowering = switch (plan.lowering) {
        .q4_0_down_f32_exact => |value| value,
        else => return error.InvalidExactFfnDownF32Lowering,
    };
    const input_dim = plan.launch.input_dim.fixed;
    const output_dim = plan.launch.output_dim.fixed;
    const threads = plan.launch.threads_per_block;
    try appendFmt(allocator, out, "extern \"C\" __global__ void {s}(\n", .{plan.kernel_id});
    try out.appendSlice(allocator,
        \\    float* dst,
        \\    const float* input,
        \\    const unsigned char* weight,
        \\    unsigned int rows,
        \\    unsigned int in_dim,
        \\    unsigned int out_dim
        \\) {
        \\    const unsigned int cols = 4u;
        \\    const unsigned int col_tile = blockIdx.x * cols;
        \\    const unsigned int row = 0u;
        \\    const unsigned int tid = threadIdx.x;
        \\    const unsigned int lane = tid & 31u;
        \\    const unsigned int warp = tid >> 5u;
        \\    const unsigned int row_blocks = in_dim / 32u;
    );
    try appendFmt(
        allocator,
        out,
        "\n    if (rows != 1u || in_dim != {d}u || out_dim != {d}u || blockDim.x != {d}u) return;\n\n",
        .{ input_dim, output_dim, threads },
    );
    try appendFmt(allocator, out, "    __shared__ float warp_partial[4][{d}];\n", .{lowering.warps_per_block});
    try out.appendSlice(allocator,
        \\    float acc[4];
        \\    #pragma unroll
        \\    for (unsigned int c = 0u; c < 4u; ++c) acc[c] = 0.0f;
        \\
        \\    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        \\        const float x = input[row * in_dim + i];
        \\        const unsigned int block = i / 32u;
        \\        const unsigned int value_lane = i - block * 32u;
        \\        const unsigned int q_offset = 2u + (value_lane & 15u);
        \\        const unsigned int high_nibble = value_lane >> 4u;
        \\        #pragma unroll
        \\        for (unsigned int c = 0u; c < 4u; ++c) {
        \\            const unsigned int col = col_tile + c;
        \\            if (col < out_dim) {
        \\                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
        \\                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
        \\            }
        \\        }
        \\    }
        \\
        \\    #pragma unroll
        \\    for (unsigned int c = 0u; c < 4u; ++c) {
        \\        const float sum = termite_warp_reduce_sum(acc[c]);
        \\        if (lane == 0u && warp < 8u) warp_partial[c][warp] = sum;
        \\    }
        \\    __syncthreads();
        \\    if (tid == 0u) {
        \\        #pragma unroll
        \\        for (unsigned int c = 0u; c < 4u; ++c) {
        \\            const unsigned int col = col_tile + c;
        \\            if (col < out_dim) {
        \\                float y = 0.0f;
        \\                #pragma unroll
        \\                for (unsigned int w = 0u; w < 8u; ++w) y += warp_partial[c][w];
        \\                dst[row * out_dim + col] = y;
        \\            }
        \\        }
        \\    }
        \\}
    );
}

fn renderQ4Q8ArgmaxBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    plan: RenderPlan,
) !void {
    _ = switch (plan.lowering) {
        .q4_0_q8_1_argmax => |value| value,
        else => return error.InvalidQ4Q8ArgmaxLowering,
    };
    try appendFmt(allocator, out, "extern \"C\" __global__ void {s}(\n", .{plan.kernel_id});
    try out.appendSlice(allocator,
        \\    float* partial_values,
        \\    unsigned int* partial_indices,
        \\    const unsigned char* q8_input,
        \\    const unsigned char* weight,
        \\    const int* suppress_token_ids,
        \\    unsigned int rows,
        \\    unsigned int in_dim,
        \\    unsigned int out_dim,
        \\    unsigned int suppress_count
        \\) {
        \\    const unsigned int cols = 8u;
        \\    const unsigned int row_blocks = 48u;
        \\    if (rows != 1u || in_dim != 1536u || out_dim != 262144u || blockDim.x != 96u) return;
        \\
        \\    const unsigned int global_tile = blockIdx.x;
        \\    const unsigned int col_tile = global_tile * cols;
        \\    const unsigned int tid = threadIdx.x;
        \\    const unsigned int lane = tid & 31u;
        \\    const unsigned int warp = tid >> 5u;
        \\    __shared__ float warp_partial[8][3];
        \\    float acc[8];
        \\#pragma unroll
        \\    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;
        \\
        \\    const unsigned int iqs = (tid & 1u) * 2u;
        \\    const unsigned int block = tid >> 1u;
        \\    if (block < row_blocks) {
        \\        const unsigned char* q8_bp = q8_input + block * 36u;
        \\        const float q8_d = antfly_half_bits_to_float(((const unsigned short*)q8_bp)[0]);
        \\        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        \\        const unsigned int q8_base0 = iqs * 4u;
        \\        const unsigned int q8_base1 = q8_base0 + 4u;
        \\        const int q8_low0 = *(const int*)(q8_values + q8_base0);
        \\        const int q8_high0 = *(const int*)(q8_values + q8_base0 + 16u);
        \\        const int q8_low1 = *(const int*)(q8_values + q8_base1);
        \\        const int q8_high1 = *(const int*)(q8_values + q8_base1 + 16u);
        \\#pragma unroll
        \\        for (unsigned int c = 0u; c < cols; ++c) {
        \\            const unsigned int col = col_tile + c;
        \\            const unsigned char* bp = weight + ((size_t)col * row_blocks + block) * 18u;
        \\            acc[c] = antfly_q4_0_q8_dot16(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
        \\        }
        \\    }
        \\
        \\#pragma unroll
        \\    for (unsigned int c = 0u; c < cols; ++c) {
        \\        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        \\        if (lane == 0u) warp_partial[c][warp] = sum;
        \\    }
        \\    __syncthreads();
        \\    if (tid != 0u) return;
        \\
        \\    float best_value = -3.402823466e+38f;
        \\    unsigned int best_index = 0xffffffffu;
        \\#pragma unroll
        \\    for (unsigned int c = 0u; c < cols; ++c) {
        \\        const unsigned int col = col_tile + c;
        \\        float value = 0.0f;
        \\#pragma unroll
        \\        for (unsigned int w = 0u; w < 3u; ++w) value += warp_partial[c][w];
        \\        bool suppressed = false;
        \\        for (unsigned int j = 0u; j < suppress_count; ++j) {
        \\            const int token_id = suppress_token_ids[j];
        \\            if (token_id >= 0 && (unsigned int)token_id == col) {
        \\                suppressed = true;
        \\                break;
        \\            }
        \\        }
        \\        if (!suppressed && (value > best_value || (value == best_value && col < best_index))) {
        \\            best_value = value;
        \\            best_index = col;
        \\        }
        \\    }
        \\    partial_values[global_tile] = best_value;
        \\    partial_indices[global_tile] = best_index;
        \\}
    );
}

fn renderQ6KQ8ArgmaxBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    plan: RenderPlan,
) !void {
    const lowering = switch (plan.lowering) {
        .q6_k_q8_1_argmax => |value| value,
        else => return error.InvalidQ6KQ8ArgmaxLowering,
    };
    const rows = plan.launch.rows.fixed;
    const input_dim = plan.launch.input_dim.fixed;
    const output_dim = plan.launch.output_dim.fixed;
    const threads = plan.launch.threads_per_block;
    const task_threads = @as(u32, lowering.input_blocks) * 16;

    // Keep this helper in the renderer-owned body rather than the runtime
    // bundle's shared helper prefix: each candidate gets a unique name and the
    // codegen marker region can regenerate it without hand-maintained copies.
    const pack4_helper = try std.fmt.allocPrint(allocator, "{s}_q6_k_pack4", .{plan.kernel_id});
    defer allocator.free(pack4_helper);
    const dot16_helper = try std.fmt.allocPrint(allocator, "{s}_q6_k_q8_1_dot16_sub", .{plan.kernel_id});
    defer allocator.free(dot16_helper);

    try appendFmt(
        allocator,
        out,
        \\static __device__ __forceinline__ int {s}(
        \\    const unsigned char* ql,
        \\    const unsigned char* qh,
        \\    unsigned int nibble_shift,
        \\    unsigned int qh_shift,
        \\    unsigned int offset
        \\) {{
        \\    const unsigned int q0 = ((unsigned int)(ql[offset + 0u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 0u] >> qh_shift) & 0x03u) << 4u);
        \\    const unsigned int q1 = ((unsigned int)(ql[offset + 1u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 1u] >> qh_shift) & 0x03u) << 4u);
        \\    const unsigned int q2 = ((unsigned int)(ql[offset + 2u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 2u] >> qh_shift) & 0x03u) << 4u);
        \\    const unsigned int q3 = ((unsigned int)(ql[offset + 3u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 3u] >> qh_shift) & 0x03u) << 4u);
        \\    const unsigned int p0 = (q0 - 32u) & 0xffu;
        \\    const unsigned int p1 = (q1 - 32u) & 0xffu;
        \\    const unsigned int p2 = (q2 - 32u) & 0xffu;
        \\    const unsigned int p3 = (q3 - 32u) & 0xffu;
        \\    return (int)(p0 | (p1 << 8u) | (p2 << 16u) | (p3 << 24u));
        \\}}
        \\
        \\static __device__ __forceinline__ int {s}(
        \\    const unsigned char* block,
        \\    unsigned int sub,
        \\    int q8_pack0,
        \\    int q8_pack1,
        \\    int q8_pack2,
        \\    int q8_pack3
        \\) {{
        \\    const unsigned int half = sub >> 3u;
        \\    const unsigned int group = (sub & 7u) >> 1u;
        \\    const unsigned int l_base = (sub & 1u) * 16u;
        \\    const unsigned int ql_off = half * 64u + (group & 1u) * 32u;
        \\    const unsigned int qh_off = half * 32u;
        \\    const unsigned int qh_shift = group << 1u;
        \\    const unsigned int nibble_shift = (group >> 1u) << 2u;
        \\    const unsigned char* ql = block + ql_off + l_base;
        \\    const unsigned char* qh = block + 128u + qh_off + l_base;
        \\    int sumi = 0;
        \\    sumi = __dp4a({s}(ql, qh, nibble_shift, qh_shift, 0u), q8_pack0, sumi);
        \\    sumi = __dp4a({s}(ql, qh, nibble_shift, qh_shift, 4u), q8_pack1, sumi);
        \\    sumi = __dp4a({s}(ql, qh, nibble_shift, qh_shift, 8u), q8_pack2, sumi);
        \\    sumi = __dp4a({s}(ql, qh, nibble_shift, qh_shift, 12u), q8_pack3, sumi);
        \\    return sumi;
        \\}}
        \\
    ,
        .{ pack4_helper, dot16_helper, pack4_helper, pack4_helper, pack4_helper, pack4_helper },
    );

    try appendFmt(allocator, out, "extern \"C\" __global__ void {s}(\n", .{plan.kernel_id});
    try out.appendSlice(allocator,
        \\    float* partial_values,
        \\    unsigned int* partial_indices,
        \\    const unsigned char* q8_input,
        \\    const unsigned char* weight,
        \\    const int* suppress_token_ids,
        \\    unsigned int rows,
        \\    unsigned int in_dim,
        \\    unsigned int out_dim,
        \\    unsigned int suppress_count
        \\) {
        \\    (void)suppress_token_ids;
    );
    try appendFmt(
        allocator,
        out,
        "\n    const unsigned int cols = {d}u;\n" ++
            "    const unsigned int row_blocks = {d}u;\n" ++
            "    const unsigned int task_threads = {d}u;\n" ++
            "    if (rows != {d}u || in_dim != {d}u || out_dim != {d}u || suppress_count != 0u || blockDim.x != {d}u) return;\n",
        .{ lowering.columns_per_block, lowering.input_blocks, task_threads, rows, input_dim, output_dim, threads },
    );
    try out.appendSlice(allocator,
        \\
        \\    const unsigned int global_tile = blockIdx.x;
        \\    const unsigned int col_tile = global_tile * cols;
        \\    const unsigned int tid = threadIdx.x;
        \\    const unsigned int lane = tid & 31u;
        \\    const unsigned int warp = tid >> 5u;
    );
    try appendFmt(allocator, out, "\n    __shared__ float warp_partial[{d}][{d}];\n", .{ lowering.columns_per_block, lowering.warps_per_block });
    try appendFmt(allocator, out, "    float acc[{d}];\n", .{lowering.columns_per_block});
    try out.appendSlice(allocator,
        \\#pragma unroll
        \\    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;
        \\
        \\    if (tid < task_threads) {
        \\        const unsigned int block = tid >> 4u;
        \\        const unsigned int sub = tid & 15u;
        \\        const unsigned int q8_sub_block = sub >> 1u;
        \\        const unsigned int q8_lane_base = (sub & 1u) * 16u;
        \\        const unsigned char* q8_bp = q8_input + (block * 8u + q8_sub_block) * 36u;
        \\        const float q8_d = antfly_half_bits_to_float(((const unsigned short*)q8_bp)[0]);
        \\        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        \\        const int q8_pack0 = *(const int*)(q8_values + q8_lane_base + 0u);
        \\        const int q8_pack1 = *(const int*)(q8_values + q8_lane_base + 4u);
        \\        const int q8_pack2 = *(const int*)(q8_values + q8_lane_base + 8u);
        \\        const int q8_pack3 = *(const int*)(q8_values + q8_lane_base + 12u);
        \\#pragma unroll
        \\        for (unsigned int c = 0u; c < cols; ++c) {
        \\            const unsigned char* bp = weight + ((col_tile + c) * row_blocks + block) * 210u;
    );
    try appendFmt(
        allocator,
        out,
        "\n            const int sumi = {s}(bp, sub, q8_pack0, q8_pack1, q8_pack2, q8_pack3);\n",
        .{dot16_helper},
    );
    try out.appendSlice(allocator,
        \\            acc[c] = (q8_d * antfly_q6_k_sub_scale_f32(bp, sub)) * (float)sumi;
        \\        }
        \\    }
        \\
        \\#pragma unroll
        \\    for (unsigned int c = 0u; c < cols; ++c) {
        \\        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        \\        if (lane == 0u) warp_partial[c][warp] = sum;
        \\    }
        \\    __syncthreads();
        \\    if (tid != 0u) return;
        \\
        \\    float best_value = -3.402823466e+38f;
        \\    unsigned int best_index = 0xffffffffu;
        \\#pragma unroll
        \\    for (unsigned int c = 0u; c < cols; ++c) {
        \\        float value = 0.0f;
        \\#pragma unroll
    );
    try appendFmt(allocator, out, "\n        for (unsigned int w = 0u; w < {d}u; ++w) value += warp_partial[c][w];\n", .{lowering.warps_per_block});
    try out.appendSlice(allocator,
        \\        const unsigned int col = col_tile + c;
        \\        if (value > best_value || (value == best_value && col < best_index)) {
        \\            best_value = value;
        \\            best_index = col;
        \\        }
        \\    }
        \\    partial_values[global_tile] = best_value;
        \\    partial_indices[global_tile] = best_index;
        \\}
    );
}

test "cuda renderer exposes exact launch metadata for every kernel family" {
    const expected = [_]struct {
        kind: KernelKind,
        threads: u16,
        rows: u16,
        cols: u16,
        shared: u32,
    }{
        .{ .kind = .q4_k_small_batch_bias_gelu, .threads = 128, .rows = 1, .cols = 1, .shared = 512 },
        .{ .kind = .q4_k_mmv, .threads = 256, .rows = 1, .cols = 1, .shared = 1024 },
        .{ .kind = .q4_0_mmv, .threads = 256, .rows = 1, .cols = 4, .shared = 128 },
        .{ .kind = .q4_0_mm, .threads = 256, .rows = 8, .cols = 4, .shared = 1024 },
        .{ .kind = .q4_0_pair_mmv, .threads = 256, .rows = 1, .cols = 4, .shared = 256 },
        .{ .kind = .q4_0_pair_activation_q8_1, .threads = 640, .rows = 1, .cols = 32, .shared = 768 },
        .{ .kind = .q4_0_pair_activation_q8_1_e2b_6144, .threads = 384, .rows = 1, .cols = 32, .shared = 512 },
        .{ .kind = .q4_0_pair_activation_q8_1_e2b_12288, .threads = 384, .rows = 1, .cols = 32, .shared = 512 },
        .{ .kind = .q4_0_pair_activation_ggml_q8_1_e2b_6144, .threads = 384, .rows = 1, .cols = 32, .shared = 512 },
        .{ .kind = .q4_0_pair_activation_ggml_q8_1_e2b_12288, .threads = 384, .rows = 1, .cols = 32, .shared = 512 },
        .{ .kind = .q4_0_down_q8_1, .threads = 256, .rows = 1, .cols = 4, .shared = 128 },
        .{ .kind = .q4_0_down_q8_1_e2b_6144, .threads = 128, .rows = 1, .cols = 4, .shared = 64 },
        .{ .kind = .q4_0_down_q8_1_e2b_12288, .threads = 256, .rows = 1, .cols = 4, .shared = 128 },
        .{ .kind = .q4_0_down_ggml_q8_1_e2b_6144, .threads = 128, .rows = 1, .cols = 1, .shared = 16 },
        .{ .kind = .q4_0_down_ggml_q8_1_e2b_12288, .threads = 128, .rows = 1, .cols = 1, .shared = 16 },
        .{ .kind = .q4_0_q8_1_argmax_e2b_tile8, .threads = 96, .rows = 1, .cols = 8, .shared = 96 },
        .{ .kind = .q6_k_q8_1_argmax_k2560_tile8, .threads = 160, .rows = 1, .cols = 8, .shared = 160 },
        .{ .kind = .q6_k_q8_1_argmax_k3840_tile8, .threads = 256, .rows = 1, .cols = 8, .shared = 256 },
    };
    for (expected) |item| {
        const plan = planFor(item.kind);
        try plan.validate();
        try std.testing.expectEqual(item.threads, plan.launch.threads_per_block);
        try std.testing.expectEqual(item.rows, plan.launch.output_rows_per_block);
        try std.testing.expectEqual(item.cols, plan.launch.output_cols_per_block);
        try std.testing.expectEqual(item.shared, plan.launch.static_shared_memory_bytes);
    }
}

test "cuda renderer keeps legacy zero-point Q8 and llama CUDA Q8_1 contracts separate" {
    const allocator = std.testing.allocator;
    const legacy_pair = planFor(.q4_0_pair_activation_q8_1_e2b_6144);
    const ggml_pair = planFor(.q4_0_pair_activation_ggml_q8_1_e2b_6144);
    const ggml_down = planFor(.q4_0_down_ggml_q8_1_e2b_6144);
    try legacy_pair.validate();
    try ggml_pair.validate();
    try ggml_down.validate();
    try std.testing.expectEqual(
        QuantDecodePrimitive.q4_0_q8_zp_dp4a,
        legacy_pair.lowering.q4_0_pair_activation_q8_1.decode,
    );
    try std.testing.expectEqual(
        QuantDecodePrimitive.q4_0_q8_1_dp4a,
        ggml_pair.lowering.q4_0_pair_activation_q8_1.decode,
    );
    try std.testing.expectEqual(@as(u16, 128), ggml_down.launch.threads_per_block);
    try std.testing.expectEqual(@as(u16, 1), ggml_down.launch.output_cols_per_block);

    const pair_body = try renderBodyAlloc(allocator, ggml_pair);
    defer allocator.free(pair_body);
    try std.testing.expect(std.mem.containsAtLeast(u8, pair_body, 1, "antfly_q4_0_ggml_q8_1_dot16"));
    try std.testing.expect(std.mem.containsAtLeast(u8, pair_body, 1, "const float q8_sum"));
    try std.testing.expect(std.mem.containsAtLeast(u8, pair_body, 1, "bp[2] = (unsigned char)(sum_bits & 0xffu)"));

    const full_source = try renderKernel(allocator, ggml_down);
    defer allocator.free(full_source);
    try std.testing.expect(std.mem.containsAtLeast(u8, full_source, 1, "- 4.0f * q8_sum"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, full_source, 1, "__vadd4"));

    const quantizer = try renderGgmlQ8_1QuantizeRowsBodyAlloc(allocator);
    defer allocator.free(quantizer);
    try std.testing.expect(std.mem.containsAtLeast(u8, quantizer, 1, "const float sum = antfly_warp_reduce_sum_f32(x)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, quantizer, 1, "bp[2] = (unsigned char)(sum_bits & 0xffu)"));
}

test "cuda renderer emits model-neutral Q4_K decode MMV" {
    const plan = planFor(.q4_k_mmv);
    try plan.validate();
    const source = try renderKernel(std.testing.allocator, plan);
    defer std.testing.allocator.free(source);
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "extern \"C\" __global__ void antfly_q4_k_mmv_f32_v1("));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_q4_k_dequant_lane(block, tid)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "rows != 1u"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "#include <math.h>"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "antfly_gelu"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "gemma"));
}

test "cuda renderer parameterizes the common q4_0 projection lowering" {
    const allocator = std.testing.allocator;
    const mmv = try renderBodyAlloc(allocator, planFor(.q4_0_mmv));
    defer allocator.free(mmv);
    const mm = try renderBodyAlloc(allocator, planFor(.q4_0_mm));
    defer allocator.free(mm);
    const pair = try renderBodyAlloc(allocator, planFor(.q4_0_pair_mmv));
    defer allocator.free(pair);

    try std.testing.expect(std.mem.containsAtLeast(u8, mmv, 1, "float acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};"));
    try std.testing.expect(std.mem.containsAtLeast(u8, mm, 1, "float acc[4][8];"));
    try std.testing.expect(std.mem.containsAtLeast(u8, mm, 1, "const int row0 = blockIdx.y << 3;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, pair, 1, "float acc[2][4];"));
    try std.testing.expect(std.mem.containsAtLeast(u8, pair, 1, "const uint8_t *weight = w == 0 ? weight_a_q4_0 : weight_b_q4_0;"));
}

test "cuda renderer rejects launch and lowering drift" {
    var bad_launch = planFor(.q4_0_pair_activation_q8_1);
    bad_launch.launch.threads_per_block = 256;
    try std.testing.expectError(error.PairActivationQ8TopologyDoesNotMatchLowering, bad_launch.validate());

    var bad_lowering = planFor(.q4_0_mmv);
    bad_lowering.lowering.q4_0_projection.row_tile = 8;
    try std.testing.expectError(error.CudaLoweringDoesNotMatchKind, bad_lowering.validate());
}

test "cuda renderer owns exact E2B Q8 FFN shape contracts" {
    const pair_6144 = planFor(.q4_0_pair_activation_q8_1_e2b_6144);
    const pair_12288 = planFor(.q4_0_pair_activation_q8_1_e2b_12288);
    const down_6144 = planFor(.q4_0_down_q8_1_e2b_6144);
    const down_12288 = planFor(.q4_0_down_q8_1_e2b_12288);
    try pair_6144.validate();
    try pair_12288.validate();
    try down_6144.validate();
    try down_12288.validate();

    try std.testing.expectEqual(@as(u32, 1536), pair_6144.launch.input_dim.fixed);
    try std.testing.expectEqual(@as(u32, 6144), pair_6144.launch.output_dim.fixed);
    try std.testing.expectEqual(@as(u16, 48), pair_6144.lowering.q4_0_pair_activation_q8_1.input_blocks);
    try std.testing.expectEqual(@as(u16, 192), pair_6144.lowering.q4_0_pair_activation_q8_1.output_blocks);
    try std.testing.expectEqual(@as(u8, 3), pair_6144.lowering.q4_0_pair_activation_q8_1.warps_per_projection_group);

    try std.testing.expectEqual(@as(u32, 1536), pair_12288.launch.input_dim.fixed);
    try std.testing.expectEqual(@as(u32, 12288), pair_12288.launch.output_dim.fixed);
    try std.testing.expectEqual(@as(u16, 384), pair_12288.lowering.q4_0_pair_activation_q8_1.output_blocks);

    try std.testing.expectEqual(@as(u32, 6144), down_6144.launch.input_dim.fixed);
    try std.testing.expectEqual(@as(u32, 1536), down_6144.launch.output_dim.fixed);
    try std.testing.expectEqual(@as(u16, 192), down_6144.lowering.q4_0_down_q8_1.input_blocks);
    try std.testing.expectEqual(@as(u16, 128), down_6144.launch.threads_per_block);

    try std.testing.expectEqual(@as(u32, 12288), down_12288.launch.input_dim.fixed);
    try std.testing.expectEqual(@as(u32, 1536), down_12288.launch.output_dim.fixed);
    try std.testing.expectEqual(@as(u16, 384), down_12288.lowering.q4_0_down_q8_1.input_blocks);
    try std.testing.expectEqual(@as(u16, 256), down_12288.launch.threads_per_block);

    var bad_pair = pair_6144;
    bad_pair.launch.output_dim.fixed = 12288;
    try std.testing.expectError(error.PairActivationQ8ShapeDoesNotMatchLowering, bad_pair.validate());
    var bad_down = down_6144;
    bad_down.launch.input_dim.fixed = 12288;
    try std.testing.expectError(error.DownQ8ShapeDoesNotMatchLowering, bad_down.validate());
}

test "cuda renderer parameterizes Q8 FFN bodies without changing E4B goldens" {
    const allocator = std.testing.allocator;
    const e4b_pair = try renderBodyAlloc(allocator, planFor(.q4_0_pair_activation_q8_1));
    defer allocator.free(e4b_pair);
    const e4b_down = try renderBodyAlloc(allocator, planFor(.q4_0_down_q8_1));
    defer allocator.free(e4b_down);
    try std.testing.expectEqualStrings(body_q4_0_pair_activation_q8_1.source, e4b_pair);
    try std.testing.expectEqualStrings(body_q4_0_down_q8_1.source, e4b_down);

    const pair = try renderBodyAlloc(allocator, planFor(.q4_0_pair_activation_q8_1_e2b_6144));
    defer allocator.free(pair);
    try std.testing.expect(std.mem.containsAtLeast(u8, pair, 1, "void antfly_q4_0_pair_activation_q8_1_e2b_6144_mmv_v1("));
    try std.testing.expect(std.mem.containsAtLeast(u8, pair, 1, "blockDim.x != 384u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, pair, 1, "gate_partial[4][4][3]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, pair, 1, "block += 48u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, pair, 1, "w < 3u"));

    const down_small = try renderBodyAlloc(allocator, planFor(.q4_0_down_q8_1_e2b_6144));
    defer allocator.free(down_small);
    try std.testing.expect(std.mem.containsAtLeast(u8, down_small, 1, "blockDim.x != 128u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, down_small, 1, "warp_partial[4][4]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, down_small, 1, "block += 64u"));

    const down_large = try renderBodyAlloc(allocator, planFor(.q4_0_down_q8_1_e2b_12288));
    defer allocator.free(down_large);
    try std.testing.expect(std.mem.containsAtLeast(u8, down_large, 1, "blockDim.x != 256u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, down_large, 1, "warp_partial[4][8]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, down_large, 1, "block += 128u"));
}

test "cuda renderer owns the E2B Q4_0 Q8_1 argmax contract" {
    const plan = planFor(.q4_0_q8_1_argmax_e2b_tile8);
    try plan.validate();
    try std.testing.expectEqual(@as(u32, 1536), plan.launch.input_dim.fixed);
    try std.testing.expectEqual(@as(u32, 262144), plan.launch.output_dim.fixed);
    try std.testing.expectEqual(@as(u16, 48), plan.lowering.q4_0_q8_1_argmax.input_blocks);
    try std.testing.expectEqual(@as(u8, 8), plan.lowering.q4_0_q8_1_argmax.columns_per_block);
    try std.testing.expectEqual(@as(u8, 3), plan.lowering.q4_0_q8_1_argmax.warps_per_block);

    const body = try renderBodyAlloc(std.testing.allocator, plan);
    defer std.testing.allocator.free(body);
    try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, "in_dim != 1536u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, "out_dim != 262144u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, "__shared__ float warp_partial[8][3]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, "value == best_value && col < best_index"));

    var bad_shape = plan;
    bad_shape.launch.input_dim.fixed = 2560;
    try std.testing.expectError(error.Q4Q8ArgmaxShapeDoesNotMatchLowering, bad_shape.validate());
    var bad_topology = plan;
    bad_topology.launch.threads_per_block = 128;
    try std.testing.expectError(error.Q4Q8ArgmaxTopologyDoesNotMatchLowering, bad_topology.validate());
}

test "cuda renderer owns the K2560 Q6_K Q8_1 argmax candidate contract" {
    const plan = planFor(.q6_k_q8_1_argmax_k2560_tile8);
    try plan.validate();
    try std.testing.expectEqual(@as(u32, 2560), plan.launch.input_dim.fixed);
    try std.testing.expectEqual(@as(u32, 262144), plan.launch.output_dim.fixed);
    try std.testing.expectEqual(@as(u16, 10), plan.lowering.q6_k_q8_1_argmax.input_blocks);
    try std.testing.expectEqual(@as(u8, 8), plan.lowering.q6_k_q8_1_argmax.columns_per_block);
    try std.testing.expectEqual(@as(u8, 5), plan.lowering.q6_k_q8_1_argmax.warps_per_block);
    try std.testing.expect(!plan.production_enabled);

    const source = try renderKernel(std.testing.allocator, plan);
    defer std.testing.allocator.free(source);
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "void antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1("));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "block + 192u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "block[208]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "q8_bp + 4u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_pack4"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_q8_1_dot16_sub"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const unsigned char* ql = block + ql_off + l_base"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const unsigned char* qh = block + 128u + qh_off + l_base"));
    try std.testing.expectEqual(@as(usize, 4), std.mem.count(u8, source, "sumi = __dp4a("));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "sumi = __dp4a(antfly_pack_q6_k_i8x4_sub"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "__shared__ float warp_partial[8][5]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const unsigned int task_threads = 160u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "if (tid < task_threads)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "suppress_count != 0u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "(void)suppress_token_ids;\n    const unsigned int cols"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const unsigned int warp = tid >> 5u;\n    __shared__ float"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "#pragma unroll\n        for (unsigned int w"));

    var bad_shape = plan;
    bad_shape.launch.input_dim.fixed = 3840;
    try std.testing.expectError(error.Q6KQ8ArgmaxShapeDoesNotMatchLowering, bad_shape.validate());
    var bad_topology = plan;
    bad_topology.launch.threads_per_block = 192;
    try std.testing.expectError(error.Q6KQ8ArgmaxTopologyDoesNotMatchLowering, bad_topology.validate());
}

test "cuda renderer keeps inactive K3840 argmax lanes in bounds and zeroed" {
    const plan = planFor(.q6_k_q8_1_argmax_k3840_tile8);
    try plan.validate();
    try std.testing.expectEqual(@as(u32, 3840), plan.launch.input_dim.fixed);
    try std.testing.expectEqual(@as(u32, 262144), plan.launch.output_dim.fixed);
    try std.testing.expectEqual(@as(u16, 15), plan.lowering.q6_k_q8_1_argmax.input_blocks);
    try std.testing.expectEqual(@as(u8, 8), plan.lowering.q6_k_q8_1_argmax.warps_per_block);
    try std.testing.expectEqual(@as(u16, 256), plan.launch.threads_per_block);
    try std.testing.expectEqual(@as(u32, 256), plan.launch.static_shared_memory_bytes);
    try std.testing.expect(!plan.production_enabled);

    const source = try renderKernel(std.testing.allocator, plan);
    defer std.testing.allocator.free(source);
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "void antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1("));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const unsigned int task_threads = 240u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "__shared__ float warp_partial[8][8]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1_q6_k_pack4"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1_q6_k_q8_1_dot16_sub"));
    try std.testing.expectEqual(@as(usize, 4), std.mem.count(u8, source, "sumi = __dp4a("));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "sumi = __dp4a(antfly_pack_q6_k_i8x4_sub"));
    const inactive_guard = std.mem.indexOf(u8, source, "if (tid < task_threads)") orelse return error.MissingInactiveLaneGuard;
    const q8_read = std.mem.indexOf(u8, source, "const unsigned char* q8_bp") orelse return error.MissingQ8Read;
    const weight_read = std.mem.indexOf(u8, source, "const unsigned char* bp = weight") orelse return error.MissingWeightRead;
    try std.testing.expect(inactive_guard < q8_read);
    try std.testing.expect(inactive_guard < weight_read);

    var too_few_threads = plan;
    too_few_threads.launch.threads_per_block = 224;
    try std.testing.expectError(error.Q6KQ8ArgmaxTopologyDoesNotMatchLowering, too_few_threads.validate());

    var more_than_one_inactive_warp = plan;
    more_than_one_inactive_warp.launch.input_dim.fixed = 3328;
    more_than_one_inactive_warp.lowering.q6_k_q8_1_argmax.input_blocks = 13;
    try std.testing.expectError(error.Q6KQ8ArgmaxTopologyDoesNotMatchLowering, more_than_one_inactive_warp.validate());
}

test "cuda renderer emits each helper once" {
    const allocator = std.testing.allocator;
    const source = try renderKernel(allocator, planFor(.q4_0_pair_activation_q8_1));
    defer allocator.free(source);
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, "float antfly_warp_reduce_sum_f32("));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, "float antfly_q4_0_q8_dot16("));
}

test "cuda renderer emits typed two-stage split-KV attention" {
    const kinds = [_]AttentionKernelKind{
        .gqa_decode_split_kv_hd256_f32,
        .gqa_decode_split_kv_hd512_f32,
        .gqa_decode_split2_kv_hd256_f32,
        .gqa_decode_split2_kv_hd512_f32,
        .gqa_decode_split4_kv_hd256_f32,
        .gqa_decode_split4_kv_hd512_f32,
    };
    for (kinds) |kind| {
        const plan = attentionPlanFor(kind);
        try plan.validate();
        try std.testing.expectEqual(GridMapping.batch_query_heads_by_splits, plan.launch.grid);
        try std.testing.expectEqual(GridMapping.batch_query_heads, plan.reduction_launch.grid);
        try std.testing.expect(plan.tiled64_launch == null);
        try std.testing.expectEqual(GridMapping.batch_query_heads, plan.serial_launch.grid);
        const expected_threads: u16 = if (plan.lowering.head_dim == 256) 256 else 512;
        try std.testing.expectEqual(expected_threads, plan.launch.threads_per_block);
        try std.testing.expectEqual(expected_threads, plan.reduction_launch.threads_per_block);
        try std.testing.expectEqual(expected_threads, plan.serial_launch.threads_per_block);
        try std.testing.expectEqual(@as(u32, 20 * @sizeOf(f32)), plan.serial_launch.static_shared_memory_bytes);
        try std.testing.expectEqual(@as(u32, if (plan.lowering.head_dim == 256) 384 else 640), plan.launch.static_shared_memory_bytes);
        try std.testing.expectEqual(
            @as(u32, 1 + 2 * @as(u32, plan.lowering.kv_splits)) * @sizeOf(f32),
            plan.reduction_launch.static_shared_memory_bytes,
        );
        try std.testing.expectEqual(plan.lowering.split_variant.kvSplits(), plan.lowering.kv_splits);
        try std.testing.expectEqual(generated_attention_query_heads_per_kv_head, plan.lowering.query_heads_per_kv_head);
        try std.testing.expectEqual(AttentionStorage.f32, plan.lowering.query_storage);
        try std.testing.expect(plan.lowering.device_decode_scalars);

        const source = try renderAttentionKernel(std.testing.allocator, plan);
        defer std.testing.allocator.free(source);
        const source_id_header = try std.fmt.allocPrint(std.testing.allocator, "// source_id={s}", .{plan.source_id});
        defer std.testing.allocator.free(source_id_header);
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, source_id_header));
        const split_text = try std.fmt.allocPrint(std.testing.allocator, "split{d}", .{plan.lowering.kv_splits});
        defer std.testing.allocator.free(split_text);
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, split_text));
        const serial = try std.fmt.allocPrint(std.testing.allocator, "extern \"C\" __global__ void {s}(", .{plan.serial_kernel_id});
        defer std.testing.allocator.free(serial);
        const stage1 = try std.fmt.allocPrint(std.testing.allocator, "extern \"C\" __global__ void {s}(", .{plan.kernel_id});
        defer std.testing.allocator.free(stage1);
        const stage2 = try std.fmt.allocPrint(std.testing.allocator, "extern \"C\" __global__ void {s}(", .{plan.reduction_kernel_id});
        defer std.testing.allocator.free(stage2);
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, serial));
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, stage1));
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, stage2));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "(num_heads % num_kv_heads) != 0u"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const unsigned int head_block = blockIdx.x / splits"));
        try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, source, "rsqrt.approx.f32"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "kv_seq_len = decode_scalars[2]"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const float next_max = fmaxf(merged_max, partial_max[partial]);"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "merge_alpha[split] = alpha;"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "merge_beta[split] = beta;"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "numerator = numerator * merge_alpha[split] + partial_values"));
        try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "merge_weight"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "block_dot += __shfl_down_sync"));
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, "__device__ __forceinline__ float termite_warp_reduce_sum_f32("));
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, "__device__ __forceinline__ float termite_block_reduce_sum_f32("));
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, "const float dot = termite_block_reduce_sum_f32(partial, warp_sums);"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "for (unsigned int ki = key_start; ki < key_end; ++ki)"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "key_end = visible < kv_seq_len ? visible : kv_seq_len;"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "unsigned int split_begin = (kv_seq_len * split) / splits;"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "unsigned int split_end = (kv_seq_len * (split + 1u)) / splits;"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "if (split_begin < key_start) split_begin = key_start;"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "if (split_end > key_end) split_end = key_end;"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "if (split_begin > split_end) split_begin = split_end;"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "for (unsigned int ki = split_begin; ki < split_end; ++ki)"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "shared_alpha = expf(shared_max_score - next_max);"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "acc = acc * shared_alpha + shared_beta * v[v_idx];"));
        try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "reduce[lane] = partial;"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "mask_len != 0u || bias_mode != 0u"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "production_baseline=termite_gqa_attention_decode_scalars_fast_f32"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "if (kv_seq_len >= split_kv_min_tokens) return;"));
        try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, source, "if (kv_seq_len < split_kv_min_tokens) return;"));

        const runtime_body = try renderAttentionBodyAlloc(std.testing.allocator, plan);
        defer std.testing.allocator.free(runtime_body);
        try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, runtime_body, "__device__ __forceinline__ float termite_block_reduce_sum_f32("));
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, runtime_body, "const float dot = termite_block_reduce_sum_f32(partial, warp_sums);"));
    }

    const hd256_id = try attentionPlanId(attentionPlanFor(.gqa_decode_split_kv_hd256_f32), std.testing.allocator);
    defer std.testing.allocator.free(hd256_id);
    const hd512_id = try attentionPlanId(attentionPlanFor(.gqa_decode_split_kv_hd512_f32), std.testing.allocator);
    defer std.testing.allocator.free(hd512_id);
    try std.testing.expect(!std.mem.eql(u8, hd256_id, hd512_id));

    const split2_id = try attentionPlanId(attentionPlanFor(.gqa_decode_split2_kv_hd256_f32), std.testing.allocator);
    defer std.testing.allocator.free(split2_id);
    const split4_id = try attentionPlanId(attentionPlanFor(.gqa_decode_split4_kv_hd256_f32), std.testing.allocator);
    defer std.testing.allocator.free(split4_id);
    try std.testing.expect(!std.mem.eql(u8, split2_id, split4_id));
    try std.testing.expect(!std.mem.eql(u8, split4_id, hd256_id));

    // The legacy split-8 entry points remain byte-for-byte named as before,
    // while their source identity now states the schedule explicitly.
    const legacy = attentionPlanFor(.gqa_decode_split_kv_hd256_f32);
    try std.testing.expectEqualStrings("antfly_gqa_attention_decode_split_kv_hd256_f32_stage1_v1", legacy.kernel_id);
    try std.testing.expectEqualStrings("antfly_gqa_attention_decode_split8_kv_hd256_f32_v1", legacy.source_id);

    const workspace = generatedAttentionWorkspaceLayout();
    try std.testing.expectEqual(@as(usize, 0), workspace.partial_values_offset);
    try std.testing.expect(workspace.partial_max_offset > workspace.partial_values_offset);
    try std.testing.expect(workspace.partial_denom_offset > workspace.partial_max_offset);
    try std.testing.expectEqual(@as(usize, 526_336), workspace.total_bytes);
    const split2_workspace = generatedAttentionWorkspaceLayoutFor(2) orelse return error.MissingSplit2Workspace;
    const split4_workspace = generatedAttentionWorkspaceLayoutFor(4) orelse return error.MissingSplit4Workspace;
    try std.testing.expectEqual(@as(usize, 131_584), split2_workspace.total_bytes);
    try std.testing.expectEqual(@as(usize, 263_168), split4_workspace.total_bytes);
    try std.testing.expect(generatedAttentionWorkspaceLayoutFor(3) == null);
}

test "cuda renderer emits paged score prework with chronological softmax" {
    const kinds = [_]AttentionKernelKind{
        .gqa_decode_score_prework_hd256_f32,
        .gqa_decode_score_prework_hd512_f32,
    };
    for (kinds) |kind| {
        const plan = attentionPlanFor(kind);
        try plan.validate();
        try std.testing.expectEqual(AttentionReduction.parallel_scores_then_serial, plan.lowering.reduction);
        try std.testing.expectEqual(AttentionSoftmax.online_chronological, plan.lowering.softmax);
        try std.testing.expectEqual(AttentionSplitVariant.score_prework, plan.lowering.split_variant);
        try std.testing.expectEqual(generated_attention_score_prework_chunks, plan.lowering.kv_splits);
        try std.testing.expectEqual(GridMapping.batch_query_heads_by_splits, plan.launch.grid);
        try std.testing.expectEqual(GridMapping.batch_query_heads, plan.reduction_launch.grid);
        const tiled64_launch = plan.tiled64_launch orelse return error.MissingTiled64ConsumerPlan;
        try std.testing.expectEqual(GridMapping.batch_query_heads_by_output_tiles, tiled64_launch.grid);
        try std.testing.expectEqual(generated_attention_score_prework_tiled64_tile_size, tiled64_launch.threads_per_block);
        try std.testing.expectEqual(generated_attention_score_prework_tiled64_tile_size, tiled64_launch.output_cols_per_block);
        try std.testing.expectEqual(
            generatedAttentionScorePreworkTiled64SharedBytes(plan.lowering.head_dim).?,
            tiled64_launch.static_shared_memory_bytes,
        );
        try std.testing.expectEqual(@as(u16, 0), plan.lowering.split_kv_min_tokens_default);
        try std.testing.expectEqual(generated_attention_score_prework_max_kv_tokens, plan.lowering.max_kv_tokens);
        try std.testing.expectEqual(AttentionStorage.paged_f16_or_polar4, plan.lowering.key_storage);
        try std.testing.expectEqual(AttentionStorage.paged_f16_or_f32, plan.lowering.value_storage);
        try std.testing.expectEqual(
            generated_attention_score_prework_recurrence_shared_bytes,
            plan.serial_launch.static_shared_memory_bytes,
        );
        try std.testing.expectEqual(plan.serial_launch.static_shared_memory_bytes, plan.reduction_launch.static_shared_memory_bytes);
        try std.testing.expectEqualStrings("termite_gqa_attention_decode_turboquant_fast_f32", plan.production_baseline);
        // Promoted for the qualified SM89 automatic selector; see the
        // score-prework promotion accounting in quant_kernel_compiler.zig.
        try std.testing.expect(plan.production_enabled);

        const source = try renderAttentionKernel(std.testing.allocator, plan);
        defer std.testing.allocator.free(source);
        const stage1 = try std.fmt.allocPrint(std.testing.allocator, "extern \"C\" __global__ void {s}(", .{plan.kernel_id});
        defer std.testing.allocator.free(stage1);
        const stage2 = try std.fmt.allocPrint(std.testing.allocator, "extern \"C\" __global__ void {s}(", .{plan.reduction_kernel_id});
        defer std.testing.allocator.free(stage2);
        const tiled64 = try std.fmt.allocPrint(std.testing.allocator, "extern \"C\" __global__ void {s}(", .{plan.tiled64_kernel_id.?});
        defer std.testing.allocator.free(tiled64);
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, stage1));
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, stage2));
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, tiled64));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 2, "const unsigned int* block_table"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 2, "termite_tq_physical_token("));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "unsigned long long physical = (unsigned long long)logical_token;"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "physical_block * (unsigned long long)page_size_tokens"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "physical > 0xffffffffull"));
        try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "physical = block_table[block_index] * page_size_tokens"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "format != 0u && format != 2u"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "value_format != 0u && value_format != 2u"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "termite_tq_f16_value(v_row, value_index)"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 2, "(ki - key_start)"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "visible_count > score_capacity"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "chunk_count != 128u"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "for (unsigned int ki = chunk_begin; ki < chunk_end; ++ki)"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "for (unsigned int ki = key_start; ki < key_end; ++ki)"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "__shared__ float shared_alpha[4096];"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "__shared__ float shared_beta[4096];"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "shared_denom = shared_denom * shared_alpha[recurrence_index] + shared_beta[recurrence_index];"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "acc *= shared_alpha[recurrence_index];"));
        try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, source, "shared_denom = shared_denom * shared_alpha[recurrence_index] + shared_beta[recurrence_index];"));
        try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, source, "acc *= shared_alpha[recurrence_index];"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const unsigned int output_tile = blockIdx.y;"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const unsigned int lane = output_tile * 64u + tile_lane;"));
        try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "shared_alpha = "));
        try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "partial_max"));
        try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "partial_denom"));
    }

    try std.testing.expectEqual(@as(?u16, 512), generatedAttentionScorePreworkTiled64MaxKvTokens(256));
    try std.testing.expectEqual(@as(?u16, 4096), generatedAttentionScorePreworkTiled64MaxKvTokens(512));
    try std.testing.expectEqual(@as(?u32, 4104), generatedAttentionScorePreworkTiled64SharedBytes(256));
    try std.testing.expectEqual(@as(?u32, 32776), generatedAttentionScorePreworkTiled64SharedBytes(512));

    const workspace = generatedAttentionWorkspaceLayoutFor(generated_attention_score_prework_chunks) orelse return error.MissingScorePreworkWorkspace;
    try std.testing.expectEqual(@as(usize, 0), workspace.partial_values_offset);
    try std.testing.expectEqual(@as(usize, 524_288), workspace.partial_max_offset);
    try std.testing.expectEqual(workspace.partial_max_offset, workspace.partial_denom_offset);
    try std.testing.expectEqual(@as(usize, 524_288), workspace.total_bytes);
}

test "cuda renderer rejects attention launch drift" {
    var plan = attentionPlanFor(.gqa_decode_split_kv_hd256_f32);
    plan.launch.threads_per_block = 128;
    try std.testing.expectError(error.CudaLaunchDoesNotMatchKind, plan.validate());

    var reduction = attentionPlanFor(.gqa_decode_split_kv_hd256_f32);
    reduction.reduction_launch.threads_per_block = 128;
    try std.testing.expectError(error.CudaLaunchDoesNotMatchKind, reduction.validate());

    var serial = attentionPlanFor(.gqa_decode_split_kv_hd256_f32);
    serial.serial_launch.threads_per_block = 128;
    try std.testing.expectError(error.CudaLaunchDoesNotMatchKind, serial.validate());

    var storage = attentionPlanFor(.gqa_decode_split_kv_hd256_f32);
    storage.lowering.key_storage = .f16;
    try std.testing.expectError(error.InvalidCudaAttentionLowering, storage.validate());

    var split_count = attentionPlanFor(.gqa_decode_split2_kv_hd256_f32);
    split_count.lowering.kv_splits = 4;
    try std.testing.expectError(error.InvalidCudaAttentionLowering, split_count.validate());

    var source_id = attentionPlanFor(.gqa_decode_split2_kv_hd256_f32);
    source_id.source_id = "antfly_gqa_attention_decode_kv_hd256_f32_v1";
    try std.testing.expectError(error.CudaAttentionSourceIdDoesNotMatchSplitCount, source_id.validate());
}

test "cuda renderer owns promoted SM89 Flash prefill contracts" {
    const hd256 = flashPrefillPlanFor(.gqa_prefill_flash_sm89_hd256_swa512_f32);
    const hd512 = flashPrefillPlanFor(.gqa_prefill_flash_sm89_hd512_global_f32);
    try hd256.validate();
    try hd512.validate();

    try std.testing.expectEqualStrings(generated_flash_prefill_hd256_kernel_id, hd256.kernel_id);
    try std.testing.expectEqualStrings(generated_flash_prefill_hd512_kernel_id, hd512.kernel_id);
    try std.testing.expectEqual(@as(u16, 512), hd256.lowering.sliding_window);
    try std.testing.expectEqual(@as(u16, 0), hd512.lowering.sliding_window);
    try std.testing.expectEqual(GridMapping.query_heads_by_query_tiles, hd256.launch.grid);
    try std.testing.expectEqual(@as(u16, 256), hd256.launch.threads_per_block);
    try std.testing.expectEqual(@as(u16, 16), hd256.launch.output_rows_per_block);
    try std.testing.expectEqual(@as(?u32, 26_564), generatedFlashPrefillDynamicSharedBytes(256));
    try std.testing.expectEqual(@as(?u32, 42_948), generatedFlashPrefillDynamicSharedBytes(512));
    try std.testing.expectEqual(@as(?u32, null), generatedFlashPrefillDynamicSharedBytes(128));
    try std.testing.expect(hd256.production_enabled);
    try std.testing.expect(hd256.runtime_default_enabled);
    try std.testing.expect(hd256.exact_token_parity_required_for_default);
    try std.testing.expect(hd256.exact_token_parity_qualified);
    try std.testing.expect(hd512.production_enabled);
    try std.testing.expect(hd512.runtime_default_enabled);

    const policy = FlashPrefillQueryLengthPolicy.gemma4_q512_or_q3_v1;
    inline for ([_]usize{ 0, 512, 1024, 1536 }) |offset| {
        try std.testing.expect(policy.accepts(512, offset));
    }
    try std.testing.expect(policy.accepts(3, 2048));
    try std.testing.expect(!policy.accepts(1, 2050));
    try std.testing.expect(!policy.accepts(512, 1));
    try std.testing.expect(!policy.accepts(3, 1536));

    // A runtime default without exact-token-parity qualification must remain
    // unrepresentable even now that the promoted plans carry the evidence.
    var unsafe_default = hd256;
    unsafe_default.exact_token_parity_qualified = false;
    try std.testing.expectError(
        error.CudaFlashPrefillDefaultRequiresExactTokenParity,
        unsafe_default.validate(),
    );
    var unqualified_production = hd256;
    unqualified_production.exact_token_parity_required_for_default = false;
    try std.testing.expectError(
        error.CudaFlashPrefillDefaultRequiresExactTokenParity,
        unqualified_production.validate(),
    );
}

test "cuda renderer emits the production Flash prefill ABI from its owned template" {
    const source = try renderFlashPrefillKernel(
        std.testing.allocator,
        flashPrefillPlanFor(.gqa_prefill_flash_sm89_hd256_swa512_f32),
    );
    defer std.testing.allocator.free(source);

    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, generated_flash_prefill_hd256_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "#define ANTFLY_FLASH_HEAD_DIM 256"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "#define ANTFLY_FLASH_SLIDING_WINDOW 512"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const float* q"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "__float2half_rn(q["));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "wmma::mma_sync("));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 4, "index += kThreads"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "index += blockDim.x"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "// Production generated Flash prefill"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "runtime_default_enabled=true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "exact_token_parity_qualified=true"));
    try std.testing.expect(std.mem.containsAtLeast(
        u8,
        source,
        1,
        "rollback: ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=off",
    ));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "_prototype"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "prefill_reference_f16"));
}

test "cuda renderer owns default-off SM89 split-K online decode contracts" {
    const hd256 = splitkOnlineDecodePlanFor(.gqa_decode_splitk_online_sm89_hd256_swa512_f16_f32);
    const hd512 = splitkOnlineDecodePlanFor(.gqa_decode_splitk_online_sm89_hd512_global_f16_f32);
    try hd256.validate();
    try hd512.validate();

    try std.testing.expectEqualStrings(generated_splitk_online_decode_hd256_kernel_id, hd256.kernel_id);
    try std.testing.expectEqualStrings(generated_splitk_online_decode_hd512_kernel_id, hd512.kernel_id);
    try std.testing.expectEqual(@as(u16, 512), hd256.lowering.sliding_window);
    try std.testing.expectEqual(@as(u16, 0), hd512.lowering.sliding_window);
    try std.testing.expectEqual(@as(u16, 512), hd256.lowering.max_visible_tokens);
    try std.testing.expectEqual(@as(u16, 4096), hd512.lowering.max_visible_tokens);
    try std.testing.expectEqual(GridMapping.batch_query_heads_by_splits, hd256.launch.grid);
    try std.testing.expectEqual(@as(u16, 128), hd256.launch.threads_per_block);
    try std.testing.expectEqual(
        generated_splitk_online_decode_static_shared_memory_bytes,
        hd256.launch.static_shared_memory_bytes,
    );
    try std.testing.expectEqual(
        generated_splitk_online_decode_static_shared_memory_bytes,
        hd512.launch.static_shared_memory_bytes,
    );
    try std.testing.expectEqual(@as(u16, 64), hd256.lowering.kv_splits);
    try std.testing.expect(!hd256.production_enabled);
    try std.testing.expect(!hd256.runtime_default_enabled);
    try std.testing.expect(hd256.exact_token_parity_required_for_default);
    try std.testing.expect(!hd256.exact_token_parity_qualified);

    const workspace = generatedSplitkOnlineDecodeWorkspaceLayout();
    try std.testing.expectEqual(@as(usize, 0), workspace.completion_counters_offset);
    try std.testing.expectEqual(@as(usize, 256), workspace.partial_values_offset);
    try std.testing.expectEqual(@as(usize, 1_048_832), workspace.partial_max_offset);
    try std.testing.expectEqual(@as(usize, 1_050_880), workspace.partial_denom_offset);
    try std.testing.expectEqual(@as(usize, 1_052_928), workspace.total_bytes);
    try std.testing.expectEqual(@as(usize, 0), workspace.partial_values_offset % 256);
    try std.testing.expectEqual(@as(usize, 0), workspace.partial_max_offset % 256);
    try std.testing.expectEqual(@as(usize, 0), workspace.partial_denom_offset % 256);
    try std.testing.expectEqual(@as(usize, 0), workspace.total_bytes % 256);

    var unsafe_default = hd256;
    unsafe_default.production_enabled = true;
    unsafe_default.runtime_default_enabled = true;
    try std.testing.expectError(
        error.CudaSplitkOnlineDecodeDefaultRequiresExactTokenParity,
        unsafe_default.validate(),
    );
}

test "cuda renderer emits graph-safe SM89 split-K online decode ABI" {
    const source = try renderSplitkOnlineDecodeKernel(
        std.testing.allocator,
        splitkOnlineDecodePlanFor(.gqa_decode_splitk_online_sm89_hd512_global_f16_f32),
    );
    defer std.testing.allocator.free(source);

    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, generated_splitk_online_decode_hd512_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "#define ANTFLY_SPLITK_ONLINE_HEAD_DIM 512"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "#define ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW 0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "unsigned* completion_counters"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "volatile float* partial_values"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "volatile float* partial_max"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "volatile float* partial_denom"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const unsigned* decode_scalars"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "__threadfence();"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "atomicAdd(&completion_counters[head], 1u)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "atomicExch(&completion_counters[head], 0u)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "for (unsigned merge_split = 0u;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "runtime_default_enabled=false"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "_prototype"));
}

test "split-KV chronological merge matches direct online softmax" {
    const scores = [_]f32{ 92.0, 89.0, 91.0, -25.0, 90.0, 88.0 };
    const values = [_]f32{ 1.0, -3.0, 5.0, 7.0, -2.0, 4.0 };
    const split_bounds = [_]usize{ 0, 2, 4, 6 };
    var partial_max: [3]f32 = undefined;
    var partial_denom: [3]f32 = undefined;
    var partial_value: [3]f32 = undefined;
    for (0..3) |split| {
        var local_max: f32 = -std.math.floatMax(f32);
        var denom: f32 = 0.0;
        var value: f32 = 0.0;
        for (scores[split_bounds[split]..split_bounds[split + 1]], values[split_bounds[split]..split_bounds[split + 1]]) |score, item| {
            const next_max = @max(local_max, score);
            const alpha = if (denom > 0.0) @exp(local_max - next_max) else 0.0;
            const beta = @exp(score - next_max);
            denom = denom * alpha + beta;
            value = value * alpha + beta * item;
            local_max = next_max;
        }
        partial_max[split] = local_max;
        partial_denom[split] = denom;
        partial_value[split] = value;
    }

    var merged_denom: f32 = 0.0;
    var merged_value: f32 = 0.0;
    var merged_max: f32 = -std.math.floatMax(f32);
    for (0..3) |split| {
        const next_max = @max(merged_max, partial_max[split]);
        const alpha = if (merged_denom > 0.0) @exp(merged_max - next_max) else 0.0;
        const beta = @exp(partial_max[split] - next_max);
        merged_denom = merged_denom * alpha + partial_denom[split] * beta;
        merged_value = merged_value * alpha + partial_value[split] * beta;
        merged_max = next_max;
    }

    var direct_denom: f32 = 0.0;
    var direct_value: f32 = 0.0;
    var direct_max: f32 = -std.math.floatMax(f32);
    for (scores, values) |score, item| {
        const next_max = @max(direct_max, score);
        const alpha = if (direct_denom > 0.0) @exp(direct_max - next_max) else 0.0;
        const beta = @exp(score - next_max);
        direct_denom = direct_denom * alpha + beta;
        direct_value = direct_value * alpha + beta * item;
        direct_max = next_max;
    }
    try std.testing.expectApproxEqAbs(direct_value / direct_denom, merged_value / merged_denom, 1e-6);
}
