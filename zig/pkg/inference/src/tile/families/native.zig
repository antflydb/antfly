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

//! Export instances of the qualified native schedules. Existing quant-codegen
//! regions remain owned by their canonical cross-backend generators.
const r = @import("../region.zig");
const program = @import("../native_program.zig");
const convolution_bindings = @import("native/convolution_bindings.zig");
const cuda_phases = @import("native/cuda_phases.zig");
const layout_bindings = @import("native/layout_bindings.zig");
const metal_phases = @import("native/metal_phases.zig");
const row_statistics = @import("native/row_statistics.zig");
const cuda_selection_sources = @import("../native_sources.zig").sources(program.render(@embedFile("native/cuda/selection.cu"), row_statistics.cuda_selection), .cuda);
const metal_convolution_sources = @import("../native_sources.zig").sources(program.render(@embedFile("native/metal/convolution.metal"), convolution_bindings.metal), .metal);
const attention_prefix_bf16_sources = @import("../native_sources.zig").sources(@embedFile("native/cuda/attention_prefix_bf16.cu"), .cuda);

pub const regions = r.regions(&.{
    r.shared(r.metalFamily("layout.paged_token", @import("native/attention_programs.zig").metal_page), &.{.{ .operation = "layout.page_table", .source = "../../lib/tile/src/layout.zig", .scope = .body, .precision = "U32 wrapping token-base entries", .schedule = "contiguous or token-base page table" }}),
    r.shared(r.cudaFamily("attention.relative_helpers", @import("native/attention_programs.zig").cuda_relative), &.{.{ .operation = "projection.ordered_products", .source = "../../lib/tile/src/native_projection.zig", .scope = .phase, .precision = "F32 combined relative products and probability VJP dot", .schedule = "ordered scalar coordinates; forward and backward share the identical score helper" }}),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/relative_attention_tensor_tiles.cuh", "", "attention.relative_online.schedule0", @import("native/attention_programs.zig").cuda_tensor_online), &.{.{ .operation = "attention.online", .source = "../../lib/tile/src/native_attention.zig", .scope = .phase, .precision = "F32 finite-sentinel guards and expf; preserve F16/BF16 publication", .schedule = "native MMA/warp schedule retains maximum, probability reduction and rescale barriers" }}),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/relative_attention_tensor_tiles.cuh", "", "attention.relative_online.schedule1", @import("native/attention_programs.zig").cuda_tensor_online), &.{.{ .operation = "attention.online", .source = "../../lib/tile/src/native_attention.zig", .scope = .phase, .precision = "F32 finite-sentinel guards and expf; preserve F16/BF16 publication", .schedule = "native MMA/warp schedule retains maximum, probability reduction and rescale barriers" }}),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/vision_bf16_attention_m32n16.cuh", "", "attention.vision_online", @import("native/attention_programs.zig").cuda_tensor_online), &.{.{ .operation = "attention.online", .source = "../../lib/tile/src/native_attention.zig", .scope = .phase, .precision = "F32 finite-sentinel guards and expf; preserve F16/BF16 publication", .schedule = "native MMA/warp schedule retains maximum, probability reduction and rescale barriers" }}),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/wmma_tile_program.cuh", "", "attention.online_row64", @import("native/attention_programs.zig")), &.{.{ .operation = "attention.online", .source = "../../lib/tile/src/native_attention.zig", .scope = .phase, .precision = "F32 state; F16 or F32 probability publication", .schedule = "two scores per lane, shuffle 16/8/4/2/1" }}),
    r.shared(r.metalFamily("projection.scalar_helpers", @import("native/projection_helpers.zig").metal), &.{ .{ .operation = "projection.dot", .source = "../../lib/tile/src/native_projection.zig", .scope = .phase, .precision = "F32 inputs; F32/F16/BF16 weights; F32 accumulator", .schedule = "serial or 32-lane strided" }, .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .precision = "F32", .schedule = "32-lane shared tree" } }),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/projection_rows_cols_tree.cuh", "", "projection.shared_tree", @import("native/row_statistics.zig").projection), &.{.{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .body, .precision = "F32", .schedule = "256 lanes; nested row/column planes" }}),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/row_program.cuh", "", "statistics.shared_tree", @import("native/row_statistics.zig")), &.{.{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .body, .precision = "F32", .schedule = "interleaved planes; runtime block width" }}),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/q8_projection_tile4.cuh", "", "statistics.q8_columns", row_statistics.cuda_include_column4), &.{.{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .precision = "F32 ordered shared tree; existing publication and nonfinite policy", .schedule = "original halving stages, column unrolling, scratch and barriers" }}),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/quant_tile4_publish.cuh", "", "statistics.quant_columns", row_statistics.cuda_include_column4), &.{.{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .precision = "F32 ordered shared tree; existing publication and nonfinite policy", .schedule = "original halving stages, column unrolling, scratch and barriers" }}),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/quant_argmax_publish.cuh", "", "statistics.quant_argmax_columns", row_statistics.cuda_include_argmax), &.{.{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .precision = "F32 ordered shared tree; existing publication and nonfinite policy", .schedule = "original halving stages, column unrolling, scratch and barriers" }}),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/q4_k_tiled_dot.cuh", "", "statistics.quant_scalar", row_statistics.cuda_include_scalar256), &.{.{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .precision = "F32 ordered shared tree; existing publication and nonfinite policy", .schedule = "original halving stages, column unrolling, scratch and barriers" }}),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/dense_qkv_tiled.cuh", "", "statistics.dense_qkv", row_statistics.cuda_include_scalar_block), &.{.{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .precision = "F32 ordered shared tree; existing publication and nonfinite policy", .schedule = "original halving stages, column unrolling, scratch and barriers" }}),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/block_local_attention.cuh", "", "statistics.block_attention", row_statistics.cuda_include_block_attention), &.{.{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .precision = "F32 ordered shared tree; existing publication and nonfinite policy", .schedule = "original halving stages, column unrolling, scratch and barriers" }}),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/score_softmax_bf16_program.cuh", "", "statistics.score_bf16", row_statistics.cuda_include_score_softmax), &.{.{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .precision = "F32 ordered shared tree; existing publication and nonfinite policy", .schedule = "original halving stages, column unrolling, scratch and barriers" }}),
    r.metalFamily("training.math", metal_phases),
    r.shared(r.metalFamily("normalization.epilogue", @import("native/metal_normalization_epilogue.zig")), &.{ .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .precision = "F32", .schedule = "raw moments, bounded shared tree", .source_index = 0 }, .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .precision = "F32; F16 widening or precomputed sumsq inputs", .schedule = "RMS shared tree", .source_index = 1 } }),
    r.metalFamily("projection.epilogue", @import("native/metal_projection_epilogue.zig")),
    r.cudaFamily("projection.epilogue", @import("native/cuda_projection_epilogue.zig")),
    r.cudaFamily("normalization.epilogue", @import("native/cuda_normalization_epilogue.zig")),
    r.metalFamily("rms_norm", @import("native/metal_rms_norm.zig")),
    r.cudaFamily("rms_norm", @import("native/cuda_rms_norm.zig")),
    r.nativeFamily("src/ops/cuda/kernels/rms_q8_1_register_rows.cuh", "", "rms_q8_1.register_rows", @import("native/cuda_rms_q8_1_register_rows.zig")),
    r.nativeFamily("src/ops/cuda/kernels/quant_rows_prefetch_l2.cuh", "", "quant_cuda.rows_prefetch_l2", @import("native/cuda_quant_rows_prefetch_l2.zig")),
    r.metalFamily("layer_norm", @import("native/metal_layer_norm.zig")),
    r.cudaFamily("layer_norm", @import("native/cuda_layer_norm.zig")),
    r.metalFamily("argmax", @import("native/metal_argmax.zig")),
    r.cudaFamily("argmax", @import("native/cuda_argmax.zig")),
    r.metalFamily("reduce.embedding_lookup", @import("native/metal_reduce_embedding_lookup.zig")),
    r.cudaFamily("reduce.embedding_lookup", @import("native/cuda_reduce_embedding_lookup.zig")),
    r.cudaFamily("reduce.rope", @import("native/cuda_reduce_rope.zig")),
    r.metalFamily("reduce.rope", @import("native/metal_reduce_rope.zig")),
    r.shared(r.metalFamily("reduce.rows", @import("native/metal_reduce_rows.zig")), &.{.{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .precision = "F32 maximum/sum; original nonfinite and empty-row policy", .schedule = "256-lane halving shared tree; native softmax/log-softmax and reduction epilogues" }}),
    r.metalFamily("reduce.scan", @import("native/metal_reduce_scan.zig")),
    r.metalFamily("reduce.gated_delta_norm", @import("native/metal_reduce_gated_delta_norm.zig")),
    r.metalFamily("reduce.recurrent_norm", @import("native/metal_reduce_recurrent_norm.zig")),
    r.metalFamily("reduce.recurrent_norm_cached", @import("native/metal_reduce_recurrent_norm_cached.zig")),
    r.shared(r.metalFamily("reduce.sample", @import("native/metal_reduce_sample.zig")), &.{
        .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 0, .precision = "F32 max/sum; disabled scratch planes untouched", .schedule = "halving threadgroup tree; conditional second maximum plane" },
        .{ .operation = "sampling.uniform", .source = "../../lib/tile/src/native_sampling.zig", .scope = .phase, .source_index = 1, .precision = "wrapping U32 avalanche; high-24-bit F32 uniform; preserve endpoint rounding", .schedule = "per-token counter hash; original random stream" },
        .{ .operation = "sampling.topk", .source = "../../lib/tile/src/native_sampling.zig", .scope = .phase, .source_index = 2, .precision = "F32 comparison; ordered stable insertion and original NaN admission", .schedule = "serial bounded insertion; per-block and final merge" },
    }),
    r.metalFamily("elementwise.gelu_backward", @import("native/metal_elementwise_gelu_backward.zig")),
    r.metalFamily("elementwise.apply", @import("native/metal_elementwise_apply.zig")),
    r.metalFamily("elementwise.lora", @import("native/metal_elementwise_lora.zig")),
    r.metalFamily("elementwise.gated_product", @import("native/metal_elementwise_gated_product.zig")),
    r.metalFamily("quant_metal.q4_0_mmv_pair", @import("native/metal_quant_metal_q4_0_mmv_pair.zig")),
    r.metalFamily("quant_metal.q4_0_mmv_linear", @import("native/metal_quant_metal_q4_0_mmv_linear.zig")),
    r.metalFamily("quant_metal.q8_0_linear_r_ext", @import("native/metal_quant_metal_q8_0_linear_r_ext.zig")),
    r.metalFamily("quant_metal.q4_0_linear_r_ext", @import("native/metal_quant_metal_q4_0_linear_r_ext.zig")),
    r.metalFamily("quant_metal.q4_0_linear_specialized", @import("native/metal_quant_metal_q4_0_linear_specialized.zig")),
    r.metalFamily("quant_metal.q4_0_linear_r_ext4", @import("native/metal_quant_metal_q4_0_linear_r_ext4.zig")),
    r.metalFamily("quant_metal.q4_0_pair_activation_mm_m32_n64", @import("native/metal_quant_metal_q4_0_pair_activation_mm_m32_n64.zig")),
    r.metalFamily("quant_metal.q4_0_pair_activation_mm_m32_n32", @import("native/metal_quant_metal_q4_0_pair_activation_mm_m32_n32.zig")),
    r.metalFamily("quant_metal.q8_0_linear_mm_sg_v2", @import("native/metal_quant_metal_q8_0_linear_mm_sg_v2.zig")),
    r.metalFamily("quant_metal.q8_0_linear_mm_sg_m64", @import("native/metal_quant_metal_q8_0_linear_mm_sg_m64.zig")),
    r.metalFamily("quant_metal.q8_0_linear_mm_sg_m64_f16", @import("native/metal_quant_metal_q8_0_linear_mm_sg_m64_f16.zig")),
    r.metalFamily("quant_metal.q8_0_kv_pair_mm_sg", @import("native/metal_quant_metal_q8_0_kv_pair_mm_sg.zig")),
    r.metalFamily("quant_metal.q8_0_pair_activation_mm_sg_v2", @import("native/metal_quant_metal_q8_0_pair_activation_mm_sg_v2.zig")),
    r.metalFamily("quant_metal.q8_0_pair_linear_r_ext", @import("native/metal_quant_metal_q8_0_pair_linear_r_ext.zig")),
    r.metalFamily("quant_metal.q8_0_pair_activation_multiply_r_ext", @import("native/metal_quant_metal_q8_0_pair_activation_multiply_r_ext.zig")),
    r.metalFamily("quant_metal.q8_0_pair_activation", @import("native/metal_quant_metal_q8_0_pair_activation.zig")),
    r.metalFamily("quant_metal.q8_0_activation_rhs_rows", @import("native/metal_quant_metal_q8_0_activation_rhs_rows.zig")),
    r.metalFamily("quant_metal.q6_k_nbodd", @import("native/metal_quant_metal_q6_k_nbodd.zig")),
    r.metalFamily("quant_metal.gemv_q1_0", @import("native/metal_quant_metal_gemv_q1_0.zig")),
    r.metalFamily("quant_metal.q4_0_id", @import("native/metal_quant_metal_q4_0_id.zig")),
    r.metalFamily("quant_metal.gemv_q8_0", @import("native/metal_quant_metal_gemv_q8_0.zig")),
    r.metalFamily("quant_metal.gemv_q8_1", @import("native/metal_quant_metal_gemv_q8_1.zig")),
    r.metalFamily("quant_metal.gemv_q2_k", @import("native/metal_quant_metal_gemv_q2_k.zig")),
    r.shared(r.metalFamily("quant_metal.q4_k_pair", @import("native/metal_quant_metal_q4_k_pair.zig")), &.{.{ .operation = "projection.quantized_subblocks", .source = "../../lib/tile/src/native_projection.zig", .scope = .phase, .precision = "Q4-K recipe decode; F32 accumulator; F32 or F16 publication", .schedule = "four SIMD groups; eight-value staging; two or four output columns" }}),
    r.metalFamily("quant_metal.gemv_q6_k", @import("native/metal_quant_metal_gemv_q6_k.zig")),
    r.metalFamily("quant_metal.gemv_iq4_nl", @import("native/metal_quant_metal_gemv_iq4_nl.zig")),
    r.metalFamily("quant_metal.gemv_nvfp4", @import("native/metal_quant_metal_gemv_nvfp4.zig")),
    r.metalFamily("quant_metal.gemv_iq2_xs", @import("native/metal_quant_metal_gemv_iq2_xs.zig")),
    r.metalFamily("quant_metal.dense_f32_1x", @import("native/metal_quant_metal_dense_f32_1x.zig")),
    r.metalFamily("quant_metal.dense_f16_1x", @import("native/metal_quant_metal_dense_f16_1x.zig")),
    r.metalFamily("quant_metal.dense_bf16_1x", @import("native/metal_quant_metal_dense_bf16_1x.zig")),
    r.metalFamily("quant_metal.dense_f32_multi_row", @import("native/metal_quant_metal_dense_f32_multi_row.zig")),
    r.metalFamily("quant_metal.dense_f16_multi_row", @import("native/metal_quant_metal_dense_f16_multi_row.zig")),
    r.metalFamily("quant_metal.dense_bf16_multi_row", @import("native/metal_quant_metal_dense_bf16_multi_row.zig")),
    r.metalFamily("quant_metal.q4_0_mm_sg", @import("native/metal_quant_metal_q4_0_mm_sg.zig")),
    r.metalFamily("quant_metal.q8_0_mm_sg", @import("native/metal_quant_metal_q8_0_mm_sg.zig")),
    r.metalFamily("quant_metal.spatial_q4_k_mm_matrix", @import("native/metal_quant_metal_spatial_q4_k_mm_matrix.zig")),
    r.metalFamily("quant_metal.spatial_q4_k_mm_matrix_in_f16", @import("native/metal_quant_metal_spatial_q4_k_mm_matrix_in_f16.zig")),
    r.metalFamily("quant_metal.bf16_gate_up_mm", @import("native/metal_quant_metal_bf16_gate_up_mm.zig")),
    r.shared(r.cudaFamily("quant_cuda.dense_f32", @import("native/cuda_quant_cuda_dense_f32.zig")), &.{ .{ .operation = "projection.dot", .source = "../../lib/tile/src/native_projection.zig", .scope = .phase, .precision = "F32", .schedule = "serial and block-strided", .source_index = 0 }, .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .precision = "F32", .schedule = "256-thread shared tree", .source_index = 0 }, .{ .operation = "projection.dot", .source = "../../lib/tile/src/native_projection.zig", .scope = .phase, .precision = "F32", .schedule = "serial bias-seeded", .source_index = 3 } }),
    r.cudaFamily("quant_cuda.linear_f32_tile", @import("native/cuda_quant_cuda_linear_f32_tile.zig")),
    r.cudaFamily("quant_cuda.q8_0_tile", @import("native/cuda_quant_cuda_q8_0_tile.zig")),
    r.cudaFamily("quant_cuda.quantize_q8_1", @import("native/cuda_quant_cuda_quantize_q8_1.zig")),
    r.cudaFamily("quant_cuda.q4_0_tile", @import("native/cuda_quant_cuda_q4_0_tile.zig")),
    r.cudaFamily("quant_cuda.tc_hmma", @import("native/cuda_quant_cuda_tc_hmma.zig")),
    r.cudaFamily("quant_cuda.k_quant", @import("native/cuda_quant_cuda_k_quant.zig")),
    r.cudaFamily("quant_cuda.pair_activation_q8_1", @import("native/cuda_quant_cuda_pair_activation_q8_1.zig")),
    r.cudaFamily("quant_cuda.cols4_tree", @import("native/cuda_quant_cuda_cols4_tree.zig")),
    r.cudaFamily("quant_cuda.qkv_q8_1", @import("native/cuda_quant_cuda_qkv_q8_1.zig")),
    r.cudaFamily("quant_cuda.q4_0_projections", @import("native/cuda_quant_cuda_q4_0_projections.zig")),
    r.cudaFamily("quant_cuda.embedding", @import("native/cuda_quant_cuda_embedding.zig")),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/encoder_multimodal_programs.cuh", "", "convolution.stencil3_tiled", convolution_bindings.cuda_tiled), &.{.{ .operation = "convolution.stencil3_tiled", .source = "../../lib/tile/src/native_convolution.zig", .scope = .body, .precision = "F32 explicit FMA, bias before reduction, padded full K tile, widened batch/channel bases", .schedule = "16x16 output and K16 staging; 16x16 threads", .threads = @import("antfly_tile").native_convolution.Stencil3x3Stride2Tile16.threads, .static_scratch_bytes = @import("antfly_tile").native_convolution.Stencil3x3Stride2Tile16.scratch_bytes }}),
    r.shared(r.metalFamily("audio.channel_norm", @import("native/audio_bindings.zig").metal_channel), &.{.{ .operation = "audio.channel_norm", .source = "../../lib/tile/src/native_audio.zig", .scope = .body, .precision = "F32 centered two-pass; Metal native contraction and max(value, zero)", .schedule = "serial strided channels" }}),
    r.nativeFamily("src/ops/cuda/kernels/encoder_multimodal_programs.cuh", "", "statistics.score_bf16_warp256", row_statistics.bf16_narrow_exports),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/encoder_multimodal_programs.cuh", "", "audio.frontend", @import("native/audio_bindings.zig").cuda_frontend), &.{.{ .operation = "audio.frontend", .source = "../../lib/tile/src/native_audio.zig", .scope = .body, .precision = "F32 explicit RN arithmetic and fmaxf(zero, value); wide batch bases", .schedule = "serial strided channels and per-element window/magnitude/log/permutation" }}),
    r.shared(r.nativeFamily("src/ops/cuda/kernels/encoder_multimodal_programs.cuh", "", "audio.fixed512", @import("native/audio_bindings.zig")), &.{ .{ .operation = "audio.fft", .source = "../../lib/tile/src/native_audio.zig", .scope = .phase, .precision = "F32 explicit __fadd_rn/__fsub_rn/__fmul_rn", .schedule = "512-point radix2; 256 threads; retain split or float2 temporaries" }, .{ .operation = "audio.mel_projection", .source = "../../lib/tile/src/native_audio.zig", .scope = .phase, .source_index = 1, .precision = "F32 sparse filter sum with explicit product/add rounding and nonfinite full-spectrum traversal", .schedule = "128 filters; fixed plan; block-wide nonfinite vote" } }),
    r.nativeFamily("src/ops/cuda/kernels/encoder_multimodal_programs.cuh", "", "quant_cuda.bshd", @import("native/cuda_quant_cuda_bshd.zig")),
    r.nativeFamily("src/ops/cuda/kernels/local_relative_attention_program.cuh", "", "attention.local_relative_source", @import("native/cuda_local_relative_source.zig")),
    r.metalFamily("attention.kv_seed", @import("native/metal_attention_kv_seed.zig")),
    r.metalFamily("attention.qkv_split", @import("native/metal_attention_qkv_split.zig")),
    r.metalFamily("attention.vision_flash_q32", @import("native/metal_attention_vision_flash_q32.zig")),
    r.metalFamily("attention.shifted_window", @import("native/metal_attention_shifted_window.zig")),
    r.metalFamily("attention.dense_causal_sg", @import("native/metal_attention_dense_causal_sg.zig")),
    r.metalFamily("attention.gqa_split", @import("native/metal_attention_gqa_split.zig")),
    r.cudaFamily("attention.block", @import("native/cuda_attention_block.zig")),
    r.cudaFamily("attention.f32_hd64_prefill", @import("native/cuda_attention_f32_hd64_prefill.zig")),
    r.cudaFamily("attention.gqa_fast", @import("native/cuda_attention_gqa_fast.zig")),
    r.cudaFamily("attention.gqa_decode", @import("native/cuda_attention_gqa_decode.zig")),
    r.cudaFamily("attention.qkv_tiled", @import("native/cuda_attention_qkv_tiled.zig")),
    r.cudaFamily("attention.warp", @import("native/cuda_attention_warp.zig")),
    r.cudaFamily("attention.ragged_tc", @import("native/cuda_attention_ragged_tc.zig")),
    r.metalFamily("projection.bf16_vec4", @import("native/metal_projection_bf16_vec4.zig")),
    r.shared(r.metalFamily("projection.matrix_rows_helper", @import("native/projection_helpers.zig").metal_matrix), &.{ .{ .operation = "projection.dot", .source = "../../lib/tile/src/native_projection.zig", .scope = .phase, .precision = "F32 ordered products and accumulation", .schedule = "static 1x8 and 4x4/8/16 register tiles; original RHS layouts and row tails" }, .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .precision = "F32 independent accumulator planes", .schedule = "lane-strided shared tree; retain native plane loop" } }),
    r.metalFamily("projection.dot_small_m", @import("native/metal_projection_dot_small_m.zig")),
    r.cudaFamily("layout.im2col", @import("native/cuda_im2col.zig")),
    r.cudaSourceFamily("projection.f16_rowpair", @embedFile("native/cuda/row_pair.cu")),
    r.cudaSourceFamily("attention.cross_split_hd64", @embedFile("native/cuda/cross_attention_split.cu")),
    r.shared(r.shared(r.cudaSourceFamily("attention.direct", program.render(@embedFile("native/cuda/attention_direct.cu"), cuda_phases.attention)), &.{ .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 5, .export_name = "termite_cross_attention_q1_f32", .precision = "F32 existing tree association and nonfinite policy", .schedule = "native scratch publication and exact halving stages" }, .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 6, .export_name = "termite_channel_scores_softmax_f32", .precision = "F32 existing tree association and nonfinite policy", .schedule = "native scratch publication and exact halving stages" } }), &.{ .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 20, .export_name = "termite_deberta_attention_f32", .precision = "F32 fmaxf/sum with original masked and empty-row policy", .schedule = "block-shared halving trees; preserve scratch-reuse barrier" }, .{ .operation = "projection.ordered_products", .source = "../../lib/tile/src/native_projection.zig", .scope = .phase, .source_index = 21, .export_name = "termite_deberta_attention_fused_f32", .precision = "F32 separate per-product accumulation", .schedule = "warp-striped relative scores" }, .{ .operation = "projection.ordered_products", .source = "../../lib/tile/src/native_projection.zig", .scope = .phase, .source_index = 30, .export_name = "termite_deberta_attention_stream_f32", .precision = "F32 separate per-product accumulation", .schedule = "two head coordinates per lane" }, .{ .operation = "projection.dot", .source = "../../lib/tile/src/native_projection.zig", .scope = .phase, .precision = "F32 ordered score dot", .schedule = "native scalar score traversal", .source_index = 0, .export_name = "termite_attention_f32" }, .{ .operation = "projection.dot", .source = "../../lib/tile/src/native_projection.zig", .scope = .phase, .precision = "F32 ordered score dot", .schedule = "native scalar score traversal", .source_index = 4, .export_name = "termite_cross_attention_f32" }, .{ .operation = "projection.dot", .source = "../../lib/tile/src/native_projection.zig", .scope = .phase, .precision = "F32 ordered score dot", .schedule = "native scalar score traversal", .source_index = 8, .export_name = "termite_gqa_attention_f32" }, .{ .operation = "attention.visibility", .source = "../../lib/tile/src/native_attention.zig", .scope = .phase, .precision = "U32 positions/window", .schedule = "causal window", .source_index = 8, .export_name = "termite_gqa_attention_f32" }, .{ .operation = "attention.visibility", .source = "../../lib/tile/src/native_attention.zig", .scope = .phase, .precision = "U32 positions/window", .schedule = "causal window", .source_index = 19, .export_name = "termite_gqa_attention_decode_turboquant_f32" } }),
    r.shared(r.cudaSourceFamily("convolution.direct", program.render(@embedFile("native/cuda/convolution.cu"), convolution_bindings.cuda)), &.{.{ .operation = "convolution.direct", .source = "../../lib/tile/src/native_convolution.zig", .scope = .body, .precision = "F32", .schedule = "NCHW grouped scalar; channel/height/width traversal" }}),
    r.shared(r.cudaSourceFamily("elementwise.direct", program.render(@embedFile("native/cuda/elementwise_direct.cu"), cuda_phases.elementwise)), &.{ .{ .operation = "statistics.broadcast", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 16, .export_name = "termite_selected_token_logprobs_f32", .precision = "F32 fmaxf/sum; preserve tree association", .schedule = "halving tree; lane-local maximum and barrier before scratch reuse" }, .{ .operation = "statistics.broadcast", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 17, .export_name = "termite_deberta_scores_softmax_f32", .precision = "F32 fmaxf/sum; original mask and zero-denominator rules", .schedule = "256 lanes; read barrier before scratch reuse" } }),
    r.cudaSourceFamily("expert.projection.direct", program.render(@embedFile("native/cuda/expert_projection.cu"), cuda_phases.projection)),
    r.cudaSourceFamily("kv.storage.direct", @embedFile("native/cuda/kv_storage.cu")),
    r.cudaSourceFamily("normalization.direct", @embedFile("native/cuda/normalization_direct.cu")),
    r.cudaSourceFamily("projection.q4q8.rows", program.render(@embedFile("native/cuda/q4_q8_rows.cu"), cuda_phases.projection)),
    r.shared(r.cudaSourceFamily("projection.direct", program.render(@embedFile("native/cuda/projection_direct.cu"), cuda_phases.projection)), &.{ .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 21, .export_name = "termite_linear_q4_k_qkv_nobias_f32_tiled", .precision = "F32 existing tree association and nonfinite policy", .schedule = "native scratch publication and exact halving stages" }, .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 22, .export_name = "termite_linear_q4_k_q4_k_f32_qkv_nobias_tiled", .precision = "F32 existing tree association and nonfinite policy", .schedule = "native scratch publication and exact halving stages" } }),
    // Preserve existing source keys after retiring the unused single-block selector.
    r.cudaFamily("selection.direct.0", struct {
        pub const source_0 = cuda_selection_sources[0];
    }),
    r.cudaFamily("selection.direct.1", struct {
        pub const source_0 = cuda_selection_sources[1];
    }),
    r.cudaFamily("selection.direct.2", struct {
        pub const source_0 = cuda_selection_sources[2];
    }),
    r.cudaFamily("selection.direct.3", struct {
        pub const source_0 = cuda_selection_sources[3];
    }),
    r.cudaFamily("selection.direct.4", struct {
        pub const source_0 = cuda_selection_sources[4];
    }),
    r.cudaFamily("selection.direct.5", struct {
        pub const source_0 = cuda_selection_sources[5];
    }),
    r.cudaFamily("selection.direct.7", struct {
        pub const source_0 = cuda_selection_sources[6];
    }),
    r.cudaFamily("selection.direct.8", struct {
        pub const source_0 = cuda_selection_sources[7];
    }),
    r.cudaFamily("selection.direct.9", struct {
        pub const source_0 = cuda_selection_sources[8];
    }),
    r.shared(r.cudaSourceFamily("sequence.transforms.direct", program.render(@embedFile("native/cuda/sequence_transforms.cu"), layout_bindings.cuda_sequence)), &.{ .{ .operation = "layout.first_match", .source = "../../lib/tile/src/native_layout.zig", .scope = .phase, .precision = "F32 rows; I64 or F32 word ids", .schedule = "coordinate scan; CPU token-major counterpart", .source_index = 1, .export_name = "termite_gliner_word_embeddings_f32" }, .{ .operation = "recurrence.gru", .source = "../../lib/tile/src/native_math.zig", .scope = .phase, .precision = "F32; native exp/tanh; optional residual", .schedule = "packed reset/update/candidate planes", .source_index = 2, .export_name = "termite_gliner_gru_combine_f32" } }),
    r.shared(r.cudaSourceFamily("tensor.layout.direct", program.render(@embedFile("native/cuda/tensor_layout.cu"), .{ .window_pack = layout_bindings.cuda.window_pack, .window_unpack = layout_bindings.cuda.window_unpack, .transpose = layout_bindings.cuda.transpose, .take_rows = layout_bindings.cuda.take_rows, .slice_rows = layout_bindings.cuda.slice_rows, .repeat_rows = layout_bindings.cuda.repeat_rows, .concat_rows = layout_bindings.cuda.concat_rows, .channel_coordinates = (@import("antfly_tile").layout.DenseIndex4{ .axes = .{ "c", "x", "y", "b" }, .extents = .{ "channels", "width", "height" } }).native("unsigned int"), .interleaved_three_stores = (@import("antfly_tile").layout.InterleavedRows{ .parts = 3 }).nativeStores(&.{ "first", "second", "third" }, "input", "idx", "src", "dim") })), &.{ .{ .operation = "layout.rows", .source = "../../lib/tile/src/native_layout.zig", .scope = .phase, .precision = "F32 bit-preserving; U32 indices", .schedule = "existing coordinate schedule", .source_index = 8, .export_name = "termite_take_rows_f32" }, .{ .operation = "layout.rows", .source = "../../lib/tile/src/native_layout.zig", .scope = .phase, .precision = "F32 bit-preserving; U32 indices", .schedule = "existing coordinate schedule", .source_index = 9, .export_name = "termite_slice_last_dim_f32" }, .{ .operation = "layout.rows", .source = "../../lib/tile/src/native_layout.zig", .scope = .phase, .precision = "F32 bit-preserving; U32 indices", .schedule = "existing coordinate schedule", .source_index = 10, .export_name = "termite_repeat_first_row_f32" }, .{ .operation = "layout.rows", .source = "../../lib/tile/src/native_layout.zig", .scope = .phase, .precision = "F32 bit-preserving; U32 indices", .schedule = "existing coordinate schedule", .source_index = 11, .export_name = "termite_concat_lastdim_f32" }, .{ .operation = "layout.transpose", .source = "../../lib/tile/src/native_layout.zig", .scope = .phase, .precision = "F32 bit-preserving; U32 indices", .schedule = "existing coordinate schedule", .source_index = 29, .export_name = "termite_primitive_transpose_2d_f32" }, .{ .operation = "layout.rows", .source = "../../lib/tile/src/native_layout.zig", .scope = .phase, .precision = "F32 bit-preserving; U32 indices", .schedule = "existing coordinate schedule", .source_index = 14, .export_name = "termite_pack_windows_f32" }, .{ .operation = "layout.rows", .source = "../../lib/tile/src/native_layout.zig", .scope = .phase, .precision = "F32 bit-preserving; U32 indices", .schedule = "existing coordinate schedule", .source_index = 15, .export_name = "termite_unpad_windows_f32" } }),
    r.shared(r.shared(r.cudaSourceFamily("training.direct", program.render(@embedFile("native/cuda/training.cu"), cuda_phases.training)), &.{ .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 2, .export_name = "termite_training_sum_squares_f32", .precision = "F32 existing tree association and nonfinite policy", .schedule = "native scratch publication and exact halving stages" }, .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 7, .export_name = "termite_deberta_attention_backward_scores_f32", .precision = "F32 existing tree association and nonfinite policy", .schedule = "native scratch publication and exact halving stages" } }), &.{.{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 7, .export_name = "termite_deberta_attention_backward_scores_f32", .precision = "F32 fmaxf/sum with original mask and zero-denominator policy", .schedule = "block-shared halving trees; retain scratch-reuse barriers" }}),
    r.encodedMetalSourceFamily("recurrence.packed_hd128", @embedFile("native/metal/packed_recurrence_hd128.m")),
    r.encodedMetalSourceFamily("projection.f16_tiled", @embedFile("native/metal/projection_f16_tiled.m")),
    r.encodedMetalSourceFamily("attention.segmented_hd64", @embedFile("native/metal/attention_segmented_hd64.m")),
    r.shared(r.shared(r.encodedMetalSourceFamily("attention.direct", program.render(@embedFile("native/metal/attention_direct.m"), metal_phases.boundary)), &.{ .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 8, .export_name = "termite_sdpa_f32_segments", .precision = "F32 existing tree association and nonfinite policy", .schedule = "native scratch publication and exact halving stages" }, .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 11, .export_name = "termite_sdpa_grouped_softmax", .precision = "F32 existing tree association and nonfinite policy", .schedule = "native scratch publication and exact halving stages" }, .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 20, .export_name = "termite_deberta_relative_score_softmax_pv_f32", .precision = "F32 existing tree association and nonfinite policy", .schedule = "native scratch publication and exact halving stages" } }), &.{ .{ .operation = "projection.ordered_products", .source = "../../lib/tile/src/native_projection.zig", .scope = .phase, .source_index = 18, .export_name = "termite_disentangled_relative_attention_f32", .precision = "F32 combined three-product expression before accumulator addition", .schedule = "ordered scalar relative scores; no reassociation with CUDA termwise schedule" }, .{ .operation = "projection.dot", .source = "../../lib/tile/src/native_projection.zig", .scope = .phase, .precision = "F32 ordered score dot", .schedule = "native scalar score traversal", .source_index = 7, .export_name = "termite_sdpa_f32" }, .{ .operation = "statistics.maximum", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .precision = "F32 native max", .schedule = "shared halving tree", .source_index = 12, .export_name = "termite_sdpa_f32_tg" }, .{ .operation = "attention.visibility", .source = "../../lib/tile/src/native_attention.zig", .scope = .phase, .precision = "U32 positions/window", .schedule = "causal window", .source_index = 21, .export_name = "termite_attention_f32" } }),
    r.shared(r.encodedMetalSourceFamily("audio.direct", program.render(@embedFile("native/metal/audio.m"), @import("native/audio_bindings.zig").metal)), &.{ .{ .operation = "audio.frontend", .source = "../../lib/tile/src/native_audio.zig", .scope = .body, .source_index = 1, .export_name = "antfly_audio_frontend", .precision = "F32 native arithmetic; U32 indexing", .schedule = "per-element window/magnitude/log/permutation and serial strided channel normalization" }, .{ .operation = "convolution.stencil3_tiled", .source = "../../lib/tile/src/native_convolution.zig", .scope = .body, .source_index = 5, .export_name = "antfly_audio_conv_tiled", .precision = "F32 native contraction; optional bias before reduction; valid K-tail traversal", .schedule = "16x16 output and K16 staging; 16x16 threads", .threads = @import("antfly_tile").native_convolution.Stencil3x3Stride2Tile16.threads, .static_scratch_bytes = @import("antfly_tile").native_convolution.Stencil3x3Stride2Tile16.scratch_bytes }, .{ .operation = "convolution.stencil3_indexing", .source = "../../lib/tile/src/native_convolution.zig", .scope = .phase, .source_index = 6, .export_name = "antfly_audio_conv_matrix", .precision = "U32 coordinates with original signed cast and padding", .schedule = "MMA 16x16 output; K32 staging; existing 8x8 fragment ownership" }, .{ .operation = "audio.fft", .source = "../../lib/tile/src/native_audio.zig", .scope = .phase, .precision = "F32 ordinary products", .schedule = "512-point radix2, 256 threads", .source_index = 2, .export_name = "antfly_audio_fft512" }, .{ .operation = "audio.fft", .source = "../../lib/tile/src/native_audio.zig", .scope = .phase, .precision = "F32 separate ADD/SUB/MUL rounding", .schedule = "512-point radix2, 256 threads", .source_index = 4, .export_name = "antfly_audio_logmel" } }),
    // Preserve existing source keys after retiring the unused F32 conversion.
    r.metalFamily("convolution.direct.0", struct {
        pub const source_0 = metal_convolution_sources[0];
    }),
    r.shared(r.metalFamily("convolution.direct.2", struct {
        pub const source_0 = metal_convolution_sources[1];
    }), &.{.{ .operation = "convolution.direct", .source = "../../lib/tile/src/native_convolution.zig", .scope = .body, .precision = "F32", .schedule = "NCHW scalar; optional bias" }}),
    r.metalFamily("convolution.direct.3", struct {
        pub const source_0 = metal_convolution_sources[2];
    }),
    r.shared(r.metalFamily("convolution.direct.4", struct {
        pub const source_0 = metal_convolution_sources[3];
    }), &.{.{ .operation = "convolution.direct", .source = "../../lib/tile/src/native_convolution.zig", .scope = .body, .precision = "F32", .schedule = "NCHW scalar; optional bias" }}),
    r.metalFamily("convolution.direct.5", struct {
        pub const source_0 = metal_convolution_sources[4];
    }),
    r.shared(r.metalSourceFamily("elementwise.direct", program.render(@embedFile("native/metal/elementwise_direct.metal"), row_statistics.metal_scalar)), &.{.{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 12, .export_name = "termite_dot_general_2d_f32_reduce", .precision = "F32; retain native max/sum, masks and nonfinite behavior", .schedule = "qualified halving shared tree and scratch publication order" }}),
    r.encodedMetalSourceFamily("expert.projection.direct", @embedFile("native/metal/expert_projection.m")),
    r.shared(r.metalSourceFamily("kv.storage.direct", program.render(@embedFile("native/metal/kv_storage.metal"), row_statistics.metal_storage)), &.{.{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 3, .export_name = "termite_encode_int8_per_head", .precision = "F32; retain native max/sum, masks and nonfinite behavior", .schedule = "qualified halving shared tree and scratch publication order" }}),
    r.shared(r.metalSourceFamily("normalization.direct", program.render(@embedFile("native/metal/normalization_direct.metal"), row_statistics.metal_direct)), &.{ .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 2, .export_name = "termite_nomic_pool_normalize", .precision = "F32; retain native max/sum, masks and nonfinite behavior", .schedule = "qualified halving shared tree and scratch publication order" }, .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 4, .export_name = "termite_normalize_embedding", .precision = "F32; retain native max/sum, masks and nonfinite behavior", .schedule = "qualified halving shared tree and scratch publication order" } }),
    r.shared(r.metalSourceFamily("projection.direct", program.render(@embedFile("native/metal/projection_direct.metal"), row_statistics.metal_pair)), &.{.{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 15, .export_name = "termite_apply_linear_pair_1x_reduce", .precision = "F32; retain native max/sum, masks and nonfinite behavior", .schedule = "qualified halving shared tree and scratch publication order" }}),
    r.metalSourceFamily("recurrence.direct", @embedFile("native/metal/recurrence.metal")),
    r.metalSourceFamily("selection.direct", @embedFile("native/metal/selection.metal")),
    r.shared(r.metalSourceFamily("sequence.transforms.direct", program.render(@embedFile("native/metal/sequence_transforms.metal"), layout_bindings.metal_sequence)), &.{ .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 0, .export_name = "termite_deberta_embeddings_f32", .precision = "F32 raw moments; preserve variance clamp and masked publication", .schedule = "two bounded 256-element planes" }, .{ .operation = "layout.first_match", .source = "../../lib/tile/src/native_layout.zig", .scope = .phase, .precision = "F32 rows; I64 or F32 word ids", .schedule = "coordinate scan; CPU token-major counterpart", .source_index = 2, .export_name = "termite_gliner_word_embeddings_f32" }, .{ .operation = "recurrence.gru", .source = "../../lib/tile/src/native_math.zig", .scope = .phase, .precision = "F32; native exp/tanh; optional residual", .schedule = "packed reset/update/candidate planes", .source_index = 3, .export_name = "termite_gliner_gru_combine_f32" } }),
    r.shared(r.shared(r.metalSourceFamily("tensor.layout.direct", program.render(@embedFile("native/metal/tensor_layout.metal"), layout_bindings.metal)), &.{ .{ .operation = "layout.rows", .source = "../../lib/tile/src/native_layout.zig", .scope = .phase, .precision = "F32 bit-preserving; U32 indices", .schedule = "existing coordinate schedule", .source_index = 8, .export_name = "termite_slice_last_dim_f32_2d" }, .{ .operation = "layout.rows", .source = "../../lib/tile/src/native_layout.zig", .scope = .phase, .precision = "F32 bit-preserving; U32 indices", .schedule = "existing coordinate schedule", .source_index = 12, .export_name = "termite_concat_lastdim_f32_2d" } }), &.{.{ .operation = "layout.transpose", .source = "../../lib/tile/src/native_layout.zig", .scope = .body, .precision = "F32", .schedule = "32x32 tile; 32x32 threads; batch grid", .source_index = 22, .export_name = "termite_transpose_last2_tiled_f32", .threads = @import("antfly_tile").native_layout.metal_transpose.physicalThreads(false), .static_scratch_bytes = @import("antfly_tile").native_layout.Transpose.scratch_bytes }}),
    r.shared(r.encodedMetalSourceFamily("training.direct", program.render(@embedFile("native/metal/training.m"), metal_phases.boundary)), &.{ .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 0, .export_name = "termite_layer_norm_bwd_dx", .precision = "F32 existing tree association and nonfinite policy", .schedule = "native scratch publication and exact halving stages" }, .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 4, .export_name = "termite_masked_bce_with_logits_loss", .precision = "F32 existing tree association and nonfinite policy", .schedule = "native scratch publication and exact halving stages" }, .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 5, .export_name = "termite_masked_bce_with_logits_backward", .precision = "F32 existing tree association and nonfinite policy", .schedule = "native scratch publication and exact halving stages" } }),
    r.shared(r.encodedMetalSourceFamily("attention.direct.abi", program.render(@embedFile("native/metal/attention_direct_abi.m"), metal_phases.boundary)), &.{ .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 15, .export_name = "termite_disentangled_relative_attention_f32_tg", .precision = "F32 existing tree association and nonfinite policy", .schedule = "native scratch publication and exact halving stages" }, .{ .operation = "statistics.tree", .source = "../../lib/tile/src/native_statistics.zig", .scope = .phase, .source_index = 16, .export_name = "termite_disentangled_relative_attention_f32_flash4", .precision = "F32 existing tree association and nonfinite policy", .schedule = "native scratch publication and exact halving stages" } }),
    r.encodedMetalSourceFamily("audio.abi", @embedFile("native/metal/audio_abi.m")),
    r.encodedMetalSourceFamily("elementwise.direct.abi", @embedFile("native/metal/elementwise_direct_abi.m")),
    r.encodedMetalSourceFamily("expert.projection.abi", @embedFile("native/metal/expert_projection_abi.m")),
    r.encodedMetalSourceFamily("projection.direct.abi", program.render(@embedFile("native/metal/projection_direct_abi.m"), metal_phases.boundary)),
    r.shared(r.encodedMetalSourceFamily("tensor.layout.abi", program.render(@embedFile("native/metal/tensor_layout_abi.m"), layout_bindings.metal_abi)), &.{ .{ .operation = "layout.windows", .source = "../../lib/tile/src/native_layout.zig", .scope = .phase, .precision = "F32 bit-preserving; U32 indices", .schedule = "token-major pack/unpack; padded zero fill", .source_index = 2, .export_name = "termite_florence_window_pack_f32" }, .{ .operation = "layout.windows", .source = "../../lib/tile/src/native_layout.zig", .scope = .phase, .precision = "F32 bit-preserving; U32 indices", .schedule = "token-major pack/unpack; padded zero fill", .source_index = 3, .export_name = "termite_florence_window_unpack_f32" } }),
    // Append a separate family to preserve historical rotary region keys.
    r.cudaSourceFamily("reduce.rotary.axis.bf16_values_f32", @import("native/cuda_reduce_rope.zig").bf16_source),
    // Appended source policies preserve every historical family/region key.
    // Preserve the live region keys after retiring the unused F32-storage .0.
    r.cudaFamily("attention.prefix.bf16.1", struct {
        pub const source_0 = attention_prefix_bf16_sources[0];
    }),
    r.cudaFamily("attention.prefix.bf16.2", struct {
        pub const source_0 = attention_prefix_bf16_sources[1] ++ attention_prefix_bf16_sources[2];
    }),
    r.cudaSourceFamily("reduce.rotary.pair_i32.bf16_source", @embedFile("native/cuda/rotary_pair_source.cu")),
    r.cudaSourceFamily("pool.spatial.gemm.f32_bf16", @embedFile("native/cuda/spatial_pool_gemm.cu")),
});
