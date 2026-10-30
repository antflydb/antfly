// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

// Shared lifecycle identity. Groups retain runtime field layout; the two
// ordinals retain creation and retirement order independently. The final
// family is the weight representation needed by optional pipelines; ALWAYS
// retains shared and non-format-specific operations. Required slots ignore it.
#pragma once

#define ANTFLY_METAL_PIPELINE_COUNT 458u

#define ANTFLY_METAL_PIPELINE_GROUP_0(X) \
    X(video_position_pipeline, @"antfly_video_position", precise_library, 55, 64, ALWAYS) \
    X(embed_absolute_position_pipeline, @"termite_embed_absolute_position", library, 0, 0, ALWAYS) \
    X(embedding_lookup_pipeline, @"termite_embedding_lookup", library, 1, 1, ALWAYS) \
    X(embedding_lookup_bf16_pipeline, @"termite_embedding_lookup_bf16", library, 2, 2, ALWAYS) \
    X(embedding_lookup_f16_pipeline, @"termite_embedding_lookup_f16", library, 3, 3, ALWAYS) \
    X(q4_0_get_rows_pipeline, @"termite_q4_0_get_rows", library, 4, 4, Q4_0) \
    X(q4_0_set_rows_pipeline, @"termite_q4_0_set_rows", library, 5, 5, Q4_0) \
    X(q4_0_cpy_q_to_f32_pipeline, @"termite_q4_0_cpy_q_to_f32", library, 6, 6, Q4_0) \
    X(q4_0_cpy_f32_to_q_pipeline, @"termite_q4_0_cpy_f32_to_q", library, 7, 7, Q4_0) \
    X(q4_1_get_rows_pipeline, @"termite_q4_1_get_rows", library, 8, 8, Q4_1) \
    X(q4_1_set_rows_pipeline, @"termite_q4_1_set_rows", library, 9, 9, Q4_1) \
    X(q4_1_cpy_q_to_f32_pipeline, @"termite_q4_1_cpy_q_to_f32", library, 10, 10, Q4_1) \
    X(q4_1_cpy_f32_to_q_pipeline, @"termite_q4_1_cpy_f32_to_q", library, 11, 11, Q4_1) \
    X(q5_0_get_rows_pipeline, @"termite_q5_0_get_rows", library, 12, 12, Q5_0) \
    X(q5_0_set_rows_pipeline, @"termite_q5_0_set_rows", library, 13, 13, Q5_0) \
    X(q5_0_cpy_q_to_f32_pipeline, @"termite_q5_0_cpy_q_to_f32", library, 14, 14, Q5_0) \
    X(q5_0_cpy_f32_to_q_pipeline, @"termite_q5_0_cpy_f32_to_q", library, 15, 15, Q5_0) \
    X(q5_1_get_rows_pipeline, @"termite_q5_1_get_rows", library, 16, 16, Q5_1) \
    X(q5_1_set_rows_pipeline, @"termite_q5_1_set_rows", library, 17, 17, Q5_1) \
    X(q5_1_cpy_q_to_f32_pipeline, @"termite_q5_1_cpy_q_to_f32", library, 18, 18, Q5_1) \
    X(q5_1_cpy_f32_to_q_pipeline, @"termite_q5_1_cpy_f32_to_q", library, 19, 19, Q5_1) \
    X(q4_k_get_rows_pipeline, @"termite_q4_k_get_rows", library, 20, 20, Q4_K) \
    X(q4_k_set_rows_pipeline, @"termite_q4_k_set_rows", library, 21, 21, Q4_K) \
    X(q4_k_cpy_q_to_f32_pipeline, @"termite_q4_k_cpy_q_to_f32", library, 22, 22, Q4_K) \
    X(q4_k_cpy_f32_to_q_pipeline, @"termite_q4_k_cpy_f32_to_q", library, 23, 23, Q4_K) \
    X(q5_k_get_rows_pipeline, @"termite_q5_k_get_rows", library, 24, 24, Q5_K) \
    X(q5_k_set_rows_pipeline, @"termite_q5_k_set_rows", library, 25, 25, Q5_K) \
    X(q5_k_cpy_q_to_f32_pipeline, @"termite_q5_k_cpy_q_to_f32", library, 26, 26, Q5_K) \
    X(q5_k_cpy_f32_to_q_pipeline, @"termite_q5_k_cpy_f32_to_q", library, 27, 27, Q5_K) \
    X(q6_k_get_rows_pipeline, @"termite_q6_k_get_rows", library, 28, 28, Q6_K) \
    X(q6_k_set_rows_pipeline, @"termite_q6_k_set_rows", library, 29, 29, Q6_K) \
    X(q6_k_cpy_q_to_f32_pipeline, @"termite_q6_k_cpy_q_to_f32", library, 30, 30, Q6_K) \
    X(q6_k_cpy_f32_to_q_pipeline, @"termite_q6_k_cpy_f32_to_q", library, 31, 31, Q6_K) \
    X(q8_0_get_rows_pipeline, @"termite_q8_0_get_rows", library, 32, 32, Q8_0) \
    X(q8_0_set_rows_pipeline, @"termite_q8_0_set_rows", library, 33, 33, Q8_0) \
    X(q8_0_cpy_q_to_f32_pipeline, @"termite_q8_0_cpy_q_to_f32", library, 34, 34, Q8_0) \
    X(q8_0_cpy_f32_to_q_pipeline, @"termite_q8_0_cpy_f32_to_q", library, 35, 35, Q8_0) \
    X(q8_1_get_rows_pipeline, @"termite_q8_1_get_rows", library, 36, 36, Q8_1) \
    X(q8_1_set_rows_pipeline, @"termite_q8_1_set_rows", library, 37, 37, Q8_1) \
    X(q8_1_cpy_q_to_f32_pipeline, @"termite_q8_1_cpy_q_to_f32", library, 38, 38, Q8_1) \
    X(q8_1_cpy_f32_to_q_pipeline, @"termite_q8_1_cpy_f32_to_q", library, 39, 39, Q8_1) \
    X(rope_pipeline, @"termite_apply_rope", precise_library, 40, 40, ALWAYS) \
    X(mrope_pipeline, @"termite_apply_mrope", precise_library, 41, 41, ALWAYS) \
    X(qwen35_conv_pipeline, @"antfly_gdn_causal_conv_f32", precise_library, 42, 42, ALWAYS) \
    X(qwen35_prefill_conv_vec4_pipeline, @"antfly_gdn_causal_conv_vec4_f32", precise_library, 43, 43, ALWAYS) \
    X(qwen35_history_pipeline, @"antfly_causal_history_f32", precise_library, 44, 44, ALWAYS) \
    X(qwen35_prepare_pipeline, @"termite_qwen35_prepare", precise_library, 45, 45, ALWAYS) \
    X(qwen35_recurrent_pipeline, @"termite_qwen35_recurrent", precise_library, 46, 46, ALWAYS) \
    X(qwen35_recurrent_columns4_pipeline, @"termite_qwen35_recurrent_columns4", precise_library, 47, 47, ALWAYS) \
    X(qwen35_recurrent_decode_pipeline, @"termite_qwen35_recurrent_decode", precise_library, 48, 48, ALWAYS) \
    X(qwen35_fused_decode_pipeline, @"termite_qwen35_fused_decode", precise_library, 49, 49, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_1(X) \
    X(qwen35_prepare_parallel_pipeline, @"termite_qwen35_prepare_parallel", precise_library, 50, 50, ALWAYS) \
    X(qwen35_norm_parallel_pipeline, @"termite_qwen35_norm_parallel", precise_library, 51, 51, ALWAYS) \
    X(qwen35_norm_pipeline, @"termite_qwen35_norm", precise_library, 52, 61, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_2(X) \
    X(vision_norm_rope_pipeline, @"termite_vision_norm_rope", precise_library, 57, 66, ALWAYS) \
    X(embedding_head_prepare_pipeline, @"termite_embedding_head_prepare", precise_library, 54, 63, ALWAYS) \
    X(rotary_table_pipeline, @"antfly_rotary_table", precise_library, 56, 65, ALWAYS) \
    X(packed_gelu_pipeline, @"termite_packed_gelu", precise_library, 53, 62, ALWAYS) \
    X(rope_pair_pipeline, @"termite_apply_rope_pair", precise_library, 58, 67, ALWAYS) \
    X(head_rms_rope_pipeline, @"termite_apply_head_rms_rope", precise_library, 59, 68, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_3(X) \
    X(attention_f32_pipeline, @"termite_attention_f32", precise_library, 60, 69, ALWAYS) \
    X(attention_f32_decode_1x_hd64_pipeline, @"termite_attention_f32_decode_1x_hd64", precise_library, 61, 70, ALWAYS) \
    X(attention_f32_decode_1x_hd64_split_stage_pipeline, @"termite_attention_f32_decode_1x_hd64_split_stage", precise_library, 62, 71, ALWAYS) \
    X(attention_f32_decode_1x_hd64_split_reduce_pipeline, @"termite_attention_f32_decode_1x_hd64_split_reduce", precise_library, 63, 72, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_4(X) \
    X(attention_f32_prefill_pipeline, @"termite_attention_f32_prefill_tiled", precise_library, 64, 73, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_5(X) \
    X(attention_paged_pipeline, @"termite_paged_attention_kv", precise_library, 65, 74, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_6(X) \
    X(attention_paged_1x_pipeline, @"termite_paged_attention_kv_1x", precise_library, 66, 75, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_7(X) \
    X(compressed_attention_store_local_pipeline, @"termite_compressed_attention_store_local", precise_library, 67, 449, ALWAYS) \
    X(compressed_attention_store_component_pipeline, @"termite_compressed_attention_store_component", precise_library, 68, 450, ALWAYS) \
    X(compressed_attention_update_component_pipeline, @"termite_compressed_attention_update_component", precise_library, 69, 451, ALWAYS) \
    X(compressed_attention_select_compressed_pipeline, @"termite_compressed_attention_select_compressed", precise_library, 70, 452, ALWAYS) \
    X(compressed_attention_hybrid_attention_pipeline, @"termite_compressed_attention_hybrid_attention", precise_library, 71, 453, ALWAYS) \
    X(paged_f32_kv_seed_pipeline, @"termite_paged_f32_kv_seed", precise_library, 72, 76, ALWAYS) \
    X(paged_f16_kv_seed_pipeline, @"termite_paged_f16_kv_seed", precise_library, 73, 77, ALWAYS) \
    X(paged_f32_v_seed_pipeline, @"termite_paged_f32_v_seed", precise_library, 74, 78, ALWAYS) \
    X(copy_bytes_pipeline, @"termite_copy_bytes", precise_library, 75, 79, ALWAYS) \
    X(slice_last_dim_f32_2d_pipeline, @"termite_slice_last_dim_f32_2d", precise_library, 76, 80, ALWAYS) \
    X(gather_axis0_f32_2d_pipeline, @"termite_gather_axis0_f32_2d", precise_library, 77, 81, ALWAYS) \
    X(gather_axis0_add_bias_f32_2d_pipeline, @"termite_gather_axis0_add_bias_f32_2d", precise_library, 78, 82, ALWAYS) \
    X(concat_lastdim_f32_2d_pipeline, @"termite_concat_lastdim_f32_2d", precise_library, 79, 83, ALWAYS) \
    X(deberta_embeddings_f32_pipeline, @"termite_deberta_embeddings_f32", precise_library, 80, 84, ALWAYS) \
    X(gliner_word_embeddings_f32_pipeline, @"termite_gliner_word_embeddings_f32", precise_library, 81, 85, ALWAYS) \
    X(gliner_boundary_f32_pipeline, @"termite_gliner_boundary_f32", precise_library, 82, 86, ALWAYS) \
    X(gliner_boundary_ffn_bias_gelu_exact_pipeline, @"termite_gliner_boundary_ffn_bias_gelu_exact", precise_library, 83, 87, ALWAYS) \
    X(gliner_boundary_ffn_bias_add_layer_norm_pipeline, @"termite_gliner_boundary_ffn_bias_add_layer_norm", precise_library, 84, 88, ALWAYS) \
    X(gliner_gru_combine_f32_pipeline, @"termite_gliner_gru_combine_f32", precise_library, 85, 89, ALWAYS) \
    X(argmax_axis_f32_pipeline, @"termite_argmax_axis_f32", precise_library, 86, 90, ALWAYS) \
    X(convert_dtype_f32_pipeline, @"termite_convert_dtype_f32", precise_library, 87, 91, ALWAYS) \
    X(cumulative_sum_f32_pipeline, @"termite_cumulative_sum_f32", precise_library, 88, 92, ALWAYS) \
    X(cumulative_sum_i64_pipeline, @"termite_cumulative_sum_i64", precise_library, 98, 102, ALWAYS) \
    X(cumulative_sum_i8_pipeline, @"termite_cumulative_sum_i8", precise_library, 97, 101, ALWAYS) \
    X(cumulative_sum_i16_pipeline, @"termite_cumulative_sum_i16", precise_library, 96, 100, ALWAYS) \
    X(cumulative_sum_u8_pipeline, @"termite_cumulative_sum_u8", precise_library, 95, 99, ALWAYS) \
    X(integer_binary_pipeline, @"termite_integer_binary", precise_library, 89, 93, ALWAYS) \
    X(select_typed_pipeline, @"termite_select_typed", precise_library, 90, 94, ALWAYS) \
    X(slice_typed_pipeline, @"termite_slice_typed", precise_library, 91, 95, ALWAYS) \
    X(gather_typed_pipeline, @"termite_gather_typed", precise_library, 92, 96, ALWAYS) \
    X(cast_typed_pipeline, @"termite_cast_typed", precise_library, 93, 97, ALWAYS) \
    X(cumulative_sum_i32_pipeline, @"termite_cumulative_sum_i32", precise_library, 94, 98, ALWAYS) \
    X(sdpa_f32_pipeline, @"termite_sdpa_f32", precise_library, 99, 103, ALWAYS) \
    X(sdpa_f32_bert_prefill_s256_hd64_q8_pipeline, @"termite_sdpa_f32_bert_prefill_s256_hd64_q8", precise_library, 100, 104, ALWAYS) \
    X(sdpa_f32_vision_hd64_q8_pipeline, @"termite_sdpa_f32_vision_hd64_q8", precise_library, 101, 105, ALWAYS) \
    X(sdpa_f32_vision_hd64_q32_pipeline, @"termite_sdpa_f32_vision_hd64_q32", precise_library, 102, 106, ALWAYS) \
    X(sdpa_f32_vision_hd64_flash_q16_pipeline, @"termite_sdpa_f32_vision_hd64_flash_q16", precise_library, 103, 107, ALWAYS) \
    X(sdpa_f32_vision_hd64_flash_q32_pipeline, @"termite_sdpa_f32_vision_hd64_flash_q32", precise_library, 104, 108, ALWAYS) \
    X(sdpa_f32_vision_hd64_flash_q32_full_pipeline, @"termite_sdpa_f32_vision_hd64_flash_q32_full", precise_library, 105, 109, ALWAYS) \
    X(sdpa_f32_nomic_q8_pipeline, @"termite_sdpa_f32_nomic_q8", precise_library, 106, 110, ALWAYS) \
    X(sdpa_f32_tg_pipeline, @"termite_sdpa_f32_tg", precise_library, 107, 111, ALWAYS) \
    X(sdpa_f32_grouped_short256_pipeline, @"termite_sdpa_f32_grouped_short256", precise_library, 108, 112, ALWAYS) \
    X(sdpa_f32_grouped_short512_pipeline, @"termite_sdpa_f32_grouped_short512", precise_library, 109, 113, ALWAYS) \
    X(sdpa_f32_grouped_matrix64_pipeline, @"termite_sdpa_f32_grouped_matrix64", precise_library, 110, 114, ALWAYS) \
    X(sdpa_f32_grouped_matrix32_pipeline, @"termite_sdpa_f32_grouped_matrix32", precise_library, 111, 115, ALWAYS) \
    X(sdpa_f32_segments_pipeline, @"termite_sdpa_f32_segments", precise_library, 112, 116, ALWAYS) \
    X(sdpa_f32_segments_tiled_pipeline, @"termite_sdpa_f32_segments_tiled", precise_library, 113, 117, ALWAYS) \
    X(sdpa_f32_segments_wide_pipeline, @"termite_sdpa_f32_segments_wide", precise_library, 114, 118, ALWAYS) \
    X(sdpa_f32_segments_blocks_pipeline, @"termite_sdpa_f32_segments_blocks", precise_library, 115, 119, ALWAYS) \
    X(sdpa_f32_grouped_blocks_pipeline, @"termite_sdpa_f32_grouped_blocks", precise_library, 125, 129, ALWAYS) \
    X(sdpa_f32_grouped_blocks256_pipeline, @"termite_sdpa_f32_grouped_blocks256", precise_library, 123, 127, ALWAYS) \
    X(sdpa_f32_grouped_blocks256x16_pipeline, @"termite_sdpa_f32_grouped_blocks256x16", precise_library, 124, 128, ALWAYS) \
    X(sdpa_f32_grouped_pack_pipeline, @"termite_sdpa_grouped_pack", precise_library, 122, 126, ALWAYS) \
    X(sdpa_f32_grouped_softmax_pipeline, @"termite_sdpa_grouped_softmax", precise_library, 121, 125, ALWAYS) \
    X(sdpa_f32_grouped_unpack_pipeline, @"termite_sdpa_grouped_unpack", precise_library, 120, 124, ALWAYS) \
    X(masked_mean_rows_pipeline, @"termite_masked_mean_rows", precise_library, 116, 120, ALWAYS) \
    X(masked_mean_batch_pipeline, @"antfly_masked_mean_batch", precise_library, 117, 121, ALWAYS) \
    X(compose_rows_pipeline, @"antfly_compose_rows", precise_library, 118, 122, ALWAYS) \
    X(normalize_embedding_pipeline, @"termite_normalize_embedding", precise_library, 119, 123, ALWAYS) \
    X(segment_training_attention_forward_pipeline, @"antfly_segment_training_attention_forward", precise_library, 126, 130, ALWAYS) \
    X(segment_training_attention_dq_pipeline, @"antfly_segment_training_attention_dq", precise_library, 127, 131, ALWAYS) \
    X(segment_training_attention_dkdv_pipeline, @"antfly_segment_training_attention_dkdv", precise_library, 128, 132, ALWAYS) \
    X(segment_training_attention_flash_forward_pipeline, @"antfly_segment_training_attention_flash_forward", precise_library, 129, 133, ALWAYS) \
    X(segment_training_attention_flash_dq_pipeline, @"antfly_segment_training_attention_flash_dq", precise_library, 130, 134, ALWAYS) \
    X(segment_training_attention_flash_dkdv_pipeline, @"antfly_segment_training_attention_flash_dkdv", precise_library, 131, 135, ALWAYS) \
    X(sdpa_f32_florence_window_hd32_pipeline, @"termite_sdpa_f32_florence_window_hd32", precise_library, 132, 136, ALWAYS) \
    X(sdpa_f32_florence_window_parallel_pipeline, @"termite_sdpa_f32_florence_window_parallel", precise_library, 133, 137, ALWAYS) \
    X(florence_window_pack_f32_pipeline, @"termite_florence_window_pack_f32", precise_library, 134, 138, ALWAYS) \
    X(florence_window_unpack_f32_pipeline, @"termite_florence_window_unpack_f32", precise_library, 135, 139, ALWAYS) \
    X(florence_channel_scores_f32_pipeline, @"termite_florence_channel_scores_f32", precise_library, 136, 140, ALWAYS) \
    X(florence_channel_scores_parallel_pipeline, @"termite_florence_channel_scores_parallel", precise_library, 137, 141, ALWAYS) \
    X(florence_channel_softmax_f32_pipeline, @"termite_florence_channel_softmax_f32", precise_library, 138, 142, ALWAYS) \
    X(florence_channel_apply_f32_pipeline, @"termite_florence_channel_apply_f32", precise_library, 139, 143, ALWAYS) \
    X(disentangled_relative_attention_f32_pipeline, @"termite_disentangled_relative_attention_f32", precise_library, 140, 144, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_8(X) \
    X(disentangled_relative_attention_f32_tg_pipeline, @"termite_disentangled_relative_attention_f32_tg", precise_library, 141, 145, ALWAYS) \
    X(disentangled_relative_attention_f32_flash4_pipeline, @"termite_disentangled_relative_attention_f32_flash4", precise_library, 142, 146, ALWAYS) \
    X(deberta_attention_pack_f32_pipeline, @"termite_deberta_attention_pack_f32", precise_library, 143, 147, ALWAYS) \
    X(deberta_relative_score_accumulate_f32_pipeline, @"termite_deberta_relative_score_accumulate_f32", precise_library, 144, 148, ALWAYS) \
    X(disentangled_relative_attention_bwd_scores_f32_pipeline, @"termite_disentangled_relative_attention_bwd_scores_f32", precise_library, 145, 149, ALWAYS) \
    X(disentangled_relative_attention_bwd_dv_f32_pipeline, @"termite_disentangled_relative_attention_bwd_dv_f32", precise_library, 146, 150, ALWAYS) \
    X(disentangled_relative_attention_bwd_dq_dk_f32_pipeline, @"termite_disentangled_relative_attention_bwd_dq_dk_f32", precise_library, 147, 151, ALWAYS) \
    X(disentangled_relative_attention_bwd_dqr_dkr_f32_pipeline, @"termite_disentangled_relative_attention_bwd_dqr_dkr_f32", precise_library, 148, 152, ALWAYS) \
    X(deberta_relative_score_gemm_f32_pipeline, @"termite_deberta_relative_score_gemm_f32", precise_library, 149, 153, ALWAYS) \
    X(deberta_relative_score_softmax_pv_f32_pipeline, @"termite_deberta_relative_score_softmax_pv_f32", precise_library, 150, 154, ALWAYS) \
    X(transpose_f32_pipeline, @"termite_transpose_f32", precise_library, 151, 155, ALWAYS) \
    X(transpose_last2_tiled_f32_pipeline, @"termite_transpose_last2_tiled_f32", precise_library, 152, 156, ALWAYS) \
    X(dot_general_2d_f32_pipeline, @"termite_dot_general_2d_f32", precise_library, 153, 157, ALWAYS) \
    X(dot_general_2d_f32_reduce_pipeline, @"termite_dot_general_2d_f32_reduce", precise_library, 154, 158, ALWAYS) \
    X(dot_general_2d_f32_small_m_tile_pipeline, @"termite_dot_general_2d_f32_small_m_tile", precise_library, 155, 159, ALWAYS) \
    X(dot_general_2d_f32_small_m_rows4_cols4_pipeline, @"termite_dot_general_2d_f32_small_m_rows4_cols4", precise_library, 156, 160, ALWAYS) \
    X(dot_general_2d_f32_small_m_rows4_cols8_pipeline, @"termite_dot_general_2d_f32_small_m_rows4_cols8", precise_library, 157, 161, ALWAYS) \
    X(dot_general_2d_f32_small_m_rows4_cols16_pipeline, @"termite_dot_general_2d_f32_small_m_rows4_cols16", precise_library, 158, 162, ALWAYS) \
    X(dot_general_rank1_outer_f32_pipeline, @"termite_dot_general_rank1_outer_f32", precise_library, 159, 163, ALWAYS) \
    X(dot_general_rank1_reduce_f32_pipeline, @"termite_dot_general_rank1_reduce_f32", precise_library, 160, 164, ALWAYS) \
    X(dot_general_batched_f32_pipeline, @"termite_dot_general_batched_f32", precise_library, 161, 165, ALWAYS) \
    X(lora_after_a_f32_pipeline, @"termite_lora_after_a_f32", precise_library, 162, 166, ALWAYS) \
    X(lora_finish_f32_pipeline, @"termite_lora_finish_f32", precise_library, 163, 167, ALWAYS) \
    X(lora_rank1_linear_f32_pipeline, @"termite_lora_rank1_linear_f32", precise_library, 164, 168, ALWAYS) \
    X(lora_grad_b_f32_pipeline, @"termite_lora_grad_b_f32", precise_library, 165, 169, ALWAYS) \
    X(lora_grad_b_transposed_f32_pipeline, @"termite_lora_grad_b_transposed_f32", precise_library, 166, 170, ALWAYS) \
    X(lora_grad_after_a_f32_pipeline, @"termite_lora_grad_after_a_f32", precise_library, 167, 171, ALWAYS) \
    X(lora_grad_a_f32_pipeline, @"termite_lora_grad_a_f32", precise_library, 168, 172, ALWAYS) \
    X(lora_rank1_grad_b_after_a_f32_pipeline, @"termite_lora_rank1_grad_b_after_a_f32", precise_library, 169, 173, ALWAYS) \
    X(scatter_add_axis0_f32_pipeline, @"termite_scatter_add_axis0_f32", precise_library, 170, 174, ALWAYS) \
    X(conv1d_f32_pipeline, @"termite_conv1d_f32", precise_library, 171, 175, ALWAYS) \
    X(conv1d_im2col_f32_pipeline, @"termite_conv1d_im2col_f32", precise_library, 172, 176, ALWAYS) \
    X(layer_norm_add_sum_rows_pipeline, @"termite_apply_add_layer_norm_sum_rows", library, 173, 177, ALWAYS) \
    X(conv2d_f32_pipeline, @"termite_conv2d_f32", precise_library, 174, 178, ALWAYS) \
    X(layer_norm_pipeline, @"termite_apply_layer_norm_1x", library, 175, 179, ALWAYS) \
    X(layer_norm_rows_pipeline, @"termite_apply_layer_norm_rows", library, 176, 180, ALWAYS) \
    X(layer_norm_add_pipeline, @"termite_apply_add_layer_norm_1x", library, 177, 181, ALWAYS) \
    X(layer_norm_add_rows_pipeline, @"termite_apply_add_layer_norm_rows", library, 178, 182, ALWAYS) \
    X(bias_activation_pipeline, @"termite_apply_bias_activation_2d", precise_library, 179, 183, ALWAYS) \
    X(bias_add_layer_norm_pipeline, @"termite_apply_bias_add_layer_norm_1x", library, 180, 184, ALWAYS) \
    X(bias_add_layer_norm_rows_pipeline, @"termite_apply_bias_add_layer_norm_rows", library, 181, 185, ALWAYS) \
    X(layer_norm_bwd_dx_pipeline, @"termite_layer_norm_bwd_dx", library, 310, 301, ALWAYS) \
    X(layer_norm_bwd_dgdb_pipeline, @"termite_layer_norm_bwd_dgdb", library, 311, 302, ALWAYS) \
    X(layer_norm_bwd_dgdb_partial_pipeline, @"termite_layer_norm_bwd_dgdb_partial", library, 312, 303, ALWAYS) \
    X(layer_norm_bwd_dgdb_reduce_pipeline, @"termite_layer_norm_bwd_dgdb_reduce", library, 313, 304, ALWAYS) \
    X(dense_qkv_split_pipeline, @"termite_dense_qkv_split_f32_2d", precise_library, 182, 186, ALWAYS) \
    X(dense_pair_split_pipeline, @"termite_dense_pair_split_f32_2d", precise_library, 183, 187, ALWAYS) \
    X(dense_qkv_split_bias_pipeline, @"termite_dense_qkv_split_bias_f32_2d", precise_library, 184, 188, ALWAYS) \
    X(dense_pair_split_bias_pipeline, @"termite_dense_pair_split_bias_f32_2d", precise_library, 185, 189, ALWAYS) \
    X(rms_norm_pipeline, @"termite_apply_rms_norm_1x", library, 186, 190, ALWAYS) \
    X(rms_norm_reduce_pipeline, @"termite_apply_rms_norm_1x_reduce", library, 187, 191, ALWAYS) \
    X(rms_inv_scale_pipeline, @"termite_compute_rms_inv_scale_1x", library, 188, 192, ALWAYS) \
    X(rms_norm_rows_pipeline, @"termite_apply_rms_norm_rows", library, 189, 193, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_9(X) \
    X(rms_norm_rows_reduce_pipeline, @"termite_apply_rms_norm_rows_reduce", library, 190, 194, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_10(X) \
    X(rms_norm_rows_simd4_reduce_pipeline, @"termite_apply_rms_norm_rows_simd4_reduce", library, 191, 197, ALWAYS) \
    X(lightonocr3_norm_cached1024_pipeline, @"termite_lightonocr3_norm_cached1024", library, 193, 195, ALWAYS) \
    X(lightonocr3_norm_cached2560_pipeline, @"termite_lightonocr3_norm_cached2560", library, 192, 196, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_11(X) \
    X(lightonocr3_gemv_pipeline, @"termite_lightonocr3_gemv_vec4", library, 207, 52, ALWAYS) \
    X(lightonocr3_gemv_rows2_pipeline, @"termite_lightonocr3_gemv_vec4_rows2", library, 208, 53, ALWAYS) \
    X(lightonocr3_gate_up_gemv_pipeline, @"termite_lightonocr3_gate_up_gemv", library, 209, 54, ALWAYS) \
    X(lightonocr3_pack_bf16_pipeline, @"termite_lightonocr3_pack_bf16", precise_library, 210, 55, ALWAYS) \
    X(lightonocr3_batch2_pipeline, @"termite_lightonocr3_batch2_vec4", library, 211, 56, ALWAYS) \
    X(lightonocr3_batch4_pipeline, @"termite_lightonocr3_batch4_vec4", library, 213, 57, ALWAYS) \
    X(lightonocr3_batch8_pipeline, @"termite_lightonocr3_batch8_vec4", library, 214, 58, ALWAYS) \
    X(lightonocr3_batch16_pipeline, @"termite_lightonocr3_batch16_vec4", library, 215, 59, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_12(X) \
    X(lightonocr3_four_row_segment_pipeline, @"termite_lightonocr3_four_row_segment", precise_library, 212, 60, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_13(X) \
    X(rms_norm_rows_triple_simd4_reduce_pipeline, @"termite_apply_rms_norm_rows_triple_simd4_reduce", library, 194, 198, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_14(X) \
    X(parallel_ffn_post_residual_simd4_pipeline, @"termite_apply_parallel_ffn_post_residual_simd4", library, 195, 199, ALWAYS) \
    X(rms_norm_add_pipeline, @"termite_apply_rms_norm_add_1x", library, 196, 200, ALWAYS) \
    X(rms_norm_add_sumsq_pipeline, @"termite_apply_rms_norm_add_1x_sumsq", library, 197, 201, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_15(X) \
    X(rms_norm_add_scale_sumsq_pipeline, @"termite_apply_rms_norm_add_scale_1x_sumsq", library, 198, 202, ALWAYS) \
    X(rms_norm_add_f16_input_pipeline, @"termite_apply_rms_norm_add_1x_f16_input", library, 199, 203, ALWAYS) \
    X(rms_norm_add_scale_pipeline, @"termite_apply_rms_norm_add_scale_1x", library, 200, 204, ALWAYS) \
    X(rms_norm_add_scale_rows_pipeline, @"termite_apply_rms_norm_add_scale_rows", library, 201, 205, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_16(X) \
    X(rms_norm_add_scale_simd4_pipeline, @"termite_apply_rms_norm_add_scale_simd4", library, 202, 206, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_17(X) \
    X(linear_pipeline, @"termite_apply_linear_1x", library, 203, 207, ALWAYS) \
    X(linear_reduce_pipeline, @"termite_apply_linear_1x_reduce", library, 204, 208, ALWAYS) \
    X(linear_bf16_pipeline, @"termite_apply_linear_bf16_1x", library, 205, 209, ALWAYS) \
    X(linear_bf16_reduce_pipeline, @"termite_apply_linear_bf16_1x_reduce", library, 206, 210, ALWAYS) \
    X(linear_f16_pipeline, @"termite_apply_linear_f16_1x", library, 216, 211, ALWAYS) \
    X(linear_f16_reduce_pipeline, @"termite_apply_linear_f16_1x_reduce", library, 217, 212, ALWAYS) \
    X(linear_f16_multi_row_pipeline, @"termite_apply_linear_f16_multi_row", library, 218, 213, ALWAYS) \
    X(linear_f16_multi_row_reduce_pipeline, @"termite_apply_linear_f16_multi_row_reduce", library, 219, 214, ALWAYS) \
    X(linear_bf16_multi_row_pipeline, @"termite_apply_linear_bf16_multi_row", library, 220, 215, ALWAYS) \
    X(linear_bf16_multi_row_reduce_pipeline, @"termite_apply_linear_bf16_multi_row_reduce", library, 221, 216, ALWAYS) \
    X(linear_bf16_multi_row_shared_reduce_pipeline, @"termite_apply_linear_bf16_multi_row_shared_reduce", library, 222, 217, ALWAYS) \
    X(linear_bf16_multi_row_tiled32_m16_pipeline, @"termite_apply_linear_bf16_multi_row_tiled32_m16", library, 223, 218, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_18(X) \
    X(linear_pair_reduce_pipeline, @"termite_apply_linear_pair_1x_reduce", library, 224, 219, ALWAYS) \
    X(linear_multi_row_pipeline, @"termite_apply_linear_multi_row", library, 225, 220, ALWAYS) \
    X(linear_multi_row_reduce_pipeline, @"termite_apply_linear_multi_row_reduce", library, 226, 221, ALWAYS) \
    X(linear_multi_row_tiled_f32_pipeline, @"termite_apply_linear_multi_row_tiled_f32", library, 227, 222, ALWAYS) \
    X(linear_multi_row_tiled_activation_f32_pipeline, @"termite_apply_linear_multi_row_tiled_activation_f32", library, 228, 223, ALWAYS) \
    X(linear_multi_row_tiled32_f32_pipeline, @"termite_apply_linear_multi_row_tiled32_f32", library, 229, 224, ALWAYS) \
    X(linear_multi_row_tiled32_activation_f32_pipeline, @"termite_apply_linear_multi_row_tiled32_activation_f32", library, 230, 225, ALWAYS) \
    X(linear_multi_row_tiled32_m16_f32_pipeline, @"termite_apply_linear_multi_row_tiled32_m16_f32", library, 231, 226, ALWAYS) \
    X(linear_multi_row_tiled32_m16_activation_f32_pipeline, @"termite_apply_linear_multi_row_tiled32_m16_activation_f32", library, 232, 227, ALWAYS) \
    X(linear_bias_pipeline, @"termite_apply_linear_bias_2d", library, 233, 454, ALWAYS) \
    X(argmax_logits_pipeline, @"termite_argmax_logits_1x", library, 234, 228, ALWAYS) \
    X(argmax_logits_partials_pipeline, @"termite_argmax_logits_partials", library, 235, 229, ALWAYS) \
    X(argmax_logits_suppress_partials_pipeline, @"termite_argmax_logits_suppress_partials", library, 236, 230, ALWAYS) \
    X(argmax_logits_reduce_pipeline, @"termite_argmax_logits_reduce", library, 237, 231, ALWAYS) \
    X(lighton_batch_argmax_partials_pipeline, @"termite_lighton_batch_argmax_partials", library, 238, 232, ALWAYS) \
    X(lighton_batch_argmax_reduce_pipeline, @"termite_lighton_batch_argmax_reduce", library, 239, 233, ALWAYS) \
    X(layer_norm_add_sum_pipeline, @"termite_apply_add_layer_norm_sum_1x", library, 240, 234, ALWAYS) \
    X(whisper_logits_partials_pipeline, @"termite_whisper_logits_partials", library, 241, 235, ALWAYS) \
    X(whisper_logits_reduce_pipeline, @"termite_whisper_logits_reduce", library, 242, 236, ALWAYS) \
    X(lm_head_top8_suppress_partials_pipeline, @"termite_lm_head_top8_suppress_partials", library, 243, 237, ALWAYS) \
    X(lm_head_top8_reduce_pipeline, @"termite_lm_head_top8_reduce", library, 244, 238, ALWAYS) \
    X(lm_head_q6_k_rescore_top8_pipeline, @"termite_lm_head_q6_k_rescore_top8", library, 245, 239, Q6_K) \
    X(argmax_logits_reduce_tg_pipeline, @"termite_argmax_logits_reduce_tg", library, 246, 240, ALWAYS) \
    X(sample_gumbel_partials_pipeline, @"termite_sample_gumbel_partials", library, 248, 242, ALWAYS) \
    X(sample_logits_pipeline, @"termite_sample_logits_1x", library, 247, 241, ALWAYS) \
    X(sample_topk_partials_pipeline, @"termite_sample_topk_partials", library, 249, 243, ALWAYS) \
    X(sample_topk_reduce_pipeline, @"termite_sample_topk_reduce", library, 250, 244, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_19(X) \
    X(activation_pipeline, @"termite_apply_activation_1x", precise_library, 251, 245, ALWAYS) \
    X(gelu_backward_pipeline, @"termite_apply_gelu_backward_1x", precise_library, 252, 246, ALWAYS) \
    X(ffn_gelu_backward_rank1_branches_pipeline, @"termite_ffn_gelu_backward_rank1_branches", precise_library, 253, 455, ALWAYS) \
    X(ffn_gelu_backward_rank1_first_branch_pipeline, @"termite_ffn_gelu_backward_rank1_first_branch", precise_library, 254, 456, ALWAYS) \
    X(ffn_gelu_backward_rank1_output_pipeline, @"termite_ffn_gelu_backward_rank1_output", precise_library, 255, 247, ALWAYS) \
    X(ffn_gelu_backward_rank1_first_output_pipeline, @"termite_ffn_gelu_backward_rank1_first_output", precise_library, 256, 248, ALWAYS) \
    X(activation_multiply_pipeline, @"termite_apply_activation_multiply_1x", precise_library, 257, 249, ALWAYS) \
    X(activation_multiply_f16_output_pipeline, @"termite_apply_activation_multiply_f16_output", precise_library, 258, 250, ALWAYS) \
    X(softmax_pipeline, @"termite_apply_softmax_rows", precise_library, 259, 251, ALWAYS) \
    X(moe_route_select_pipeline, @"termite_moe_route_select", precise_library, 260, 252, ALWAYS) \
    X(moe_route_select_tg_pipeline, @"termite_moe_route_select_tg", precise_library, 261, 253, ALWAYS) \
    X(moe_route_select_tg_register_pipeline, @"termite_moe_route_select_tg_register", precise_library, 262, 254, ALWAYS) \
    X(moe_route_select_tg_register_v2_pipeline, @"termite_moe_route_select_tg_register_v2", precise_library, 263, 255, ALWAYS) \
    X(moe_route_select_tg_map_pipeline, @"termite_moe_route_select_tg_map", precise_library, 264, 256, ALWAYS) \
    X(moe_route_map_slots_pipeline, @"termite_moe_route_map_slots", precise_library, 265, 257, ALWAYS) \
    X(moe_slot_q4_0_gate_up_pipeline, @"termite_moe_slot_q4_0_gate_up", library, 266, 258, Q4_0) \
    X(moe_slot_q4_0_down_pipeline, @"termite_moe_slot_q4_0_down", library, 267, 259, Q4_0) \
    X(moe_slot_reduce_pipeline, @"termite_moe_slot_reduce", precise_library, 268, 260, ALWAYS) \
    X(mapped_page_touch_pipeline, @"termite_mapped_page_touch", library, 269, 261, ALWAYS) \
    X(mapped_selected_expert_page_touch_pipeline, @"termite_mapped_selected_expert_page_touch", library, 270, 262, ALWAYS) \
    X(masked_bce_loss_pipeline, @"termite_masked_bce_with_logits_loss", precise_library, 271, 263, ALWAYS) \
    X(masked_bce_backward_pipeline, @"termite_masked_bce_with_logits_backward", precise_library, 272, 264, ALWAYS) \
    X(reduce_last_dim_pipeline, @"termite_reduce_last_dim_rows", precise_library, 273, 265, ALWAYS) \
    X(nomic_pool_normalize_pipeline, @"termite_nomic_pool_normalize", precise_library, 274, 266, ALWAYS) \
    X(reduce_axis_f32_pipeline, @"termite_reduce_axis_f32", precise_library, 275, 267, ALWAYS) \
    X(multiply_reduce_last_dim_pipeline, @"termite_multiply_reduce_last_dim_rows", precise_library, 276, 268, ALWAYS) \
    X(broadcast_last_dim_pipeline, @"termite_broadcast_last_dim_f32_2d", precise_library, 277, 269, ALWAYS) \
    X(broadcast_f32_pipeline, @"termite_broadcast_f32", precise_library, 278, 270, ALWAYS) \
    X(multiply_pipeline, @"termite_apply_multiply_1x", library, 279, 271, ALWAYS) \
    X(scale_pipeline, @"termite_apply_scale_1x", library, 280, 272, ALWAYS) \
    X(norm_residual_pipeline, @"termite_norm_residual", precise_library, 281, 273, ALWAYS) \
    X(gemma4_audio_clamp_silu_pipeline, @"termite_clamp_silu", precise_library, 282, 274, ALWAYS) \
    X(spatial_pool_prepare_pipeline, @"termite_spatial_pool_prepare", precise_library, 292, 275, ALWAYS) \
    X(audio_frontend_pipeline, @"antfly_audio_frontend", precise_library, 291, 276, ALWAYS) \
    X(audio_clipped_linear_pipeline, @"antfly_clipped_linear", precise_library, 283, 277, ALWAYS) \
    X(audio_attention_parallel_pipeline, @"antfly_audio_attention_parallel", precise_library, 284, 278, ALWAYS) \
    X(audio_norm_clamp_pipeline, @"antfly_norm_clamp", precise_library, 285, 279, ALWAYS) \
    X(audio_logmel_pipeline, @"antfly_audio_logmel", precise_library, 286, 280, ALWAYS) \
    X(audio_conv_matrix_pipeline, @"antfly_audio_conv_matrix", precise_library, 287, 281, ALWAYS) \
    X(audio_conv_tiled_pipeline, @"antfly_audio_conv_tiled", precise_library, 288, 282, ALWAYS) \
    X(audio_fft512_pipeline, @"antfly_audio_fft512", precise_library, 289, 283, ALWAYS) \
    X(gemma4_audio_clamp_pipeline, @"termite_gemma4_audio_clamp", library, 293, 284, ALWAYS) \
    X(gemma4_audio_glu_pipeline, @"termite_gemma4_audio_glu_rows", precise_library, 294, 285, ALWAYS) \
    X(gemma4_audio_dwconv_pipeline, @"antfly_causal_depthwise_segmented_f32", library, 295, 286, ALWAYS) \
    X(gemma4_audio_flatten_pipeline, @"termite_gemma4_audio_flatten_ctf", library, 296, 287, ALWAYS) \
    X(gemma4_audio_channel_norm_pipeline, @"termite_gemma4_audio_channel_norm_relu", precise_library, 297, 288, ALWAYS) \
    X(gemma4_audio_attention_pipeline, @"termite_gemma4_audio_local_attention", precise_library, 298, 289, ALWAYS) \
    X(gemma4_audio_attention_tg_pipeline, @"termite_gemma4_audio_local_attention_tg", precise_library, 299, 290, ALWAYS) \
    X(add_pipeline, @"termite_apply_add_1x", library, 300, 291, ALWAYS) \
    X(add_scale_pipeline, @"termite_apply_add_scale_1x", library, 301, 292, ALWAYS) \
    X(scaled_add_scale_pipeline, @"termite_apply_scaled_add_scale_1x", library, 302, 293, ALWAYS) \
    X(multiply_add_pipeline, @"termite_apply_multiply_add_1x", library, 303, 294, ALWAYS) \
    X(multiply_add2_pipeline, @"termite_apply_multiply_add2_1x", library, 304, 295, ALWAYS) \
    X(training_accumulate_pipeline, @"termite_training_accumulate_f32", library, 305, 296, ALWAYS) \
    X(training_adamw_pipeline, @"termite_training_adamw_f32", library, 306, 297, ALWAYS) \
    X(training_accumulate_into_pipeline, @"termite_training_accumulate_into_f32", library, 307, 298, ALWAYS) \
    X(training_adamw_into_pipeline, @"termite_training_adamw_into_f32", library, 308, 299, ALWAYS) \
    X(training_sumsq_pipeline, @"termite_training_sumsq_f32", library, 309, 300, ALWAYS) \
    X(subtract_pipeline, @"termite_apply_subtract_1x", library, 314, 305, ALWAYS) \
    X(divide_pipeline, @"termite_apply_divide_1x", library, 315, 306, ALWAYS) \
    X(less_than_pipeline, @"termite_apply_less_than_1x", library, 316, 307, ALWAYS) \
    X(where_select_pipeline, @"termite_apply_where_select_1x", library, 317, 308, ALWAYS) \
    X(i2_s_quantize_pipeline, @"termite_quantize_rows_i8", library, 318, 309, ALWAYS) \
    X(q1_0_pipeline, @"termite_q1_0_linear", library, 319, 310, ALWAYS) \
    X(i8_s_pipeline, @"termite_i8_s_linear", library, 320, 311, ALWAYS) \
    X(q2_k_pipeline, @"termite_q2_k_linear", library, 321, 312, Q2_K) \
    X(antfly_q2_k_small_batch_pipeline, @"antfly_q2_k_small_batch_msl_v1", library, 322, 313, Q2_K) \
    X(antfly_q2_k_small_batch_bias_pipeline, @"antfly_q2_k_small_batch_bias_msl_v1", library, 323, 314, Q2_K) \
    X(antfly_q2_k_small_batch_bias_gelu_pipeline, @"antfly_q2_k_small_batch_bias_gelu_msl_v1", library, 324, 315, Q2_K) \
    X(q3_k_pipeline, @"termite_q3_k_linear", library, 325, 316, Q3_K) \
    X(antfly_q3_k_small_batch_pipeline, @"antfly_q3_k_small_batch_msl_v1", library, 326, 317, Q3_K) \
    X(antfly_q3_k_small_batch_bias_pipeline, @"antfly_q3_k_small_batch_bias_msl_v1", library, 327, 318, Q3_K) \
    X(antfly_q3_k_small_batch_bias_gelu_pipeline, @"antfly_q3_k_small_batch_bias_gelu_msl_v1", library, 328, 319, Q3_K) \
    X(q4_k_pipeline, @"termite_q4_k_linear", library, 329, 320, Q4_K) \
    X(antfly_q4_k_small_batch_bias_gelu_pipeline, @"antfly_q4_k_small_batch_bias_gelu_msl_v1", library, 335, 326, Q4_K) \
    X(q4_k_reduce_pipeline, @"termite_q4_k_linear_1x_reduce", library, 336, 327, Q4_K) \
    X(q4_k_pair_pipeline, @"termite_q4_k_pair_linear", library, 338, 329, Q4_K) \
    X(q4_k_pair_reduce_pipeline, @"termite_q4_k_pair_linear_1x_reduce", library, 339, 330, Q4_K) \
    X(q4_k_pair_activation_reduce_pipeline, @"termite_q4_k_pair_activation_1x_reduce", library, 340, 331, Q4_K) \
    X(q4_k_pair_activation_reduce_f16_output_pipeline, @"termite_q4_k_pair_activation_1x_reduce_out_f16", library, 341, 332, Q4_K) \
    X(q4_k_activation_rhs_reduce_pipeline, @"termite_q4_k_activation_rhs_1x_reduce", library, 342, 333, Q4_K) \
    X(q4_0_activation_rhs_reduce_pipeline, @"termite_q4_0_activation_rhs_1x_reduce", library, 343, 334, Q4_0) \
    X(q4_0_activation_rhs_reduce_f16_output_pipeline, @"termite_q4_0_activation_rhs_1x_reduce_out_f16", library, 344, 335, Q4_0) \
    X(q4_0_pipeline, @"termite_q4_0_linear", library, 345, 336, Q4_0) \
    X(q4_0_id_pipeline, @"termite_q4_0_linear_id", library, 346, 337, Q4_0) \
    X(q4_0_id_a4b_gate_up_pipeline, @"termite_q4_0_linear_id_a4b_gate_up", library, 347, 338, Q4_0) \
    X(q4_0_id_a4b_down_pipeline, @"termite_q4_0_linear_id_a4b_down", library, 348, 339, Q4_0) \
    X(q4_0_pair_id_pipeline, @"termite_q4_0_pair_linear_id", library, 349, 340, Q4_0) \
    X(moe_merged_gate_up_activation_pipeline, @"termite_moe_merged_gate_up_activation", library, 350, 341, ALWAYS) \
    X(moe_scatter_add_pipeline, @"termite_moe_scatter_add", library, 351, 342, ALWAYS) \
    X(antfly_q4_0_small_batch_pipeline, @"antfly_q4_0_small_batch_msl_v1", library, 352, 343, Q4_0) \
    X(q4_0_pair_pipeline, @"termite_q4_0_pair_linear", library, 353, 344, Q4_0) \
    X(q4_0_pair_reduce_pipeline, @"termite_q4_0_pair_linear_1x_reduce", library, 354, 345, Q4_0) \
    X(q4_0_pair_activation_reduce_pipeline, @"termite_q4_0_pair_activation_1x_reduce", library, 355, 346, Q4_0)

#define ANTFLY_METAL_PIPELINE_GROUP_20(X) \
    X(q4_0_pair_activation_small_batch_r2_pipeline, @"termite_q4_0_pair_activation_multiply_r2_ext", library, 356, 347, Q4_0) \
    X(q4_0_pair_activation_small_batch_r3_pipeline, @"termite_q4_0_pair_activation_multiply_r3_ext", library, 357, 348, Q4_0) \
    X(q4_0_pair_activation_small_batch_r4_pipeline, @"termite_q4_0_pair_activation_multiply_r4_ext", library, 358, 349, Q4_0) \
    X(q4_0_pair_activation_small_batch_pipeline, @"termite_q4_0_pair_activation_multiply_r5_ext", library, 359, 350, Q4_0) \
    X(q4_0_pair_activation_reduce_f16_output_pipeline, @"termite_q4_0_pair_activation_1x_reduce_out_f16", library, 360, 351, Q4_0) \
    X(q4_0_pair_activation_rms_scale_reduce_f16_output_pipeline, @"termite_q4_0_pair_activation_rms_scale_1x_reduce_out_f16", library, 361, 352, Q4_0) \
    X(q4_0_reduce_pipeline, @"termite_q4_0_linear_1x_reduce", library, 362, 353, Q4_0) \
    X(q4_0_reduce_nr4_nsg4_pipeline, @"termite_q4_0_linear_1x_reduce_nr4_nsg4", library, 363, 354, Q4_0) \
    X(q4_0_reduce_nr4_nsg4_nq8_pipeline, @"termite_q4_0_linear_1x_reduce_nr4_nsg4_nq8", library, 364, 355, Q4_0) \
    X(q4_0_reduce_nr8_nsg4_pipeline, @"termite_q4_0_linear_1x_reduce_nr8_nsg4", library, 365, 356, Q4_0)

#define ANTFLY_METAL_PIPELINE_GROUP_21(X) \
    X(q4_0_reduce_small_pipeline, @"termite_q4_0_linear_1x_reduce_small", library, 366, 357, Q4_0) \
    X(q4_0_reduce_sumsq_pipeline, @"termite_q4_0_linear_1x_reduce_sumsq", library, 367, 358, Q4_0) \
    X(q4_0_reduce_f16_input_pipeline, @"termite_q4_0_linear_1x_reduce_in_f16", library, 368, 359, Q4_0) \
    X(q4_0_reduce_f16_input_small_pipeline, @"termite_q4_0_linear_1x_reduce_in_f16_small", library, 369, 360, Q4_0) \
    X(q4_0_reduce_f16_input_sumsq_pipeline, @"termite_q4_0_linear_1x_reduce_in_f16_sumsq", library, 370, 361, Q4_0) \
    X(q4_0_reduce_f16_output_pipeline, @"termite_q4_0_linear_1x_reduce_out_f16", library, 371, 362, Q4_0) \
    X(q4_0_reduce_f16_input_f16_output_pipeline, @"termite_q4_0_linear_1x_reduce_in_f16_out_f16", library, 372, 363, Q4_0)

#define ANTFLY_METAL_PIPELINE_GROUP_22(X) \
    X(q4_1_pipeline, @"termite_q4_1_linear", library, 373, 364, Q4_1) \
    X(antfly_q4_1_small_batch_pipeline, @"antfly_q4_1_small_batch_msl_v1", library, 374, 365, Q4_1) \
    X(q4_1_reduce_pipeline, @"termite_q4_1_linear_1x_reduce", library, 375, 366, Q4_1) \
    X(q5_0_pipeline, @"termite_q5_0_linear", library, 376, 367, Q5_0) \
    X(antfly_q5_0_small_batch_pipeline, @"antfly_q5_0_small_batch_msl_v1", library, 377, 368, Q5_0) \
    X(q5_0_reduce_pipeline, @"termite_q5_0_linear_1x_reduce", library, 378, 369, Q5_0) \
    X(q5_1_pipeline, @"termite_q5_1_linear", library, 379, 370, Q5_1) \
    X(antfly_q5_1_small_batch_pipeline, @"antfly_q5_1_small_batch_msl_v1", library, 380, 371, Q5_1) \
    X(q5_1_reduce_pipeline, @"termite_q5_1_linear_1x_reduce", library, 381, 372, Q5_1) \
    X(q8_0_pipeline, @"termite_q8_0_linear", library, 382, 373, Q8_0) \
    X(q8_0_pair_pipeline, @"termite_q8_0_pair_linear", library, 383, 374, Q8_0) \
    X(q8_0_mmv_pipeline, @"termite_q8_0_linear_mmv", library, 384, 375, Q8_0) \
    X(q8_0_mmv_f16_input_pipeline, @"termite_q8_0_linear_mmv_in_f16", library, 385, 376, Q8_0) \
    X(q8_0_rms_scale_mmv_pipeline, @"termite_q8_0_linear_rms_scale_mmv", library, 386, 377, Q8_0) \
    X(q8_0_small_batch_r2_pipeline, @"termite_q8_0_linear_r2_ext", library, 387, 378, Q8_0) \
    X(q8_0_small_batch_r3_pipeline, @"termite_q8_0_linear_r3_ext", library, 388, 379, Q8_0) \
    X(q8_0_small_batch_r4_pipeline, @"termite_q8_0_linear_r4_ext", library, 389, 380, Q8_0) \
    X(q8_0_small_batch_pipeline, @"termite_q8_0_linear_r5_ext", library, 390, 381, Q8_0) \
    X(antfly_q8_0_small_batch_pipeline, @"antfly_q8_0_small_batch_msl_v1", library, 391, 382, Q8_0) \
    X(antfly_q8_0_small_batch_bias_pipeline, @"antfly_q8_0_small_batch_bias_msl_v1", library, 392, 383, Q8_0) \
    X(antfly_q8_0_small_batch_bias_gelu_pipeline, @"antfly_q8_0_small_batch_bias_gelu_msl_v1", library, 393, 384, Q8_0) \
    X(antfly_q8_0_small_batch_relu_pipeline, @"antfly_q8_0_small_batch_relu_msl_v1", library, 394, 385, Q8_0)

#define ANTFLY_METAL_PIPELINE_GROUP_23(X) \
    X(q8_0_pair_mmv_pipeline, @"termite_q8_0_pair_linear_mmv", library, 395, 386, Q8_0) \
    X(q8_0_pair_small_batch_r2_pipeline, @"termite_q8_0_pair_linear_r2_ext", library, 396, 387, Q8_0) \
    X(q8_0_pair_small_batch_r3_pipeline, @"termite_q8_0_pair_linear_r3_ext", library, 397, 388, Q8_0) \
    X(q8_0_pair_small_batch_r4_pipeline, @"termite_q8_0_pair_linear_r4_ext", library, 398, 389, Q8_0) \
    X(q8_0_pair_small_batch_pipeline, @"termite_q8_0_pair_linear_r5_ext", library, 399, 390, Q8_0) \
    X(q8_0_qkv_mmv_pipeline, @"termite_q8_0_qkv_linear_mmv", library, 400, 391, Q8_0)

#define ANTFLY_METAL_PIPELINE_GROUP_24(X) \
    X(q8_0_pair_activation_reduce_pipeline, @"termite_q8_0_pair_activation_multiply_1x_reduce4", library, 401, 392, Q8_0) \
    X(q8_0_pair_activation_mmv_pipeline, @"termite_q8_0_pair_activation_multiply_1r_ext", library, 402, 393, Q8_0) \
    X(q8_0_pair_activation_rms_scale_mmv_pipeline, @"termite_q8_0_pair_activation_multiply_rms_scale_1r_ext", library, 403, 394, Q8_0) \
    X(q8_0_pair_activation_rms_scale_mmv_f16_output_pipeline, @"termite_q8_0_pair_activation_multiply_rms_scale_1r_ext_out_f16", library, 404, 395, Q8_0) \
    X(q8_0_pair_activation_small_batch_r2_pipeline, @"termite_q8_0_pair_activation_multiply_r2_ext", library, 405, 396, Q8_0) \
    X(q8_0_pair_activation_small_batch_r3_pipeline, @"termite_q8_0_pair_activation_multiply_r3_ext", library, 406, 397, Q8_0) \
    X(q8_0_pair_activation_small_batch_r4_pipeline, @"termite_q8_0_pair_activation_multiply_r4_ext", library, 407, 398, Q8_0) \
    X(q8_0_pair_activation_small_batch_pipeline, @"termite_q8_0_pair_activation_multiply_r5_ext", library, 408, 399, Q8_0)

#define ANTFLY_METAL_PIPELINE_GROUP_25(X) \
    X(q8_0_activation_multiply_reduce_pipeline, @"termite_q8_0_activation_multiply_1x_reduce4", library, 409, 400, Q8_0) \
    X(q8_0_activation_multiply_mmv_pipeline, @"termite_q8_0_activation_multiply_1r_ext", library, 410, 401, Q8_0) \
    X(q8_1_pipeline, @"termite_q8_1_linear", library, 411, 402, Q8_1) \
    X(antfly_q8_1_small_batch_pipeline, @"antfly_q8_1_small_batch_msl_v1", library, 412, 403, Q8_1) \
    X(q8_1_reduce_pipeline, @"termite_q8_1_linear_1x_reduce", library, 413, 404, Q8_1) \
    X(q5_k_pipeline, @"termite_q5_k_linear", library, 414, 405, Q5_K) \
    X(antfly_q4_k_small_batch_pipeline, @"antfly_q4_k_small_batch_msl_v1", library, 330, 321, Q4_K) \
    X(florence_q4_k_mm_matrix_pipeline, @"termite_florence_q4_k_mm_matrix_m64_n64", library, 331, 322, Q4_K) \
    X(florence_q4_k_mm_matrix_f16_input_pipeline, @"termite_florence_q4_k_mm_matrix_m64_n64_in_f16", library, 332, 323, Q4_K) \
    X(florence_q4_k_mm_nr4_pipeline, @"termite_florence_q4_k_mm_nr4", library, 333, 324, Q4_K) \
    X(antfly_q4_k_small_batch_bias_pipeline, @"antfly_q4_k_small_batch_bias_msl_v1", library, 334, 325, Q4_K) \
    X(antfly_q5_k_small_batch_pipeline, @"antfly_q5_k_small_batch_msl_v1", library, 415, 406, Q5_K) \
    X(antfly_q5_k_small_batch_bias_pipeline, @"antfly_q5_k_small_batch_bias_msl_v1", library, 416, 407, Q5_K) \
    X(antfly_q5_k_small_batch_bias_gelu_pipeline, @"antfly_q5_k_small_batch_bias_gelu_msl_v1", library, 417, 408, Q5_K) \
    X(q5_k_reduce_pipeline, @"termite_q5_k_linear_1x_reduce", library, 418, 409, Q5_K) \
    X(q4_k_reduce_v2_pipeline, @"termite_q4_k_linear_1x_reduce_v2", library, 337, 328, Q4_K) \
    X(q6_k_pipeline, @"termite_q6_k_linear", library, 419, 410, Q6_K) \
    X(antfly_q6_k_small_batch_pipeline, @"antfly_q6_k_small_batch_msl_v1", library, 420, 411, Q6_K) \
    X(q6_k_high_row_mm_matrix_pipeline, @"termite_q6_k_high_row_mm_matrix_m64_n64", library, 421, 412, Q6_K) \
    X(antfly_q6_k_small_batch_bias_pipeline, @"antfly_q6_k_small_batch_bias_msl_v1", library, 422, 413, Q6_K) \
    X(antfly_q6_k_small_batch_bias_gelu_pipeline, @"antfly_q6_k_small_batch_bias_gelu_msl_v1", library, 423, 414, Q6_K) \
    X(q6_k_reduce_pipeline, @"termite_q6_k_linear_1x_reduce", library, 424, 415, Q6_K) \
    X(q6_k_reduce_nsg4_pipeline, @"termite_q6_k_linear_1x_reduce_nsg4", library, 425, 416, Q6_K) \
    X(q6_k_reduce_nsg8_pipeline, @"termite_q6_k_linear_1x_reduce_nsg8", library, 426, 417, Q6_K) \
    X(q6_k_reduce_nbodd_pipeline, @"termite_q6_k_linear_1x_reduce_nbodd", library, 427, 418, Q6_K) \
    X(q6_k_reduce_nbodd_nr1_pipeline, @"termite_q6_k_linear_1x_reduce_nbodd_nr1", library, 428, 419, Q6_K) \
    X(q6_k_reduce_nbodd_nr4_nsg1_pipeline, @"termite_q6_k_linear_1x_reduce_nbodd_nr4_nsg1", library, 429, 420, Q6_K) \
    X(q6_k_reduce_nbodd_nr4_nsg2_pipeline, @"termite_q6_k_linear_1x_reduce_nbodd_nr4_nsg2", library, 430, 421, Q6_K) \
    X(q6_k_reduce_nbodd_nr6_nsg1_pipeline, @"termite_q6_k_linear_1x_reduce_nbodd_nr6_nsg1", library, 431, 422, Q6_K) \
    X(q6_k_reduce_nbodd_nr8_nsg1_pipeline, @"termite_q6_k_linear_1x_reduce_nbodd_nr8_nsg1", library, 432, 423, Q6_K) \
    X(q6_k_r2_reduce_pipeline, @"termite_q6_k_linear_r2_reduce", library, 433, 424, Q6_K) \
    X(q6_k_reduce_f16_input_pipeline, @"termite_q6_k_linear_1x_reduce_in_f16", library, 434, 425, Q6_K) \
    X(q6_k_pair_pipeline, @"termite_q6_k_pair_linear", library, 435, 426, Q6_K) \
    X(q8_k_pipeline, @"termite_q8_k_linear", library, 436, 427, Q8_K) \
    X(antfly_q8_k_small_batch_pipeline, @"antfly_q8_k_small_batch_msl_v1", library, 437, 428, Q8_K) \
    X(q8_k_reduce_pipeline, @"termite_q8_k_linear_1x_reduce", library, 438, 429, Q8_K) \
    X(iq4_nl_pipeline, @"termite_iq4_nl_linear", library, 439, 430, ALWAYS) \
    X(iq4_nl_reduce_pipeline, @"termite_iq4_nl_linear_1x_reduce", library, 440, 431, ALWAYS) \
    X(iq4_xs_pipeline, @"termite_iq4_xs_linear", library, 441, 432, ALWAYS) \
    X(iq4_xs_reduce_pipeline, @"termite_iq4_xs_linear_1x_reduce", library, 442, 433, ALWAYS) \
    X(mxfp4_pipeline, @"termite_mxfp4_linear", library, 443, 434, ALWAYS) \
    X(mxfp4_reduce_pipeline, @"termite_mxfp4_linear_1x_reduce", library, 444, 435, ALWAYS) \
    X(nvfp4_pipeline, @"termite_nvfp4_linear", library, 445, 436, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_26(X) \
    X(iq2_xs_pipeline, @"termite_iq2_xs_linear", library, 446, 437, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_27(X) \
    X(i2_s_pipeline, @"termite_i2_s_linear", library, 447, 438, ALWAYS) \
    X(i2_s_pair_pipeline, @"termite_i2_s_pair_linear", library, 448, 439, ALWAYS) \
    X(i2_s_linear_i8_pipeline, @"termite_i2_s_linear_i8", library, 449, 440, ALWAYS) \
    X(tl1_pipeline, @"termite_tl1_linear", library, 450, 441, ALWAYS) \
    X(tl2_pipeline, @"termite_tl2_linear", library, 451, 442, ALWAYS) \
    X(encode_polar4_key_pipeline, @"termite_encode_polar4_key", library, 452, 443, ALWAYS) \
    X(encode_turbo3_key_pipeline, @"termite_encode_turbo3_key", library, 453, 444, ALWAYS) \
    X(encode_f16_convert_pipeline, @"termite_encode_f16_convert", library, 454, 445, ALWAYS) \
    X(encode_int8_per_head_pipeline, @"termite_encode_int8_per_head", library, 455, 446, ALWAYS) \
    X(polar4_attention_span_pipeline, @"termite_polar4_attention_span", library, 456, 447, ALWAYS) \
    X(turbo3_attention_span_pipeline, @"termite_turbo3_attention_span", library, 457, 448, ALWAYS)

#define ANTFLY_METAL_PIPELINE_GROUP_28(X) \
    X(audio_pad_pipeline, @"antfly_audio_pad", precise_library, 290, 457, ALWAYS)

#define ANTFLY_METAL_PIPELINES(X) \
    ANTFLY_METAL_PIPELINE_GROUP_0(X) \
    ANTFLY_METAL_PIPELINE_GROUP_1(X) \
    ANTFLY_METAL_PIPELINE_GROUP_2(X) \
    ANTFLY_METAL_PIPELINE_GROUP_3(X) \
    ANTFLY_METAL_PIPELINE_GROUP_4(X) \
    ANTFLY_METAL_PIPELINE_GROUP_5(X) \
    ANTFLY_METAL_PIPELINE_GROUP_6(X) \
    ANTFLY_METAL_PIPELINE_GROUP_7(X) \
    ANTFLY_METAL_PIPELINE_GROUP_8(X) \
    ANTFLY_METAL_PIPELINE_GROUP_9(X) \
    ANTFLY_METAL_PIPELINE_GROUP_10(X) \
    ANTFLY_METAL_PIPELINE_GROUP_11(X) \
    ANTFLY_METAL_PIPELINE_GROUP_12(X) \
    ANTFLY_METAL_PIPELINE_GROUP_13(X) \
    ANTFLY_METAL_PIPELINE_GROUP_14(X) \
    ANTFLY_METAL_PIPELINE_GROUP_15(X) \
    ANTFLY_METAL_PIPELINE_GROUP_16(X) \
    ANTFLY_METAL_PIPELINE_GROUP_17(X) \
    ANTFLY_METAL_PIPELINE_GROUP_18(X) \
    ANTFLY_METAL_PIPELINE_GROUP_19(X) \
    ANTFLY_METAL_PIPELINE_GROUP_20(X) \
    ANTFLY_METAL_PIPELINE_GROUP_21(X) \
    ANTFLY_METAL_PIPELINE_GROUP_22(X) \
    ANTFLY_METAL_PIPELINE_GROUP_23(X) \
    ANTFLY_METAL_PIPELINE_GROUP_24(X) \
    ANTFLY_METAL_PIPELINE_GROUP_25(X) \
    ANTFLY_METAL_PIPELINE_GROUP_26(X) \
    ANTFLY_METAL_PIPELINE_GROUP_27(X) \
    ANTFLY_METAL_PIPELINE_GROUP_28(X)

// Runtime creation failure policy and its diagnostic order. Conditional
// requests retain their independent gates in the constructor. Optional fields
// here participate only in diagnostics when another requirement fails.
#define ANTFLY_METAL_PIPELINE_VALIDATION(X) \
    X(embed_absolute_position_pipeline, 1, "embed_absolute_position") \
    X(embedding_lookup_pipeline, 1, "embedding_lookup") \
    X(q4_0_get_rows_pipeline, 1, "q4_0_get_rows") \
    X(q4_0_set_rows_pipeline, 1, "q4_0_set_rows") \
    X(q4_0_cpy_q_to_f32_pipeline, 1, "q4_0_cpy_q_to_f32") \
    X(q4_0_cpy_f32_to_q_pipeline, 1, "q4_0_cpy_f32_to_q") \
    X(q4_1_get_rows_pipeline, 1, "q4_1_get_rows") \
    X(q4_1_set_rows_pipeline, 1, "q4_1_set_rows") \
    X(q4_1_cpy_q_to_f32_pipeline, 1, "q4_1_cpy_q_to_f32") \
    X(q4_1_cpy_f32_to_q_pipeline, 1, "q4_1_cpy_f32_to_q") \
    X(q5_0_get_rows_pipeline, 1, "q5_0_get_rows") \
    X(q5_0_set_rows_pipeline, 1, "q5_0_set_rows") \
    X(q5_0_cpy_q_to_f32_pipeline, 1, "q5_0_cpy_q_to_f32") \
    X(q5_0_cpy_f32_to_q_pipeline, 1, "q5_0_cpy_f32_to_q") \
    X(q5_1_get_rows_pipeline, 1, "q5_1_get_rows") \
    X(q5_1_set_rows_pipeline, 1, "q5_1_set_rows") \
    X(q5_1_cpy_q_to_f32_pipeline, 1, "q5_1_cpy_q_to_f32") \
    X(q5_1_cpy_f32_to_q_pipeline, 1, "q5_1_cpy_f32_to_q") \
    X(q4_k_get_rows_pipeline, 1, "q4_k_get_rows") \
    X(q4_k_set_rows_pipeline, 1, "q4_k_set_rows") \
    X(q4_k_cpy_q_to_f32_pipeline, 1, "q4_k_cpy_q_to_f32") \
    X(q4_k_cpy_f32_to_q_pipeline, 1, "q4_k_cpy_f32_to_q") \
    X(q5_k_get_rows_pipeline, 1, "q5_k_get_rows") \
    X(q5_k_set_rows_pipeline, 1, "q5_k_set_rows") \
    X(q5_k_cpy_q_to_f32_pipeline, 1, "q5_k_cpy_q_to_f32") \
    X(q5_k_cpy_f32_to_q_pipeline, 1, "q5_k_cpy_f32_to_q") \
    X(q6_k_get_rows_pipeline, 1, "q6_k_get_rows") \
    X(q6_k_set_rows_pipeline, 1, "q6_k_set_rows") \
    X(q6_k_cpy_q_to_f32_pipeline, 1, "q6_k_cpy_q_to_f32") \
    X(q6_k_cpy_f32_to_q_pipeline, 1, "q6_k_cpy_f32_to_q") \
    X(q8_0_get_rows_pipeline, 1, "q8_0_get_rows") \
    X(q8_0_set_rows_pipeline, 1, "q8_0_set_rows") \
    X(q8_0_cpy_q_to_f32_pipeline, 1, "q8_0_cpy_q_to_f32") \
    X(q8_0_cpy_f32_to_q_pipeline, 1, "q8_0_cpy_f32_to_q") \
    X(q8_1_get_rows_pipeline, 1, "q8_1_get_rows") \
    X(q8_1_set_rows_pipeline, 1, "q8_1_set_rows") \
    X(q8_1_cpy_q_to_f32_pipeline, 1, "q8_1_cpy_q_to_f32") \
    X(q8_1_cpy_f32_to_q_pipeline, 1, "q8_1_cpy_f32_to_q") \
    X(rope_pipeline, 1, "rope") \
    X(mrope_pipeline, 0, "mrope") \
    X(rope_pair_pipeline, 1, "rope_pair") \
    X(head_rms_rope_pipeline, 1, "head_rms_rope") \
    X(attention_f32_pipeline, 1, "attention_f32") \
    X(attention_f32_prefill_pipeline, 1, "attention_f32_prefill") \
    X(attention_paged_pipeline, 1, "attention_paged") \
    X(paged_f32_kv_seed_pipeline, 1, "paged_f32_kv_seed") \
    X(paged_f16_kv_seed_pipeline, 1, "paged_f16_kv_seed") \
    X(paged_f32_v_seed_pipeline, 1, "paged_f32_v_seed") \
    X(slice_last_dim_f32_2d_pipeline, 1, "slice_last_dim_f32_2d") \
    X(gather_axis0_add_bias_f32_2d_pipeline, 1, "gather_axis0_add_bias_f32_2d") \
    X(transpose_f32_pipeline, 1, "transpose_f32") \
    X(dot_general_2d_f32_pipeline, 1, "dot_general_2d_f32") \
    X(dot_general_2d_f32_reduce_pipeline, 1, "dot_general_2d_f32_reduce") \
    X(dot_general_rank1_outer_f32_pipeline, 0, "dot_general_rank1_outer_f32") \
    X(dot_general_rank1_reduce_f32_pipeline, 0, "dot_general_rank1_reduce_f32") \
    X(dot_general_batched_f32_pipeline, 1, "dot_general_batched_f32") \
    X(lora_after_a_f32_pipeline, 1, "lora_after_a_f32") \
    X(lora_finish_f32_pipeline, 1, "lora_finish_f32") \
    X(lora_rank1_linear_f32_pipeline, 0, "lora_rank1_linear_f32") \
    X(lora_grad_b_f32_pipeline, 0, "lora_grad_b_f32") \
    X(lora_grad_b_transposed_f32_pipeline, 0, "lora_grad_b_transposed_f32") \
    X(lora_grad_after_a_f32_pipeline, 0, "lora_grad_after_a_f32") \
    X(lora_grad_a_f32_pipeline, 0, "lora_grad_a_f32") \
    X(lora_rank1_grad_b_after_a_f32_pipeline, 0, "lora_rank1_grad_b_after_a_f32") \
    X(scatter_add_axis0_f32_pipeline, 1, "scatter_add_axis0_f32") \
    X(conv1d_f32_pipeline, 1, "conv1d_f32") \
    X(conv2d_f32_pipeline, 1, "conv2d_f32") \
    X(layer_norm_pipeline, 1, "layer_norm") \
    X(rms_norm_pipeline, 1, "rms_norm") \
    X(rms_norm_reduce_pipeline, 1, "rms_norm_reduce") \
    X(rms_norm_rows_pipeline, 1, "rms_norm_rows") \
    X(rms_norm_add_pipeline, 1, "rms_norm_add") \
    X(rms_norm_add_sumsq_pipeline, 1, "rms_norm_add_sumsq") \
    X(rms_norm_add_f16_input_pipeline, 1, "rms_norm_add_f16_input") \
    X(rms_norm_add_scale_pipeline, 1, "rms_norm_add_scale") \
    X(rms_norm_add_scale_rows_pipeline, 1, "rms_norm_add_scale_rows") \
    X(linear_pipeline, 1, "linear") \
    X(linear_bias_pipeline, 1, "linear_bias") \
    X(linear_reduce_pipeline, 1, "linear_reduce") \
    X(linear_bf16_pipeline, 1, "linear_bf16") \
    X(linear_bf16_reduce_pipeline, 1, "linear_bf16_reduce") \
    X(linear_f16_pipeline, 1, "linear_f16") \
    X(linear_f16_reduce_pipeline, 1, "linear_f16_reduce") \
    X(linear_f16_multi_row_pipeline, 1, "linear_f16_multi_row") \
    X(linear_f16_multi_row_reduce_pipeline, 1, "linear_f16_multi_row_reduce") \
    X(linear_bf16_multi_row_pipeline, 1, "linear_bf16_multi_row") \
    X(linear_bf16_multi_row_reduce_pipeline, 1, "linear_bf16_multi_row_reduce") \
    X(linear_bf16_multi_row_tiled32_m16_pipeline, 1, "linear_bf16_multi_row_tiled32_m16") \
    X(linear_pair_reduce_pipeline, 1, "linear_pair_reduce") \
    X(linear_multi_row_pipeline, 1, "linear_multi_row") \
    X(linear_multi_row_reduce_pipeline, 1, "linear_multi_row_reduce") \
    X(argmax_logits_pipeline, 1, "argmax_logits") \
    X(argmax_logits_partials_pipeline, 1, "argmax_logits_partials") \
    X(argmax_logits_suppress_partials_pipeline, 1, "argmax_logits_suppress_partials") \
    X(argmax_logits_reduce_pipeline, 1, "argmax_logits_reduce") \
    X(sample_logits_pipeline, 1, "sample_logits") \
    X(sample_topk_partials_pipeline, 1, "sample_topk_partials") \
    X(sample_topk_reduce_pipeline, 1, "sample_topk_reduce") \
    X(activation_pipeline, 1, "activation") \
    X(gelu_backward_pipeline, 1, "gelu_backward") \
    X(activation_multiply_pipeline, 1, "activation_multiply") \
    X(softmax_pipeline, 1, "softmax") \
    X(reduce_last_dim_pipeline, 1, "reduce_last_dim") \
    X(reduce_axis_f32_pipeline, 1, "reduce_axis_f32") \
    X(multiply_reduce_last_dim_pipeline, 1, "multiply_reduce_last_dim") \
    X(broadcast_last_dim_pipeline, 1, "broadcast_last_dim") \
    X(broadcast_f32_pipeline, 1, "broadcast_f32") \
    X(multiply_pipeline, 1, "multiply") \
    X(scale_pipeline, 1, "scale") \
    X(add_pipeline, 1, "add") \
    X(add_scale_pipeline, 1, "add_scale") \
    X(subtract_pipeline, 1, "subtract") \
    X(divide_pipeline, 1, "divide") \
    X(less_than_pipeline, 1, "less_than") \
    X(where_select_pipeline, 1, "where_select") \
    X(i2_s_quantize_pipeline, 1, "i2_s_quantize") \
    X(q1_0_pipeline, 1, "q1_0") \
    X(i8_s_pipeline, 1, "i8_s") \
    X(q2_k_pipeline, 1, "q2_k") \
    X(q3_k_pipeline, 1, "q3_k") \
    X(q4_k_pipeline, 1, "q4_k") \
    X(q4_k_reduce_pipeline, 1, "q4_k_reduce") \
    X(q4_k_pair_pipeline, 1, "q4_k_pair") \
    X(q4_k_pair_reduce_pipeline, 0, "q4_k_pair_reduce") \
    X(q4_k_pair_activation_reduce_pipeline, 1, "q4_k_pair_activation_reduce") \
    X(q4_k_pair_activation_reduce_f16_output_pipeline, 1, "q4_k_pair_activation_reduce_f16_output") \
    X(q4_k_activation_rhs_reduce_pipeline, 1, "q4_k_activation_rhs_reduce") \
    X(q4_0_activation_rhs_reduce_pipeline, 1, "q4_0_activation_rhs_reduce") \
    X(q4_0_activation_rhs_reduce_f16_output_pipeline, 1, "q4_0_activation_rhs_reduce_f16_output") \
    X(q4_0_pipeline, 1, "q4_0") \
    X(q4_0_pair_pipeline, 1, "q4_0_pair") \
    X(q4_0_pair_reduce_pipeline, 1, "q4_0_pair_reduce") \
    X(q4_0_pair_activation_reduce_pipeline, 1, "q4_0_pair_activation_reduce") \
    X(q4_0_pair_activation_reduce_f16_output_pipeline, 1, "q4_0_pair_activation_reduce_f16_output") \
    X(q4_0_pair_activation_rms_scale_reduce_f16_output_pipeline, 1, "q4_0_pair_activation_rms_scale_reduce_f16_output") \
    X(q4_0_reduce_pipeline, 1, "q4_0_reduce") \
    X(q4_0_reduce_sumsq_pipeline, 1, "q4_0_reduce_sumsq") \
    X(q4_0_reduce_f16_input_pipeline, 1, "q4_0_reduce_f16_input") \
    X(q4_0_reduce_f16_input_sumsq_pipeline, 1, "q4_0_reduce_f16_input_sumsq") \
    X(q4_0_reduce_f16_output_pipeline, 1, "q4_0_reduce_f16_output") \
    X(q4_0_reduce_f16_input_f16_output_pipeline, 1, "q4_0_reduce_f16_input_f16_output") \
    X(q4_1_pipeline, 1, "q4_1") \
    X(q4_1_reduce_pipeline, 1, "q4_1_reduce") \
    X(q5_0_pipeline, 1, "q5_0") \
    X(q5_0_reduce_pipeline, 1, "q5_0_reduce") \
    X(q5_1_pipeline, 1, "q5_1") \
    X(q5_1_reduce_pipeline, 1, "q5_1_reduce") \
    X(q8_0_pipeline, 1, "q8_0") \
    X(q8_0_pair_pipeline, 1, "q8_0_pair") \
    X(q8_0_mmv_pipeline, 1, "q8_0_mmv") \
    X(q8_0_rms_scale_mmv_pipeline, 1, "q8_0_rms_scale_mmv") \
    X(q8_0_small_batch_pipeline, 1, "q8_0_small_batch") \
    X(antfly_q8_0_small_batch_pipeline, 0, "antfly_q8_0_small_batch") \
    X(q8_0_pair_mmv_pipeline, 1, "q8_0_pair_mmv") \
    X(q8_0_pair_small_batch_pipeline, 1, "q8_0_pair_small_batch") \
    X(q8_0_qkv_mmv_pipeline, 1, "q8_0_qkv_mmv") \
    X(q8_0_pair_activation_reduce_pipeline, 1, "q8_0_pair_activation_reduce") \
    X(q8_0_pair_activation_mmv_pipeline, 1, "q8_0_pair_activation_mmv") \
    X(q8_0_pair_activation_small_batch_pipeline, 1, "q8_0_pair_activation_small_batch") \
    X(q8_0_activation_multiply_reduce_pipeline, 1, "q8_0_activation_multiply_reduce") \
    X(q8_0_activation_multiply_mmv_pipeline, 1, "q8_0_activation_multiply_mmv") \
    X(q8_1_pipeline, 1, "q8_1") \
    X(q8_1_reduce_pipeline, 1, "q8_1_reduce") \
    X(q5_k_pipeline, 1, "q5_k") \
    X(antfly_q5_k_small_batch_pipeline, 0, "antfly_q5_k_small_batch") \
    X(q5_k_reduce_pipeline, 1, "q5_k_reduce") \
    X(q6_k_pipeline, 1, "q6_k") \
    X(antfly_q6_k_small_batch_pipeline, 0, "antfly_q6_k_small_batch") \
    X(q6_k_reduce_pipeline, 1, "q6_k_reduce") \
    X(q6_k_pair_pipeline, 1, "q6_k_pair") \
    X(q8_k_pipeline, 1, "q8_k") \
    X(q8_k_reduce_pipeline, 1, "q8_k_reduce") \
    X(iq4_nl_pipeline, 1, "iq4_nl") \
    X(iq4_nl_reduce_pipeline, 1, "iq4_nl_reduce") \
    X(iq4_xs_pipeline, 1, "iq4_xs") \
    X(iq4_xs_reduce_pipeline, 1, "iq4_xs_reduce") \
    X(mxfp4_pipeline, 1, "mxfp4") \
    X(mxfp4_reduce_pipeline, 1, "mxfp4_reduce") \
    X(nvfp4_pipeline, 1, "nvfp4") \
    X(iq2_xs_pipeline, 1, "iq2_xs") \
    X(i2_s_pipeline, 1, "i2_s") \
    X(i2_s_pair_pipeline, 1, "i2_s_pair") \
    X(i2_s_linear_i8_pipeline, 1, "i2_s_linear_i8") \
    X(tl1_pipeline, 1, "tl1") \
    X(tl2_pipeline, 1, "tl2") \
    X(encode_polar4_key_pipeline, 1, "encode_polar4_key") \
    X(encode_turbo3_key_pipeline, 1, "encode_turbo3_key") \
    X(encode_f16_convert_pipeline, 0, "encode_f16_convert") \
    X(encode_int8_per_head_pipeline, 0, "encode_int8_per_head") \
    X(polar4_attention_span_pipeline, 1, "polar4_attention_span") \
    X(turbo3_attention_span_pipeline, 1, "turbo3_attention_span")
