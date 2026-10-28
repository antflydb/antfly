// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

// Shared lifecycle identity. Groups retain runtime field layout; the two
// ordinals retain creation and retirement order independently.
#pragma once

#define ANTFLY_METAL_PIPELINE_COUNT 462u

#define ANTFLY_METAL_PIPELINE_GROUP_0(X) \
    X(video_position_pipeline, @"antfly_video_position", precise_library, 55, 64) \
    X(embed_absolute_position_pipeline, @"termite_embed_absolute_position", library, 0, 0) \
    X(embedding_lookup_pipeline, @"termite_embedding_lookup", library, 1, 1) \
    X(embedding_lookup_bf16_pipeline, @"termite_embedding_lookup_bf16", library, 2, 2) \
    X(embedding_lookup_f16_pipeline, @"termite_embedding_lookup_f16", library, 3, 3) \
    X(q4_0_get_rows_pipeline, @"termite_q4_0_get_rows", library, 4, 4) \
    X(q4_0_set_rows_pipeline, @"termite_q4_0_set_rows", library, 5, 5) \
    X(q4_0_cpy_q_to_f32_pipeline, @"termite_q4_0_cpy_q_to_f32", library, 6, 6) \
    X(q4_0_cpy_f32_to_q_pipeline, @"termite_q4_0_cpy_f32_to_q", library, 7, 7) \
    X(q4_1_get_rows_pipeline, @"termite_q4_1_get_rows", library, 8, 8) \
    X(q4_1_set_rows_pipeline, @"termite_q4_1_set_rows", library, 9, 9) \
    X(q4_1_cpy_q_to_f32_pipeline, @"termite_q4_1_cpy_q_to_f32", library, 10, 10) \
    X(q4_1_cpy_f32_to_q_pipeline, @"termite_q4_1_cpy_f32_to_q", library, 11, 11) \
    X(q5_0_get_rows_pipeline, @"termite_q5_0_get_rows", library, 12, 12) \
    X(q5_0_set_rows_pipeline, @"termite_q5_0_set_rows", library, 13, 13) \
    X(q5_0_cpy_q_to_f32_pipeline, @"termite_q5_0_cpy_q_to_f32", library, 14, 14) \
    X(q5_0_cpy_f32_to_q_pipeline, @"termite_q5_0_cpy_f32_to_q", library, 15, 15) \
    X(q5_1_get_rows_pipeline, @"termite_q5_1_get_rows", library, 16, 16) \
    X(q5_1_set_rows_pipeline, @"termite_q5_1_set_rows", library, 17, 17) \
    X(q5_1_cpy_q_to_f32_pipeline, @"termite_q5_1_cpy_q_to_f32", library, 18, 18) \
    X(q5_1_cpy_f32_to_q_pipeline, @"termite_q5_1_cpy_f32_to_q", library, 19, 19) \
    X(q4_k_get_rows_pipeline, @"termite_q4_k_get_rows", library, 20, 20) \
    X(q4_k_set_rows_pipeline, @"termite_q4_k_set_rows", library, 21, 21) \
    X(q4_k_cpy_q_to_f32_pipeline, @"termite_q4_k_cpy_q_to_f32", library, 22, 22) \
    X(q4_k_cpy_f32_to_q_pipeline, @"termite_q4_k_cpy_f32_to_q", library, 23, 23) \
    X(q5_k_get_rows_pipeline, @"termite_q5_k_get_rows", library, 24, 24) \
    X(q5_k_set_rows_pipeline, @"termite_q5_k_set_rows", library, 25, 25) \
    X(q5_k_cpy_q_to_f32_pipeline, @"termite_q5_k_cpy_q_to_f32", library, 26, 26) \
    X(q5_k_cpy_f32_to_q_pipeline, @"termite_q5_k_cpy_f32_to_q", library, 27, 27) \
    X(q6_k_get_rows_pipeline, @"termite_q6_k_get_rows", library, 28, 28) \
    X(q6_k_set_rows_pipeline, @"termite_q6_k_set_rows", library, 29, 29) \
    X(q6_k_cpy_q_to_f32_pipeline, @"termite_q6_k_cpy_q_to_f32", library, 30, 30) \
    X(q6_k_cpy_f32_to_q_pipeline, @"termite_q6_k_cpy_f32_to_q", library, 31, 31) \
    X(q8_0_get_rows_pipeline, @"termite_q8_0_get_rows", library, 32, 32) \
    X(q8_0_set_rows_pipeline, @"termite_q8_0_set_rows", library, 33, 33) \
    X(q8_0_cpy_q_to_f32_pipeline, @"termite_q8_0_cpy_q_to_f32", library, 34, 34) \
    X(q8_0_cpy_f32_to_q_pipeline, @"termite_q8_0_cpy_f32_to_q", library, 35, 35) \
    X(q8_1_get_rows_pipeline, @"termite_q8_1_get_rows", library, 36, 36) \
    X(q8_1_set_rows_pipeline, @"termite_q8_1_set_rows", library, 37, 37) \
    X(q8_1_cpy_q_to_f32_pipeline, @"termite_q8_1_cpy_q_to_f32", library, 38, 38) \
    X(q8_1_cpy_f32_to_q_pipeline, @"termite_q8_1_cpy_f32_to_q", library, 39, 39) \
    X(rope_pipeline, @"termite_apply_rope", precise_library, 40, 40) \
    X(mrope_pipeline, @"termite_apply_mrope", precise_library, 41, 41) \
    X(qwen35_conv_pipeline, @"termite_qwen35_conv", precise_library, 42, 42) \
    X(qwen35_prefill_conv_vec4_pipeline, @"termite_qwen35_prefill_conv_vec4", precise_library, 43, 43) \
    X(qwen35_history_pipeline, @"termite_qwen35_history", precise_library, 44, 44) \
    X(qwen35_prepare_pipeline, @"termite_qwen35_prepare", precise_library, 45, 45) \
    X(qwen35_recurrent_pipeline, @"termite_qwen35_recurrent", precise_library, 46, 46) \
    X(qwen35_recurrent_columns4_pipeline, @"termite_qwen35_recurrent_columns4", precise_library, 47, 47) \
    X(qwen35_recurrent_decode_pipeline, @"termite_qwen35_recurrent_decode", precise_library, 48, 48) \
    X(qwen35_fused_decode_pipeline, @"termite_qwen35_fused_decode", precise_library, 49, 49)

#define ANTFLY_METAL_PIPELINE_GROUP_1(X) \
    X(qwen35_prepare_parallel_pipeline, @"termite_qwen35_prepare_parallel", precise_library, 50, 50) \
    X(qwen35_norm_parallel_pipeline, @"termite_qwen35_norm_parallel", precise_library, 51, 51) \
    X(qwen35_norm_pipeline, @"termite_qwen35_norm", precise_library, 52, 61)

#define ANTFLY_METAL_PIPELINE_GROUP_2(X) \
    X(vision_norm_rope_pipeline, @"termite_vision_norm_rope", precise_library, 57, 66) \
    X(embedding_head_prepare_pipeline, @"termite_embedding_head_prepare", precise_library, 54, 63) \
    X(rotary_table_pipeline, @"antfly_rotary_table", precise_library, 56, 65) \
    X(packed_gelu_pipeline, @"termite_packed_gelu", precise_library, 53, 62) \
    X(rope_pair_pipeline, @"termite_apply_rope_pair", precise_library, 58, 67) \
    X(head_rms_rope_pipeline, @"termite_apply_head_rms_rope", precise_library, 59, 68)

#define ANTFLY_METAL_PIPELINE_GROUP_3(X) \
    X(attention_f32_pipeline, @"termite_attention_f32", precise_library, 60, 69) \
    X(attention_f32_decode_1x_hd64_pipeline, @"termite_attention_f32_decode_1x_hd64", precise_library, 61, 70) \
    X(attention_f32_decode_1x_hd64_split_stage_pipeline, @"termite_attention_f32_decode_1x_hd64_split_stage", precise_library, 62, 71) \
    X(attention_f32_decode_1x_hd64_split_reduce_pipeline, @"termite_attention_f32_decode_1x_hd64_split_reduce", precise_library, 63, 72)

#define ANTFLY_METAL_PIPELINE_GROUP_4(X) \
    X(attention_f32_prefill_pipeline, @"termite_attention_f32_prefill_tiled", precise_library, 64, 73)

#define ANTFLY_METAL_PIPELINE_GROUP_5(X) \
    X(attention_paged_pipeline, @"termite_paged_attention_kv", precise_library, 65, 74)

#define ANTFLY_METAL_PIPELINE_GROUP_6(X) \
    X(attention_paged_1x_pipeline, @"termite_paged_attention_kv_1x", precise_library, 66, 75)

#define ANTFLY_METAL_PIPELINE_GROUP_7(X) \
    X(compressed_attention_store_local_pipeline, @"termite_compressed_attention_store_local", precise_library, 67, 453) \
    X(compressed_attention_store_component_pipeline, @"termite_compressed_attention_store_component", precise_library, 68, 454) \
    X(compressed_attention_update_component_pipeline, @"termite_compressed_attention_update_component", precise_library, 69, 455) \
    X(compressed_attention_select_compressed_pipeline, @"termite_compressed_attention_select_compressed", precise_library, 70, 456) \
    X(compressed_attention_hybrid_attention_pipeline, @"termite_compressed_attention_hybrid_attention", precise_library, 71, 457) \
    X(paged_f32_kv_seed_pipeline, @"termite_paged_f32_kv_seed", precise_library, 72, 76) \
    X(paged_f16_kv_seed_pipeline, @"termite_paged_f16_kv_seed", precise_library, 73, 77) \
    X(paged_f32_v_seed_pipeline, @"termite_paged_f32_v_seed", precise_library, 74, 78) \
    X(copy_bytes_pipeline, @"termite_copy_bytes", precise_library, 75, 79) \
    X(slice_last_dim_f32_2d_pipeline, @"termite_slice_last_dim_f32_2d", precise_library, 76, 80) \
    X(gather_axis0_f32_2d_pipeline, @"termite_gather_axis0_f32_2d", precise_library, 77, 81) \
    X(gather_axis0_add_bias_f32_2d_pipeline, @"termite_gather_axis0_add_bias_f32_2d", precise_library, 78, 82) \
    X(concat_lastdim_f32_2d_pipeline, @"termite_concat_lastdim_f32_2d", precise_library, 79, 83) \
    X(deberta_embeddings_f32_pipeline, @"termite_deberta_embeddings_f32", precise_library, 80, 84) \
    X(gliner_word_embeddings_f32_pipeline, @"termite_gliner_word_embeddings_f32", precise_library, 81, 85) \
    X(gliner_boundary_f32_pipeline, @"termite_gliner_boundary_f32", precise_library, 82, 86) \
    X(gliner_boundary_ffn_bias_gelu_exact_pipeline, @"termite_gliner_boundary_ffn_bias_gelu_exact", precise_library, 83, 87) \
    X(gliner_boundary_ffn_bias_add_layer_norm_pipeline, @"termite_gliner_boundary_ffn_bias_add_layer_norm", precise_library, 84, 88) \
    X(gliner_gru_combine_f32_pipeline, @"termite_gliner_gru_combine_f32", precise_library, 85, 89) \
    X(argmax_axis_f32_pipeline, @"termite_argmax_axis_f32", precise_library, 86, 90) \
    X(convert_dtype_f32_pipeline, @"termite_convert_dtype_f32", precise_library, 87, 91) \
    X(cumulative_sum_f32_pipeline, @"termite_cumulative_sum_f32", precise_library, 88, 92) \
    X(cumulative_sum_i64_pipeline, @"termite_cumulative_sum_i64", precise_library, 98, 102) \
    X(cumulative_sum_i8_pipeline, @"termite_cumulative_sum_i8", precise_library, 97, 101) \
    X(cumulative_sum_i16_pipeline, @"termite_cumulative_sum_i16", precise_library, 96, 100) \
    X(cumulative_sum_u8_pipeline, @"termite_cumulative_sum_u8", precise_library, 95, 99) \
    X(integer_binary_pipeline, @"termite_integer_binary", precise_library, 89, 93) \
    X(select_typed_pipeline, @"termite_select_typed", precise_library, 90, 94) \
    X(slice_typed_pipeline, @"termite_slice_typed", precise_library, 91, 95) \
    X(gather_typed_pipeline, @"termite_gather_typed", precise_library, 92, 96) \
    X(cast_typed_pipeline, @"termite_cast_typed", precise_library, 93, 97) \
    X(cumulative_sum_i32_pipeline, @"termite_cumulative_sum_i32", precise_library, 94, 98) \
    X(sdpa_f32_pipeline, @"termite_sdpa_f32", precise_library, 99, 103) \
    X(sdpa_f32_bert_prefill_s256_hd64_q8_pipeline, @"termite_sdpa_f32_bert_prefill_s256_hd64_q8", precise_library, 100, 104) \
    X(sdpa_f32_vision_hd64_q8_pipeline, @"termite_sdpa_f32_vision_hd64_q8", precise_library, 101, 105) \
    X(sdpa_f32_vision_hd64_q32_pipeline, @"termite_sdpa_f32_vision_hd64_q32", precise_library, 102, 106) \
    X(sdpa_f32_vision_hd64_flash_q16_pipeline, @"termite_sdpa_f32_vision_hd64_flash_q16", precise_library, 103, 107) \
    X(sdpa_f32_vision_hd64_flash_q32_pipeline, @"termite_sdpa_f32_vision_hd64_flash_q32", precise_library, 104, 108) \
    X(sdpa_f32_vision_hd64_flash_q32_full_pipeline, @"termite_sdpa_f32_vision_hd64_flash_q32_full", precise_library, 105, 109) \
    X(sdpa_f32_nomic_q8_pipeline, @"termite_sdpa_f32_nomic_q8", precise_library, 106, 110) \
    X(sdpa_f32_tg_pipeline, @"termite_sdpa_f32_tg", precise_library, 107, 111) \
    X(sdpa_f32_grouped_short256_pipeline, @"termite_sdpa_f32_grouped_short256", precise_library, 108, 112) \
    X(sdpa_f32_grouped_short512_pipeline, @"termite_sdpa_f32_grouped_short512", precise_library, 109, 113) \
    X(sdpa_f32_grouped_matrix64_pipeline, @"termite_sdpa_f32_grouped_matrix64", precise_library, 110, 114) \
    X(sdpa_f32_grouped_matrix32_pipeline, @"termite_sdpa_f32_grouped_matrix32", precise_library, 111, 115) \
    X(sdpa_f32_segments_pipeline, @"termite_sdpa_f32_segments", precise_library, 112, 116) \
    X(sdpa_f32_segments_tiled_pipeline, @"termite_sdpa_f32_segments_tiled", precise_library, 113, 117) \
    X(sdpa_f32_segments_wide_pipeline, @"termite_sdpa_f32_segments_wide", precise_library, 114, 118) \
    X(sdpa_f32_segments_blocks_pipeline, @"termite_sdpa_f32_segments_blocks", precise_library, 115, 119) \
    X(sdpa_f32_grouped_blocks_pipeline, @"termite_sdpa_f32_grouped_blocks", precise_library, 125, 129) \
    X(sdpa_f32_grouped_blocks256_pipeline, @"termite_sdpa_f32_grouped_blocks256", precise_library, 123, 127) \
    X(sdpa_f32_grouped_blocks256x16_pipeline, @"termite_sdpa_f32_grouped_blocks256x16", precise_library, 124, 128) \
    X(sdpa_f32_grouped_pack_pipeline, @"termite_sdpa_grouped_pack", precise_library, 122, 126) \
    X(sdpa_f32_grouped_softmax_pipeline, @"termite_sdpa_grouped_softmax", precise_library, 121, 125) \
    X(sdpa_f32_grouped_unpack_pipeline, @"termite_sdpa_grouped_unpack", precise_library, 120, 124) \
    X(masked_mean_rows_pipeline, @"termite_masked_mean_rows", precise_library, 116, 120) \
    X(masked_mean_batch_pipeline, @"antfly_masked_mean_batch", precise_library, 117, 121) \
    X(compose_rows_pipeline, @"antfly_compose_rows", precise_library, 118, 122) \
    X(normalize_embedding_pipeline, @"termite_normalize_embedding", precise_library, 119, 123) \
    X(modernbert_training_attention_forward_pipeline, @"termite_modernbert_training_attention_forward", precise_library, 126, 130) \
    X(modernbert_training_attention_dq_pipeline, @"termite_modernbert_training_attention_dq", precise_library, 127, 131) \
    X(modernbert_training_attention_dkdv_pipeline, @"termite_modernbert_training_attention_dkdv", precise_library, 128, 132) \
    X(modernbert_training_attention_flash_forward_pipeline, @"termite_modernbert_training_attention_flash_forward", precise_library, 129, 133) \
    X(modernbert_training_attention_flash_dq_pipeline, @"termite_modernbert_training_attention_flash_dq", precise_library, 130, 134) \
    X(modernbert_training_attention_flash_dkdv_pipeline, @"termite_modernbert_training_attention_flash_dkdv", precise_library, 131, 135) \
    X(sdpa_f32_florence_window_hd32_pipeline, @"termite_sdpa_f32_florence_window_hd32", precise_library, 132, 136) \
    X(sdpa_f32_florence_window_parallel_pipeline, @"termite_sdpa_f32_florence_window_parallel", precise_library, 133, 137) \
    X(florence_window_pack_f32_pipeline, @"termite_florence_window_pack_f32", precise_library, 134, 138) \
    X(florence_window_unpack_f32_pipeline, @"termite_florence_window_unpack_f32", precise_library, 135, 139) \
    X(florence_channel_scores_f32_pipeline, @"termite_florence_channel_scores_f32", precise_library, 136, 140) \
    X(florence_channel_scores_parallel_pipeline, @"termite_florence_channel_scores_parallel", precise_library, 137, 141) \
    X(florence_channel_softmax_f32_pipeline, @"termite_florence_channel_softmax_f32", precise_library, 138, 142) \
    X(florence_channel_apply_f32_pipeline, @"termite_florence_channel_apply_f32", precise_library, 139, 143) \
    X(disentangled_relative_attention_f32_pipeline, @"termite_disentangled_relative_attention_f32", precise_library, 140, 144)

#define ANTFLY_METAL_PIPELINE_GROUP_8(X) \
    X(disentangled_relative_attention_f32_tg_pipeline, @"termite_disentangled_relative_attention_f32_tg", precise_library, 141, 145) \
    X(disentangled_relative_attention_f32_flash4_pipeline, @"termite_disentangled_relative_attention_f32_flash4", precise_library, 142, 146) \
    X(deberta_attention_pack_f32_pipeline, @"termite_deberta_attention_pack_f32", precise_library, 143, 147) \
    X(deberta_relative_score_accumulate_f32_pipeline, @"termite_deberta_relative_score_accumulate_f32", precise_library, 144, 148) \
    X(disentangled_relative_attention_bwd_scores_f32_pipeline, @"termite_disentangled_relative_attention_bwd_scores_f32", precise_library, 145, 149) \
    X(disentangled_relative_attention_bwd_dv_f32_pipeline, @"termite_disentangled_relative_attention_bwd_dv_f32", precise_library, 146, 150) \
    X(disentangled_relative_attention_bwd_dq_dk_f32_pipeline, @"termite_disentangled_relative_attention_bwd_dq_dk_f32", precise_library, 147, 151) \
    X(disentangled_relative_attention_bwd_dqr_dkr_f32_pipeline, @"termite_disentangled_relative_attention_bwd_dqr_dkr_f32", precise_library, 148, 152) \
    X(deberta_relative_score_gemm_f32_pipeline, @"termite_deberta_relative_score_gemm_f32", precise_library, 149, 153) \
    X(deberta_relative_score_softmax_pv_f32_pipeline, @"termite_deberta_relative_score_softmax_pv_f32", precise_library, 150, 154) \
    X(transpose_f32_pipeline, @"termite_transpose_f32", precise_library, 151, 155) \
    X(transpose_last2_tiled_f32_pipeline, @"termite_transpose_last2_tiled_f32", precise_library, 152, 156) \
    X(dot_general_2d_f32_pipeline, @"termite_dot_general_2d_f32", precise_library, 153, 157) \
    X(dot_general_2d_f32_reduce_pipeline, @"termite_dot_general_2d_f32_reduce", precise_library, 154, 158) \
    X(dot_general_2d_f32_small_m_tile_pipeline, @"termite_dot_general_2d_f32_small_m_tile", precise_library, 155, 159) \
    X(dot_general_2d_f32_small_m_rows4_cols4_pipeline, @"termite_dot_general_2d_f32_small_m_rows4_cols4", precise_library, 156, 160) \
    X(dot_general_2d_f32_small_m_rows4_cols8_pipeline, @"termite_dot_general_2d_f32_small_m_rows4_cols8", precise_library, 157, 161) \
    X(dot_general_2d_f32_small_m_rows4_cols16_pipeline, @"termite_dot_general_2d_f32_small_m_rows4_cols16", precise_library, 158, 162) \
    X(dot_general_rank1_outer_f32_pipeline, @"termite_dot_general_rank1_outer_f32", precise_library, 159, 163) \
    X(dot_general_rank1_reduce_f32_pipeline, @"termite_dot_general_rank1_reduce_f32", precise_library, 160, 164) \
    X(dot_general_batched_f32_pipeline, @"termite_dot_general_batched_f32", precise_library, 161, 165) \
    X(lora_after_a_f32_pipeline, @"termite_lora_after_a_f32", precise_library, 162, 166) \
    X(lora_finish_f32_pipeline, @"termite_lora_finish_f32", precise_library, 163, 167) \
    X(lora_rank1_linear_f32_pipeline, @"termite_lora_rank1_linear_f32", precise_library, 164, 168) \
    X(lora_grad_b_f32_pipeline, @"termite_lora_grad_b_f32", precise_library, 165, 169) \
    X(lora_grad_b_transposed_f32_pipeline, @"termite_lora_grad_b_transposed_f32", precise_library, 166, 170) \
    X(lora_grad_after_a_f32_pipeline, @"termite_lora_grad_after_a_f32", precise_library, 167, 171) \
    X(lora_grad_a_f32_pipeline, @"termite_lora_grad_a_f32", precise_library, 168, 172) \
    X(lora_rank1_grad_b_after_a_f32_pipeline, @"termite_lora_rank1_grad_b_after_a_f32", precise_library, 169, 173) \
    X(scatter_add_axis0_f32_pipeline, @"termite_scatter_add_axis0_f32", precise_library, 170, 174) \
    X(conv1d_f32_pipeline, @"termite_conv1d_f32", precise_library, 171, 175) \
    X(conv1d_im2col_f32_pipeline, @"termite_conv1d_im2col_f32", precise_library, 172, 176) \
    X(layer_norm_add_sum_rows_pipeline, @"termite_apply_add_layer_norm_sum_rows", library, 173, 177) \
    X(conv2d_f32_pipeline, @"termite_conv2d_f32", precise_library, 174, 178) \
    X(layer_norm_pipeline, @"termite_apply_layer_norm_1x", library, 175, 179) \
    X(layer_norm_rows_pipeline, @"termite_apply_layer_norm_rows", library, 176, 180) \
    X(layer_norm_add_pipeline, @"termite_apply_add_layer_norm_1x", library, 177, 181) \
    X(layer_norm_add_rows_pipeline, @"termite_apply_add_layer_norm_rows", library, 178, 182) \
    X(bias_activation_pipeline, @"termite_apply_bias_activation_2d", precise_library, 179, 183) \
    X(bias_add_layer_norm_pipeline, @"termite_apply_bias_add_layer_norm_1x", library, 180, 184) \
    X(bias_add_layer_norm_rows_pipeline, @"termite_apply_bias_add_layer_norm_rows", library, 181, 185) \
    X(layer_norm_bwd_dx_pipeline, @"termite_layer_norm_bwd_dx", library, 312, 303) \
    X(layer_norm_bwd_dgdb_pipeline, @"termite_layer_norm_bwd_dgdb", library, 313, 304) \
    X(layer_norm_bwd_dgdb_partial_pipeline, @"termite_layer_norm_bwd_dgdb_partial", library, 314, 305) \
    X(layer_norm_bwd_dgdb_reduce_pipeline, @"termite_layer_norm_bwd_dgdb_reduce", library, 315, 306) \
    X(dense_qkv_split_pipeline, @"termite_dense_qkv_split_f32_2d", precise_library, 182, 186) \
    X(dense_pair_split_pipeline, @"termite_dense_pair_split_f32_2d", precise_library, 183, 187) \
    X(dense_qkv_split_bias_pipeline, @"termite_dense_qkv_split_bias_f32_2d", precise_library, 184, 188) \
    X(dense_pair_split_bias_pipeline, @"termite_dense_pair_split_bias_f32_2d", precise_library, 185, 189) \
    X(dense_qkv_direct_pipeline, @"termite_dense_qkv_direct_f32_2d", precise_library, 186, 190) \
    X(dense_pair_direct_pipeline, @"termite_dense_pair_direct_f32_2d", precise_library, 187, 191) \
    X(rms_norm_pipeline, @"termite_apply_rms_norm_1x", library, 188, 192) \
    X(rms_norm_reduce_pipeline, @"termite_apply_rms_norm_1x_reduce", library, 189, 193) \
    X(rms_inv_scale_pipeline, @"termite_compute_rms_inv_scale_1x", library, 190, 194) \
    X(rms_norm_rows_pipeline, @"termite_apply_rms_norm_rows", library, 191, 195)

#define ANTFLY_METAL_PIPELINE_GROUP_9(X) \
    X(rms_norm_rows_reduce_pipeline, @"termite_apply_rms_norm_rows_reduce", library, 192, 196)

#define ANTFLY_METAL_PIPELINE_GROUP_10(X) \
    X(rms_norm_rows_simd4_reduce_pipeline, @"termite_apply_rms_norm_rows_simd4_reduce", library, 193, 199) \
    X(lightonocr3_norm_cached1024_pipeline, @"termite_lightonocr3_norm_cached1024", library, 195, 197) \
    X(lightonocr3_norm_cached2560_pipeline, @"termite_lightonocr3_norm_cached2560", library, 194, 198)

#define ANTFLY_METAL_PIPELINE_GROUP_11(X) \
    X(lightonocr3_gemv_pipeline, @"termite_lightonocr3_gemv_vec4", library, 209, 52) \
    X(lightonocr3_gemv_rows2_pipeline, @"termite_lightonocr3_gemv_vec4_rows2", library, 210, 53) \
    X(lightonocr3_gate_up_gemv_pipeline, @"termite_lightonocr3_gate_up_gemv", library, 211, 54) \
    X(lightonocr3_pack_bf16_pipeline, @"termite_lightonocr3_pack_bf16", precise_library, 212, 55) \
    X(lightonocr3_batch2_pipeline, @"termite_lightonocr3_batch2_vec4", library, 213, 56) \
    X(lightonocr3_batch4_pipeline, @"termite_lightonocr3_batch4_vec4", library, 215, 57) \
    X(lightonocr3_batch8_pipeline, @"termite_lightonocr3_batch8_vec4", library, 216, 58) \
    X(lightonocr3_batch16_pipeline, @"termite_lightonocr3_batch16_vec4", library, 217, 59)

#define ANTFLY_METAL_PIPELINE_GROUP_12(X) \
    X(lightonocr3_four_row_segment_pipeline, @"termite_lightonocr3_four_row_segment", precise_library, 214, 60)

#define ANTFLY_METAL_PIPELINE_GROUP_13(X) \
    X(rms_norm_rows_triple_simd4_reduce_pipeline, @"termite_apply_rms_norm_rows_triple_simd4_reduce", library, 196, 200)

#define ANTFLY_METAL_PIPELINE_GROUP_14(X) \
    X(parallel_ffn_post_residual_simd4_pipeline, @"termite_apply_parallel_ffn_post_residual_simd4", library, 197, 201) \
    X(rms_norm_add_pipeline, @"termite_apply_rms_norm_add_1x", library, 198, 202) \
    X(rms_norm_add_sumsq_pipeline, @"termite_apply_rms_norm_add_1x_sumsq", library, 199, 203)

#define ANTFLY_METAL_PIPELINE_GROUP_15(X) \
    X(rms_norm_add_scale_sumsq_pipeline, @"termite_apply_rms_norm_add_scale_1x_sumsq", library, 200, 204) \
    X(rms_norm_add_f16_input_pipeline, @"termite_apply_rms_norm_add_1x_f16_input", library, 201, 205) \
    X(rms_norm_add_scale_pipeline, @"termite_apply_rms_norm_add_scale_1x", library, 202, 206) \
    X(rms_norm_add_scale_rows_pipeline, @"termite_apply_rms_norm_add_scale_rows", library, 203, 207)

#define ANTFLY_METAL_PIPELINE_GROUP_16(X) \
    X(rms_norm_add_scale_simd4_pipeline, @"termite_apply_rms_norm_add_scale_simd4", library, 204, 208)

#define ANTFLY_METAL_PIPELINE_GROUP_17(X) \
    X(linear_pipeline, @"termite_apply_linear_1x", library, 205, 209) \
    X(linear_reduce_pipeline, @"termite_apply_linear_1x_reduce", library, 206, 210) \
    X(linear_bf16_pipeline, @"termite_apply_linear_bf16_1x", library, 207, 211) \
    X(linear_bf16_reduce_pipeline, @"termite_apply_linear_bf16_1x_reduce", library, 208, 212) \
    X(linear_f16_pipeline, @"termite_apply_linear_f16_1x", library, 218, 213) \
    X(linear_f16_reduce_pipeline, @"termite_apply_linear_f16_1x_reduce", library, 219, 214) \
    X(linear_f16_multi_row_pipeline, @"termite_apply_linear_f16_multi_row", library, 220, 215) \
    X(linear_f16_multi_row_reduce_pipeline, @"termite_apply_linear_f16_multi_row_reduce", library, 221, 216) \
    X(linear_bf16_multi_row_pipeline, @"termite_apply_linear_bf16_multi_row", library, 222, 217) \
    X(linear_bf16_multi_row_reduce_pipeline, @"termite_apply_linear_bf16_multi_row_reduce", library, 223, 218) \
    X(linear_bf16_multi_row_shared_reduce_pipeline, @"termite_apply_linear_bf16_multi_row_shared_reduce", library, 224, 219) \
    X(linear_bf16_multi_row_tiled32_m16_pipeline, @"termite_apply_linear_bf16_multi_row_tiled32_m16", library, 225, 220)

#define ANTFLY_METAL_PIPELINE_GROUP_18(X) \
    X(linear_pair_reduce_pipeline, @"termite_apply_linear_pair_1x_reduce", library, 226, 221) \
    X(linear_multi_row_pipeline, @"termite_apply_linear_multi_row", library, 227, 222) \
    X(linear_multi_row_reduce_pipeline, @"termite_apply_linear_multi_row_reduce", library, 228, 223) \
    X(linear_multi_row_tiled_f32_pipeline, @"termite_apply_linear_multi_row_tiled_f32", library, 229, 224) \
    X(linear_multi_row_tiled_activation_f32_pipeline, @"termite_apply_linear_multi_row_tiled_activation_f32", library, 230, 225) \
    X(linear_multi_row_tiled32_f32_pipeline, @"termite_apply_linear_multi_row_tiled32_f32", library, 231, 226) \
    X(linear_multi_row_tiled32_activation_f32_pipeline, @"termite_apply_linear_multi_row_tiled32_activation_f32", library, 232, 227) \
    X(linear_multi_row_tiled32_m16_f32_pipeline, @"termite_apply_linear_multi_row_tiled32_m16_f32", library, 233, 228) \
    X(linear_multi_row_tiled32_m16_activation_f32_pipeline, @"termite_apply_linear_multi_row_tiled32_m16_activation_f32", library, 234, 229) \
    X(linear_bias_pipeline, @"termite_apply_linear_bias_2d", library, 235, 458) \
    X(argmax_logits_pipeline, @"termite_argmax_logits_1x", library, 236, 230) \
    X(argmax_logits_partials_pipeline, @"termite_argmax_logits_partials", library, 237, 231) \
    X(argmax_logits_suppress_partials_pipeline, @"termite_argmax_logits_suppress_partials", library, 238, 232) \
    X(argmax_logits_reduce_pipeline, @"termite_argmax_logits_reduce", library, 239, 233) \
    X(lighton_batch_argmax_partials_pipeline, @"termite_lighton_batch_argmax_partials", library, 240, 234) \
    X(lighton_batch_argmax_reduce_pipeline, @"termite_lighton_batch_argmax_reduce", library, 241, 235) \
    X(layer_norm_add_sum_pipeline, @"termite_apply_add_layer_norm_sum_1x", library, 242, 236) \
    X(whisper_logits_partials_pipeline, @"termite_whisper_logits_partials", library, 243, 237) \
    X(whisper_logits_reduce_pipeline, @"termite_whisper_logits_reduce", library, 244, 238) \
    X(lm_head_top8_suppress_partials_pipeline, @"termite_lm_head_top8_suppress_partials", library, 245, 239) \
    X(lm_head_top8_reduce_pipeline, @"termite_lm_head_top8_reduce", library, 246, 240) \
    X(lm_head_q6_k_rescore_top8_pipeline, @"termite_lm_head_q6_k_rescore_top8", library, 247, 241) \
    X(argmax_logits_reduce_tg_pipeline, @"termite_argmax_logits_reduce_tg", library, 248, 242) \
    X(sample_gumbel_partials_pipeline, @"termite_sample_gumbel_partials", library, 250, 244) \
    X(sample_logits_pipeline, @"termite_sample_logits_1x", library, 249, 243) \
    X(sample_topk_partials_pipeline, @"termite_sample_topk_partials", library, 251, 245) \
    X(sample_topk_reduce_pipeline, @"termite_sample_topk_reduce", library, 252, 246)

#define ANTFLY_METAL_PIPELINE_GROUP_19(X) \
    X(activation_pipeline, @"termite_apply_activation_1x", precise_library, 253, 247) \
    X(gelu_backward_pipeline, @"termite_apply_gelu_backward_1x", precise_library, 254, 248) \
    X(ffn_gelu_backward_rank1_branches_pipeline, @"termite_ffn_gelu_backward_rank1_branches", precise_library, 255, 459) \
    X(ffn_gelu_backward_rank1_first_branch_pipeline, @"termite_ffn_gelu_backward_rank1_first_branch", precise_library, 256, 460) \
    X(ffn_gelu_backward_rank1_output_pipeline, @"termite_ffn_gelu_backward_rank1_output", precise_library, 257, 249) \
    X(ffn_gelu_backward_rank1_first_output_pipeline, @"termite_ffn_gelu_backward_rank1_first_output", precise_library, 258, 250) \
    X(activation_multiply_pipeline, @"termite_apply_activation_multiply_1x", precise_library, 259, 251) \
    X(activation_multiply_f16_output_pipeline, @"termite_apply_activation_multiply_f16_output", precise_library, 260, 252) \
    X(softmax_pipeline, @"termite_apply_softmax_rows", precise_library, 261, 253) \
    X(moe_route_select_pipeline, @"termite_moe_route_select", precise_library, 262, 254) \
    X(moe_route_select_tg_pipeline, @"termite_moe_route_select_tg", precise_library, 263, 255) \
    X(moe_route_select_tg_register_pipeline, @"termite_moe_route_select_tg_register", precise_library, 264, 256) \
    X(moe_route_select_tg_register_v2_pipeline, @"termite_moe_route_select_tg_register_v2", precise_library, 265, 257) \
    X(moe_route_select_tg_map_pipeline, @"termite_moe_route_select_tg_map", precise_library, 266, 258) \
    X(moe_route_map_slots_pipeline, @"termite_moe_route_map_slots", precise_library, 267, 259) \
    X(moe_slot_q4_0_gate_up_pipeline, @"termite_moe_slot_q4_0_gate_up", library, 268, 260) \
    X(moe_slot_q4_0_down_pipeline, @"termite_moe_slot_q4_0_down", library, 269, 261) \
    X(moe_slot_reduce_pipeline, @"termite_moe_slot_reduce", precise_library, 270, 262) \
    X(mapped_page_touch_pipeline, @"termite_mapped_page_touch", library, 271, 263) \
    X(mapped_selected_expert_page_touch_pipeline, @"termite_mapped_selected_expert_page_touch", library, 272, 264) \
    X(masked_bce_loss_pipeline, @"termite_masked_bce_with_logits_loss", precise_library, 273, 265) \
    X(masked_bce_backward_pipeline, @"termite_masked_bce_with_logits_backward", precise_library, 274, 266) \
    X(reduce_last_dim_pipeline, @"termite_reduce_last_dim_rows", precise_library, 275, 267) \
    X(nomic_pool_normalize_pipeline, @"termite_nomic_pool_normalize", precise_library, 276, 268) \
    X(reduce_axis_f32_pipeline, @"termite_reduce_axis_f32", precise_library, 277, 269) \
    X(multiply_reduce_last_dim_pipeline, @"termite_multiply_reduce_last_dim_rows", precise_library, 278, 270) \
    X(broadcast_last_dim_pipeline, @"termite_broadcast_last_dim_f32_2d", precise_library, 279, 271) \
    X(broadcast_f32_pipeline, @"termite_broadcast_f32", precise_library, 280, 272) \
    X(multiply_pipeline, @"termite_apply_multiply_1x", library, 281, 273) \
    X(scale_pipeline, @"termite_apply_scale_1x", library, 282, 274) \
    X(norm_residual_pipeline, @"termite_norm_residual", precise_library, 283, 275) \
    X(gemma4_audio_clamp_silu_pipeline, @"termite_clamp_silu", precise_library, 284, 276) \
    X(spatial_pool_prepare_pipeline, @"termite_spatial_pool_prepare", precise_library, 294, 277) \
    X(audio_frontend_pipeline, @"antfly_audio_frontend", precise_library, 293, 278) \
    X(audio_clipped_linear_pipeline, @"antfly_clipped_linear", precise_library, 285, 279) \
    X(audio_attention_parallel_pipeline, @"antfly_audio_attention_parallel", precise_library, 286, 280) \
    X(audio_norm_clamp_pipeline, @"antfly_norm_clamp", precise_library, 287, 281) \
    X(audio_logmel_pipeline, @"antfly_audio_logmel", precise_library, 288, 282) \
    X(audio_conv_matrix_pipeline, @"antfly_audio_conv_matrix", precise_library, 289, 283) \
    X(audio_conv_tiled_pipeline, @"antfly_audio_conv_tiled", precise_library, 290, 284) \
    X(audio_fft512_pipeline, @"antfly_audio_fft512", precise_library, 291, 285) \
    X(gemma4_audio_clamp_pipeline, @"termite_gemma4_audio_clamp", library, 295, 286) \
    X(gemma4_audio_glu_pipeline, @"termite_gemma4_audio_glu_rows", precise_library, 296, 287) \
    X(gemma4_audio_dwconv_pipeline, @"termite_gemma4_audio_dwconv_causal", library, 297, 288) \
    X(gemma4_audio_flatten_pipeline, @"termite_gemma4_audio_flatten_ctf", library, 298, 289) \
    X(gemma4_audio_channel_norm_pipeline, @"termite_gemma4_audio_channel_norm_relu", precise_library, 299, 290) \
    X(gemma4_audio_attention_pipeline, @"termite_gemma4_audio_local_attention", precise_library, 300, 291) \
    X(gemma4_audio_attention_tg_pipeline, @"termite_gemma4_audio_local_attention_tg", precise_library, 301, 292) \
    X(add_pipeline, @"termite_apply_add_1x", library, 302, 293) \
    X(add_scale_pipeline, @"termite_apply_add_scale_1x", library, 303, 294) \
    X(scaled_add_scale_pipeline, @"termite_apply_scaled_add_scale_1x", library, 304, 295) \
    X(multiply_add_pipeline, @"termite_apply_multiply_add_1x", library, 305, 296) \
    X(multiply_add2_pipeline, @"termite_apply_multiply_add2_1x", library, 306, 297) \
    X(training_accumulate_pipeline, @"termite_training_accumulate_f32", library, 307, 298) \
    X(training_adamw_pipeline, @"termite_training_adamw_f32", library, 308, 299) \
    X(training_accumulate_into_pipeline, @"termite_training_accumulate_into_f32", library, 309, 300) \
    X(training_adamw_into_pipeline, @"termite_training_adamw_into_f32", library, 310, 301) \
    X(training_sumsq_pipeline, @"termite_training_sumsq_f32", library, 311, 302) \
    X(subtract_pipeline, @"termite_apply_subtract_1x", library, 316, 307) \
    X(divide_pipeline, @"termite_apply_divide_1x", library, 317, 308) \
    X(less_than_pipeline, @"termite_apply_less_than_1x", library, 318, 309) \
    X(where_select_pipeline, @"termite_apply_where_select_1x", library, 319, 310) \
    X(i2_s_quantize_pipeline, @"termite_quantize_rows_i8", library, 320, 311) \
    X(q1_0_pipeline, @"termite_q1_0_linear", library, 321, 312) \
    X(i8_s_pipeline, @"termite_i8_s_linear", library, 322, 313) \
    X(q2_k_pipeline, @"termite_q2_k_linear", library, 323, 314) \
    X(antfly_q2_k_small_batch_pipeline, @"antfly_q2_k_small_batch_msl_v1", library, 324, 315) \
    X(antfly_q2_k_small_batch_bias_pipeline, @"antfly_q2_k_small_batch_bias_msl_v1", library, 325, 316) \
    X(antfly_q2_k_small_batch_bias_gelu_pipeline, @"antfly_q2_k_small_batch_bias_gelu_msl_v1", library, 326, 317) \
    X(q3_k_pipeline, @"termite_q3_k_linear", library, 327, 318) \
    X(antfly_q3_k_small_batch_pipeline, @"antfly_q3_k_small_batch_msl_v1", library, 328, 319) \
    X(antfly_q3_k_small_batch_bias_pipeline, @"antfly_q3_k_small_batch_bias_msl_v1", library, 329, 320) \
    X(antfly_q3_k_small_batch_bias_gelu_pipeline, @"antfly_q3_k_small_batch_bias_gelu_msl_v1", library, 330, 321) \
    X(q4_k_pipeline, @"termite_q4_k_linear", library, 331, 322) \
    X(antfly_q4_k_small_batch_bias_gelu_pipeline, @"antfly_q4_k_small_batch_bias_gelu_msl_v1", library, 337, 328) \
    X(q4_k_reduce_pipeline, @"termite_q4_k_linear_1x_reduce", library, 338, 329) \
    X(q4_k_pair_pipeline, @"termite_q4_k_pair_linear", library, 340, 331) \
    X(q4_k_pair_reduce_pipeline, @"termite_q4_k_pair_linear_1x_reduce", library, 341, 332) \
    X(q4_k_pair_activation_reduce_pipeline, @"termite_q4_k_pair_activation_1x_reduce", library, 342, 333) \
    X(q4_k_pair_activation_reduce_f16_output_pipeline, @"termite_q4_k_pair_activation_1x_reduce_out_f16", library, 343, 334) \
    X(q4_k_activation_rhs_reduce_pipeline, @"termite_q4_k_activation_rhs_1x_reduce", library, 344, 335) \
    X(q4_0_activation_rhs_reduce_pipeline, @"termite_q4_0_activation_rhs_1x_reduce", library, 345, 336) \
    X(q4_0_activation_rhs_reduce_f16_output_pipeline, @"termite_q4_0_activation_rhs_1x_reduce_out_f16", library, 346, 337) \
    X(q4_0_pipeline, @"termite_q4_0_linear", library, 347, 338) \
    X(q4_0_id_pipeline, @"termite_q4_0_linear_id", library, 348, 339) \
    X(q4_0_id_a4b_gate_up_pipeline, @"termite_q4_0_linear_id_a4b_gate_up", library, 349, 340) \
    X(q4_0_id_a4b_down_pipeline, @"termite_q4_0_linear_id_a4b_down", library, 350, 341) \
    X(q4_0_pair_id_pipeline, @"termite_q4_0_pair_linear_id", library, 351, 342) \
    X(q4_0_pair_activation_id_pipeline, @"termite_q4_0_pair_activation_id", precise_library, 352, 343) \
    X(moe_merged_gate_up_activation_pipeline, @"termite_moe_merged_gate_up_activation", library, 353, 344) \
    X(moe_scatter_add_pipeline, @"termite_moe_scatter_add", library, 354, 345) \
    X(antfly_q4_0_small_batch_pipeline, @"antfly_q4_0_small_batch_msl_v1", library, 355, 346) \
    X(q4_0_pair_pipeline, @"termite_q4_0_pair_linear", library, 356, 347) \
    X(q4_0_pair_reduce_pipeline, @"termite_q4_0_pair_linear_1x_reduce", library, 357, 348) \
    X(q4_0_pair_activation_reduce_pipeline, @"termite_q4_0_pair_activation_1x_reduce", library, 358, 349)

#define ANTFLY_METAL_PIPELINE_GROUP_20(X) \
    X(q4_0_pair_activation_small_batch_r2_pipeline, @"termite_q4_0_pair_activation_multiply_r2_ext", library, 359, 350) \
    X(q4_0_pair_activation_small_batch_r3_pipeline, @"termite_q4_0_pair_activation_multiply_r3_ext", library, 360, 351) \
    X(q4_0_pair_activation_small_batch_r4_pipeline, @"termite_q4_0_pair_activation_multiply_r4_ext", library, 361, 352) \
    X(q4_0_pair_activation_small_batch_pipeline, @"termite_q4_0_pair_activation_multiply_r5_ext", library, 362, 353) \
    X(q4_0_pair_activation_reduce_f16_output_pipeline, @"termite_q4_0_pair_activation_1x_reduce_out_f16", library, 363, 354) \
    X(q4_0_pair_activation_rms_scale_reduce_f16_output_pipeline, @"termite_q4_0_pair_activation_rms_scale_1x_reduce_out_f16", library, 364, 355) \
    X(q4_0_reduce_pipeline, @"termite_q4_0_linear_1x_reduce", library, 365, 356) \
    X(q4_0_reduce_nr4_nsg4_pipeline, @"termite_q4_0_linear_1x_reduce_nr4_nsg4", library, 366, 357) \
    X(q4_0_reduce_nr4_nsg4_nq8_pipeline, @"termite_q4_0_linear_1x_reduce_nr4_nsg4_nq8", library, 367, 358) \
    X(q4_0_reduce_nr8_nsg4_pipeline, @"termite_q4_0_linear_1x_reduce_nr8_nsg4", library, 368, 359)

#define ANTFLY_METAL_PIPELINE_GROUP_21(X) \
    X(q4_0_reduce_small_pipeline, @"termite_q4_0_linear_1x_reduce_small", library, 369, 360) \
    X(q4_0_reduce_sumsq_pipeline, @"termite_q4_0_linear_1x_reduce_sumsq", library, 370, 361) \
    X(q4_0_reduce_f16_input_pipeline, @"termite_q4_0_linear_1x_reduce_in_f16", library, 371, 362) \
    X(q4_0_reduce_f16_input_small_pipeline, @"termite_q4_0_linear_1x_reduce_in_f16_small", library, 372, 363) \
    X(q4_0_reduce_f16_input_sumsq_pipeline, @"termite_q4_0_linear_1x_reduce_in_f16_sumsq", library, 373, 364) \
    X(q4_0_reduce_f16_output_pipeline, @"termite_q4_0_linear_1x_reduce_out_f16", library, 374, 365) \
    X(q4_0_reduce_f16_input_f16_output_pipeline, @"termite_q4_0_linear_1x_reduce_in_f16_out_f16", library, 375, 366)

#define ANTFLY_METAL_PIPELINE_GROUP_22(X) \
    X(q4_1_pipeline, @"termite_q4_1_linear", library, 376, 367) \
    X(antfly_q4_1_small_batch_pipeline, @"antfly_q4_1_small_batch_msl_v1", library, 377, 368) \
    X(q4_1_reduce_pipeline, @"termite_q4_1_linear_1x_reduce", library, 378, 369) \
    X(q5_0_pipeline, @"termite_q5_0_linear", library, 379, 370) \
    X(antfly_q5_0_small_batch_pipeline, @"antfly_q5_0_small_batch_msl_v1", library, 380, 371) \
    X(q5_0_reduce_pipeline, @"termite_q5_0_linear_1x_reduce", library, 381, 372) \
    X(q5_1_pipeline, @"termite_q5_1_linear", library, 382, 373) \
    X(antfly_q5_1_small_batch_pipeline, @"antfly_q5_1_small_batch_msl_v1", library, 383, 374) \
    X(q5_1_reduce_pipeline, @"termite_q5_1_linear_1x_reduce", library, 384, 375) \
    X(q8_0_pipeline, @"termite_q8_0_linear", library, 385, 376) \
    X(q8_0_pair_pipeline, @"termite_q8_0_pair_linear", library, 386, 377) \
    X(q8_0_mmv_pipeline, @"termite_q8_0_linear_mmv", library, 387, 378) \
    X(q8_0_mmv_f16_input_pipeline, @"termite_q8_0_linear_mmv_in_f16", library, 388, 379) \
    X(q8_0_rms_scale_mmv_pipeline, @"termite_q8_0_linear_rms_scale_mmv", library, 389, 380) \
    X(q8_0_small_batch_r2_pipeline, @"termite_q8_0_linear_r2_ext", library, 390, 381) \
    X(q8_0_small_batch_r3_pipeline, @"termite_q8_0_linear_r3_ext", library, 391, 382) \
    X(q8_0_small_batch_r4_pipeline, @"termite_q8_0_linear_r4_ext", library, 392, 383) \
    X(q8_0_small_batch_pipeline, @"termite_q8_0_linear_r5_ext", library, 393, 384) \
    X(antfly_q8_0_small_batch_pipeline, @"antfly_q8_0_small_batch_msl_v1", library, 394, 385) \
    X(antfly_q8_0_small_batch_bias_pipeline, @"antfly_q8_0_small_batch_bias_msl_v1", library, 395, 386) \
    X(antfly_q8_0_small_batch_bias_gelu_pipeline, @"antfly_q8_0_small_batch_bias_gelu_msl_v1", library, 396, 387) \
    X(antfly_q8_0_small_batch_relu_pipeline, @"antfly_q8_0_small_batch_relu_msl_v1", library, 397, 388)

#define ANTFLY_METAL_PIPELINE_GROUP_23(X) \
    X(q8_0_pair_mmv_pipeline, @"termite_q8_0_pair_linear_mmv", library, 398, 389) \
    X(q8_0_pair_small_batch_r2_pipeline, @"termite_q8_0_pair_linear_r2_ext", library, 399, 390) \
    X(q8_0_pair_small_batch_r3_pipeline, @"termite_q8_0_pair_linear_r3_ext", library, 400, 391) \
    X(q8_0_pair_small_batch_r4_pipeline, @"termite_q8_0_pair_linear_r4_ext", library, 401, 392) \
    X(q8_0_pair_small_batch_pipeline, @"termite_q8_0_pair_linear_r5_ext", library, 402, 393) \
    X(q8_0_qkv_mmv_pipeline, @"termite_q8_0_qkv_linear_mmv", library, 403, 394)

#define ANTFLY_METAL_PIPELINE_GROUP_24(X) \
    X(q8_0_pair_activation_reduce_pipeline, @"termite_q8_0_pair_activation_multiply_1x_reduce4", library, 404, 395) \
    X(q8_0_pair_activation_mmv_pipeline, @"termite_q8_0_pair_activation_multiply_1r_ext", library, 405, 396) \
    X(q8_0_pair_activation_rms_scale_mmv_pipeline, @"termite_q8_0_pair_activation_multiply_rms_scale_1r_ext", library, 406, 397) \
    X(q8_0_pair_activation_rms_scale_mmv_f16_output_pipeline, @"termite_q8_0_pair_activation_multiply_rms_scale_1r_ext_out_f16", library, 407, 398) \
    X(q8_0_pair_activation_small_batch_r2_pipeline, @"termite_q8_0_pair_activation_multiply_r2_ext", library, 408, 399) \
    X(q8_0_pair_activation_small_batch_r3_pipeline, @"termite_q8_0_pair_activation_multiply_r3_ext", library, 409, 400) \
    X(q8_0_pair_activation_small_batch_r4_pipeline, @"termite_q8_0_pair_activation_multiply_r4_ext", library, 410, 401) \
    X(q8_0_pair_activation_small_batch_pipeline, @"termite_q8_0_pair_activation_multiply_r5_ext", library, 411, 402)

#define ANTFLY_METAL_PIPELINE_GROUP_25(X) \
    X(q8_0_activation_multiply_reduce_pipeline, @"termite_q8_0_activation_multiply_1x_reduce4", library, 412, 403) \
    X(q8_0_activation_multiply_mmv_pipeline, @"termite_q8_0_activation_multiply_1r_ext", library, 413, 404) \
    X(q8_1_pipeline, @"termite_q8_1_linear", library, 414, 405) \
    X(antfly_q8_1_small_batch_pipeline, @"antfly_q8_1_small_batch_msl_v1", library, 415, 406) \
    X(q8_1_reduce_pipeline, @"termite_q8_1_linear_1x_reduce", library, 416, 407) \
    X(q5_k_pipeline, @"termite_q5_k_linear", library, 417, 408) \
    X(antfly_q4_k_small_batch_pipeline, @"antfly_q4_k_small_batch_msl_v1", library, 332, 323) \
    X(florence_q4_k_mm_matrix_pipeline, @"termite_florence_q4_k_mm_matrix_m64_n64", library, 333, 324) \
    X(florence_q4_k_mm_matrix_f16_input_pipeline, @"termite_florence_q4_k_mm_matrix_m64_n64_in_f16", library, 334, 325) \
    X(florence_q4_k_mm_nr4_pipeline, @"termite_florence_q4_k_mm_nr4", library, 335, 326) \
    X(antfly_q4_k_small_batch_bias_pipeline, @"antfly_q4_k_small_batch_bias_msl_v1", library, 336, 327) \
    X(antfly_q5_k_small_batch_pipeline, @"antfly_q5_k_small_batch_msl_v1", library, 418, 409) \
    X(antfly_q5_k_small_batch_bias_pipeline, @"antfly_q5_k_small_batch_bias_msl_v1", library, 419, 410) \
    X(antfly_q5_k_small_batch_bias_gelu_pipeline, @"antfly_q5_k_small_batch_bias_gelu_msl_v1", library, 420, 411) \
    X(q5_k_reduce_pipeline, @"termite_q5_k_linear_1x_reduce", library, 421, 412) \
    X(q4_k_reduce_v2_pipeline, @"termite_q4_k_linear_1x_reduce_v2", library, 339, 330) \
    X(q6_k_pipeline, @"termite_q6_k_linear", library, 422, 413) \
    X(antfly_q6_k_small_batch_pipeline, @"antfly_q6_k_small_batch_msl_v1", library, 423, 414) \
    X(q6_k_high_row_mm_matrix_pipeline, @"termite_q6_k_high_row_mm_matrix_m64_n64", library, 424, 415) \
    X(antfly_q6_k_small_batch_bias_pipeline, @"antfly_q6_k_small_batch_bias_msl_v1", library, 425, 416) \
    X(antfly_q6_k_small_batch_bias_gelu_pipeline, @"antfly_q6_k_small_batch_bias_gelu_msl_v1", library, 426, 417) \
    X(q6_k_reduce_pipeline, @"termite_q6_k_linear_1x_reduce", library, 427, 418) \
    X(q6_k_reduce_nsg4_pipeline, @"termite_q6_k_linear_1x_reduce_nsg4", library, 428, 419) \
    X(q6_k_reduce_nsg8_pipeline, @"termite_q6_k_linear_1x_reduce_nsg8", library, 429, 420) \
    X(q6_k_reduce_nbodd_pipeline, @"termite_q6_k_linear_1x_reduce_nbodd", library, 430, 421) \
    X(q6_k_reduce_nbodd_nr1_pipeline, @"termite_q6_k_linear_1x_reduce_nbodd_nr1", library, 431, 422) \
    X(q6_k_reduce_nbodd_nr4_nsg1_pipeline, @"termite_q6_k_linear_1x_reduce_nbodd_nr4_nsg1", library, 432, 423) \
    X(q6_k_reduce_nbodd_nr4_nsg2_pipeline, @"termite_q6_k_linear_1x_reduce_nbodd_nr4_nsg2", library, 433, 424) \
    X(q6_k_reduce_nbodd_nr6_nsg1_pipeline, @"termite_q6_k_linear_1x_reduce_nbodd_nr6_nsg1", library, 434, 425) \
    X(q6_k_reduce_nbodd_nr8_nsg1_pipeline, @"termite_q6_k_linear_1x_reduce_nbodd_nr8_nsg1", library, 435, 426) \
    X(q6_k_r2_reduce_pipeline, @"termite_q6_k_linear_r2_reduce", library, 436, 427) \
    X(q6_k_reduce_f16_input_pipeline, @"termite_q6_k_linear_1x_reduce_in_f16", library, 437, 428) \
    X(q6_k_pair_pipeline, @"termite_q6_k_pair_linear", library, 438, 429) \
    X(q8_k_pipeline, @"termite_q8_k_linear", library, 439, 430) \
    X(antfly_q8_k_small_batch_pipeline, @"antfly_q8_k_small_batch_msl_v1", library, 440, 431) \
    X(q8_k_reduce_pipeline, @"termite_q8_k_linear_1x_reduce", library, 441, 432) \
    X(iq4_nl_pipeline, @"termite_iq4_nl_linear", library, 442, 433) \
    X(iq4_nl_reduce_pipeline, @"termite_iq4_nl_linear_1x_reduce", library, 443, 434) \
    X(iq4_xs_pipeline, @"termite_iq4_xs_linear", library, 444, 435) \
    X(iq4_xs_reduce_pipeline, @"termite_iq4_xs_linear_1x_reduce", library, 445, 436) \
    X(mxfp4_pipeline, @"termite_mxfp4_linear", library, 446, 437) \
    X(mxfp4_reduce_pipeline, @"termite_mxfp4_linear_1x_reduce", library, 447, 438) \
    X(nvfp4_pipeline, @"termite_nvfp4_linear", library, 448, 439)

#define ANTFLY_METAL_PIPELINE_GROUP_26(X) \
    X(iq2_xs_pipeline, @"termite_iq2_xs_linear", library, 449, 440)

#define ANTFLY_METAL_PIPELINE_GROUP_27(X) \
    X(i2_s_pipeline, @"termite_i2_s_linear", library, 450, 441) \
    X(i2_s_pair_pipeline, @"termite_i2_s_pair_linear", library, 451, 442) \
    X(i2_s_linear_i8_pipeline, @"termite_i2_s_linear_i8", library, 452, 443) \
    X(i2_s_pair_i8_pipeline, @"termite_i2_s_pair_linear_i8", library, 453, 444) \
    X(tl1_pipeline, @"termite_tl1_linear", library, 454, 445) \
    X(tl2_pipeline, @"termite_tl2_linear", library, 455, 446) \
    X(encode_polar4_key_pipeline, @"termite_encode_polar4_key", library, 456, 447) \
    X(encode_turbo3_key_pipeline, @"termite_encode_turbo3_key", library, 457, 448) \
    X(encode_f16_convert_pipeline, @"termite_encode_f16_convert", library, 458, 449) \
    X(encode_int8_per_head_pipeline, @"termite_encode_int8_per_head", library, 459, 450) \
    X(polar4_attention_span_pipeline, @"termite_polar4_attention_span", library, 460, 451) \
    X(turbo3_attention_span_pipeline, @"termite_turbo3_attention_span", library, 461, 452)

#define ANTFLY_METAL_PIPELINE_GROUP_28(X) \
    X(audio_pad_pipeline, @"antfly_audio_pad", precise_library, 292, 461)

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
