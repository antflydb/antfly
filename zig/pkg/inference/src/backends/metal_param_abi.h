// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

// quant-kernel-codegen:begin scalar Metal ABI (do not edit)
#define TERMITE_METAL_ABI_C_u32 uint32_t
#define TERMITE_METAL_ABI_C_i32 int32_t
#define TERMITE_METAL_ABI_C_f32 float
#define TERMITE_METAL_ABI_MSL_u32 "uint"
#define TERMITE_METAL_ABI_MSL_i32 "int"
#define TERMITE_METAL_ABI_MSL_f32 "float"
#define TERMITE_METAL_ABI_FIELD_C(Type, Field) TERMITE_METAL_ABI_C_##Type Field;
#define TERMITE_METAL_ABI_FIELD_MSL(Type, Field) TERMITE_METAL_ABI_MSL_##Type " " #Field "; "
#define TERMITE_METAL_ABI_FIELDS(Name, F) TERMITE_METAL_ABI_FIELDS_##Name(F)
#define TERMITE_METAL_ABI_DECLARE(Name) \
    typedef struct { TERMITE_METAL_ABI_FIELDS(Name, TERMITE_METAL_ABI_FIELD_C) } termite_metal_##Name##_params;
#define TERMITE_METAL_ABI_DECLARE_TAGGED(Name) \
    typedef struct termite_metal_##Name##_params { TERMITE_METAL_ABI_FIELDS(Name, TERMITE_METAL_ABI_FIELD_C) } termite_metal_##Name##_params;
#define TERMITE_METAL_ABI_MSL(Name) \
    "struct termite_metal_" #Name "_params { " \
    TERMITE_METAL_ABI_FIELDS(Name, TERMITE_METAL_ABI_FIELD_MSL) "};\n"
#define TERMITE_METAL_ABI_FIELDS_sample_logits(F) \
    F(u32, out_dim) F(f32, temperature) F(u32, top_k) F(f32, top_p) F(f32, min_p) F(f32, repetition_penalty) \
    F(f32, frequency_penalty) F(f32, presence_penalty) F(u32, penalty_count) F(u32, seed) \
    F(f32, final_logit_softcap) F(u32, defer_monotonic_max)

#define TERMITE_METAL_ABI_FIELDS_linear_id(F) \
    F(u32, rows) F(u32, in_dim) F(u32, out_dim) F(u32, row_blocks) F(u32, source_out_dim) F(u32, row_offset) \
    F(u32, expert_count) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_a4b_down_reduce(F) \
    F(u32, has_expert_scale) F(u32, reserved0) F(u32, reserved1) F(u32, reserved2)

#define TERMITE_METAL_ABI_FIELDS_moe_scatter(F) \
    F(u32, base_rows) F(u32, route_rows) F(u32, dim) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_key_scores(F) \
    F(u32, q_len) F(u32, block_tokens) F(u32, num_heads) F(u32, num_kv_heads) F(u32, head_dim) \
    F(u32, key_row_bytes)

#define TERMITE_METAL_ABI_FIELDS_tl1(F) \
    F(u32, rows) F(u32, in_dim) F(u32, out_dim) F(u32, packed_len) F(u32, bm) F(u32, cfg_by) F(u32, bmm)

#define TERMITE_METAL_ABI_FIELDS_tl2(F) \
    F(u32, rows) F(u32, in_dim) F(u32, out_dim) F(u32, scale_off) F(u32, three_value_len) F(u32, three_sign_len) \
    F(u32, bm) F(u32, cfg_by) F(u32, bmm) F(u32, three_cols) F(u32, two_cols)

#define TERMITE_METAL_ABI_FIELDS_embed_absolute_position(F) \
    F(u32, token_id) F(u32, position_id) F(u32, hidden_size)

#define TERMITE_METAL_ABI_FIELDS_embedding_lookup(F) \
    F(u32, total) F(u32, dim)

#define TERMITE_METAL_ABI_FIELDS_quant_rows(F) \
    F(u32, total) F(u32, dim) F(u32, source_rows) F(u32, row_offset) F(f32, scale)

#define TERMITE_METAL_ABI_FIELDS_rope(F) \
    F(u32, total_chunks) F(u32, head_dim) F(u32, rope_dim) F(f32, theta) F(f32, freq_scale) \
    F(u32, consecutive_pairs)

#define TERMITE_METAL_ABI_FIELDS_mrope(F) \
    F(u32, total_chunks) F(u32, token_count) F(u32, head_dim) F(u32, heads_per_token) F(u32, section_t) \
    F(u32, section_h) F(u32, section_w) F(u32, mode) F(f32, theta) F(f32, freq_scale)

#define TERMITE_METAL_ABI_FIELDS_attention_f32(F) \
    F(u32, q_len) F(u32, kv_len) F(u32, num_heads) F(u32, num_kv_heads) F(u32, head_dim) \
    F(u32, query_position_offset) F(u32, kv_position_offset) F(u32, sliding_window) F(u32, total_sequence_len) \
    F(u32, has_bias) F(u32, has_mask) F(u32, batch) F(u32, reserved0)

#define TERMITE_METAL_ABI_FIELDS_florence_window(F) \
    F(u32, batch) F(u32, height) F(u32, width) F(u32, dim) F(u32, window_size) F(u32, padded_h) F(u32, padded_w) \
    F(u32, window_area) F(u32, window_count) F(u32, reserved0) F(u32, reserved1) F(u32, reserved2)

#define TERMITE_METAL_ABI_FIELDS_florence_channel(F) \
    F(u32, batch) F(u32, seq_len) F(u32, dim) F(u32, groups) F(u32, channels_per_group) F(u32, reserved0) \
    F(u32, reserved1) F(u32, reserved2)

#define TERMITE_METAL_ABI_FIELDS_compressed_attention_store_local(F) \
    F(u32, query_rows) F(u32, query_abs_start) F(u32, head_dim) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_compressed_attention_component(F) \
    F(u32, query_rows) F(u32, query_abs_start) F(u32, total_tokens) F(u32, compress_rate) F(u32, row_dim) \
    F(u32, gate_width) F(u32, row_count) F(u32, rope_dim) F(f32, theta) F(f32, freq_scale) F(f32, eps) \
    F(u32, consecutive_pairs) F(u32, bias_rows) F(u32, reserved0) F(u32, reserved1) F(u32, reserved2)

#define TERMITE_METAL_ABI_FIELDS_compressed_attention(F) \
    F(u32, query_abs_start) F(u32, query_rows) F(u32, token_count) F(u32, compressed_rows) F(u32, num_heads) \
    F(u32, head_dim) F(u32, sliding_window) F(u32, top_k) F(u32, has_indexer) F(u32, index_rows) \
    F(u32, index_heads) F(u32, index_head_dim) F(u32, has_sinks) F(u32, reserved0) F(f32, scale) \
    F(f32, reserved1)

#define TERMITE_METAL_ABI_FIELDS_layer_norm_bwd(F) \
    F(u32, hidden_size) F(u32, rows) F(f32, eps) F(u32, reserved0)

#define TERMITE_METAL_ABI_FIELDS_apply_layer_norm_scale(F) \
    F(u32, hidden_size) F(f32, eps) F(f32, scale) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_apply_row_norm(F) \
    F(u32, rows) F(u32, hidden_size) F(f32, eps)

#define TERMITE_METAL_ABI_FIELDS_apply_row_norm_scale(F) \
    F(u32, rows) F(u32, hidden_size) F(f32, eps) F(f32, scale)

#define TERMITE_METAL_ABI_FIELDS_apply_linear(F) \
    F(u32, in_dim) F(u32, out_dim)

#define TERMITE_METAL_ABI_FIELDS_quantize_rows(F) \
    F(u32, rows) F(u32, dim)

#define TERMITE_METAL_ABI_FIELDS_topk_reduce(F) \
    F(u32, candidate_count) F(u32, top_k) F(f32, top_p) F(f32, min_p) F(u32, seed)

#define TERMITE_METAL_ABI_FIELDS_apply_activation(F) \
    F(u32, activation_kind) F(u32, rows) F(u32, dim)

#define TERMITE_METAL_ABI_FIELDS_apply_add(F) \
    F(u32, rows) F(u32, dim) F(u32, flags) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_norm_residual(F) \
    F(u32, dim) F(f32, eps) F(f32, norm_scale) F(f32, output_scale)

#define TERMITE_METAL_ABI_FIELDS_gemma4_audio_clamp(F) \
    F(u32, total) F(u32, flags) F(f32, min_value) F(f32, max_value)

#define TERMITE_METAL_ABI_FIELDS_gemma4_audio_rows(F) \
    F(u32, rows) F(u32, dim) F(u32, extra) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_gemma4_audio_channel_norm(F) \
    F(u32, channels) F(u32, positions) F(f32, eps) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_training_accumulate(F) \
    F(u32, elem_count) F(f32, scale) F(u32, first) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_training_accumulate_into(F) \
    F(u32, elem_count) F(f32, divisor) F(f32, renormalize) F(u32, flags)

#define TERMITE_METAL_ABI_FIELDS_training_adamw(F) \
    F(u32, elem_count) F(f32, lr) F(f32, beta1) F(f32, beta2) F(f32, eps) F(f32, weight_decay) \
    F(f32, bias_correction1) F(f32, bias_correction2) F(f32, grad_scale) F(u32, reserved0) F(u32, reserved1) \
    F(u32, reserved2)

#define TERMITE_METAL_ABI_FIELDS_training_sumsq(F) \
    F(u32, elem_count) F(u32, reserved0) F(u32, reserved1) F(u32, reserved2)

#define TERMITE_METAL_ABI_FIELDS_apply_softmax(F) \
    F(u32, rows) F(u32, dim) F(u32, log_softmax) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_moe_route_select(F) \
    F(u32, rows) F(u32, num_experts) F(u32, top_k) F(f32, logit_scale)

#define TERMITE_METAL_ABI_FIELDS_moe_route_map_slots(F) \
    F(u32, rows) F(u32, num_experts) F(u32, top_k) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_moe_slot_q4_0_gate_up(F) \
    F(u32, route_rows) F(u32, total_rows) F(u32, in_dim) F(u32, out_dim) F(u32, row_blocks) F(u32, gate_offset) \
    F(u32, up_offset) F(u32, expert_stride) F(u32, slot_count) F(u32, top_k)

#define TERMITE_METAL_ABI_FIELDS_moe_slot_q4_0_down(F) \
    F(u32, route_rows) F(u32, in_dim) F(u32, out_dim) F(u32, row_blocks) F(u32, down_offset) \
    F(u32, expert_stride) F(u32, slot_count) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_moe_slot_reduce(F) \
    F(u32, rows) F(u32, top_k) F(u32, dim) F(u32, has_expert_scale)

#define TERMITE_METAL_ABI_FIELDS_mapped_page_touch(F) \
    F(u32, page_size) F(u32, page_count)

#define TERMITE_METAL_ABI_FIELDS_mapped_selected_expert_page_touch(F) \
    F(u32, route_count) F(u32, expert_count) F(u32, source_out_dim) F(u32, row_offset) F(u32, logical_out_dim) \
    F(u32, row_stride) F(u32, page_size) F(u32, max_pages_per_route) F(u32, weight_view_offset)

#define TERMITE_METAL_ABI_FIELDS_reduce_last_dim(F) \
    F(u32, rows) F(u32, dim) F(u32, kind) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_nomic_pool(F) \
    F(u32, batch) F(u32, seq_len) F(u32, hidden_size) F(u32, normalize)

#define TERMITE_METAL_ABI_FIELDS_masked_bce(F) \
    F(u32, elem_count) F(u32, mean_reduction) F(f32, positive_weight) F(f32, negative_weight) F(f32, eps) \
    F(u32, reserved0) F(u32, reserved1) F(u32, reserved2)

#define TERMITE_METAL_ABI_FIELDS_broadcast_last_dim(F) \
    F(u32, rows) F(u32, in_dim) F(u32, out_dim) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_paged_attention(F) \
    F(u32, q_len) F(u32, kv_tokens) F(u32, num_heads) F(u32, num_kv_heads) F(u32, head_dim) \
    F(u32, key_row_bytes) F(u32, base_key_row_bytes) F(u32, query_position_offset) F(u32, kv_position_offset) \
    F(u32, sliding_window) F(u32, v_row_stride) F(u32, page_size) F(u32, block_count) \
    F(u32, contiguous_base_token) F(u32, contiguous_blocks) F(u32, format) F(u32, v_element_bytes) \
    F(u32, has_sinks) F(f32, softcap) F(u32, swa_scan_clamp) F(u32, has_explicit_positions)

#define TERMITE_METAL_ABI_FIELDS_decode_gqa_split(F) \
    F(u32, split_count) F(u32, max_splits) F(u32, key_chunk) F(u32, reduce_simdgroups)

#define TERMITE_METAL_ABI_FIELDS_encode_key(F) \
    F(u32, rows) F(u32, num_kv_heads) F(u32, head_dim) F(u32, key_row_bytes) F(u32, base_key_row_bytes)

#define TERMITE_METAL_ABI_FIELDS_paged_f32_kv_seed(F) \
    F(u32, total_tokens) F(u32, suffix_tokens) F(u32, values_per_row) F(u32, page_size) F(u32, key_row_floats) \
    F(u32, v_row_stride)

#define TERMITE_METAL_ABI_FIELDS_slice_last_dim_f32_2d(F) \
    F(u32, rows) F(u32, cols) F(u32, start) F(u32, out_cols)

#define TERMITE_METAL_ABI_FIELDS_gather_axis0_f32_2d(F) \
    F(u32, rows) F(u32, cols) F(u32, index_count) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_concat_lastdim_f32_2d(F) \
    F(u32, rows) F(u32, dim_a) F(u32, dim_b) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_dense_qkv_split_bias(F) \
    F(u32, rows) F(u32, q_out_dim) F(u32, kv_out_dim) F(u32, total_out_dim)

#define TERMITE_METAL_ABI_FIELDS_dense_pair_split_bias(F) \
    F(u32, rows) F(u32, out_dim) F(u32, total_out_dim) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_dense_qkv_direct(F) \
    F(u32, rows) F(u32, in_dim) F(u32, q_out_dim) F(u32, kv_out_dim)

#define TERMITE_METAL_ABI_FIELDS_dense_pair_direct(F) \
    F(u32, rows) F(u32, in_dim) F(u32, out_dim) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_deberta_embeddings_f32(F) \
    F(u32, total) F(u32, rows) F(u32, dim) F(f32, eps)

#define TERMITE_METAL_ABI_FIELDS_gliner_word_embeddings_f32(F) \
    F(u32, batch) F(u32, seq_len) F(u32, hidden_size) F(u32, num_words)

#define TERMITE_METAL_ABI_FIELDS_gliner_gru_combine_f32(F) \
    F(u32, rows) F(u32, dim) F(u32, reserved0) F(u32, reserved1)

#define TERMITE_METAL_ABI_FIELDS_argmax_axis_f32(F) \
    F(u32, outer) F(u32, axis_dim) F(u32, inner) F(u32, reserved)

#define TERMITE_METAL_ABI_FIELDS_convert_dtype_f32(F) \
    F(u32, elem_count) F(u32, kind) F(u32, reserved0) F(u32, reserved1)

#define TERMITE_METAL_ABI_FIELDS_sdpa_f32(F) \
    F(u32, batch) F(u32, seq_len) F(u32, num_heads) F(u32, head_dim) F(u32, bias_mode) F(u32, has_mask) \
    F(u32, layout) F(u32, reserved1)

#define TERMITE_METAL_ABI_FIELDS_disentangled_relative_attention_f32(F) \
    F(u32, batch) F(u32, seq_len) F(u32, num_heads) F(u32, head_dim) F(u32, has_mask) F(u32, reserved0) \
    F(u32, reserved1) F(u32, reserved2)

#define TERMITE_METAL_ABI_FIELDS_deberta_relative_score_gemm_f32(F) \
    F(u32, batch) F(u32, seq_len) F(u32, rel_len) F(u32, num_heads) F(u32, head_dim) F(u32, mode) \
    F(u32, reserved0) F(u32, reserved1)

#define TERMITE_METAL_ABI_FIELDS_transpose_last2(F) \
    F(u32, batch) F(u32, rows) F(u32, cols) F(u32, reserved0)

#define TERMITE_METAL_ABI_FIELDS_dot_general_2d_f32(F) \
    F(u32, m) F(u32, n) F(u32, k) F(u32, rhs_contract_axis)

#define TERMITE_METAL_ABI_FIELDS_dot_general_batched_f32(F) \
    F(u32, batch_count) F(u32, m) F(u32, n) F(u32, k) F(u32, rhs_contract_axis) F(u32, reserved0) \
    F(u32, reserved1) F(u32, reserved2)

#define TERMITE_METAL_ABI_FIELDS_lora_linear_f32(F) \
    F(u32, rows) F(u32, in_dim) F(u32, rank) F(u32, out_dim) F(f32, scale) F(u32, reserved0) F(u32, reserved1) \
    F(u32, reserved2)

#define TERMITE_METAL_ABI_FIELDS_scatter_add_axis0_f32(F) \
    F(u32, out_rows) F(u32, value_rows) F(u32, dim) F(u32, reserved0)

#define TERMITE_METAL_ABI_FIELDS_conv1d_f32(F) \
    F(u32, batch) F(u32, in_channels) F(u32, out_channels) F(u32, time_steps) F(u32, kernel_size) F(u32, stride) \
    F(u32, padding) F(u32, out_time) F(u32, has_bias) F(u32, reserved0) F(u32, reserved1) F(u32, reserved2)

#define TERMITE_METAL_ABI_FIELDS_conv1d_im2col(F) \
    F(u32, batch) F(u32, in_channels) F(u32, time_steps) F(u32, kernel_size) F(u32, stride) F(u32, padding) \
    F(u32, out_time) F(u32, time_major)

#define TERMITE_METAL_ABI_FIELDS_conv2d_f32(F) \
    F(u32, batch) F(u32, in_channels) F(u32, out_channels) F(u32, height) F(u32, width) F(u32, kernel_h) \
    F(u32, kernel_w) F(u32, stride_h) F(u32, stride_w) F(u32, padding_h) F(u32, padding_w) F(u32, groups) \
    F(u32, out_h) F(u32, out_w) F(u32, has_bias) F(u32, reserved0)

// quant-kernel-codegen:end scalar Metal ABI
