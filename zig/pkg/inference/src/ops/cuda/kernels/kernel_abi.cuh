// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

// Shared launch ABIs for handwritten CUDA entry points. These are parameter
// lists only: topology, math, storage policies, and bodies remain native CUDA.
// Keep each public symbol explicit at its definition. A family may use this
// list only when parameter names, types, and order match exactly; a different
// layout needs its own ABI. Generated kernels retain their renderer-owned ABI.

#define ANTFLY_CUDA_DENSE_ATTENTION_PARAMS \
    float* dst, \
    const float* q, \
    const float* k, \
    const float* v, \
    const long long* mask, \
    const float* bias, \
    unsigned int batch, \
    unsigned int seq_len, \
    unsigned int num_heads, \
    unsigned int head_dim, \
    unsigned int causal, \
    unsigned int has_mask, \
    unsigned int bias_mode, \
    unsigned int head_major

#define ANTFLY_CUDA_PAGED_ATTENTION_PARAMS \
    float* dst, \
    const float* q, \
    const unsigned char* k, \
    const unsigned char* v, \
    const unsigned int* block_table, \
    const unsigned char* attn_or_mask, \
    const float* bias, \
    unsigned int batch, \
    unsigned int q_seq_len, \
    unsigned int kv_seq_len, \
    unsigned int num_heads, \
    unsigned int num_kv_heads, \
    unsigned int head_dim, \
    unsigned int query_position_offset, \
    unsigned int kv_position_offset, \
    unsigned int sliding_window, \
    unsigned int total_sequence_len, \
    unsigned int mask_len, \
    unsigned int bias_mode, \
    unsigned int key_row_bytes, \
    unsigned int base_key_row_bytes, \
    unsigned int value_row_bytes, \
    unsigned int block_count, \
    unsigned int page_size_tokens, \
    unsigned int format, \
    unsigned int value_format, \
    unsigned int physical_token_capacity, \
    const unsigned int* decode_scalars

#define ANTFLY_CUDA_QUANT_LINEAR_PARAMS \
    float* dst, \
    const float* input, \
    const unsigned char* weight, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_QUANT_LINEAR_RESIDUAL_PARAMS \
    float* dst, \
    const float* input, \
    const unsigned char* weight, \
    const float* bias, \
    const float* residual, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_QUANT_LINEAR_BIAS_PARAMS \
    float* dst, \
    const float* input, \
    const unsigned char* weight, \
    const float* bias, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_QUANT_GATED_DOWN_PARAMS \
    float* dst, \
    const float* gate, \
    const float* up, \
    const unsigned char* weight, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim, \
    unsigned int activation

#define ANTFLY_CUDA_Q8_INPUT_LINEAR_PARAMS \
    float* dst, \
    const unsigned char* q8_input, \
    const unsigned char* weight, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_PACKED_LINEAR_BIAS_PARAMS \
    float* dst, \
    const float* input, \
    const unsigned char* packed_weight, \
    const float* bias, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_QUANT_PAIR_PARAMS \
    float* dst_a, \
    float* dst_b, \
    const float* input, \
    const unsigned char* weight_a, \
    const unsigned char* weight_b, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_Q8_INPUT_PAIR_PARAMS \
    float* dst_a, \
    float* dst_b, \
    const unsigned char* q8_input, \
    const unsigned char* weight_a, \
    const unsigned char* weight_b, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_Q8_INPUT_PAIR_ACTIVATION_PARAMS \
    float* dst, \
    const unsigned char* q8_input, \
    const unsigned char* weight_gate, \
    const unsigned char* weight_up, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim, \
    unsigned int activation

#define ANTFLY_CUDA_QUANT_ARGMAX_PARAMS \
    float* partial_values, \
    unsigned int* partial_indices, \
    const float* input, \
    const unsigned char* weight, \
    const int* suppress_token_ids, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim, \
    unsigned int suppress_count

#define ANTFLY_CUDA_QUANT_QKV_PARAMS \
    float* dst_q, \
    float* dst_k, \
    float* dst_v, \
    const float* input, \
    const unsigned char* weight_q, \
    const unsigned char* weight_k, \
    const unsigned char* weight_v, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int q_out_dim, \
    unsigned int kv_out_dim

#define ANTFLY_CUDA_Q8_INPUT_QKV_PARAMS \
    float* dst_q, \
    float* dst_k, \
    float* dst_v, \
    const unsigned char* q8_input, \
    const unsigned char* weight_q, \
    const unsigned char* weight_k, \
    const unsigned char* weight_v, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int q_out_dim, \
    unsigned int kv_out_dim

#define ANTFLY_CUDA_TQ_SPLIT_STAGE1_PARAMS \
float* partial_acc, \
float* partial_meta, \
const float* q, \
const unsigned char* k, \
const unsigned char* v, \
const unsigned int* block_table, \
unsigned int kv_seq_len, \
unsigned int num_heads, \
unsigned int num_kv_heads, \
unsigned int head_dim, \
unsigned int query_position_offset, \
unsigned int kv_position_offset, \
unsigned int sliding_window, \
unsigned int key_row_bytes, \
unsigned int value_row_bytes, \
unsigned int block_count, \
unsigned int page_size_tokens, \
unsigned int format, \
unsigned int value_format, \
unsigned int physical_token_capacity, \
const unsigned int* decode_scalars, \
unsigned int chunk_size, \
unsigned int chunk_count

#define ANTFLY_CUDA_DEBERTA_ATTENTION_PARAMS \
float* dst, \
const float* q, \
const float* k, \
const float* v, \
const float* q_r, \
const float* k_r, \
const long long* mask, \
unsigned int batch, \
unsigned int seq_len, \
unsigned int num_heads, \
unsigned int head_dim

#define ANTFLY_CUDA_KV_SUFFIX_DENSE_PARAMS \
float* k_dst, \
float* v_dst, \
const float* k_src, \
const float* v_src, \
const unsigned int* decode_scalars, \
unsigned int suffix_token_count, \
unsigned int row_width, \
unsigned int fallback_total_token_count

#define ANTFLY_CUDA_KV_SUFFIX_TQ_PARAMS \
unsigned char* k_dst, \
unsigned char* v_dst, \
const unsigned int* block_table, \
const float* k_src, \
const float* v_src, \
const unsigned int* decode_scalars, \
unsigned int suffix_token_count, \
unsigned int row_width, \
unsigned int num_kv_heads, \
unsigned int head_dim, \
unsigned int key_row_bytes, \
unsigned int base_key_row_bytes, \
unsigned int value_row_bytes, \
unsigned int fallback_total_token_count, \
unsigned int block_count, \
unsigned int page_size_tokens, \
unsigned int format, \
unsigned int value_format, \
unsigned int physical_token_capacity

#define ANTFLY_CUDA_Q8_QUANT_ARGMAX_PARAMS \
float* partial_values, \
unsigned int* partial_indices, \
const unsigned char* input_q8_1, \
const unsigned char* weight, \
const int* suppress_token_ids, \
unsigned int rows, \
unsigned int in_dim, \
unsigned int out_dim, \
unsigned int suppress_count

#define ANTFLY_CUDA_TRAIN_ADAMW_PARAMS \
float* weight, \
const float* grad, \
float* m, \
float* v, \
unsigned int count, \
float lr, \
float beta1, \
float beta2, \
float eps, \
float weight_decay, \
float bias_correction1, \
float bias_correction2, \
float grad_scale

#define ANTFLY_CUDA_LAYER_NORM_BACKWARD_PARAMS \
float* output, \
const float* input, \
const float* gamma, \
const float* d_y, \
unsigned int rows, \
unsigned int dim, \
float eps

#define ANTFLY_CUDA_PRIMITIVE_SOFTMAX_PARAMS \
float* output, \
const float* input, \
unsigned int count, \
unsigned int last_dim, \
unsigned int log_softmax

#define ANTFLY_CUDA_GLINER_BOUNDARY_PARAMS \
    const float* q, \
    const float* k, \
    const float* v, \
    const float* q_r, \
    const float* k_r, \
    const float* bucket_values, \
    const float* mask_values, \
    const float* unused7, \
    const float* unused8, \
    const float* unused9, \
    float* dst, \
    Gliner25BoundaryParams p, \
    unsigned int work_items

// Forward normalization and selected rotary/index contracts. Names, widths and order are native-owned.

#define ANTFLY_CUDA_RMS_NORM_PARAMS \
    float* dst, \
    const float* input, \
    const float* weight, \
    unsigned int rows, \
    unsigned int dim, \
    float eps

#define ANTFLY_CUDA_RMS_NORM_F32_BF16_PARAMS \
    float* dst, \
    unsigned short* dst_bf16, \
    const float* input, \
    const float* weight, \
    unsigned int rows, \
    unsigned int dim, \
    float eps

#define ANTFLY_CUDA_RMS_NORM_ADD_F32_BF16_PARAMS \
    float* dst, \
    unsigned short* dst_bf16, \
    const float* input, \
    const float* weight, \
    const float* residual, \
    unsigned int rows, \
    unsigned int dim, \
    float eps

#define ANTFLY_CUDA_RMS_NORM_ADD_PARAMS \
    float* dst, \
    const float* input, \
    const float* weight, \
    const float* residual, \
    unsigned int rows, \
    unsigned int dim, \
    float eps

#define ANTFLY_CUDA_RMS_NORM_ADD_MUL_SCALAR_PARAMS \
    float* dst, \
    const float* input, \
    const float* weight, \
    const float* residual, \
    const float* scalar, \
    unsigned int rows, \
    unsigned int dim, \
    float eps

#define ANTFLY_CUDA_RMS_NORM_BARE_PARAMS \
    float* dst, \
    const float* input, \
    unsigned int rows, \
    unsigned int dim, \
    float eps

#define ANTFLY_CUDA_GEMMA4_A4B_RMS_NORM_TRIPLE_PARAMS \
    float* first, \
    float* second, \
    float* third, \
    const float* input, \
    const float* first_weight, \
    const float* second_weight, \
    const float* third_weight, \
    unsigned int rows, \
    unsigned int dim, \
    float eps

#define ANTFLY_CUDA_LAYER_NORM_PARAMS \
    float* dst, \
    const float* input, \
    const float* gamma, \
    const float* beta, \
    unsigned int rows, \
    unsigned int dim, \
    float eps

#define ANTFLY_CUDA_ADD_LAYER_NORM_PARAMS \
    float* dst, \
    const float* a, \
    const float* b, \
    const float* gamma, \
    const float* beta, \
    unsigned int rows, \
    unsigned int dim, \
    float eps

#define ANTFLY_CUDA_ROPE_PARAMS \
    float* dst, \
    const float* input, \
    unsigned int total_chunks, \
    unsigned int head_dim, \
    unsigned int rope_dim, \
    float theta, \
    float freq_scale, \
    unsigned int position_offset, \
    unsigned int seq_len, \
    unsigned int chunks_per_position, \
    unsigned int consecutive_pairs

#define ANTFLY_CUDA_ROPE_DECODE_SCALARS_PARAMS \
    float* dst, \
    const float* input, \
    unsigned int total_chunks, \
    unsigned int head_dim, \
    unsigned int rope_dim, \
    float theta, \
    float freq_scale, \
    unsigned int position_offset, \
    unsigned int seq_len, \
    unsigned int chunks_per_position, \
    unsigned int consecutive_pairs, \
    const unsigned int* decode_scalars

#define ANTFLY_CUDA_ROPE_SCALED_PARAMS \
    float* dst, \
    const float* input, \
    unsigned int total_chunks, \
    unsigned int head_dim, \
    unsigned int rope_dim, \
    float theta, \
    float freq_scale, \
    unsigned int position_offset, \
    unsigned int seq_len, \
    unsigned int chunks_per_position, \
    unsigned int consecutive_pairs, \
    float scale

#define ANTFLY_CUDA_ROPE_SCALED_DECODE_SCALARS_PARAMS \
    float* dst, \
    const float* input, \
    unsigned int total_chunks, \
    unsigned int head_dim, \
    unsigned int rope_dim, \
    float theta, \
    float freq_scale, \
    unsigned int position_offset, \
    unsigned int seq_len, \
    unsigned int chunks_per_position, \
    unsigned int consecutive_pairs, \
    float scale, \
    const unsigned int* decode_scalars

#define ANTFLY_CUDA_ROPE_PER_ITEM_PARAMS \
    float* dst, \
    const float* input, \
    const unsigned int* query_lengths, \
    const unsigned int* position_offsets, \
    unsigned int batch, \
    unsigned int max_seq_len, \
    unsigned int num_heads, \
    unsigned int head_dim, \
    unsigned int rope_dim, \
    float theta, \
    float freq_scale, \
    unsigned int consecutive_pairs

#define ANTFLY_CUDA_QWEN3VL_VISION_ROPE_PARAMS \
    float* dst, \
    const float* input, \
    const unsigned int* positions, \
    unsigned int total_chunks, \
    unsigned int token_count, \
    unsigned int heads_per_token, \
    unsigned int head_dim, \
    float theta

#define ANTFLY_CUDA_RMS_NORM_HEADS_ROPE_PARAMS \
    float* dst, \
    const float* input, \
    const float* weight, \
    unsigned int total_chunks, \
    unsigned int head_dim, \
    unsigned int rope_dim, \
    float eps, \
    float theta, \
    float freq_scale, \
    unsigned int position_offset, \
    unsigned int seq_len, \
    unsigned int chunks_per_position, \
    unsigned int consecutive_pairs, \
    float output_scale

#define ANTFLY_CUDA_RMS_NORM_HEADS_ROPE_DECODE_SCALARS_PARAMS \
    float* dst, \
    const float* input, \
    const float* weight, \
    unsigned int total_chunks, \
    unsigned int head_dim, \
    unsigned int rope_dim, \
    float eps, \
    float theta, \
    float freq_scale, \
    unsigned int position_offset, \
    unsigned int seq_len, \
    unsigned int chunks_per_position, \
    unsigned int consecutive_pairs, \
    float output_scale, \
    const unsigned int* decode_scalars

#define ANTFLY_CUDA_PRIMITIVE_BATCHED_DOT_PARAMS \
    float* output, \
    const float* lhs, \
    const float* rhs, \
    unsigned int batch_count, \
    unsigned int m, \
    unsigned int n, \
    unsigned int k, \
    unsigned int lhs_contract_last, \
    unsigned int rhs_contract_last

#define ANTFLY_CUDA_PRIMITIVE_GATHER_PARAMS \
    float* output, \
    const float* input, \
    const float* indices, \
    unsigned int output_count, \
    unsigned int index_count, \
    unsigned int axis_extent, \
    unsigned int suffix_size

#define ANTFLY_CUDA_PRIMITIVE_GATHER_BF16_PARAMS \
    float* output, \
    const unsigned short* input, \
    const float* indices, \
    unsigned int output_count, \
    unsigned int index_count, \
    unsigned int axis_extent, \
    unsigned int suffix_size

#define ANTFLY_CUDA_LINEAR_F32_PARAMS \
    float* dst, \
    const float* input, \
    const float* weight, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_LINEAR_BF16_WEIGHT_F32_TILED_PARAMS \
    float* dst, \
    const float* input, \
    const unsigned short* weight, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_GEMMA4_MTP_PREPROJECT_F32_PARAMS \
    float* dst, \
    const float* target_embedding, \
    const float* activation, \
    const void* weight, \
    unsigned int backbone_hidden, \
    unsigned int draft_hidden, \
    unsigned int concat_order, \
    unsigned int weight_dtype

#define ANTFLY_CUDA_GEMMA4_MTP_MASKED_SELECT_F32_PARAMS \
    unsigned int* dst, \
    const float* assistant_hidden, \
    const float* logits, \
    const void* centroid_weight, \
    const float* token_ordering, \
    unsigned int hidden_size, \
    unsigned int vocab_size, \
    unsigned int num_centroids, \
    unsigned int top_k, \
    unsigned int use_inverse_ordering, \
    unsigned int centroid_weight_dtype

#define ANTFLY_CUDA_GEMMA4_MTP_MASKED_SELECT_HIDDEN_F32_PARAMS \
    unsigned int* dst, \
    const float* assistant_hidden, \
    const void* lm_head_weight, \
    const void* centroid_weight, \
    const float* token_ordering, \
    unsigned int hidden_size, \
    unsigned int vocab_size, \
    unsigned int num_centroids, \
    unsigned int top_k, \
    unsigned int use_inverse_ordering, \
    unsigned int lm_head_dtype, \
    unsigned int centroid_weight_dtype

#define ANTFLY_CUDA_LINEAR_PAIR_BIAS_F32_TILE4_R2_PARAMS \
    float* dst_a, \
    float* dst_b, \
    const float* input, \
    const float* weight_a, \
    const float* bias_a, \
    const float* weight_b, \
    const float* bias_b, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_LINEAR_TRIPLE_BIAS_F32_TILE4_R2_PARAMS \
    float* dst_a, \
    float* dst_b, \
    float* dst_c, \
    const float* input, \
    const float* weight_a, \
    const float* bias_a, \
    const float* weight_b, \
    const float* bias_b, \
    const float* weight_c, \
    const float* bias_c, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_EMBEDDING_LOOKUP_BF16_WEIGHT_F32_PARAMS \
    float* dst, \
    const unsigned short* weight, \
    const long long* ids, \
    unsigned int total, \
    unsigned int dim, \
    float scale

#define ANTFLY_CUDA_GEMMA4_A4B_Q4_0_GATE_UP_ROWS_Q8_1_F32_PARAMS \
    float* dst, \
    const unsigned char* q8_input, \
    const unsigned char* gate_base, \
    const unsigned char* up_base, \
    const unsigned int* route_ids, \
    unsigned long long gate_expert_stride, \
    unsigned long long up_expert_stride, \
    unsigned int rows, \
    unsigned int top_k, \
    unsigned int in_dim, \
    unsigned int out_dim, \
    unsigned int activation

#define ANTFLY_CUDA_GEMMA4_A4B_Q4_0_DOWN_ROWS_Q8_1_F32_PARAMS \
    float* expert_output, \
    const unsigned char* q8_activated, \
    const unsigned char* down_base, \
    const unsigned int* route_ids, \
    unsigned long long down_expert_stride, \
    unsigned int rows, \
    unsigned int top_k, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_EMBEDDING_LOOKUP_Q4_K_F32_PARAMS \
    float* dst, \
    const unsigned char* weight, \
    const long long* ids, \
    unsigned int total, \
    unsigned int dim, \
    float scale

#define ANTFLY_CUDA_EMBEDDING_LOOKUP_I32_Q4_K_F32_PARAMS \
    float* dst, \
    const unsigned char* weight, \
    const int* ids, \
    unsigned int total, \
    unsigned int dim, \
    float scale

#define ANTFLY_CUDA_RMS_NORM_ADD_WEIGHTED_EMBEDDING_I32_Q6_K_F32_PARAMS \
    float* dst, \
    const float* input, \
    const float* norm_weight, \
    const unsigned char* embedding_weight, \
    const int* ids, \
    unsigned int total, \
    unsigned int num_groups, \
    unsigned int group_dim, \
    unsigned int embedding_dim, \
    float eps, \
    float lhs_scale, \
    float rhs_scale

#define ANTFLY_CUDA_CONV2D_F32_PARAMS \
    float* dst, \
    const float* input, \
    const float* weight, \
    const float* bias, \
    unsigned int batch, \
    unsigned int in_channels, \
    unsigned int out_channels, \
    unsigned int height, \
    unsigned int width, \
    unsigned int kernel_h, \
    unsigned int kernel_w, \
    unsigned int stride_h, \
    unsigned int stride_w, \
    unsigned int padding_h, \
    unsigned int padding_w, \
    unsigned int groups, \
    unsigned int out_h, \
    unsigned int out_w

#define ANTFLY_CUDA_TOKEN_TO_NCHW_F32_PARAMS \
    float* dst, \
    const float* src, \
    unsigned int batch, \
    unsigned int channels, \
    unsigned int height, \
    unsigned int width

#define ANTFLY_CUDA_PACK_WINDOWS_F32_PARAMS \
    float* dst, \
    const float* src, \
    unsigned int batch, \
    unsigned int height, \
    unsigned int width, \
    unsigned int dim, \
    unsigned int window_size, \
    unsigned int padded_h, \
    unsigned int padded_w

#define ANTFLY_CUDA_FLORENCE_VISION_TAIL_SOURCES_F32_PARAMS \
    float* dst, \
    const float* tokens, \
    const void* row_embed, \
    const void* col_embed, \
    const void* temporal_embed, \
    unsigned int batch, \
    unsigned int height, \
    unsigned int width, \
    unsigned int dim, \
    unsigned int has_temporal, \
    unsigned int row_dtype, \
    unsigned int col_dtype, \
    unsigned int temporal_dtype

#define ANTFLY_CUDA_QWEN3VL_MROPE_F32_PARAMS \
    float* dst, \
    const float* input, \
    const unsigned int* positions, \
    unsigned int total_chunks, \
    unsigned int token_count, \
    unsigned int chunks_per_token, \
    unsigned int head_dim, \
    float theta, \
    float freq_scale, \
    unsigned int section_height, \
    unsigned int section_width

#define ANTFLY_CUDA_DEBERTA_ATTENTION_F32_PARAMS \
    float* dst, \
    const float* q, \
    const float* k, \
    const float* v, \
    const float* q_r, \
    const float* k_r, \
    const long long* mask, \
    const float* attn_bias, \
    unsigned int use_bias, \
    unsigned int batch, \
    unsigned int seq_len, \
    unsigned int num_heads, \
    unsigned int head_dim

#define ANTFLY_CUDA_DEBERTA_ATTENTION_BACKWARD_SCORES_F32_PARAMS \
    float* probabilities, \
    float* d_scores, \
    const float* q, \
    const float* k, \
    const float* v, \
    const float* q_r, \
    const float* k_r, \
    const long long* mask, \
    const float* attn_bias, \
    unsigned int use_bias, \
    const float* d_out, \
    unsigned int batch, \
    unsigned int seq_len, \
    unsigned int num_heads, \
    unsigned int head_dim

#define ANTFLY_CUDA_DEBERTA_ATTENTION_BACKWARD_F32_PARAMS \
    float* dst, \
    const float* probabilities, \
    const float* d_scores, \
    const float* q, \
    const float* k, \
    const float* q_r, \
    const float* k_r, \
    const long long* mask, \
    const float* attn_bias, \
    unsigned int use_bias, \
    const float* d_out, \
    unsigned int batch, \
    unsigned int seq_len, \
    unsigned int num_heads, \
    unsigned int head_dim

#define ANTFLY_CUDA_MASKED_BCE_BACKWARD_F32_PARAMS \
    float* output, \
    const float* logits, \
    const float* labels, \
    const float* mask, \
    const float* upstream, \
    const float* accum, \
    unsigned int count, \
    float positive_weight, \
    float negative_weight, \
    float eps, \
    unsigned int mean_reduction

#define ANTFLY_CUDA_DEBERTA_ATTENTION_STREAM_F16_PARAMS \
    float* dst, \
    const unsigned short* q, \
    const unsigned short* k, \
    const unsigned short* v, \
    const unsigned short* q_r, \
    const unsigned short* k_r, \
    const long long* mask, \
    unsigned int batch, \
    unsigned int seq_len, \
    unsigned int num_heads, \
    unsigned int head_dim

#define ANTFLY_CUDA_DEBERTA_PACK_HEADS_F16_PARAMS \
    unsigned short* q_out, \
    unsigned short* k_out, \
    unsigned short* v_out, \
    unsigned short* q_r_out, \
    unsigned short* k_r_out, \
    const float* q, \
    const float* k, \
    const float* v, \
    const float* q_r, \
    const float* k_r, \
    unsigned int batch, \
    unsigned int seq_len, \
    unsigned int num_heads, \
    unsigned int head_dim

#define ANTFLY_CUDA_LINEAR_Q4_0_ACTIVATION_SLICE_LAST_DIM_F32_TILE4_PARAMS \
    float* dst, \
    const float* input, \
    const unsigned char* weight, \
    const float* source, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim, \
    unsigned int source_cols, \
    unsigned int source_start, \
    unsigned int activation

#define ANTFLY_CUDA_LINEAR_Q4_K_TRIPLE_BIAS_F32_TC_HMMA_PARAMS \
    float* dst_a, \
    float* dst_b, \
    float* dst_c, \
    const float* input, \
    const unsigned char* packed_weight_a, \
    const float* bias_a, \
    const unsigned char* packed_weight_b, \
    const float* bias_b, \
    const unsigned char* packed_weight_c, \
    const float* bias_c, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_LINEAR_Q4_K_TRIPLE_BIAS_F32_PARAMS \
    float* dst_a, \
    float* dst_b, \
    float* dst_c, \
    const float* input, \
    const unsigned char* weight_a, \
    const float* bias_a, \
    const unsigned char* weight_b, \
    const float* bias_b, \
    const unsigned char* weight_c, \
    const float* bias_c, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_LINEAR_Q4_K_PAIR_BIAS_F32_TILED_PARAMS \
    float* dst_a, \
    float* dst_b, \
    const float* input, \
    const unsigned char* weight_a, \
    const float* bias_a, \
    const unsigned char* weight_b, \
    const float* bias_b, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int out_dim

#define ANTFLY_CUDA_LINEAR_F32_QKV_NOBIAS_TILED_PARAMS \
    float* dst_q, \
    float* dst_k, \
    float* dst_v, \
    const float* input, \
    const float* weight_q, \
    const float* weight_k, \
    const float* weight_v, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int q_out_dim, \
    unsigned int kv_out_dim

#define ANTFLY_CUDA_LINEAR_BF16_WEIGHT_F32_QKV_NOBIAS_TILED_PARAMS \
    float* dst_q, \
    float* dst_k, \
    float* dst_v, \
    const float* input, \
    const unsigned short* weight_q, \
    const unsigned short* weight_k, \
    const unsigned short* weight_v, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int q_out_dim, \
    unsigned int kv_out_dim

#define ANTFLY_CUDA_LINEAR_Q4_K_Q4_K_F32_QKV_NOBIAS_TILED_PARAMS \
    float* dst_q, \
    float* dst_k, \
    float* dst_v, \
    const float* input, \
    const unsigned char* weight_q, \
    const unsigned char* weight_k, \
    const float* weight_v, \
    unsigned int rows, \
    unsigned int in_dim, \
    unsigned int q_out_dim, \
    unsigned int kv_out_dim

#define ANTFLY_CUDA_PRIMITIVE_GATHER_I32_PARAMS \
    float* output, \
    const float* input, \
    const int* indices, \
    unsigned int output_count, \
    unsigned int index_count, \
    unsigned int axis_extent, \
    unsigned int suffix_size

#define ANTFLY_CUDA_PRIMITIVE_GATHER_I64_PARAMS \
    float* output, \
    const float* input, \
    const long long* indices, \
    unsigned int output_count, \
    unsigned int index_count, \
    unsigned int axis_extent, \
    unsigned int suffix_size

#define ANTFLY_CUDA_PRIMITIVE_GATHER_BF16_I32_PARAMS \
    float* output, \
    const unsigned short* input, \
    const int* indices, \
    unsigned int output_count, \
    unsigned int index_count, \
    unsigned int axis_extent, \
    unsigned int suffix_size

#define ANTFLY_CUDA_PRIMITIVE_GATHER_BF16_I64_PARAMS \
    float* output, \
    const unsigned short* input, \
    const long long* indices, \
    unsigned int output_count, \
    unsigned int index_count, \
    unsigned int axis_extent, \
    unsigned int suffix_size

#define ANTFLY_CUDA_PRIMITIVE_SCATTER_PARAMS \
    float* output, \
    const float* input, \
    const float* indices, \
    unsigned int rows, \
    unsigned int cols, \
    unsigned int output_rows

#define ANTFLY_CUDA_PRIMITIVE_SCATTER_I32_PARAMS \
    float* output, \
    const float* input, \
    const int* indices, \
    unsigned int rows, \
    unsigned int cols, \
    unsigned int output_rows

#define ANTFLY_CUDA_PRIMITIVE_SCATTER_I64_PARAMS \
    float* output, \
    const float* input, \
    const long long* indices, \
    unsigned int rows, \
    unsigned int cols, \
    unsigned int output_rows
