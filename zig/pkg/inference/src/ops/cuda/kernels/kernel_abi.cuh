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
