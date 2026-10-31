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

// native-program:entry termite_gemma4_mtp_preproject_f32
extern "C" __global__ void termite_gemma4_mtp_preproject_f32(ANTFLY_CUDA_GEMMA4_MTP_PREPROJECT_F32_PARAMS) {
    __shared__ float partial[256];
    unsigned int row = blockIdx.x;
    unsigned int tid = threadIdx.x;
    if (row >= draft_hidden || backbone_hidden == 0u) return;

    unsigned int in_dim = backbone_hidden * 2u;
    unsigned int row_base = row * in_dim;
    float sum = 0.0f;
    for (unsigned int i = tid; i < backbone_hidden; i += blockDim.x) {
        float first = target_embedding[i];
        float second = activation[i];
        if (concat_order == 1u) {
            first = activation[i];
            second = target_embedding[i];
        }
        sum += first * termite_mtp_load_preproject_weight(weight, row_base + i, weight_dtype);
        sum += second * termite_mtp_load_preproject_weight(weight, row_base + backbone_hidden + i, weight_dtype);
    }
    partial[tid] = sum;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[row] = partial[0];
}
// native-program:entry termite_gemma4_mtp_masked_select_f32
extern "C" __global__ void termite_gemma4_mtp_masked_select_f32(ANTFLY_CUDA_GEMMA4_MTP_MASKED_SELECT_F32_PARAMS) {
    termite_mtp_masked_select_impl(dst, assistant_hidden, termite_mtp_logits_score{logits}, centroid_weight, token_ordering, hidden_size, vocab_size, num_centroids, top_k, use_inverse_ordering, centroid_weight_dtype);
}
// native-program:entry termite_gemma4_mtp_centroid_scores_hidden_f32
extern "C" __global__ void termite_gemma4_mtp_centroid_scores_hidden_f32(
    float* centroid_scores,
    const float* assistant_hidden,
    const void* centroid_weight,
    unsigned int hidden_size,
    unsigned int num_centroids,
    unsigned int centroid_weight_dtype
) {
    __shared__ float partial[256];
    unsigned int centroid = blockIdx.x;
    unsigned int tid = threadIdx.x;
    if (centroid >= num_centroids || hidden_size == 0u) return;

    unsigned int row_base = centroid * hidden_size;
    float sum = 0.0f;
    for (unsigned int i = tid; i < hidden_size; i += blockDim.x) {
        sum += assistant_hidden[i] * termite_mtp_load_preproject_weight(centroid_weight, row_base + i, centroid_weight_dtype);
    }
    partial[tid] = sum;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) centroid_scores[centroid] = partial[0];
}
// native-program:entry termite_gemma4_mtp_centroid_topk_u32
extern "C" __global__ void termite_gemma4_mtp_centroid_topk_u32(
    unsigned int* top_centroids,
    const float* centroid_scores,
    unsigned int num_centroids,
    unsigned int top_k
) {
    __shared__ float top_scores[128];
    __shared__ unsigned int top_ids[128];
    if (threadIdx.x != 0u) return;
    if (top_k > 128u) top_k = 128u;
    if (top_k == 0u) return;
    for (unsigned int i = 0u; i < top_k; ++i) {
        top_scores[i] = -3.402823466e+38f;
        top_ids[i] = 0u;
    }
    for (unsigned int centroid = 0u; centroid < num_centroids; ++centroid) {
        float score = centroid_scores[centroid];
        unsigned int insert_at = top_k;
        for (unsigned int i = 0u; i < top_k; ++i) {
            if (score > top_scores[i]) {
                insert_at = i;
                break;
            }
        }
        if (insert_at == top_k) continue;
        for (unsigned int i = top_k - 1u; i > insert_at; --i) {
            top_scores[i] = top_scores[i - 1u];
            top_ids[i] = top_ids[i - 1u];
        }
        top_scores[insert_at] = score;
        top_ids[insert_at] = centroid;
    }
    for (unsigned int i = 0u; i < top_k; ++i) top_centroids[i] = top_ids[i];
}
// native-program:entry termite_gemma4_mtp_restricted_lm_head_scores_f32
extern "C" __global__ void termite_gemma4_mtp_restricted_lm_head_scores_f32(
    float* partial_values,
    unsigned int* partial_tokens,
    const float* assistant_hidden,
    const void* lm_head_weight,
    const float* token_ordering,
    const unsigned int* top_centroids,
    unsigned int hidden_size,
    unsigned int vocab_size,
    unsigned int num_centroids,
    unsigned int top_k,
    unsigned int lm_head_dtype
) {
    __shared__ float partial[256];
    unsigned int candidate = blockIdx.x;
    unsigned int tid = threadIdx.x;
    unsigned int cluster_size = num_centroids == 0u ? 0u : vocab_size / num_centroids;
    unsigned int candidate_count = top_k * cluster_size;
    if (candidate >= candidate_count || cluster_size == 0u || hidden_size == 0u) return;

    unsigned int top_slot = candidate / cluster_size;
    unsigned int offset = candidate - top_slot * cluster_size;
    unsigned int ordered_pos = top_centroids[top_slot] * cluster_size + offset;
    float token_f = ordered_pos < vocab_size ? token_ordering[ordered_pos] : -1.0f;
    unsigned int token = token_f >= 0.0f ? (unsigned int)token_f : 0u;
    bool valid = token_f >= 0.0f && token < vocab_size;

    float sum = valid ? 0.0f : -3.402823466e+38f;
    if (valid) {
        unsigned int row_base = token * hidden_size;
        for (unsigned int i = tid; i < hidden_size; i += blockDim.x) {
            sum += assistant_hidden[i] * termite_mtp_load_preproject_weight(lm_head_weight, row_base + i, lm_head_dtype);
        }
    }
    partial[tid] = sum;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) {
        partial_values[candidate] = partial[0];
        partial_tokens[candidate] = token;
    }
}
// native-program:entry termite_gemma4_mtp_reduce_token_scores_f32
extern "C" __global__ void termite_gemma4_mtp_reduce_token_scores_f32(
    unsigned int* dst,
    const float* partial_values,
    const unsigned int* partial_tokens,
    unsigned int candidate_count
) {
    __shared__ float block_values[256];
    __shared__ unsigned int block_indices[256];
    unsigned int tid = threadIdx.x;
    float best_score = -3.402823466e+38f;
    unsigned int best_token = 0u;
    for (unsigned int i = tid; i < candidate_count; i += blockDim.x) {
        float score = partial_values[i];
        unsigned int token = partial_tokens[i];
        if (score > best_score || (score == best_score && token < best_token)) {
            best_score = score;
            best_token = token;
        }
    }
    block_values[tid] = best_score;
    block_indices[tid] = best_token;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            float rhs_score = block_values[tid + stride];
            unsigned int rhs_token = block_indices[tid + stride];
            float lhs_score = block_values[tid];
            unsigned int lhs_token = block_indices[tid];
            if (rhs_score > lhs_score || (rhs_score == lhs_score && rhs_token < lhs_token)) {
                block_values[tid] = rhs_score;
                block_indices[tid] = rhs_token;
            }
        }
        __syncthreads();
    }
    if (tid == 0u) dst[0] = block_indices[0];
}
// native-program:entry termite_gemma4_mtp_masked_select_hidden_f32
extern "C" __global__ void termite_gemma4_mtp_masked_select_hidden_f32(ANTFLY_CUDA_GEMMA4_MTP_MASKED_SELECT_HIDDEN_F32_PARAMS) {
    termite_mtp_masked_select_impl(dst, assistant_hidden, termite_mtp_hidden_score{assistant_hidden, lm_head_weight, hidden_size, lm_head_dtype}, centroid_weight, token_ordering, hidden_size, vocab_size, num_centroids, top_k, use_inverse_ordering, centroid_weight_dtype);
}
// native-program:entry termite_gemma4_mtp_masked_argmax_f32
extern "C" __global__ void termite_gemma4_mtp_masked_argmax_f32(
    unsigned int* dst,
    const float* logits,
    const float* centroid_logits,
    const float* token_ordering,
    unsigned int vocab_size,
    unsigned int num_centroids,
    unsigned int top_k,
    unsigned int use_inverse_ordering
) {

    if (vocab_size == 0u || num_centroids == 0u || top_k == 0u) {
        if (threadIdx.x == 0u) dst[0] = 0u;
        return;
    }
    if (top_k > 128u) top_k = 128u;
    unsigned int cluster_size = vocab_size / num_centroids;
    if (cluster_size == 0u) {
        if (threadIdx.x == 0u) dst[0] = 0u;
        return;
    }

    termite_mtp_rank_and_select(dst, termite_mtp_logits_score{logits}, centroid_logits, token_ordering, vocab_size, num_centroids, top_k, use_inverse_ordering, cluster_size);
}
// native-program:entry termite_gemma4_mtp_verify_commit_u32
extern "C" __global__ void termite_gemma4_mtp_verify_commit_u32(
    unsigned int* result,
    const unsigned int* target_choices,
    const long long* draft_tokens,
    const int* eos_token_ids,
    unsigned int draft_count,
    unsigned int eos_count,
    unsigned int accept_bonus
) {
    if (threadIdx.x != 0u || blockIdx.x != 0u) return;

    unsigned int matched_drafts = 0u;
    unsigned int accepted = 0u;
    unsigned int correction_added = 0u;
    unsigned int had_bonus = 0u;
    unsigned int bonus_skipped = 0u;
    unsigned int hit_eos = 0u;
    unsigned int correction_token = 0u;
    unsigned int bonus_token = 0u;
    unsigned int invalid_model_output = 0u;

    for (unsigned int i = 0u; i < draft_count; ++i) {
        long long draft_raw = draft_tokens[i];
        if (draft_raw < 0ll) {
            invalid_model_output = 1u;
            break;
        }
        unsigned int draft = (unsigned int)draft_raw;
        unsigned int target_choice = target_choices[i];
        if (target_choice == draft) {
            matched_drafts += 1u;
            accepted += 1u;
            if (termite_gemma4_mtp_token_is_eos_u32(target_choice, eos_token_ids, eos_count)) {
                hit_eos = 1u;
                break;
            }
        } else {
            correction_added = 1u;
            correction_token = target_choice;
            accepted = matched_drafts + 1u;
            if (termite_gemma4_mtp_token_is_eos_u32(target_choice, eos_token_ids, eos_count)) {
                hit_eos = 1u;
            }
            break;
        }
    }

    unsigned int can_bonus = (invalid_model_output == 0u && matched_drafts == draft_count && hit_eos == 0u) ? 1u : 0u;
    if (can_bonus != 0u && accept_bonus != 0u) {
        had_bonus = 1u;
        bonus_token = target_choices[draft_count];
        accepted += 1u;
        if (termite_gemma4_mtp_token_is_eos_u32(bonus_token, eos_token_ids, eos_count)) {
            hit_eos = 1u;
        }
    } else if (can_bonus != 0u) {
        bonus_skipped = 1u;
    }

    unsigned int commit_forward_required = (correction_added != 0u || had_bonus != 0u) ? 1u : 0u;
    unsigned int accepted_hidden_valid = (accepted != 0u && commit_forward_required == 0u) ? 1u : 0u;
    unsigned int accepted_hidden_row = accepted_hidden_valid != 0u ? accepted - 1u : 0xffffffffu;

    result[0] = matched_drafts;
    result[1] = accepted;
    result[2] = correction_added;
    result[3] = had_bonus;
    result[4] = bonus_skipped;
    result[5] = hit_eos;
    result[6] = commit_forward_required;
    result[7] = accepted_hidden_valid;
    result[8] = accepted_hidden_row;
    result[9] = correction_token;
    result[10] = bonus_token;
    result[11] = invalid_model_output;
}
// native-program:entry termite_gemma4_a4b_topk_rows_f32
extern "C" __global__ void termite_gemma4_a4b_topk_rows_f32(
    unsigned int* route_ids,
    float* route_weights,
    const float* logits,
    const float* expert_scale,
    unsigned int rows,
    unsigned int num_experts,
    unsigned int top_k,
    float logit_scale
) {
    const unsigned int row = blockIdx.x;
    const unsigned int tid = threadIdx.x;
    if (row >= rows || num_experts == 0u || num_experts > 128u ||
        top_k == 0u || top_k > 8u || top_k > num_experts || blockDim.x != 128u) return;
    __shared__ float values[128];
    __shared__ unsigned int ids[128];
    __shared__ unsigned int selected_ids[8];
    __shared__ float selected_logits[8];
    const bool valid = tid < num_experts;
    values[tid] = valid ? __fmul_rn(logits[(size_t)row * num_experts + tid], logit_scale) : -3.402823466e+38f;
    ids[tid] = valid ? tid : 0xffffffffu;
    __syncthreads();

    for (unsigned int slot = 0u; slot < top_k; ++slot) {
        bool already_selected = false;
        for (unsigned int prior = 0u; prior < slot; ++prior) already_selected |= selected_ids[prior] == tid;
        float candidate = valid && !already_selected ?
            __fmul_rn(logits[(size_t)row * num_experts + tid], logit_scale) : -3.402823466e+38f;
        values[tid] = candidate;
        ids[tid] = valid && !already_selected ? tid : 0xffffffffu;
        __syncthreads();
        for (unsigned int stride = 64u; stride != 0u; stride >>= 1u) {
            if (tid < stride && termite_a4b_route_better(values[tid + stride], ids[tid + stride], values[tid], ids[tid])) {
                values[tid] = values[tid + stride];
                ids[tid] = ids[tid + stride];
            }
            __syncthreads();
        }
        if (tid == 0u) {
            selected_ids[slot] = ids[0];
            selected_logits[slot] = values[0];
        }
        __syncthreads();
    }

    if (tid == 0u) {
        float selected_sum = 0.0f;
        float selected_exp[8];
        const float max_selected = selected_logits[0];
        for (unsigned int slot = 0u; slot < top_k; ++slot) {
            selected_exp[slot] = termite_a4b_exp_approx_f32(__fsub_rn(selected_logits[slot], max_selected));
            selected_sum = __fadd_rn(selected_sum, selected_exp[slot]);
        }
        for (unsigned int slot = 0u; slot < top_k; ++slot) {
            const unsigned int expert = selected_ids[slot];
            float weight = selected_sum > 0.0f ? selected_exp[slot] / selected_sum : 0.0f;
            if (expert_scale != nullptr) weight = __fmul_rn(weight, expert_scale[expert]);
            route_ids[(size_t)row * top_k + slot] = expert;
            route_weights[(size_t)row * top_k + slot] = weight;
        }
    }
}
