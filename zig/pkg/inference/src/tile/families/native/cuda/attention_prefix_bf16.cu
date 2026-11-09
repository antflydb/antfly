// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

// native-program:entry termite_softmax_scores_bf16_prefix
#include "../kernels/score_softmax_bf16_program.cuh"
extern "C" __global__ void termite_softmax_scores_bf16_prefix(
    ANTFLY_CUDA_SOFTMAX_SCORES_BF16_PREFIX_PARAMS) {
    antfly_score_softmax_bf16<AntflyScoreMask::prefix_keys, AntflyProbabilityPublication::normalized_bf16>(
        probs, scores, nullptr, sequence, 0u, rows, 0u, tile_capacity, valid_keys);
}

// native-program:entry termite_attention_bf16_bshd_native_masked_m32n16
extern "C" __global__ __launch_bounds__(256) void termite_attention_bf16_bshd_native_masked_m32n16(
    ANTFLY_CUDA_ATTENTION_BF16_BSHD_NATIVE_MASKED_M32N16_PARAMS) {
    if (valid_sequence == 0u || valid_sequence > seq_len) return;
#define TERMITE_VISION_BF16_NATIVE_IO
#define TERMITE_VISION_BF16_KEY_LIMIT valid_sequence
#include "../kernels/vision_bf16_attention_m32n16.cuh"
}

// native-program:entry antfly_attention_bf16_bshd_prefix_rf_q64_k64_hd64
extern "C" __global__ __launch_bounds__(128, 4) void antfly_attention_bf16_bshd_prefix_rf_q64_k64_hd64(
    ANTFLY_CUDA_ATTENTION_BF16_BSHD_PREFIX_RF_Q64_K64_HD64_PARAMS) {
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 800
    if (!isfinite(score_scale) || score_scale <= 0.0f || heads == 0u || heads > 64u) return;
    antfly_attention_block_bf16<AntflyAttentionSchedule::EfficientBF16, 64, false, true>(
        dst, q, k, v, nullptr, batch, physical_sequence, key_sequence,
        heads, heads, 0u, nullptr, nullptr, 1u, query_sequence, score_scale);
#endif
}
