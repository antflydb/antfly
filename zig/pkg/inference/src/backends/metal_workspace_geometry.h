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

#ifndef ANTFLY_METAL_WORKSPACE_GEOMETRY_H
#define ANTFLY_METAL_WORKSPACE_GEOMETRY_H

// SDK-free logical geometry shared by actual allocation producers and cold
// admission. These sizes are NOT SDK allocation bounds or physical credit.
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

_Static_assert(sizeof(float) == 4 && sizeof(uint32_t) == 4, "workspace scalar ABI changed");

typedef struct antfly_metal_greedy_workspace_sizes {
    size_t scratch_bytes;
    size_t token_bytes;
} antfly_metal_greedy_workspace_sizes;

typedef struct antfly_metal_host_staging_sizes {
    size_t offset_bytes;
    size_t required_bytes;
    size_t next_offset_bytes;
    size_t capacity_bytes;
} antfly_metal_host_staging_sizes;

static inline bool antfly_workspace_geometry_add(size_t a, size_t b, size_t *out) {
    if (a > SIZE_MAX - b) return false;
    *out = a + b;
    return true;
}
static inline bool antfly_workspace_geometry_mul(size_t a, size_t b, size_t *out) {
    if (b != 0 && a > SIZE_MAX / b) return false;
    *out = a * b;
    return true;
}
static inline bool antfly_workspace_geometry_align256(size_t value, size_t *out) {
    if (value > SIZE_MAX - 255u) return false;
    *out = (value + 255u) & ~(size_t)255u;
    return true;
}

// On failure no output is mutated. Layers/vocabulary retain the real
// producer's validity contract; the same scratch is reused across layers.
static inline int antfly_workspace_geometry_greedy(
    size_t hidden, size_t intermediate, size_t layers,
    size_t heads, size_t kv_heads, size_t head_dim,
    size_t vocab, size_t kv_tokens, antfly_metal_greedy_workspace_sizes *out
) {
    if (out == NULL) return -1;
    if (!hidden || !intermediate || !layers || !heads || !kv_heads || !head_dim || !vocab) return -2;
    size_t h, q, k, mlp, scratch, piece;
    if (!antfly_workspace_geometry_mul(hidden, sizeof(float), &h) ||
        !antfly_workspace_geometry_mul(heads, head_dim, &q) ||
        !antfly_workspace_geometry_mul(q, sizeof(float), &q) ||
        !antfly_workspace_geometry_mul(kv_heads, head_dim, &k) ||
        !antfly_workspace_geometry_mul(k, sizeof(float), &k) ||
        !antfly_workspace_geometry_mul(intermediate, sizeof(float), &mlp) ||
        !antfly_workspace_geometry_mul(mlp, 2u, &mlp) ||
        !antfly_workspace_geometry_mul(h, 2u, &scratch) ||
        !antfly_workspace_geometry_add(scratch, q, &scratch) ||
        !antfly_workspace_geometry_mul(k, 2u, &piece) ||
        !antfly_workspace_geometry_add(scratch, piece, &scratch) ||
        !antfly_workspace_geometry_add(scratch, q, &scratch) ||
        !antfly_workspace_geometry_add(scratch, mlp, &scratch) ||
        !antfly_workspace_geometry_add(scratch, h, &scratch) ||
        !antfly_workspace_geometry_mul(kv_tokens, sizeof(uint32_t), &piece) ||
        !antfly_workspace_geometry_add(scratch, piece, &scratch) ||
        !antfly_workspace_geometry_add(scratch, 4096u, &scratch)) return -3;
    const antfly_metal_greedy_workspace_sizes value = { scratch, sizeof(uint32_t) };
    *out = value;
    return 0;
}

// SAME optional F16 K/V staging selector/size as dense causal attention.
// The zero request is the family-preparation path with no attention work.
typedef struct antfly_metal_dense_attention_request {
    size_t sequence, heads, kv_heads, head_dim;
} antfly_metal_dense_attention_request;
enum { ANTFLY_DENSE_F16KV_CAPABILITIES = 15u };
static inline int antfly_workspace_geometry_dense_f16kv(
    const antfly_metal_dense_attention_request *request, uint32_t capabilities, size_t *out
) {
    if (!request || !out) return -1;
    if (capabilities & ~ANTFLY_DENSE_F16KV_CAPABILITIES) return -2;
    if (!request->sequence) { *out = 0; return 0; }
    if (!request->heads || !request->kv_heads || !request->head_dim ||
        request->heads % request->kv_heads != 0) return -2;
    if (capabilities != ANTFLY_DENSE_F16KV_CAPABILITIES || request->sequence < 16u ||
        request->head_dim > 128u || request->head_dim % 32u != 0u) { *out = 0; return 0; }
    size_t bytes;
    if (!antfly_workspace_geometry_mul(request->sequence, request->kv_heads, &bytes) ||
        !antfly_workspace_geometry_mul(bytes, request->head_dim, &bytes) ||
        !antfly_workspace_geometry_mul(bytes, sizeof(uint16_t), &bytes)) return -3;
    *out = bytes;
    return 0;
}
// One transaction replaces BOTH members, even if only one member is absent.
// An existing other member still owns its old SDK/root; it earns no reuse
// credit for the replacement. This predicate grants no allocation authority.
static inline bool antfly_workspace_geometry_dense_f16kv_replace(
    size_t capacity, size_t required, bool key_present, bool value_present
) {
    return required && (capacity < required || !key_present || !value_present);
}

// SAME fixed scratch roles reserved by the authored dense causal prefill
// block. Logical sizes only; concrete device bounds and current roots remain
// the physical planner's responsibility. Publish nothing on overflow/error.
enum {
    ANTFLY_PREFILL_SCRATCH_COUNT = 21,
    ANTFLY_PREFILL_DIRECT_HIDDEN_BASE = 6,
    ANTFLY_PREFILL_DIRECT_Q_SLOT = 10,
    ANTFLY_PREFILL_LOGITS_SLOT = 11,
    ANTFLY_PREFILL_HOT_HIDDEN_BASE = 17,
    ANTFLY_PREFILL_HOT_INTERMEDIATE_BASE = 21,
};
typedef struct antfly_metal_prefill_scratch_sizes {
    size_t q_bytes, kv_bytes, direct_hidden_bytes, intermediate_bytes, tail_logits_bytes;
    size_t slots[ANTFLY_PREFILL_SCRATCH_COUNT];
    size_t bytes[ANTFLY_PREFILL_SCRATCH_COUNT];
} antfly_metal_prefill_scratch_sizes;
static inline int antfly_workspace_geometry_prefill_scratch(
    size_t rows, size_t heads, size_t kv_heads, size_t head_dim,
    size_t hidden, size_t intermediate, size_t tail_vocab,
    antfly_metal_prefill_scratch_sizes *out
) {
    if (!out) return -1;
    if (!rows || !heads || !kv_heads || !head_dim) return -2;
    size_t q, kv, h, ffn, tail;
    if (!antfly_workspace_geometry_mul(rows, heads, &q) ||
        !antfly_workspace_geometry_mul(q, head_dim, &q) ||
        !antfly_workspace_geometry_mul(q, sizeof(float), &q) ||
        !antfly_workspace_geometry_mul(rows, kv_heads, &kv) ||
        !antfly_workspace_geometry_mul(kv, head_dim, &kv) ||
        !antfly_workspace_geometry_mul(kv, sizeof(float), &kv) ||
        !antfly_workspace_geometry_mul(rows, hidden, &h) ||
        !antfly_workspace_geometry_mul(h, sizeof(float), &h) ||
        !antfly_workspace_geometry_mul(rows, intermediate, &ffn) ||
        !antfly_workspace_geometry_mul(ffn, sizeof(float), &ffn) ||
        !antfly_workspace_geometry_mul(tail_vocab, sizeof(float), &tail)) return -3;
    const size_t direct = q > h ? q : h;
    const antfly_metal_prefill_scratch_sizes value = {
        .q_bytes = q, .kv_bytes = kv, .direct_hidden_bytes = direct,
        .intermediate_bytes = ffn, .tail_logits_bytes = tail,
        .slots = { 0, 1, 2, 3, 4, 5,
            ANTFLY_PREFILL_DIRECT_HIDDEN_BASE, ANTFLY_PREFILL_DIRECT_HIDDEN_BASE + 1,
            ANTFLY_PREFILL_DIRECT_HIDDEN_BASE + 2, ANTFLY_PREFILL_DIRECT_HIDDEN_BASE + 3,
            ANTFLY_PREFILL_DIRECT_Q_SLOT, ANTFLY_PREFILL_LOGITS_SLOT,
            ANTFLY_PREFILL_HOT_HIDDEN_BASE, ANTFLY_PREFILL_HOT_HIDDEN_BASE + 1,
            ANTFLY_PREFILL_HOT_HIDDEN_BASE + 2, ANTFLY_PREFILL_HOT_HIDDEN_BASE + 3,
            ANTFLY_PREFILL_HOT_INTERMEDIATE_BASE, ANTFLY_PREFILL_HOT_INTERMEDIATE_BASE + 1,
            ANTFLY_PREFILL_HOT_INTERMEDIATE_BASE + 2, ANTFLY_PREFILL_HOT_INTERMEDIATE_BASE + 3,
            ANTFLY_PREFILL_HOT_INTERMEDIATE_BASE + 4 },
        .bytes = { q, kv, kv, q, kv, kv, direct, direct, direct, direct, q, tail,
            direct, direct, direct, direct, ffn, ffn, ffn, ffn, ffn },
    };
    *out = value;
    return 0;
}

// Standalone staging reuses offset zero and never advances a frame cursor.
// Active staging uses the selected one of the runtime's TWO frame banks;
// callers plan each real bank separately and must not add independent peaks.
// Grow from the ACTUAL current capacity, including non-power-of-two lineage.
static inline int antfly_workspace_geometry_host_staging(
    size_t current_capacity, size_t current_offset, size_t length,
    uint32_t buffer_present, uint32_t standalone,
    antfly_metal_host_staging_sizes *out
) {
    if (out == NULL) return -1;
    if (!length || buffer_present > 1u || standalone > 1u) return -2;
    antfly_metal_host_staging_sizes value;
    value.offset_bytes = 0;
    value.next_offset_bytes = current_offset;
    if (!standalone && !antfly_workspace_geometry_align256(current_offset, &value.offset_bytes)) return -3;
    if (!antfly_workspace_geometry_add(value.offset_bytes, length, &value.required_bytes)) return -3;
    if (!standalone && !antfly_workspace_geometry_align256(value.required_bytes, &value.next_offset_bytes)) return -3;
    value.capacity_bytes = current_capacity;
    if (!buffer_present || current_capacity < value.required_bytes) {
        if (value.capacity_bytes < 4096u) value.capacity_bytes = 4096u;
        while (value.capacity_bytes < value.required_bytes) {
            if (value.capacity_bytes > SIZE_MAX / 2u) {
                value.capacity_bytes = value.required_bytes;
                break;
            }
            value.capacity_bytes *= 2u;
        }
    }
    *out = value;
    return 0;
}

// Value-only cold bridge; implementations call the SAME inline functions as
// the C producers. No runtime/device/provider pointer or SDK dependency.
int antfly_metal_greedy_workspace_requirements(
    size_t hidden, size_t intermediate, size_t layers,
    size_t heads, size_t kv_heads, size_t head_dim,
    size_t vocab, size_t kv_tokens, antfly_metal_greedy_workspace_sizes *out);
int antfly_metal_host_staging_requirements(
    size_t current_capacity, size_t current_offset, size_t length,
    uint32_t buffer_present, uint32_t standalone,
    antfly_metal_host_staging_sizes *out);
// Actual row-producing SDK wrappers and cold declarations share these spans.
// index_bytes describes ONE explicit u32/F32 row-index array, not a source
// allocation. It is zero only when a caller chooses not to declare that role.
typedef struct antfly_metal_row_workspace_sizes {
    size_t output_bytes;
    size_t index_bytes;
} antfly_metal_row_workspace_sizes;
static inline int antfly_workspace_geometry_rows(size_t rows, size_t width,
    antfly_metal_row_workspace_sizes *out) {
    if (!out) return -1;
    if (!rows || !width) return -2;
    antfly_metal_row_workspace_sizes value;
    size_t count;
    if (!antfly_workspace_geometry_mul(rows, width, &count) ||
        !antfly_workspace_geometry_mul(count, sizeof(float), &value.output_bytes) ||
        !antfly_workspace_geometry_mul(rows, sizeof(uint32_t), &value.index_bytes)) return -3;
    *out = value;
    return 0;
}

typedef struct antfly_metal_pool_workspace_sizes {
    size_t input_bytes;
    size_t output_bytes;
    size_t metadata_bytes;
} antfly_metal_pool_workspace_sizes;
static inline int antfly_workspace_geometry_pool(size_t batch, size_t rows,
    size_t width, size_t dimensions, uint32_t segmented,
    antfly_metal_pool_workspace_sizes *out) {
    if (!out) return -1;
    if (!batch || batch > 64u || !rows || !width || !dimensions ||
        dimensions > width || segmented > 1u) return -2;
    antfly_metal_pool_workspace_sizes value;
    antfly_metal_row_workspace_sizes input, output;
    size_t input_rows, metadata_count;
    if (!antfly_workspace_geometry_mul(segmented ? 1u : batch, rows, &input_rows) ||
        antfly_workspace_geometry_rows(input_rows, width, &input) != 0 ||
        antfly_workspace_geometry_rows(batch, dimensions, &output) != 0) return -3;
    if (segmented) {
        if (!antfly_workspace_geometry_mul(batch, 2u, &metadata_count)) return -3;
    } else if (!antfly_workspace_geometry_mul(batch, rows, &metadata_count) ||
               !antfly_workspace_geometry_add(metadata_count, batch, &metadata_count)) return -3;
    if (!antfly_workspace_geometry_mul(metadata_count, sizeof(int64_t), &value.metadata_bytes)) return -3;
    value.input_bytes = input.output_bytes;
    value.output_bytes = output.output_bytes;
    *out = value;
    return 0;
}
int antfly_metal_row_workspace_requirements(size_t rows, size_t width,
    antfly_metal_row_workspace_sizes *out);
int antfly_metal_pool_workspace_requirements(size_t batch, size_t rows,
    size_t width, size_t dimensions, uint32_t segmented,
    antfly_metal_pool_workspace_sizes *out);

// Canonical sequence_encoder.poolNormalize is the active-frame u32-mask
// program, not the separate i64 maskedMeanBatch callback.
typedef struct antfly_metal_frame_pool_workspace_sizes {
    size_t input_bytes;
    size_t output_bytes;
    size_t mask_bytes;
} antfly_metal_frame_pool_workspace_sizes;
static inline int antfly_workspace_geometry_frame_pool(size_t batch, size_t sequence,
    size_t hidden, antfly_metal_frame_pool_workspace_sizes *out) {
    if (!out) return -1;
    if (!batch || !sequence || !hidden) return -2;
    size_t tokens;
    antfly_metal_row_workspace_sizes input, output;
    if (!antfly_workspace_geometry_mul(batch, sequence, &tokens) ||
        antfly_workspace_geometry_rows(tokens, hidden, &input) != 0 ||
        antfly_workspace_geometry_rows(batch, hidden, &output) != 0) return -3;
    const antfly_metal_frame_pool_workspace_sizes value = { input.output_bytes, output.output_bytes, input.index_bytes };
    *out = value;
    return 0;
}
int antfly_metal_frame_pool_workspace_requirements(size_t batch, size_t sequence,
    size_t hidden, antfly_metal_frame_pool_workspace_sizes *out);
#endif
