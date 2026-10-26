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

// Standalone dispatcher, full-output and independent CPU checks. Build like
// check_metal_projections.m. --benchmark emits balanced old/new JSON samples.
#include "../../src/backends/metal_kernels.m"
_Static_assert(sizeof(termite_metal_paged_attention_params) == 84, "Prefill Metal parameter ABI");
_Static_assert(offsetof(termite_metal_paged_attention_params, softcap) == 72, "Prefill softcap ABI");

static int check(termite_metal_decode_runtime *r, size_t n, size_t prefix, size_t heads, size_t page,
                 bool padded, int poison, bool benchmark) {
    const size_t kh = heads / 4, width = heads * 256, kv = n + prefix;
    const size_t blocks = (kv + page - 1) / page, physical = blocks * page;
    const size_t kstride = kh * 256 + (padded ? 16 : 0), vstride = kh * 256 + (padded ? 32 : 0);
    const size_t count = n * width, slot = 7;
    id<MTLBuffer> q = [r->device newBufferWithLength:(count + 8) * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> k = [r->device newBufferWithLength:physical * kstride * 2
                                             options:MTLResourceStorageModeShared];
    id<MTLBuffer> v = [r->device newBufferWithLength:physical * vstride * 2
                                             options:MTLResourceStorageModeShared];
    id<MTLBuffer> y = [r->device newBufferWithLength:(count + 8) * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> producer = [r->device newBufferWithLength:count * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> consumer = [r->device newBufferWithLength:count * 4 options:MTLResourceStorageModeShared];
    uint32_t *table = malloc(blocks * 4);
    float *gold = malloc(count * 4);
    if (!q || !k || !v || !y || !producer || !consumer || !table || !gold)
        return 1;
    uint32_t rng = 123;
    for (size_t i = 0; i < count; i++) {
        rng = rng * 1664525u + 1013904223u;
        ((float *)producer.contents)[i] = ((int)(rng >> 16) - 32768) * .00002f * (padded ? 8 : 1);
    }
    for (size_t i = 0; i < physical * kstride; i++) {
        rng = rng * 1664525u + 1013904223u;
        ((__fp16 *)k.contents)[i] = ((int)(rng >> 16) - 32768) * .00002f;
    }
    for (size_t i = 0; i < physical * vstride; i++) {
        rng = rng * 1664525u + 1013904223u;
        ((__fp16 *)v.contents)[i] = ((int)(rng >> 16) - 32768) * .00002f;
    }
    for (size_t i = 0; i < blocks; i++)
        table[i] = (uint32_t)((blocks - 1 - i) * page);
    if (poison == 1)
        for (size_t i = 0; i < width; i++)
            ((float *)producer.contents)[i] = NAN;
    if (poison == 2)
        for (size_t i = 0; i < kh * 256; i++)
            ((__fp16 *)k.contents)[table[0] * kstride + i] = NAN;
    if (poison == 3)
        ((__fp16 *)v.contents)[(table[(kv - 1) / page] + (kv - 1) % page) * vstride] = NAN;
    if (poison == 4)
        ((__fp16 *)v.contents)[(table[1 / page] + 1 % page) * vstride] = INFINITY;
    memcpy((char *)q.contents + 16, producer.contents, count * 4);
    for (size_t i = 0; i < 4; i++)
        ((uint32_t *)q.contents)[i] = ((uint32_t *)q.contents)[count + 4 + i] = 0xdeadbeef;
    r->attention_span_encoded_key_buffers[slot] = k;
    r->attention_span_v_buffers[slot] = v;
    r->attention_span_slot_key_row_bytes[slot] = kstride * 2;
    r->attention_span_slot_v_row_stride[slot] = vstride;
    const size_t trials = benchmark ? 30 : 1, modes = benchmark ? 2 : 9;
    float worst = 0;
    for (size_t trial = 0; trial < trials; trial++)
        for (size_t order = 0; order < modes; order++) {
            const size_t mode = benchmark ? (order + trial % 2) % 2 : order;
            if (mode == 0 || mode == 3)
                setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_ATTENTION_MPP", "1", 1);
            else
                unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_ATTENTION_MPP");
            id<MTLComputePipelineState> saved = r->lightonocr3_prefill_attention_mpp_pipeline;
            id<MTLComputePipelineState> saved32 = r->lightonocr3_prefill_attention_mpp32_pipeline;
            id<MTLComputePipelineState> saved_direct = r->lightonocr3_prefill_attention_direct_pipeline;
            if (mode == 2) {
                r->lightonocr3_prefill_attention_mpp_pipeline = nil;
                r->lightonocr3_prefill_attention_mpp32_pipeline = nil;
                r->lightonocr3_prefill_attention_direct_pipeline = nil;
            }
            if (mode == 7 || mode == 8)
                r->lightonocr3_prefill_attention_direct_pipeline = nil;
            if (mode == 7)
                r->lightonocr3_prefill_attention_mpp32_pipeline = nil;
            termite_metal_decode_runtime_set_qwen35_ocr_vector_loads(r, mode != 4);
            const uint64_t before = r->lightonocr3_prefill_attention_mpp_dispatches;
            uint64_t short_before[2], short_after[2];
            termite_metal_decode_runtime_lighton_short_prefill_stats(r, short_before);
            const uint64_t before_direct = r->lightonocr3_prefill_attention_direct_dispatches;
            for (size_t i = 0; i < count + 8; i++)
                ((uint32_t *)y.contents)[i] = 0xdeadbeef;
            if (termite_metal_decode_runtime_begin_frame(r))
                return 2;
            if (mode >= 5) {
                if (termite_metal_decode_runtime_begin_planned_compute_scope(
                        r, TERMITE_METAL_COMPUTE_SOURCE_ATTENTION, TERMITE_METAL_COMPUTE_REGION_OTHER))
                    return 3;
                for (size_t i = 0; i < count; i++)
                    ((float *)q.contents)[i + 4] = -.9f;
                if (termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)producer, 0, n, width, 0,
                                                           width, (__bridge void *)q, 16))
                    return 4;
            }
            const uint64_t start = clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
            int rc;
            if (mode == 6) {
                rc = termite_metal_encode_paged_attention_slot_on_encoder(
                    r, r->active_planned_compute_encoder, slot, 3, q, 16, table, blocks, page, n, kv, heads,
                    kh, 256, kstride * 2, kstride * 2, prefix + 17, 17, 0, 0, NULL, 0, y, 16, -1);
            } else {
                rc = termite_metal_decode_runtime_attention_paged_slot_device(
                    r, slot, 3, (__bridge void *)q, 16, table, blocks, page, n, kv, heads, kh, 256,
                    kstride * 2, kstride * 2, prefix + 17, 17, 0, 0, NULL, 0, (__bridge void *)y, 16);
            }
            if (rc) {
                fprintf(stderr, "encode failure %d\n", rc);
                return 5;
            }
            if (mode >= 5 && termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)y, 16, n, width, 0,
                                                                    width, (__bridge void *)consumer, 0))
                return 6;
            if (termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r))
                return 7;
            const uint64_t elapsed = clock_gettime_nsec_np(CLOCK_UPTIME_RAW) - start;
            r->lightonocr3_prefill_attention_mpp_pipeline = saved;
            r->lightonocr3_prefill_attention_mpp32_pipeline = saved32;
            r->lightonocr3_prefill_attention_direct_pipeline = saved_direct;
            const bool short_shape = n < 128 && kv >= 256 && page % 16 == 0 &&
                ((heads == 16 && n >= 32) || (heads == 8 && n >= 64));
            const bool short_direct = short_shape && (mode == 1 || mode == 5 || mode == 6);
            const bool eligible = (n >= 128 || short_direct) && n <= 2048 && kv <= 4096;
            if (r->lightonocr3_prefill_attention_mpp_dispatches - before !=
                ((mode == 1 || mode >= 5) && eligible ? 1 : 0))
                return 8;
            const bool expected_direct = eligible && (mode == 1 || mode == 5 || mode == 6) && page % 16 == 0;
            if (r->lightonocr3_prefill_attention_direct_dispatches - before_direct !=
                (expected_direct ? 1 : 0))
                return 15;
            termite_metal_decode_runtime_lighton_short_prefill_stats(r, short_after);
            if (short_after[0] != short_before[0] ||
                short_after[1] - short_before[1] != (expected_direct && n < 128 ? 1 : 0)) return 16;
            if (benchmark)
                printf("{\"heads\":%zu,\"queries\":%zu,\"keys\":%zu,\"trial\":%zu,\"mode\":%zu,\"wall_us\":%."
                       "4f,\"gpu_us\":%.4f}\n",
                       heads, n, kv, trial, mode, elapsed / 1000.0, r->last_frame_gpu_nanos / 1000.0);
            const float *got = (float *)y.contents + 4;
            if (mode == 0)
                memcpy(gold, got, count * 4);
            // The first balanced trial can run new before old only on later trials,
            // when the deterministic reference is already available.
            if (mode != 0)
                for (size_t i = 0; i < count; i++) {
                    if (isnan(got[i]) != isnan(gold[i]) || isinf(got[i]) != isinf(gold[i]) ||
                        (isinf(got[i]) && signbit(got[i]) != signbit(gold[i]))) {
                        fprintf(stderr, "nonfinite mismatch mode=%zu i=%zu got=%g expected=%g\n", mode, i,
                                got[i], gold[i]);
                        return 9;
                    }
                    if (isfinite(got[i]) && isfinite(gold[i]))
                        worst = fmaxf(worst, fabsf(got[i] - gold[i]));
                }
            if (mode >= 5 && memcmp(consumer.contents, got, count * 4))
                return 10;
            for (size_t i = 0; i < 4; i++)
                if (((uint32_t *)y.contents)[i] != 0xdeadbeef ||
                    ((uint32_t *)y.contents)[count + 4 + i] != 0xdeadbeef ||
                    ((uint32_t *)q.contents)[i] != 0xdeadbeef ||
                    ((uint32_t *)q.contents)[count + 4 + i] != 0xdeadbeef)
                    return 11;
        }
    // Independent double-precision samples cover first/tail queries and heads.
    double cpu_error = 0;
    if (!poison)
        for (size_t row_case = 0; row_case < 2; row_case++)
            for (size_t hcase = 0; hcase < 2; hcase++)
                for (size_t dcase = 0; dcase < 2; dcase++) {
                    size_t row = row_case ? n - 1 : 0, h = hcase ? heads - 1 : 0, d = dcase ? 255 : 0;
                    size_t key_head = h / 4;
                    double best = -INFINITY, sum = 0, value = 0;
                    for (size_t t = 0; t <= prefix + row; t++) {
                        size_t phys = table[t / page] + t % page;
                        double score = 0;
                        for (size_t i = 0; i < 256; i++)
                            score += (double)((float *)producer.contents)[row * width + h * 256 + i] *
                                     ((__fp16 *)k.contents)[phys * kstride + key_head * 256 + i];
                        score *= .0625;
                        double next = fmax(best, score), c = exp(best - next), weight = exp(score - next);
                        sum = sum * c + weight;
                        value =
                            value * c + weight * ((__fp16 *)v.contents)[phys * vstride + key_head * 256 + d];
                        best = next;
                    }
                    cpu_error = fmax(cpu_error, fabs(gold[row * width + h * 256 + d] - value / sum));
                }
    fprintf(
        stderr,
        "prefill_check queries=%zu keys=%zu heads=%zu page=%zu padded=%d poison=%d max_abs=%g cpu_error=%g\n",
        n, kv, heads, page, padded, poison, worst, cpu_error);
    if (!benchmark) {
        memset((char *)y.contents + 16, 0x5a, count * 4);
        if (termite_metal_decode_runtime_begin_frame(r) ||
            termite_metal_decode_runtime_begin_planned_compute_scope(
                r, TERMITE_METAL_COMPUTE_SOURCE_ATTENTION, TERMITE_METAL_COMPUTE_REGION_OTHER) ||
            termite_metal_decode_runtime_attention_paged_slot_device(
                r, slot, 3, (__bridge void *)q, 16, table, blocks, page, n, kv, heads, kh, 256, kstride * 2,
                kstride * 2, prefix + 17, 17, 0, 0, NULL, 0, (__bridge void *)y, 16) ||
            termite_metal_decode_runtime_cancel_frame(r) || r->active_planned_compute_encoder != nil ||
            r->active_frame_retained_resources != NULL)
            return 13;
        for (size_t i = 0; i < count; i++)
            if (((uint32_t *)y.contents)[i + 4] != 0x5a5a5a5a)
                return 14;
    }
    r->attention_span_encoded_key_buffers[slot] = nil;
    r->attention_span_v_buffers[slot] = nil;
    free(table);
    free(gold);
    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_ATTENTION_MPP");
    termite_metal_decode_runtime_set_qwen35_ocr_vector_loads(r, 1);
    return worst > 2e-4 || cpu_error > 2e-4 ? 12 : 0;
}

int main(int argc, const char **argv) {
    @autoreleasepool {
        termite_metal_decode_runtime *r = termite_metal_decode_runtime_create();
        if (!r)
            return 1;
        termite_metal_decode_runtime_set_lightonocr3_projections(r, 1);
        termite_metal_decode_runtime_set_qwen35_ocr_vector_loads(r, 1);
        if (!r->lightonocr3_projections_enabled)
            return 77;
        if (!r->lightonocr3_prefill_attention_mpp_pipeline ||
            !r->lightonocr3_prefill_attention_mpp32_pipeline ||
            !r->lightonocr3_prefill_attention_direct_pipeline)
            return 2;
        setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_PREFILL_ATTENTION_MPP", "1", 1);
        const bool benchmark = argc == 2 && strcmp(argv[1], "--benchmark") == 0;
        const size_t lengths[] = {8, 17, 65, 127, 128, 129, 432, 662, 1024, 2048};
        for (size_t h = 8; h <= 16; h += 8)
            for (size_t i = 0; i < sizeof(lengths) / sizeof(lengths[0]); i++) {
                int rc = check(r, lengths[i], 0, h, 128, false, 0, benchmark);
                if (rc) {
                    fprintf(stderr, "FAIL heads=%zu seq=%zu rc=%d\n", h, lengths[i], rc);
                    return 10 + rc;
                }
            }
        if (!benchmark)
            for (size_t h = 8; h <= 16; h += 8) {
                if (check(r, 129, 63, h, 16, true, 0, false) || check(r, 432, 129, h, 128, true, 0, false))
                    return 30;
                for (int poison = 1; poison <= 4; poison++)
                    if (check(r, 129, 0, h, 128, false, poison, false))
                        return 31;
                if (check(r, 129, 63, h, 8, false, 0, false))
                    return 32;
                for (int poison = 1; poison <= 4; poison++)
                    if (check(r, 432, 0, h, 16, true, poison, false))
                        return 33;
            }
        if (!benchmark) {
            const size_t short_queries[] = {1,2,17,31,32,33,42,48,63,64,65,95,127};
            for (size_t h=8;h<=16;h+=8) for (size_t i=0;i<sizeof(short_queries)/sizeof(short_queries[0]);i++)
                for (size_t prefix=256;prefix<=1024;prefix*=4) for (size_t page=16;page<=128;page*=8)
                    if (check(r,short_queries[i],prefix,h,page,i%2,0,false)) return 34;
            for (size_t h=8;h<=16;h+=8) {
                if (check(r,32,256,h,8,true,0,false) || check(r,64,256,h,8,true,0,false)) return 35;
                for (int poison=1;poison<=4;poison++)
                    if (check(r,64,256,h,16,true,poison,false)) return 36;
            }
        }
        termite_metal_decode_runtime_destroy(r);
    }
    return 0;
}
