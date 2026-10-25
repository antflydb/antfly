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

// Standalone numerical checks for the production Metal projection dispatcher.
// clang -O2 -fobjc-arc -framework Foundation -framework Metal \
//   -framework MetalPerformanceShaders -framework Accelerate \
//   check_metal_projections.m -o /tmp/check-lighton-projections
#include "../../src/backends/metal_kernels.m"

static uint16_t round_bf16(float value) {
    uint32_t bits;
    memcpy(&bits, &value, 4);
    return (bits & 0x7fffffffu) > 0x7f800000u ? (uint16_t)((bits >> 16) | 0x40u)
                                              : (uint16_t)((bits + 0x7fffu + ((bits >> 16) & 1u)) >> 16);
}
static float expand_bf16(uint16_t value) {
    uint32_t bits = (uint32_t)value << 16;
    float f;
    memcpy(&f, &bits, 4);
    return f;
}
static int apply_projection(termite_metal_decode_runtime *r, size_t rows, size_t in, size_t out,
                            id<MTLBuffer> input, size_t offset_floats, id<MTLBuffer> result) {
    uint32_t *storage = result.contents;
    const size_t end = 4 + rows * out;
    for (size_t i = 0; i < 4; i++)
        storage[i] = 0xdeadbeefu;
    for (size_t i = 0; i < 8; i++)
        storage[end + i] = 0xcafebabeu;
    const bool owns_frame = !r->active_frame_cb;
    if (owns_frame && (termite_metal_decode_runtime_begin_frame(r) ||
        termite_metal_decode_runtime_set_active_frame_regime(r, TERMITE_METAL_FRAME_REGIME_DECODE))) return -200;
    const int rc = rows == 1 ? termite_metal_decode_runtime_apply_linear_device(r, 0, (__bridge void *)input,
                                                                                offset_floats * 4, in, out,
                                                                                (__bridge void *)result, 16)
                             : termite_metal_decode_runtime_apply_linear_multi_row_device(
                                   r, 0, (__bridge void *)input, offset_floats * 4, rows, in, out,
                                   (__bridge void *)result, 16);
    if (rc) {
        if (owns_frame) termite_metal_decode_runtime_cancel_frame(r);
        return rc;
    }
    if (owns_frame && (termite_metal_decode_runtime_submit_frame(r) ||
        termite_metal_decode_runtime_wait_frame(r))) return -201;
    for (size_t i = 0; i < 4; i++)
        if (storage[i] != 0xdeadbeefu)
            return -101;
    for (size_t i = 0; i < 8; i++)
        if (storage[end + i] != 0xcafebabeu)
            return -102;
    return 0;
}
static int projection(termite_metal_decode_runtime *r, size_t rows, size_t in, size_t out,
                      size_t offset_floats) {
    id<MTLBuffer> input = [r->device newBufferWithLength:(rows * in + offset_floats) * 4
                                                 options:MTLResourceStorageModeShared];
    id<MTLBuffer> result = [r->device newBufferWithLength:(rows * out + 12) * 4
                                                  options:MTLResourceStorageModeShared];
    float *x = (float *)input.contents + offset_floats, *got = (float *)result.contents + 4;
    uint16_t *weights = malloc(in * out * 2);
    float *bias = malloc(out * 4), *gold = malloc(rows * out * 4);
    if (!input || !result || !weights || !bias || !gold)
        return 1;
    uint32_t rng = 123;
    for (size_t i = 0; i < in * out; i++) {
        rng = rng * 1664525u + 1013904223u;
        weights[i] = round_bf16(((float)((int)(rng >> 16) - 32768) / 32768) * .05f);
    }
    for (size_t i = 0; i < rows * in; i++)
        x[i] = sinf((float)i * .013f) * .7f;
    for (size_t i = 0; i < out; i++)
        bias[i] = cosf((float)i * .019f) * .2f;
    int rc = termite_metal_decode_runtime_prepare_linear_bf16(r, 0, (uint8_t *)weights, in * out * 2, bias, in, out);
    if (rc)
        return 2;
    termite_metal_decode_runtime_set_lightonocr3_projections(r, 0);
    termite_metal_decode_runtime_set_qwen35_ocr_vector_loads(r, 0);
    rc = apply_projection(r, rows, in, out, input, offset_floats, result);
    if (rc)
        return 3;
    memcpy(gold, got, rows * out * 4);
    // A few double-precision dot products independently check the reference.
    for (size_t t = 0; t < rows; t += rows > 1 ? rows - 1 : 1)
        for (size_t c = 0; c < out; c += out > 1 ? out - 1 : 1) {
            double expected = bias[c];
            for (size_t k = 0; k < in; k++)
                expected += (double)x[t * in + k] * expand_bf16(weights[c * in + k]);
            if (fabs(expected - gold[t * out + c]) > 2e-4)
                return 4;
        }
    termite_metal_decode_runtime_set_lightonocr3_projections(r, 1);
    termite_metal_decode_runtime_set_qwen35_ocr_vector_loads(r, 1);
    for (int bf16 = 0; bf16 < 3; bf16++) {
        if (bf16 && rows < 17)
            continue;
        if (bf16) {
            unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS");
            setenv(bf16 == 1 ? "TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS" : "TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS", "1", 1);
            for (size_t i = 0; i < rows * in; i++)
                x[i] = expand_bf16(round_bf16(x[i]));
            // The BF16-input reference uses precisely the rounded input values.
            setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PROJECTIONS", "1", 1);
            rc = apply_projection(r, rows, in, out, input, offset_floats, result);
            if (rc)
                return 5;
            memcpy(gold, got, rows * out * 4);
            unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PROJECTIONS");
        }
        rc = apply_projection(r, rows, in, out, input, offset_floats, result);
        if (rc)
            return 6;
        float error = 0;
        for (size_t i = 0; i < rows * out; i++) {
            if (!isfinite(got[i]))
                return 7;
            error = fmaxf(error, fabsf(got[i] - gold[i]));
        }
        fprintf(stderr, "projection rows=%zu in=%zu out=%zu input_offset=%zu bf16_input=%d max_abs=%g\n", rows, in, out,
                offset_floats * 4, bf16, error);
        if (error > 2e-4f)
            return 8;
        const bool recurrent_gate = rows>=1 && rows<=8 &&
            ((in==1024&&out==16)||(in==2560&&out==32)) &&
            r->lightonocr3_recurrent_gate_vectors[in==2560][rows-1]!=nil &&
            !termite_metal_env_flag_enabled(getenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_LOCAL_KERNELS"));
        const bool narrow_prefill =
            rows >= 32 && rows <= 2048 &&
            ((in == 1024 && out == 16 && r->lightonocr3_prefill_gate8_16 != nil) ||
             (in == 2560 && out == 32 && r->lightonocr3_prefill_gate16_32 != nil)) &&
            !termite_metal_env_flag_enabled(getenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_PROJECTIONS"));
        const bool post_prefill =
            rows >= 32 && rows <= 2048 && in == 2048 && out == 1024 &&
            r->lightonocr3_mpp_small_tile_pipeline != nil &&
            !termite_metal_env_flag_enabled(getenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_PROJECTIONS"));
        if (!bf16 && ((rows >= 2 && rows <= 8) || recurrent_gate || narrow_prefill || post_prefill) && offset_floats % 4 == 0) {
            id<MTLBuffer> source = [r->device newBufferWithBytes:input.contents
                                                          length:input.length
                                                         options:MTLResourceStorageModeShared];
            id<MTLBuffer> zero = [r->device newBufferWithLength:input.length
                                                        options:MTLResourceStorageModeShared];
            if (!source || !zero)
                return 30;
            memset(zero.contents, 0, zero.length);
            memset(input.contents, 0, input.length);
            if (termite_metal_decode_runtime_begin_frame(r) ||
                termite_metal_decode_runtime_begin_planned_compute_scope(
                    r, TERMITE_METAL_COMPUTE_SOURCE_DENSE_LINEAR, TERMITE_METAL_COMPUTE_REGION_FFN) ||
                termite_metal_decode_runtime_apply_add_device(
                    r, (__bridge void *)source, offset_floats * 4, (__bridge void *)zero, offset_floats * 4,
                    rows * in, (__bridge void *)input, offset_floats * 4))
                return 31;
            int local = narrow_prefill ? termite_metal_try_lightonocr3_prefill_gate(
                                             r, 0, input, offset_floats * 4, rows, in, out, result, 16)
                        : post_prefill ? termite_metal_try_lightonocr3_mpp(r, 0, input, offset_floats * 4,
                                                                           rows, in, out, result, 16)
                                       : termite_metal_try_lightonocr3_local_projection(
                                             r, 0, input, offset_floats * 4, rows, in, out, result, 16);
            if ((recurrent_gate || narrow_prefill || post_prefill) && local != 1)
                return 36;
            if (local < 0 || termite_metal_decode_runtime_end_planned_compute_scope(r) ||
                termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r))
                return 32;
            if (local == 1) {
                for (size_t i = 0; i < rows * out; i++)
                    if (!isfinite(got[i]) || fabsf(got[i] - gold[i]) > 2e-4f)
                        return 33;
                uint32_t *raw = result.contents;
                for (size_t i = 0; i < 4; i++)
                    if (raw[i] != 0xdeadbeefu)
                        return 34;
                for (size_t i = 4 + rows * out; i < 12 + rows * out; i++)
                    if (raw[i] != 0xcafebabeu)
                        return 35;
                fprintf(stderr,
                        "local_projection_planned_producer rows=%zu in=%zu out=%zu full_output=pass "
                        "canaries=pass\n",
                        rows, in, out);
            }
        }
        // The master rollback must retain the existing numerical result.
        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PROJECTIONS", "1", 1);
        rc = apply_projection(r, rows, in, out, input, offset_floats, result);
        if (rc)
            return 9;
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PROJECTIONS");
        for (size_t i = 0; i < rows * out; i++)
            if (got[i] != gold[i])
                return 10;
    }
    unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS");
    unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS");
    termite_metal_decode_runtime_clear_linear_slot(r, 0);
    free(weights);
    free(bias);
    free(gold);
    return 0;
}
static int gate_up(termite_metal_decode_runtime *r, size_t in, size_t out) {
    float *x = malloc(in * 4), *bias = calloc(out, 4), *gold = malloc(out * 4);
    uint16_t *w = malloc(in * out * 2);
    for (size_t i = 0; i < in; i++)
        x[i] = sinf((float)i * .017f) * .7f;
    uint32_t rng = 27;
    for (size_t i = 0; i < in * out; i++) {
        rng = rng * 1664525u + 1013904223u;
        w[i] = round_bf16(((float)((int)(rng >> 16) - 32768) / 32768) * .05f);
    }
    id<MTLBuffer> input = [r->device newBufferWithBytes:x length:in * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> gate = [r->device newBufferWithLength:out * 4 options:MTLResourceStorageModeShared],
                  up = [r->device newBufferWithLength:out * 4 options:MTLResourceStorageModeShared],
                  output = [r->device newBufferWithLength:out * 4 options:MTLResourceStorageModeShared];
    if (termite_metal_decode_runtime_prepare_linear_bf16(r, 0, (uint8_t *)w, in * out * 2, bias, in, out))
        return 1;
    for (size_t i = 0; i < out; i++)
        bias[i] = .03f;
    if (termite_metal_decode_runtime_prepare_linear_bf16(r, 1, (uint8_t *)w, in * out * 2, bias, in, out))
        return 2;
    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PROJECTIONS", "1", 1);
    if (termite_metal_decode_runtime_apply_linear_device(r, 0, (__bridge void *)input, 0, in, out,
                                                         (__bridge void *)gate, 0) ||
        termite_metal_decode_runtime_apply_linear_device(r, 1, (__bridge void *)input, 0, in, out, (__bridge void *)up,
                                                         0))
        return 3;
    float *g = gate.contents, *u = up.contents;
    for (size_t i = 0; i < out; i++)
        gold[i] = (g[i] / (1 + expf(-g[i]))) * u[i];
    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PROJECTIONS");
    if (!termite_metal_decode_runtime_should_apply_bf16_gate_up(r, 0, 1, 0, 1, in, out))
        return 4;
    if (termite_metal_decode_runtime_should_apply_bf16_gate_up(r, 0, 1, 4, 1, in, out))
        return 5;
    if (termite_metal_decode_runtime_apply_bf16_gate_up_device(r, 0, 1, (__bridge void *)input, 0, 1, in, out,
                                                               (__bridge void *)output, 0))
        return 6;
    float error = 0, *got = output.contents;
    for (size_t i = 0; i < out; i++)
        error = fmaxf(error, fabsf(got[i] - gold[i]));
    fprintf(stderr, "fused gate/up max_abs=%g\n", error);
    if (error > 2e-4f)
        return 7;
    termite_metal_decode_runtime_clear_linear_slot(r, 0);
    termite_metal_decode_runtime_clear_linear_slot(r, 1);
    free(x);
    free(bias);
    free(gold);
    free(w);
    return 0;
}
static int pack_edges(termite_metal_decode_runtime *r) {
    uint32_t bits[] = {0,           0x80000000u, 0x7f800000u, 0xff800000u, 0x7f800001u, 0xffc12345u,
                       0x3f808000u, 0x3f818000u, 1,           0x00008000u, 0x00018000u};
    uint32_t n = sizeof(bits) / 4;
    id<MTLBuffer> input = [r->device newBufferWithBytes:bits length:sizeof(bits) options:MTLResourceStorageModeShared],
                  output = [r->device newBufferWithLength:n * 2 options:MTLResourceStorageModeShared];
    id<MTLCommandBuffer> cb = [r->queue commandBuffer];
    id<MTLComputeCommandEncoder> enc = [cb computeCommandEncoder];
    [enc setComputePipelineState:r->lightonocr3_pack_bf16_pipeline];
    [enc setBuffer:input offset:0 atIndex:0];
    [enc setBuffer:output offset:0 atIndex:1];
    [enc setBytes:&n length:4 atIndex:2];
    [enc dispatchThreads:MTLSizeMake(n, 1, 1) threadsPerThreadgroup:MTLSizeMake(n, 1, 1)];
    [enc endEncoding];
    [cb commit];
    [cb waitUntilCompleted];
    uint16_t *got = output.contents;
    for (size_t i = 0; i < n; i++) {
        float v;
        memcpy(&v, bits + i, 4);
        if (got[i] != round_bf16(v))
            return 1;
    }
    fprintf(stderr, "BF16 pack edges: %u values passed\n", n);
    return 0;
}
// Exercise both public norm entry points with aligned and scalar-aligned views.
static int cached_norm_views_check(termite_metal_decode_runtime *r, size_t hidden) {
    const size_t rows = 33, count = rows * hidden;
    id<MTLBuffer> x = [r->device newBufferWithLength:(count + 12) * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> w = [r->device newBufferWithLength:(hidden + 12) * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> y = [r->device newBufferWithLength:(count + 12) * 4 options:MTLResourceStorageModeShared];
    if (!x || !w || !y)
        return 1;
    for (size_t mode = 0; mode < 5; mode++) {
        const size_t xo = mode == 1 ? 1 : 4, wo = mode == 2 ? 1 : 4, yo = mode == 3 ? 1 : 4;
        for (size_t i = 0; i < count; i++)
            ((float *)x.contents)[xo + i] = sinf(i * .013f) * .7f;
        for (size_t i = 0; i < hidden; i++)
            ((float *)w.contents)[wo + i] = .5f + (i % 7) * .1f;
        for (size_t i = 0; i < count + 12; i++)
            ((uint32_t *)y.contents)[i] = 0xdeadbeef;
        id<MTLComputePipelineState> a = r->lightonocr3_norm_cached1024_pipeline,
                                    b = r->lightonocr3_norm_cached2560_pipeline;
        if (mode == 4) {
            r->lightonocr3_norm_cached1024_pipeline = nil;
            r->lightonocr3_norm_cached2560_pipeline = nil;
        }
        int rc = termite_metal_decode_runtime_apply_rms_norm_weight_device(
            r, (__bridge void *)x, xo * 4, (__bridge void *)w, wo * 4, rows, hidden, 1e-6f,
            (__bridge void *)y, yo * 4);
        r->lightonocr3_norm_cached1024_pipeline = a;
        r->lightonocr3_norm_cached2560_pipeline = b;
        if (rc)
            return 2;
        for (size_t row = 0; row < rows; row++) {
            double sum = 0;
            for (size_t col = 0; col < hidden; col++) {
                double v = ((float *)x.contents)[xo + row * hidden + col];
                sum += v * v;
            }
            double scale = 1 / sqrt(sum / hidden + 1e-6);
            for (size_t col = 0; col < hidden; col++) {
                float got = ((float *)y.contents)[yo + row * hidden + col];
                double want =
                    ((float *)x.contents)[xo + row * hidden + col] * scale * ((float *)w.contents)[wo + col];
                if (!isfinite(got) || fabs(got - want) > 2e-5)
                    return 3;
            }
        }
        for (size_t i = 0; i < yo; i++)
            if (((uint32_t *)y.contents)[i] != 0xdeadbeef)
                return 4;
        for (size_t i = yo + count; i < count + 12; i++)
            if (((uint32_t *)y.contents)[i] != 0xdeadbeef)
                return 5;
    }
    fprintf(stderr,
            "cached_norm_views hidden=%zu aligned_scalar_offsets_missing_pipeline=pass cpu_all_outputs=pass "
            "canaries=pass\n",
            hidden);
    return 0;
}
static int norm_rows(termite_metal_decode_runtime *r, size_t rows, size_t hidden) {
    size_t count = rows * hidden;
    float *x = malloc(count * 4), *w = malloc(hidden * 4), *gold = malloc(count * 4);
    for (size_t i = 0; i < count; i++)
        x[i] = sinf((float)i * .017f) * .7f;
    for (size_t i = 0; i < hidden; i++)
        w[i] = .5f + (float)(i % 7) * .1f;
    id<MTLBuffer> input = [r->device newBufferWithBytes:x length:count * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> output = [r->device newBufferWithLength:count * 4 options:MTLResourceStorageModeShared];
    if (termite_metal_decode_runtime_prepare_rms_norm(r, 0, w, hidden))
        return 1;
    termite_metal_decode_runtime_set_lightonocr3_projections(r, 0);
    termite_metal_decode_runtime_set_qwen35_ocr_vector_loads(r, 0);
    if (termite_metal_a4b_simd4_rms_norm_rows_preferred(r, rows, hidden))
        return 2;
    if (termite_metal_decode_runtime_apply_rms_norm_rows_device(r, 0, (__bridge void *)input, 0, rows, hidden, 1e-6f,
                                                                (__bridge void *)output, 0))
        return 3;
    memcpy(gold, output.contents, count * 4);
    termite_metal_decode_runtime_set_lightonocr3_projections(r, 1);
    termite_metal_decode_runtime_set_qwen35_ocr_vector_loads(r, 1);
    bool expected = (rows <= 32 && (hidden == 256 || hidden == 2560)) ||
        (rows >= 1 && rows <= 16 && (hidden == 1024 || hidden == 128));
    if (termite_metal_a4b_simd4_rms_norm_rows_preferred(r, rows, hidden) != expected)
        return 4;
    bool prefill_expected=(rows>=33 && rows<=32768 && (hidden==2560||hidden==256)) || (rows>=9 && rows<=32768 && (hidden==1024||hidden==128));
    if((termite_metal_lightonocr3_prefill_norm_threads(r,rows,hidden)!=0)!=prefill_expected)return 9;
    if (prefill_expected && (hidden == 1024 || hidden == 2560)) {
        if (termite_metal_lightonocr3_cached_norm_pipeline(r,rows,hidden,16,16,16) == nil) return 11;
        for (size_t j=0;j<3;j++) if (termite_metal_lightonocr3_cached_norm_pipeline(r,rows,hidden,j==0?4:16,j==1?4:16,j==2?4:16) != nil) return 12;
        id<MTLComputePipelineState> saved1024=r->lightonocr3_norm_cached1024_pipeline, saved2560=r->lightonocr3_norm_cached2560_pipeline;
        r->lightonocr3_norm_cached1024_pipeline=nil; r->lightonocr3_norm_cached2560_pipeline=nil;
        if (termite_metal_lightonocr3_cached_norm_pipeline(r,rows,hidden,16,16,16) != nil) return 13;
        r->lightonocr3_norm_cached1024_pipeline=saved1024; r->lightonocr3_norm_cached2560_pipeline=saved2560;
    }
    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_NORM","1",1);
    if(termite_metal_lightonocr3_prefill_norm_threads(r,rows,hidden)!=0)return 10;
    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_NORM");
    if (termite_metal_decode_runtime_apply_rms_norm_rows_device(r, 0, (__bridge void *)input, 0, rows, hidden, 1e-6f,
                                                                (__bridge void *)output, 0))
        return 5;
    float error = 0, *got = output.contents;
    for (size_t row = 0; row < rows; row++) {
        double sum = 0;
        for (size_t i = 0; i < hidden; i++)
            sum += (double)x[row * hidden + i] * x[row * hidden + i];
        double scale = 1.0 / sqrt(sum / hidden + 1e-6);
        for (size_t i = 0; i < hidden; i++) {
            size_t at = row * hidden + i;
            if (!isfinite(got[at]) || fabs(got[at] - x[at] * scale * w[i]) > 2e-5)
                return 6;
            error = fmaxf(error, fabsf(got[at] - gold[at]));
        }
    }
    if (error > 2e-5f)
        return 7;
    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PARALLEL_NORM", "1", 1);
    if (termite_metal_a4b_simd4_rms_norm_rows_preferred(r, rows, hidden))
        return 8;
    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PARALLEL_NORM");
    fprintf(stderr, "parallel norm rows=%zu hidden=%zu max_abs=%g\n", rows, hidden, error);
    termite_metal_decode_runtime_clear_rms_norm_slot(r, 0);
    free(x);
    free(w);
    free(gold);
    return 0;
}
typedef struct {
    termite_metal_decode_runtime *runtime;
    const float *weight;
    const float *bias;
    void *input;
    void *output;
    int rc;
} LifetimeProbe;

static void lifetime_probe_iteration(void *raw) {
    LifetimeProbe *p=raw;
    p->rc=termite_metal_decode_runtime_prepare_linear(p->runtime,0,p->weight,p->bias,2560,9216);
    if(!p->rc)p->rc=termite_metal_decode_runtime_prefer_linear_mps(p->runtime,0);
    if(!p->rc)p->rc=termite_metal_decode_runtime_apply_linear_multi_row_device(p->runtime,0,p->input,0,8,2560,9216,p->output,0);
    if(!p->rc)p->rc=termite_metal_decode_runtime_clear_linear_slot(p->runtime,0);
}

static int benchmark_lifetime(termite_metal_decode_runtime *r) {
    float *weight=calloc(2560*9216,4), *bias=calloc(9216,4);
    id<MTLBuffer> input=[r->device newBufferWithLength:8*2560*4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> output=[r->device newBufferWithLength:8*9216*4 options:MTLResourceStorageModePrivate];
    if(!weight||!bias||!input||!output)return 1;
    LifetimeProbe p={r,weight,bias,(__bridge void *)input,(__bridge void *)output,0};
    for(size_t pooled=0;pooled<2;pooled++) {
        const size_t before=r->device.currentAllocatedSize;
        for(size_t iteration=0;iteration<4;iteration++) {
            if(pooled)termite_metal_with_autorelease_pool(&p,lifetime_probe_iteration);
            else lifetime_probe_iteration(&p);
            if(p.rc)return p.rc;
            fprintf(stderr,"metal_lifetime mode=%s iteration=%zu allocated_bytes=%zu delta_bytes=%lld\n",pooled?"pooled":"bare",iteration,(size_t)r->device.currentAllocatedSize,(long long)r->device.currentAllocatedSize-(long long)before);
        }
    }
    free(weight);free(bias);
    return 0;
}

static int benchmark_small_rows(termite_metal_decode_runtime *r) {
    const size_t shapes[][2] = {{1024, 6144}, {1024, 3584}, {3584, 1024}, {2560, 8192}, {2560, 9216}, {9216, 2560}, {2560, 248320}};
    const size_t batch_sizes[] = {2, 4, 8};
    const char *modes[] = {"production", "tiled", "vec4", "generic", "tensorops_f32", "tensorops_bf16"};
    for (size_t shape = 0; shape < sizeof(shapes)/sizeof(shapes[0]); shape++) {
        const size_t in = shapes[shape][0], out = shapes[shape][1];
        uint16_t *weights = malloc(in*out*2);
        float *bias = calloc(out, 4);
        if (!weights || !bias) return 1;
        uint32_t rng=123;
        for (size_t i=0;i<in*out;i++) { rng=rng*1664525u+1013904223u; weights[i]=round_bf16(((int)(rng>>16)-32768)*0.000001f); }
        if (termite_metal_decode_runtime_prepare_linear_bf16(r, 0, (uint8_t *)weights, in*out*2, bias, in, out)) return 2;
        free(weights); free(bias);
        for (size_t b=0;b<3;b++) {
            const size_t rows=batch_sizes[b];
            id<MTLBuffer> input = [r->device newBufferWithLength:rows*in*4 options:MTLResourceStorageModeShared];
            id<MTLBuffer> output = [r->device newBufferWithLength:rows*out*4 options:MTLResourceStorageModePrivate];
            if (!input || !output) return 3;
            float *values=input.contents;
            for(size_t i=0;i<rows*in;i++) values[i]=sinf((float)i*.01f);
            for(size_t mode=0;mode<6;mode++) {
                unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_PROJECTIONS");
                unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_TILED_PROJECTIONS");
                unsetenv("TERMITE_METAL_LIGHTONOCR3_SMALL_MPP");
                unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS");
                unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_SMALL_MPP");
                unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_FFN_SMALL_TILE");
                unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_QKV_SMALL_TILE");
                unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_DOWN_SPLIT");
                unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_FOUR_ROW_TILE");
                if(mode>0) {
                    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_SMALL_MPP","1",1);
                    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_FFN_SMALL_TILE","1",1);
                    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_QKV_SMALL_TILE","1",1);
                    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_DOWN_SPLIT","1",1);
                    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_FOUR_ROW_TILE","1",1);
                }
                if(mode==2)setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_TILED_PROJECTIONS","1",1);
                if(mode==3)setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_PROJECTIONS","1",1);
                if(mode>=4)unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_SMALL_MPP");
                if(mode>=4)setenv("TERMITE_METAL_LIGHTONOCR3_SMALL_MPP","1",1);
                if(mode==5)setenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS","1",1);
                for(size_t i=0;i<2;i++) if(termite_metal_decode_runtime_apply_linear_multi_row_device(r,0,(__bridge void *)input,0,rows,in,out,(__bridge void *)output,0))return 4;
                const uint64_t start=clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
                uint64_t gpu=0;
                for(size_t i=0;i<20;i++) {
                    if(termite_metal_decode_runtime_begin_frame(r) ||
                       termite_metal_decode_runtime_apply_linear_multi_row_device(r,0,(__bridge void *)input,0,rows,in,out,(__bridge void *)output,0) ||
                       termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r)) return 5;
                    gpu += r->last_frame_gpu_nanos;
                }
                const uint64_t elapsed=clock_gettime_nsec_np(CLOCK_UPTIME_RAW)-start;
                fprintf(stderr,"small_rows_projection rows=%zu in=%zu out=%zu mode=%s mean_us=%.2f gpu_mean_us=%.2f\n",rows,in,out,modes[mode],(double)elapsed/20000.0,(double)gpu/20000.0);
            }
        }
    }
    return 0;
}

// Compare the fused decoder to the qualified staged implementation, including
// every intermediate output and persistent state, cold starts and continuation.
static int fused_core_check(termite_metal_decode_runtime *r, size_t kh, size_t vh, bool benchmark) {
    size_t channels = (2 * kh + vh) * 128;
    size_t counts[15] = {channels,     channels * 4,   vh,       vh,     vh,       vh,       channels * 4,
                         channels * 4, vh * 128 * 128, channels, vh * 2, vh * 128, vh * 128, 128,
                         vh * 128};
    id<MTLBuffer> buffers[2][15];
    void *handles[2][15];
    size_t offsets[15] = {0};
    for (size_t mode = 0; mode < 2; mode++)
        for (size_t j = 0; j < 15; j++) {
            buffers[mode][j] = [r->device newBufferWithLength:counts[j] * 4
                                                      options:MTLResourceStorageModeShared];
            if (!buffers[mode][j])
                return 1;
            handles[mode][j] = (__bridge void *)buffers[mode][j];
            float *p = buffers[mode][j].contents;
            for (size_t i = 0; i < counts[j]; i++)
                p[i] = j == 13 ? 0.5f + (i % 7) * .1f : sinf((float)i * .013f + (float)j) * .1f;
        }
    for (size_t step = 0; step < 5; step++) {
        for (size_t mode = 0; mode < 2; mode++) {
            if (mode == 0)
                setenv("TERMITE_METAL_DISABLE_QWEN35_FUSED_DECODE", "1", 1);
            else
                unsetenv("TERMITE_METAL_DISABLE_QWEN35_FUSED_DECODE");
            id<MTLComputePipelineState> saved = r->qwen35_fused_decode_pipeline;
            id<MTLComputePipelineState> saved_parallel = r->lightonocr3_recurrent_parallel_pipeline;
            if (mode == 1 && step == 4) {
                r->qwen35_fused_decode_pipeline = nil;
                r->lightonocr3_recurrent_parallel_pipeline = nil;
            }
            int rc = termite_metal_decode_runtime_qwen35_core_device(r, handles[mode], offsets, 1, kh, vh,
                                                                     128, 4, step != 0);
            r->qwen35_fused_decode_pipeline = saved;
            r->lightonocr3_recurrent_parallel_pipeline = saved_parallel;
            if (rc)
                return 2;
        }
        const size_t outputs[] = {7, 8, 9, 10, 11, 14};
        float worst = 0;
        for (size_t k = 0; k < 6; k++) {
            size_t j = outputs[k];
            float *a = buffers[0][j].contents, *b = buffers[1][j].contents;
            for (size_t i = 0; i < counts[j]; i++) {
                if (!isfinite(a[i]) || !isfinite(b[i]))
                    return 3;
                worst = fmaxf(worst, fabsf(a[i] - b[i]));
            }
        }
        fprintf(stderr, "fused_core_check key_heads=%zu value_heads=%zu step=%zu fallback=%d max_abs=%g\n",
                kh, vh, step, step == 4, worst);
        if (worst > 2e-4f)
            return 4;
        for (size_t mode = 0; mode < 2; mode++) {
            id<MTLBuffer> swap = buffers[mode][6];
            buffers[mode][6] = buffers[mode][7];
            buffers[mode][7] = swap;
            handles[mode][6] = (__bridge void *)buffers[mode][6];
            handles[mode][7] = (__bridge void *)buffers[mode][7];
        }
    }
    if (benchmark)
        for (size_t mode = 0; mode < 2; mode++) {
            if (mode == 0)
                setenv("TERMITE_METAL_DISABLE_QWEN35_FUSED_DECODE", "1", 1);
            else
                unsetenv("TERMITE_METAL_DISABLE_QWEN35_FUSED_DECODE");
            uint64_t gpu = 0, start = clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
            for (size_t i = 0; i < 100; i++) {
                if (termite_metal_decode_runtime_begin_frame(r) ||
                    termite_metal_decode_runtime_qwen35_core_device(r, handles[mode], offsets, 1, kh, vh, 128,
                                                                    4, 1) ||
                    termite_metal_decode_runtime_submit_frame(r) ||
                    termite_metal_decode_runtime_wait_frame(r))
                    return 5;
                gpu += r->last_frame_gpu_nanos;
            }
            uint64_t elapsed = clock_gettime_nsec_np(CLOCK_UPTIME_RAW) - start;
            fprintf(
                stderr,
                "fused_core_benchmark key_heads=%zu value_heads=%zu mode=%s mean_us=%.2f gpu_mean_us=%.2f\n",
                kh, vh, mode ? "fused" : "staged", (double)elapsed / 100000.0, (double)gpu / 100000.0);
        }
    unsetenv("TERMITE_METAL_DISABLE_QWEN35_FUSED_DECODE");
    return 0;
}

// Real decoder head counts, distinct page histories, and nonzero byte offsets.
// Compare the concurrent encoder with independent staged page invocations.
// Compare parallel prefill normalization with the original full recurrent core,
// including every intermediate, continued state, offsets and fallback routes.
static int prefill_core_check(termite_metal_decode_runtime *r, size_t seq, size_t vh, bool initialized,
                              bool benchmark) {
    const size_t kh = 16, channels = (2 * kh + vh) * 128;
    const size_t counts[15] = {
        seq * channels, channels * 4, seq * vh,       seq * vh,       vh,           vh,
        channels * 4,   channels * 4, vh * 128 * 128, seq * channels, seq * vh * 2, seq * vh * 128,
        seq * vh * 128, 128,          seq * vh * 128};
    id<MTLBuffer> buffers[2][15];
    void *handles[2][15];
    size_t offsets[15];
    for (size_t j = 0; j < 15; j++)
        offsets[j] = 16;
    for (size_t mode = 0; mode < 2; mode++)
        for (size_t j = 0; j < 15; j++) {
            buffers[mode][j] = [r->device newBufferWithLength:(counts[j] + 8) * 4
                                                      options:MTLResourceStorageModeShared];
            if (!buffers[mode][j])
                return 1;
            handles[mode][j] = (__bridge void *)buffers[mode][j];
            uint32_t *raw = buffers[mode][j].contents;
            for (size_t i = 0; i < 4; i++)
                raw[i] = raw[counts[j] + 4 + i] = 0xdeadbeef;
            for (size_t i = 0; i < counts[j]; i++)
                ((float *)raw)[i + 4] = j == 13 ? .5f + (i % 7) * .1f : sinf(i * .013f + j) * .1f;
        }
    void *state_seed = malloc(counts[8] * 4);
    if (!state_seed)
        return 2;
    memcpy(state_seed, (char *)buffers[0][8].contents + 16, counts[8] * 4);
    setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_PREFILL_CORE_NORM", "1", 1);
    const size_t trials = benchmark ? 30 : 1, modes = benchmark ? 2 : 4;
    float worst = 0;
    for (size_t trial = 0; trial < trials; trial++)
        for (size_t order = 0; order < modes; order++) {
            const size_t variant = benchmark ? (order + trial % 2) % 2 : order, mode = variant == 0 ? 0 : 1;
            memcpy((char *)buffers[mode][8].contents + 16, state_seed, counts[8] * 4);
            if (variant == 0 || variant == 3)
                setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_CORE_NORM", "1", 1);
            else
                unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_CORE_NORM");
            id<MTLComputePipelineState> saved = r->lightonocr3_prefill_prepare_pipeline;
            if (variant == 2)
                r->lightonocr3_prefill_prepare_pipeline = nil;
            uint64_t before = r->lightonocr3_prefill_norm_dispatches;
            if (termite_metal_decode_runtime_begin_frame(r))
                return 3;
            uint64_t start = clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
            if (termite_metal_decode_runtime_qwen35_core_device(r, handles[mode], offsets, seq, kh, vh, 128,
                                                                4, initialized) ||
                termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r))
                return 4;
            uint64_t elapsed = clock_gettime_nsec_np(CLOCK_UPTIME_RAW) - start;
            r->lightonocr3_prefill_prepare_pipeline = saved;
            if (r->lightonocr3_prefill_norm_dispatches - before != (variant == 1 && seq >= 128 ? 1 : 0))
                return 5;
            if (benchmark)
                printf("{\"seq\":%zu,\"heads\":%zu,\"initialized\":%d,\"trial\":%zu,\"mode\":%zu,\"wall_us\":"
                       "%.4f,\"gpu_us\":%.4f}\n",
                       seq, vh, initialized, trial, variant, elapsed / 1000.0,
                       r->last_frame_gpu_nanos / 1000.0);
            if (variant != 0) {
                const size_t outputs[] = {7, 8, 9, 10, 11, 14};
                for (size_t k = 0; k < 6; k++) {
                    size_t j = outputs[k];
                    const float *gold = (float *)buffers[0][j].contents + 4,
                                *got = (float *)buffers[1][j].contents + 4;
                    for (size_t i = 0; i < counts[j]; i++) {
                        if (!isfinite(got[i]) || !isfinite(gold[i]))
                            return 6;
                        worst = fmaxf(worst, fabsf(got[i] - gold[i]));
                    }
                }
            }
            for (size_t j = 0; j < 15; j++)
                for (size_t i = 0; i < 4; i++) {
                    const uint32_t *raw = buffers[mode][j].contents;
                    if (raw[i] != 0xdeadbeef || raw[counts[j] + 4 + i] != 0xdeadbeef)
                        return 7;
                }
        }
    fprintf(stderr, "prefill_core_check seq=%zu heads=%zu initialized=%d max_abs=%g\n", seq, vh, initialized,
            worst);
    free(state_seed);
    unsetenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_PREFILL_CORE_NORM");
    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_CORE_NORM");
    return worst > 2e-4 ? 8 : 0;
}
static int compute_copy_check(termite_metal_decode_runtime *r) {
    id<MTLBuffer> a = [r->device newBufferWithLength:131104 options:MTLResourceStorageModeShared];
    id<MTLBuffer> b = [r->device newBufferWithLength:131104 options:MTLResourceStorageModeShared];
    id<MTLBuffer> c = [r->device newBufferWithLength:131104 options:MTLResourceStorageModeShared];
    if (!a || !b || !c || !r->lightonocr3_copy_pipeline)
        return 1;
    for (size_t i = 0; i < a.length / 4; i++)
        ((uint32_t *)a.contents)[i] = (uint32_t)i * 2654435761u;
    for (size_t mode = 0; mode < 6; mode++) {
        memset(b.contents, 0x5a, b.length);
        memset(c.contents, 0xa5, c.length);
        if (termite_metal_decode_runtime_begin_frame(r))
            return 2;
        r->active_frame_regime =
            mode == 4 ? TERMITE_METAL_FRAME_REGIME_PREFILL : TERMITE_METAL_FRAME_REGIME_DECODE;
        if (mode == 1)
            setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_COMPUTE_COPIES", "1", 1);
        id<MTLComputePipelineState> saved = r->lightonocr3_copy_pipeline;
        if (mode == 2)
            r->lightonocr3_copy_pipeline = nil;
        size_t length = mode == 3 ? 131072 : 16384;
        size_t offset = mode == 5 ? 4 : 16;
        uint64_t before = r->lightonocr3_copy_dispatches;
        if (mode == 0 && termite_metal_lightonocr3_copy_on_frame(r, a, 16, a, 32, 64) != 0) return 11;
        if (termite_metal_buffer_copy(r, (__bridge void *)a, offset, (__bridge void *)b, offset, length) ||
            termite_metal_buffer_copy(r, (__bridge void *)b, offset, (__bridge void *)c, offset, length) ||
            termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r))
            return 3;
        r->lightonocr3_copy_pipeline = saved;
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_COMPUTE_COPIES");
        if (r->lightonocr3_copy_dispatches - before != (mode == 0 ? 2 : 0))
            return 4;
        if (r->last_frame_blit_encoder_count != (mode == 0 ? 0 : 2) ||
            (mode == 0 && r->last_frame_compute_encoder_count != 1)) return 12;
        if (memcmp((char *)a.contents + offset, (char *)b.contents + offset, length) ||
            memcmp((char *)a.contents + offset, (char *)c.contents + offset, length))
            return 5;
        for (size_t i = 0; i < b.length; i++)
            if ((i < offset || i >= offset + length) &&
                (((uint8_t *)b.contents)[i] != 0x5a || ((uint8_t *)c.contents)[i] != 0xa5))
                return 6;
    }
    memset(b.contents, 0x5a, b.length);
    if (termite_metal_decode_runtime_begin_frame(r))
        return 7;
    r->active_frame_regime = TERMITE_METAL_FRAME_REGIME_DECODE;
    if (termite_metal_buffer_copy(r, (__bridge void *)a, 16, (__bridge void *)b, 16, 16384) ||
        termite_metal_decode_runtime_cancel_frame(r) || r->active_frame_retained_resources != NULL ||
        r->active_planned_compute_encoder != nil)
        return 8;
    for (size_t i = 0; i < b.length; i++)
        if (((uint8_t *)b.contents)[i] != 0x5a)
            return 9;
    if (termite_metal_buffer_copy(r, (__bridge void *)a, SIZE_MAX - 15, (__bridge void *)b, 16, 32) != -2)
        return 10;
    fprintf(stderr, "compute_copy_check bitwise_payload=pass producer_consumer=pass canaries=pass "
                    "fallback=pass cancel=pass\n");
    return 0;
}
static int fused_batch_core_check(termite_metal_decode_runtime *r, size_t kh, size_t vh, size_t pages,
                                  bool benchmark) {
    const size_t channels = (2 * kh + vh) * 128;
    const size_t counts[15] = {
        channels,       channels * 4, vh,     vh,       vh,       vh,  channels * 4, channels * 4,
        vh * 128 * 128, channels,     vh * 2, vh * 128, vh * 128, 128, vh * 128};
    id<MTLBuffer> buffers[2][16][15];
    void *handles[2][16][15];
    size_t offsets[2][16][15];
    for (size_t mode = 0; mode < 2; mode++)
        for (size_t page = 0; page < pages; page++)
            for (size_t j = 0; j < 15; j++)
                offsets[mode][page][j] = 16;
    for (size_t mode = 0; mode < 2; mode++)
        for (size_t page = 0; page < pages; page++)
            for (size_t j = 0; j < 15; j++) {
                buffers[mode][page][j] = [r->device newBufferWithLength:(counts[j] + 8) * 4
                                                                options:MTLResourceStorageModeShared];
                if (!buffers[mode][page][j])
                    return 1;
                handles[mode][page][j] = (__bridge void *)buffers[mode][page][j];
                uint32_t *raw = buffers[mode][page][j].contents;
                for (size_t i = 0; i < 4; i++)
                    raw[i] = raw[counts[j] + 4 + i] = 0xdeadbeefu;
                float *values = (float *)raw + 4;
                for (size_t i = 0; i < counts[j]; i++)
                    values[i] = j == 13 ? .5f + (i % 7) * .1f : sinf(i * .013f + j + page * .31f) * .1f;
            }
    // Disjoint final rows share one destination, with guard bands per row.
    id<MTLBuffer> joined = [r->device newBufferWithLength:pages * (counts[14] + 8) * 4
                                                  options:MTLResourceStorageModeShared];
    if (!joined)
        return 27;
    for (size_t i = 0; i < joined.length / 4; i++)
        ((uint32_t *)joined.contents)[i] = 0xdeadbeef;
    for (size_t page = 0; page < pages; page++) {
        offsets[1][page][14] = 16 + page * (counts[14] + 8) * 4;
        buffers[1][page][14] = joined;
        handles[1][page][14] = (__bridge void *)joined;
    }
    // One workspace contains all conv/gate/core rows, matching the packed
    // host dispatch. Keep guard bands around every independently written span.
    const size_t conv_words = pages * (counts[9] + 8), gate_words = pages * (counts[10] + 8);
    const size_t scratch_words = conv_words + gate_words + pages * (counts[11] + 8);
    id<MTLBuffer> workspace = [r->device newBufferWithLength:scratch_words * 4 options:MTLResourceStorageModeShared];
    if (!workspace) return 28;
    for (size_t i=0;i<scratch_words;i++) ((uint32_t *)workspace.contents)[i]=0xdeadbeef;
    for (size_t page=0;page<pages;page++) {
        offsets[1][page][9] = (4 + page * (counts[9] + 8)) * 4;
        offsets[1][page][10] = (conv_words + 4 + page * (counts[10] + 8)) * 4;
        offsets[1][page][11] = (conv_words + gate_words + 4 + page * (counts[11] + 8)) * 4;
        for (size_t j=9;j<=11;j++) {
            buffers[1][page][j]=workspace;
            handles[1][page][j]=(__bridge void *)workspace;
        }
    }
    // Check shared immutable weights as well as independent page state.
    const size_t shared[] = {1, 4, 5, 13};
    for (size_t mode = 0; mode < 2; mode++)
        for (size_t page = 1; page < pages; page++)
            for (size_t k = 0; k < 4; k++) {
                size_t j = shared[k];
                buffers[mode][page][j] = buffers[mode][0][j];
                handles[mode][page][j] = handles[mode][0][j];
            }
    id<MTLBuffer> producers[16], consumers[16];
    for (size_t page = 0; page < pages; page++) {
        producers[page] = [r->device newBufferWithLength:channels * 4 options:MTLResourceStorageModeShared];
        consumers[page] = [r->device newBufferWithLength:vh * 128 * 4 options:MTLResourceStorageModeShared];
        if (!producers[page] || !consumers[page])
            return 10;
    }
    for (size_t step = 0; step < 8; step++) {
        // GPU producer writes deliberately poisoned input; the consumer copies
        // the recurrent output before submission through the same planned scope.
        for (size_t page = 0; page < pages; page++)
            for (size_t i = 0; i < channels; i++) {
                float value = sinf(i * .017f + step * .23f + page * .31f) * .1f;
                ((float *)producers[page].contents)[i] = value;
                ((float *)buffers[0][page][0].contents)[i + 4] = value;
                ((float *)buffers[1][page][0].contents)[i + 4] = -.9f;
            }
        if (step % 2)
            setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING", "1", 1);
        else
            unsetenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING");
        setenv("TERMITE_METAL_DISABLE_QWEN35_FUSED_DECODE", "1", 1);
        for (size_t page = 0; page < pages; page++)
            if (termite_metal_decode_runtime_qwen35_core_device(r, handles[0][page], offsets[0][page], 1, kh,
                                                                vh, 128, 4, step != 0 || page % 2))
                return 2;
        unsetenv("TERMITE_METAL_DISABLE_QWEN35_FUSED_DECODE");
        id<MTLComputePipelineState> saved = r->lightonocr3_recurrent_parallel_pipeline;
        if (step == 6)
            r->lightonocr3_recurrent_parallel_pipeline = nil;
        if (step == 7)
            setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_RECURRENT_PARALLEL", "1", 1);
        uint64_t before = r->lightonocr3_recurrent_parallel_dispatches;
        if (termite_metal_decode_runtime_begin_frame(r))
            return 3;
        if ((step == 2 || step == 3) &&
            termite_metal_decode_runtime_begin_planned_compute_scope(r, TERMITE_METAL_COMPUTE_SOURCE_OTHER,
                                                                     TERMITE_METAL_COMPUTE_REGION_OTHER))
            return 11;
        for (size_t page = 0; page < pages; page++)
            if (termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)producers[page], 0, 1, channels, 0,
                                                       channels, handles[1][page][0], 16))
                return 12;
        if (pages > 1 && termite_metal_decode_runtime_qwen35_core_batch_begin(r, pages) != 1)
            return 3;
        for (size_t page = 0; page < pages; page++)
            if (termite_metal_decode_runtime_qwen35_core_device(r, handles[1][page], offsets[1][page], 1, kh,
                                                                vh, 128, 4, step != 0 || page % 2))
                return 4;
        if (pages > 1 && termite_metal_decode_runtime_qwen35_core_batch_end(r))
            return 5;
        for (size_t page = 0; page < pages; page++)
            if (termite_metal_buffer_slice_last_dim_2d(r, handles[1][page][14], offsets[1][page][14], 1,
                                                       vh * 128, 0, vh * 128,
                                                       (__bridge void *)consumers[page], 0))
                return 13;
        if (termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r))
            return 5;
        r->lightonocr3_recurrent_parallel_pipeline = saved;
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_RECURRENT_PARALLEL");
        if (r->lightonocr3_recurrent_parallel_dispatches - before != (step < 6 ? 1 : 0))
            return 9;
        for (size_t page = 0; page < pages; page++)
            if (memcmp(consumers[page].contents, (char *)buffers[1][page][14].contents + offsets[1][page][14],
                       vh * 128 * 4))
                return 14;
        const size_t outputs[] = {7, 8, 9, 10, 11, 14};
        float worst = 0;
        for (size_t page = 0; page < pages; page++)
            for (size_t k = 0; k < 6; k++) {
                const size_t j = outputs[k];
                const float *gold = (float *)((char *)buffers[0][page][j].contents + offsets[0][page][j]),
                            *got = (float *)((char *)buffers[1][page][j].contents + offsets[1][page][j]);
                for (size_t i = 0; i < counts[j]; i++) {
                    if (!isfinite(got[i]) || !isfinite(gold[i]))
                        return 6;
                    worst = fmaxf(worst, fabsf(got[i] - gold[i]));
                }
            }
        for (size_t mode = 0; mode < 2; mode++)
            for (size_t page = 0; page < pages; page++)
                for (size_t j = 0; j < 15; j++) {
                    const uint32_t *raw = (const uint32_t *)((const char *)buffers[mode][page][j].contents +
                                                             offsets[mode][page][j] - 16);
                    for (size_t i = 0; i < 4; i++)
                        if (raw[i] != 0xdeadbeefu || raw[counts[j] + 4 + i] != 0xdeadbeefu)
                            return 7;
                }
        fprintf(
            stderr,
            "fused_batch_core_check pages=%zu key_heads=%zu value_heads=%zu offset=16 step=%zu fallback=%d "
            "max_abs=%g\n",
            pages, kh, vh, step, step >= 6, worst);
        if (worst > 2e-4f)
            return 8;
        for (size_t mode = 0; mode < 2; mode++)
            for (size_t page = 0; page < pages; page++) {
                id<MTLBuffer> swap = buffers[mode][page][6];
                buffers[mode][page][6] = buffers[mode][page][7];
                buffers[mode][page][7] = swap;
                handles[mode][page][6] = (__bridge void *)buffers[mode][page][6];
                handles[mode][page][7] = (__bridge void *)buffers[mode][page][7];
            }
    }
    if (benchmark) {
        void *initial[16][15];
        for (size_t page = 0; page < pages; page++)
            for (size_t j = 0; j < 15; j++) {
                initial[page][j] = malloc(counts[j] * 4);
                if (!initial[page][j])
                    return 15;
                memcpy(initial[page][j], (char *)buffers[0][page][j].contents + 16, counts[j] * 4);
            }
        const size_t orders[6][3] = {{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}};
        for (size_t trial = 0; trial < 36; trial++)
            for (size_t order = 0; order < 3; order++) {
                const size_t variant = orders[trial % 6][order], mode = variant == 0 ? 0 : 1;
                if (variant == 0)
                    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_RECURRENT_PARALLEL", "1", 1);
                else
                    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_RECURRENT_PARALLEL");
                if (variant == 2)
                    setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING", "1", 1);
                else
                    unsetenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING");
                for (size_t page = 0; page < pages; page++)
                    for (size_t j = 0; j < 15; j++)
                        memcpy((char *)buffers[mode][page][j].contents + offsets[mode][page][j],
                               initial[page][j], counts[j] * 4);
                const uint64_t dispatch_before = r->lightonocr3_recurrent_parallel_dispatches;
                if (termite_metal_decode_runtime_begin_frame(r))
                    return 16;
                const uint64_t start = clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
                for (size_t repeat = 0; repeat < 16; repeat++) {
                    if (pages > 1 && termite_metal_decode_runtime_qwen35_core_batch_begin(r, pages) != 1)
                        return 17;
                    for (size_t page = 0; page < pages; page++)
                        if (termite_metal_decode_runtime_qwen35_core_device(
                                r, handles[mode][page], offsets[mode][page], 1, kh, vh, 128, 4, 1))
                            return 18;
                    if (pages > 1 && termite_metal_decode_runtime_qwen35_core_batch_end(r))
                        return 19;
                    for (size_t page = 0; page < pages; page++) {
                        id<MTLBuffer> swap = buffers[mode][page][6];
                        buffers[mode][page][6] = buffers[mode][page][7];
                        buffers[mode][page][7] = swap;
                        handles[mode][page][6] = (__bridge void *)buffers[mode][page][6];
                        handles[mode][page][7] = (__bridge void *)buffers[mode][page][7];
                    }
                }
                const uint64_t encoded = clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
                if (termite_metal_decode_runtime_submit_frame(r) ||
                    termite_metal_decode_runtime_wait_frame(r))
                    return 20;
                if (r->lightonocr3_recurrent_parallel_dispatches - dispatch_before != (variant == 0 ? 0 : 16))
                    return 21;
                printf("{\"heads\":%zu,\"pages\":%zu,\"trial\":%zu,\"variant\":%zu,\"encode_us\":%.6f,\"gpu_"
                       "us\":%.6f}\n",
                       vh, pages, trial, variant, (encoded - start) / 16000.0,
                       r->last_frame_gpu_nanos / 16000.0);
            }
        for (size_t page = 0; page < pages; page++)
            for (size_t j = 0; j < 15; j++)
                free(initial[page][j]);
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_RECURRENT_PARALLEL");
    }
    // Cancel after encoding the indirect pointer table. No state or output may
    // become visible, and frame retention/planned ownership must be cleared.
    setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING", "1", 1);
    void *state_before[16];
    for (size_t page = 0; page < pages; page++) {
        state_before[page] = malloc(counts[8] * 4);
        if (!state_before[page])
            return 22;
        memcpy(state_before[page], (char *)buffers[1][page][8].contents + 16, counts[8] * 4);
    }
    if (termite_metal_decode_runtime_begin_frame(r) ||
        (pages > 1 && termite_metal_decode_runtime_qwen35_core_batch_begin(r, pages) != 1))
        return 23;
    for (size_t page = 0; page < pages; page++)
        if (termite_metal_decode_runtime_qwen35_core_device(r, handles[1][page], offsets[1][page], 1, kh, vh,
                                                            128, 4, 1))
            return 24;
    if ((pages > 1 && termite_metal_decode_runtime_qwen35_core_batch_end(r)) ||
        termite_metal_decode_runtime_cancel_frame(r) || r->active_planned_compute_encoder != nil ||
        r->active_frame_retained_resources != NULL || r->qwen35_core_batch_active)
        return 25;
    for (size_t page = 0; page < pages; page++) {
        if (memcmp(state_before[page], (char *)buffers[1][page][8].contents + 16, counts[8] * 4))
            return 26;
        free(state_before[page]);
    }
    unsetenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING");
    return 0;
}

static int florence_channel_check(termite_metal_decode_runtime *r, size_t batch, size_t seq, size_t dim,
                                  size_t groups, bool benchmark) {
    const size_t total = batch * seq * dim, cpg = dim / groups;
    id<MTLBuffer> input = [r->device newBufferWithLength:total * 3 * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> output = [r->device newBufferWithLength:total * 4 options:MTLResourceStorageModeShared];
    float *gold = malloc(total * 4);
    if (!input || !output || !gold)
        return 1;
    float *x = input.contents;
    for (size_t i = 0; i < total * 3; i++)
        x[i] = sinf((float)i * .013f) * .3f;
    for (size_t mode = 0; mode < 3; mode++) {
        if (mode == 0)
            setenv("TERMITE_METAL_DISABLE_FLORENCE2_CHANNEL_REDUCTION", "1", 1);
        else
            unsetenv("TERMITE_METAL_DISABLE_FLORENCE2_CHANNEL_REDUCTION");
        id<MTLComputePipelineState> saved = r->florence_channel_scores_parallel_pipeline;
        if (mode == 2)
            r->florence_channel_scores_parallel_pipeline = nil;
        int rc = termite_metal_decode_runtime_florence_channel_attention_f32_device(
            r, (__bridge void *)input, 0, (__bridge void *)output, 0, batch, seq, dim, groups);
        r->florence_channel_scores_parallel_pipeline = saved;
        if (rc)
            return 2;
        float *got = output.contents;
        if (mode == 0)
            memcpy(gold, got, total * 4);
        float worst = 0;
        for (size_t i = 0; i < total; i++) {
            if (!isfinite(got[i]))
                return 3;
            worst = fmaxf(worst, fabsf(got[i] - gold[i]));
        }
        // Independent double-precision score/softmax/value samples.
        double cpu_error = 0;
        for (size_t b = 0; b < batch; b++)
            for (size_t g = 0; g < groups; g++)
                for (size_t qc = 0; qc < cpg; qc += cpg - 1) {
                    double scores[128], best = -INFINITY, denom = 0;
                    if (cpg < 2 || cpg > 128)
                        return 4;
                    for (size_t kc = 0; kc < cpg; kc++) {
                        double sum = 0;
                        for (size_t n = 0; n < seq; n++) {
                            size_t base = (b * seq + n) * dim * 3 + g * cpg;
                            sum += (double)x[base + qc] * x[base + dim + kc];
                        }
                        scores[kc] = sum / sqrt((double)seq);
                        best = fmax(best, scores[kc]);
                    }
                    for (size_t kc = 0; kc < cpg; kc++) {
                        scores[kc] = exp(scores[kc] - best);
                        denom += scores[kc];
                    }
                    for (size_t n = 0; n < seq; n += seq > 1 ? seq - 1 : 1) {
                        double sum = 0;
                        size_t base = (b * seq + n) * dim * 3 + 2 * dim + g * cpg;
                        for (size_t kc = 0; kc < cpg; kc++)
                            sum += scores[kc] * x[base + kc];
                        cpu_error =
                            fmax(cpu_error, fabs(sum / denom - got[(b * seq + n) * dim + g * cpg + qc]));
                    }
                }
        fprintf(stderr,
                "florence_channel_check batch=%zu seq=%zu dim=%zu groups=%zu mode=%zu max_abs=%g "
                "cpu_max_abs=%g\n",
                batch, seq, dim, groups, mode, worst, cpu_error);
        if (worst > 2e-4f || cpu_error > 2e-4)
            return 5;
    }
    if (benchmark) {
        double gpu[2] = {0}, wall[2] = {0};
        for (size_t trial = 0; trial < 30; trial++)
            for (size_t mode = 0; mode < 2; mode++) {
                if (mode == 0)
                    setenv("TERMITE_METAL_DISABLE_FLORENCE2_CHANNEL_REDUCTION", "1", 1);
                else
                    unsetenv("TERMITE_METAL_DISABLE_FLORENCE2_CHANNEL_REDUCTION");
                uint64_t start = clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
                if (termite_metal_decode_runtime_begin_frame(r) ||
                    termite_metal_decode_runtime_florence_channel_attention_f32_device(
                        r, (__bridge void *)input, 0, (__bridge void *)output, 0, batch, seq, dim, groups) ||
                    termite_metal_decode_runtime_submit_frame(r) ||
                    termite_metal_decode_runtime_wait_frame(r))
                    return 6;
                if (trial >= 10) {
                    gpu[mode] += r->last_frame_gpu_nanos;
                    wall[mode] += clock_gettime_nsec_np(CLOCK_UPTIME_RAW) - start;
                }
            }
        for (size_t mode = 0; mode < 2; mode++)
            fprintf(stderr,
                    "florence_channel_benchmark batch=%zu seq=%zu dim=%zu groups=%zu mode=%s mean_us=%.2f "
                    "gpu_mean_us=%.2f\n",
                    batch, seq, dim, groups, mode ? "parallel" : "serial", wall[mode] / 20000,
                    gpu[mode] / 20000);
    }
    unsetenv("TERMITE_METAL_DISABLE_FLORENCE2_CHANNEL_REDUCTION");
    free(gold);
    return 0;
}

static size_t window_index(size_t b, size_t h, size_t n, size_t d, size_t seq, size_t heads, int layout) {
    return layout ? ((b * seq + n) * heads + h) * 32 + d : ((b * heads + h) * seq + n) * 32 + d;
}
static int florence_window_check(termite_metal_decode_runtime *r, size_t batch, size_t seq, size_t heads,
                                 int layout, bool benchmark) {
    size_t count = batch * seq * heads * 32;
    id<MTLBuffer> q = [r->device newBufferWithLength:count * 4 options:MTLResourceStorageModeShared],
                  k = [r->device newBufferWithLength:count * 4 options:MTLResourceStorageModeShared],
                  v = [r->device newBufferWithLength:count * 4 options:MTLResourceStorageModeShared],
                  out = [r->device newBufferWithLength:count * 4 options:MTLResourceStorageModeShared];
    if (!q || !k || !v || !out)
        return 1;
    float *gold = malloc(count * 4), *qs = q.contents, *ks = k.contents, *vs = v.contents;
    if (!gold)
        return 2;
    for (size_t i = 0; i < count; i++) {
        qs[i] = sinf(i * .013f) * 3;
        ks[i] = cosf(i * .017f) * 3;
        vs[i] = sinf(i * .019f);
    }
    r->florence_window_sdpa_enabled = 1;
    for (size_t mode = 0; mode < 3; mode++) {
        if (mode == 0)
            setenv("TERMITE_METAL_DISABLE_FLORENCE2_WINDOW_SOFTMAX", "1", 1);
        else
            unsetenv("TERMITE_METAL_DISABLE_FLORENCE2_WINDOW_SOFTMAX");
        id<MTLComputePipelineState> saved = r->sdpa_f32_florence_window_parallel_pipeline;
        if (mode == 2)
            r->sdpa_f32_florence_window_parallel_pipeline = nil;
        int rc = termite_metal_decode_runtime_sdpa_f32_device(
            r, (__bridge void *)q, 0, (__bridge void *)k, 0, (__bridge void *)v, 0, NULL, 0, NULL, 0, batch,
            seq, heads, 32, 0, 0, layout, 0, 0, (__bridge void *)out, 0);
        r->sdpa_f32_florence_window_parallel_pipeline = saved;
        if (rc)
            return 3;
        float *got = out.contents;
        if (mode == 0)
            memcpy(gold, got, count * 4);
        float worst = 0;
        for (size_t i = 0; i < count; i++) {
            if (!isfinite(got[i]))
                return 4;
            worst = fmaxf(worst, fabsf(gold[i] - got[i]));
        }
        double cpu_error = 0;
        for (size_t b = 0; b < batch; b++)
            for (size_t h = 0; h < heads; h++)
                for (size_t qi = 0; qi < seq; qi += seq > 1 ? seq - 1 : 1) {
                    double scores[256], best = -INFINITY, sum = 0;
                    for (size_t ki = 0; ki < seq; ki++) {
                        double dot = 0;
                        for (size_t d = 0; d < 32; d++)
                            dot += (double)qs[window_index(b, h, qi, d, seq, heads, layout)] *
                                   ks[window_index(b, h, ki, d, seq, heads, layout)];
                        scores[ki] = dot / sqrt(32.0);
                        best = fmax(best, scores[ki]);
                    }
                    for (size_t ki = 0; ki < seq; ki++) {
                        scores[ki] = exp(scores[ki] - best);
                        sum += scores[ki];
                    }
                    for (size_t d = 0; d < 32; d += 31) {
                        double value = 0;
                        for (size_t ki = 0; ki < seq; ki++)
                            value += scores[ki] * vs[window_index(b, h, ki, d, seq, heads, layout)];
                        cpu_error = fmax(cpu_error, fabs(value / sum -
                                                         got[window_index(b, h, qi, d, seq, heads, layout)]));
                    }
                }
        fprintf(stderr,
                "florence_window_check batch=%zu seq=%zu heads=%zu layout=%d mode=%zu max_abs=%g "
                "cpu_max_abs=%g\n",
                batch, seq, heads, layout, mode, worst, cpu_error);
        if (worst > 2e-4f || cpu_error > 2e-4)
            return 5;
    }
    if (benchmark) {
        double gpu[2] = {0}, wall[2] = {0};
        for (size_t trial = 0; trial < 30; trial++)
            for (size_t mode = 0; mode < 2; mode++) {
                if (mode == 0)
                    setenv("TERMITE_METAL_DISABLE_FLORENCE2_WINDOW_SOFTMAX", "1", 1);
                else
                    unsetenv("TERMITE_METAL_DISABLE_FLORENCE2_WINDOW_SOFTMAX");
                uint64_t start = clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
                if (termite_metal_decode_runtime_begin_frame(r) ||
                    termite_metal_decode_runtime_sdpa_f32_device(
                        r, (__bridge void *)q, 0, (__bridge void *)k, 0, (__bridge void *)v, 0, NULL, 0, NULL,
                        0, batch, seq, heads, 32, 0, 0, layout, 0, 0, (__bridge void *)out, 0) ||
                    termite_metal_decode_runtime_submit_frame(r) ||
                    termite_metal_decode_runtime_wait_frame(r))
                    return 6;
                if (trial >= 10) {
                    gpu[mode] += r->last_frame_gpu_nanos;
                    wall[mode] += clock_gettime_nsec_np(CLOCK_UPTIME_RAW) - start;
                }
            }
        for (size_t mode = 0; mode < 2; mode++)
            fprintf(stderr,
                    "florence_window_benchmark batch=%zu seq=%zu heads=%zu layout=%d mode=%s mean_us=%.2f "
                    "gpu_mean_us=%.2f\n",
                    batch, seq, heads, layout, mode ? "parallel" : "serial", wall[mode] / 20000,
                    gpu[mode] / 20000);
    }
    unsetenv("TERMITE_METAL_DISABLE_FLORENCE2_WINDOW_SOFTMAX");
    free(gold);
    return 0;
}

// Alternate the production dispatcher and its rollback on the same nonuniform
// weights. Include partial rows that deliberately retain their prior kernel.
static int small_projection_timing(termite_metal_decode_runtime *r, size_t in, size_t out, size_t rows) {
    const char *rollback = rows == 4 && ((in == 1024 && out == 3584) || (in == 2560 && (out == 8192 || out == 9216))) ? "TERMITE_METAL_DISABLE_LIGHTONOCR3_FOUR_ROW_TILE" : (in == 3584 || in == 9216) ? "TERMITE_METAL_DISABLE_LIGHTONOCR3_DOWN_SPLIT" : (out == 6144 || out == 8192) ? "TERMITE_METAL_DISABLE_LIGHTONOCR3_QKV_SMALL_TILE" : "TERMITE_METAL_DISABLE_LIGHTONOCR3_FFN_SMALL_TILE";
    uint16_t *weights = malloc(in * out * 2);
    float *bias = calloc(out, 4);
    id<MTLBuffer> input = [r->device newBufferWithLength:rows * in * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> output = [r->device newBufferWithLength:rows * out * 4
                                                  options:MTLResourceStorageModeShared];
    if (!weights || !bias || !input || !output)
        return 1;
    uint32_t rng = 123;
    for (size_t i = 0; i < in * out; i++) {
        rng = rng * 1664525u + 1013904223u;
        weights[i] = round_bf16(((int)(rng >> 16) - 32768) * .000001f);
    }
    float *x = input.contents;
    for (size_t i = 0; i < rows * in; i++)
        x[i] = sinf(i * .01f);
    double expected = 0;
    for (size_t i = 0; i < in; i++)
        expected += (double)x[i] * expand_bf16(weights[i]);
    if (termite_metal_decode_runtime_prepare_linear_bf16(r, 0, (uint8_t *)weights, in * out * 2, bias, in,
                                                         out))
        return 2;
    free(weights);
    free(bias);
    double gpu[2] = {0}, wall[2] = {0};
    for (size_t trial = 0; trial < 30; trial++)
        for (size_t mode = 0; mode < 2; mode++) {
            if (mode == 0)
                setenv(rollback, "1", 1);
            else
                unsetenv(rollback);
            uint64_t start = clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
            if (termite_metal_decode_runtime_begin_frame(r) ||
                (rows == 1 ? termite_metal_decode_runtime_apply_linear_device(r, 0, (__bridge void *)input, 0, in, out, (__bridge void *)output, 0)
                           : termite_metal_decode_runtime_apply_linear_multi_row_device(r, 0, (__bridge void *)input, 0, rows, in, out, (__bridge void *)output, 0)) ||
                termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r))
                return 3;
            if (fabs(((float *)output.contents)[0] - expected) > 2e-4)
                return 4;
            if (trial >= 10) {
                gpu[mode] += r->last_frame_gpu_nanos;
                wall[mode] += clock_gettime_nsec_np(CLOCK_UPTIME_RAW) - start;
            }
        }
    for (size_t mode = 0; mode < 2; mode++)
        fprintf(stderr, "small_projection_benchmark rows=%zu in=%zu out=%zu mode=%s mean_us=%.2f gpu_mean_us=%.2f\n",
                rows, in, out, mode ? "production" : "rollback", wall[mode] / 20000, gpu[mode] / 20000);
    unsetenv(rollback);
    return 0;
}

// The OCR specialization packs Q/probabilities to F16 for SIMD matrix math;
// compare F32 outputs with the scalar route and independent CPU softmax samples.
static int ocr_gqa_check(termite_metal_decode_runtime *r, size_t n, size_t heads, size_t kv_heads,
                         float amplitude, bool benchmark) {
    const size_t dim = 256, page = 128, physical = ((n + page - 1) / page) * page, width = heads * dim;
    id<MTLBuffer> q = [r->device newBufferWithLength:(width + 8) * 4 options:MTLResourceStorageModeShared],
                  k = [r->device newBufferWithLength:physical * kv_heads * dim * 2
                                             options:MTLResourceStorageModeShared],
                  v = [r->device newBufferWithLength:physical * kv_heads * dim * 2
                                             options:MTLResourceStorageModeShared],
                  blocks = [r->device newBufferWithLength:(physical / page) * 4
                                                  options:MTLResourceStorageModeShared],
                  y = [r->device newBufferWithLength:(width + 8) * 4 options:MTLResourceStorageModeShared];
    float *gold = malloc(width * 4);
    if (!q || !k || !v || !blocks || !y || !gold)
        return 1;
    float *qs = (float *)q.contents + 4;
    __fp16 *ks = k.contents, *vs = v.contents;
    uint32_t *table = blocks.contents, rng = 123;
    for (size_t i = 0; i < width; i++) {
        rng = rng * 1664525u + 1013904223u;
        qs[i] = ((int)(rng >> 16) - 32768) * .00002f * amplitude;
    }
    for (size_t i = 0; i < physical * kv_heads * dim; i++) {
        rng = rng * 1664525u + 1013904223u;
        ks[i] = ((int)(rng >> 16) - 32768) * .00002f;
        rng = rng * 1664525u + 1013904223u;
        vs[i] = ((int)(rng >> 16) - 32768) * .00002f;
    }
    for (size_t i = 0; i < physical / page; i++)
        table[i] = (uint32_t)((physical / page - 1 - i) * page);
    double expected[4];
    for (size_t hi = 0; hi < 2; hi++)
        for (size_t di = 0; di < 2; di++) {
            const size_t h = hi ? (heads - 1) : 0, d = di ? 255 : 0, kh = h / (heads / kv_heads);
            double best = -INFINITY, denom = 0, numerator = 0;
            for (size_t t = 0; t < n; t++) {
                size_t phys = table[t / page] + t % page;
                double score = 0;
                for (size_t j = 0; j < dim; j++)
                    score += (double)qs[h * dim + j] * ks[(phys * kv_heads + kh) * dim + j];
                score *= .0625;
                double next = fmax(best, score), correction = exp(best - next), weight = exp(score - next);
                denom = denom * correction + weight;
                numerator = numerator * correction + weight * vs[(phys * kv_heads + kh) * dim + d];
                best = next;
            }
            expected[hi * 2 + di] = numerator / denom;
        }
    termite_metal_paged_attention_params p = {.q_len = 1,
                                              .kv_tokens = (uint32_t)n,
                                              .num_heads = (uint32_t)heads,
                                              .num_kv_heads = (uint32_t)kv_heads,
                                              .head_dim = 256,
                                              .key_row_bytes = (uint32_t)(kv_heads * dim * 2),
                                              .base_key_row_bytes = (uint32_t)(kv_heads * dim * 2),
                                              .query_position_offset = (uint32_t)n - 1,
                                              .kv_position_offset = 0,
                                              .sliding_window = 0,
                                              .v_row_stride = (uint32_t)(kv_heads * dim),
                                              .page_size = 128,
                                              .block_count = (uint32_t)(physical / page),
                                              .format = 3,
                                              .v_element_bytes = 2};
    double gpu[2] = {0}, wall[2] = {0}, cpu_error = 0;
    float worst = 0;
    const size_t trials = benchmark ? 30 : 1;
    for (size_t trial = 0; trial < trials; trial++)
        for (size_t mode = 0; mode < (benchmark ? 2 : 4); mode++) {
            if (mode == 0)
                setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_GQA_SPLIT", "1", 1);
            else
                unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_GQA_SPLIT");
            id<MTLComputePipelineState> saved = r->lightonocr3_gqa_split_stage_pipeline;
            if (mode == 2)
                r->lightonocr3_gqa_split_stage_pipeline = nil;
            if (mode == 3)
                termite_metal_decode_runtime_set_qwen35_ocr_vector_loads(r, 0);
            uint32_t *raw = y.contents;
            for (size_t i = 0; i < 4; i++)
                raw[i] = raw[width + 4 + i] = 0xdeadbeefu;
            uint64_t begin = clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
            if (termite_metal_decode_runtime_begin_frame(r))
                return 2;
            termite_metal_decode_gqa_split_launch launch;
            id<MTLBuffer> scratch = nil;
            bool strict = false;
            const bool split = termite_metal_decode_gqa_split_eligible(
                r, r, 3, 1, n, 16, heads, kv_heads, 256, kv_heads * dim * 2, kv_heads * dim, page, 0, 0, NULL,
                &scratch, &launch, &strict);
            const bool expected_split = mode == 1 && n >= 192 && n <= 2048;
            if (split != expected_split || strict)
                return 3;
            id<MTLComputeCommandEncoder> enc = termite_metal_tracked_compute_command_encoder_for(
                r->active_frame_cb, TERMITE_METAL_COMPUTE_SOURCE_ATTENTION);
            [enc setBuffer:q offset:16 atIndex:0];
            [enc setBuffer:k offset:0 atIndex:1];
            [enc setBuffer:v offset:0 atIndex:2];
            [enc setBuffer:blocks offset:0 atIndex:3];
            [enc setBuffer:q offset:0 atIndex:4];
            if (split) {
                if (termite_metal_encode_decode_gqa_split_dispatch(r, r, enc, &p, scratch, &launch, y, 16,
                                                                   width * 4, -1))
                    return 4;
            } else {
                [enc setComputePipelineState:r->attention_paged_1x_pipeline];
                [enc setBuffer:y offset:16 atIndex:5];
                [enc setBytes:&p length:sizeof(p) atIndex:6];
                [enc setThreadgroupMemoryLength:(2 * n + 256) * 4 atIndex:0];
                [enc dispatchThreadgroups:MTLSizeMake(1, heads, 1)
                    threadsPerThreadgroup:MTLSizeMake(256, 1, 1)];
            }
            [enc endEncoding];
            if (termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r))
                return 5;
            r->lightonocr3_gqa_split_stage_pipeline = saved;
            if (mode == 3)
                termite_metal_decode_runtime_set_qwen35_ocr_vector_loads(r, 1);

            if (benchmark && trial >= 10) {
                gpu[mode] += r->last_frame_gpu_nanos;
                wall[mode] += clock_gettime_nsec_np(CLOCK_UPTIME_RAW) - begin;
            }
            float *got = (float *)y.contents + 4;
            if (mode == 0)
                memcpy(gold, got, width * 4);
            for (size_t i = 0; i < width; i++) {
                if (!isfinite(got[i]) || !isfinite(gold[i]))
                    return 6;
                worst = fmaxf(worst, fabsf(got[i] - gold[i]));
            }
            for (size_t hi = 0; hi < 2; hi++)
                for (size_t di = 0; di < 2; di++)
                    cpu_error = fmax(cpu_error, fabs(expected[hi * 2 + di] -
                                                     got[(hi ? (heads - 1) : 0) * dim + (di ? 255 : 0)]));
            for (size_t i = 0; i < 4; i++)
                if (raw[i] != 0xdeadbeefu || raw[width + 4 + i] != 0xdeadbeefu)
                    return 7;
        }
    fprintf(stderr,
            "ocr_gqa_check kv=%zu heads=%zu kv_heads=%zu amplitude=%g offset=16 max_abs=%g cpu_max_abs=%g\n",
            n, heads, kv_heads, amplitude, worst, cpu_error);
    if (benchmark)
        for (size_t mode = 0; mode < 2; mode++)
            fprintf(stderr,
                    "ocr_gqa_benchmark kv=%zu heads=%zu kv_heads=%zu mode=%s gpu_us=%.2f wall_us=%.2f\n", n,
                    heads, kv_heads, mode ? "split" : "rollback", gpu[mode] / 20000, wall[mode] / 20000);
    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_GQA_SPLIT");
    free(gold);
    return worst > 2e-4f || cpu_error > 2e-4 ? 8 : 0;
}

// Verify the frame-owned temporary survives interleaved projection/attention,
// and that frame N+1 never borrows frame N's submitted scratch slot.
static int down_split_frame_check(termite_metal_decode_runtime *r, size_t in, size_t out, size_t rows, bool coalesced) {
    if (coalesced) setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING","1",1);
    const size_t count = rows * out;
    uint16_t *weights = malloc(in * out * 2);
    float *bias = calloc(out, 4), *gold = malloc(count * 4);
    id<MTLBuffer> x = [r->device newBufferWithLength:(rows * in + 4) * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> y[3];
    for (size_t j = 0; j < 3; j++) {
        y[j] = [r->device newBufferWithLength:(count + 12) * 4 options:MTLResourceStorageModeShared];
        uint32_t *raw = y[j].contents;
        for (size_t i = 0; i < count + 12; i++) raw[i] = 0xdeadbeefu;
    }
    if (!weights || !bias || !gold || !x || !y[0] || !y[1] || !y[2]) return 1;
    uint32_t rng = 123;
    for (size_t i = 0; i < in * out; i++) {
        rng = rng * 1664525u + 1013904223u;
        weights[i] = round_bf16(((int)(rng >> 16) - 32768) * .000001f);
    }
    for (size_t i = 0; i < rows * in; i++) ((float *)x.contents)[i + 4] = sinf(i * .01f);
    if (termite_metal_decode_runtime_prepare_linear_bf16(r, 0, (uint8_t *)weights, in * out * 2, bias, in, out)) return 2;
    free(weights); free(bias);
    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_DOWN_SPLIT", "1", 1);
    if (termite_metal_decode_runtime_apply_linear_multi_row_device(r, 0, (__bridge void *)x, 16, rows, in, out, (__bridge void *)y[0], 16)) return 3;
    memcpy(gold, (float *)y[0].contents + 4, count * 4);
    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_DOWN_SPLIT");
    // Constant attention has an independent exact result, while using the
    // same physical split scratch between two down-projection dispatches.
    id<MTLBuffer> q = [r->device newBufferWithLength:8 * 256 * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> k = [r->device newBufferWithLength:256 * 2 * 256 * 2 options:MTLResourceStorageModeShared];
    id<MTLBuffer> v = [r->device newBufferWithLength:k.length options:MTLResourceStorageModeShared];
    id<MTLBuffer> blocks = [r->device newBufferWithLength:4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> attn = [r->device newBufferWithLength:q.length options:MTLResourceStorageModeShared];
    memset(q.contents, 0, q.length); memset(k.contents, 0, k.length); *(uint32_t *)blocks.contents = 0;
    for (size_t i = 0; i < v.length / 2; i++) ((uint16_t *)v.contents)[i] = 0x3d00;
    termite_metal_paged_attention_params p = {.q_len=1,.kv_tokens=192,.num_heads=8,.num_kv_heads=2,.head_dim=256,
        .key_row_bytes=1024,.base_key_row_bytes=1024,.query_position_offset=191,.v_row_stride=512,
        .page_size=256,.block_count=1,.format=3,.v_element_bytes=2};
    // Produce the input in a planned encoder, then consume it in a split
    // encoder. This catches real reader dispatch and transition ordering.
    id<MTLBuffer> source = [r->device newBufferWithBytes:x.contents length:x.length options:MTLResourceStorageModeShared];
    id<MTLBuffer> zero = [r->device newBufferWithLength:x.length options:MTLResourceStorageModeShared];
    if (!source || !zero) return 40;
    memset(zero.contents, 0, zero.length);
    memset(x.contents, 0, x.length);
    if ((termite_metal_decode_runtime_begin_frame(r) || (coalesced && termite_metal_decode_runtime_set_active_frame_regime(r, TERMITE_METAL_FRAME_REGIME_DECODE)))) return 4;
    if (termite_metal_decode_runtime_begin_planned_compute_scope(r, TERMITE_METAL_COMPUTE_SOURCE_FFN, TERMITE_METAL_COMPUTE_REGION_FFN)) return 41;
    if (termite_metal_decode_runtime_apply_add_device(r, (__bridge void *)source, 16, (__bridge void *)zero, 16, rows * in, (__bridge void *)x, 16)) return 42;
    if (r->active_planned_compute_encoder == nil) return 43;
    if (termite_metal_try_lightonocr3_down_split(r, 0, x, 16, rows, in, out, y[0], 16) != 1) return 5;
    if ((r->active_planned_compute_encoder != nil) != coalesced || termite_metal_decode_runtime_end_planned_compute_scope(r)) return 44;
    termite_metal_decode_gqa_split_launch launch;
    id<MTLBuffer> scratch = nil; bool strict = false;
    if (!termite_metal_decode_gqa_split_eligible(r,r,3,1,192,0,8,2,256,1024,512,256,0,0,NULL,&scratch,&launch,&strict) || strict) return 6;
    id<MTLComputeCommandEncoder> enc = termite_metal_tracked_compute_command_encoder_for(r->active_frame_cb, TERMITE_METAL_COMPUTE_SOURCE_ATTENTION);
    [enc setBuffer:q offset:0 atIndex:0]; [enc setBuffer:k offset:0 atIndex:1]; [enc setBuffer:v offset:0 atIndex:2];
    [enc setBuffer:blocks offset:0 atIndex:3]; [enc setBuffer:q offset:0 atIndex:4];
    if (termite_metal_encode_decode_gqa_split_dispatch(r,r,enc,&p,scratch,&launch,attn,0,attn.length,-1)) return 7;
    [enc endEncoding];
    if (termite_metal_try_lightonocr3_down_split(r,0,x,16,rows,in,out,y[1],16) != 1) return 8;
    if (termite_metal_decode_runtime_submit_frame(r)) return 9;
    id<MTLBuffer> submitted_scratch = r->submitted_frame_decode_gqa_split_scratch_slot == 0
        ? r->attention_decode_gqa_split_scratch_buffer : r->attention_decode_gqa_split_scratch_buffer_alt;
    if ((termite_metal_decode_runtime_begin_frame(r) || (coalesced && termite_metal_decode_runtime_set_active_frame_regime(r, TERMITE_METAL_FRAME_REGIME_DECODE)))) return 10;
    if (termite_metal_decode_gqa_split_scratch_for_encoding(r) == submitted_scratch) return 11;
    if (termite_metal_try_lightonocr3_down_split(r,0,x,16,rows,in,out,y[2],16) != 1) return 12;
    if (termite_metal_decode_runtime_wait_frame(r) || termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r)) return 13;
    float worst = 0;
    for (size_t j = 0; j < 3; j++) {
        uint32_t *raw = y[j].contents;
        for (size_t i = 0; i < count; i++) {
            const float got = ((float *)y[j].contents)[i + 4];
            if (!isfinite(got)) return 19;
            worst = fmaxf(worst,fabsf(gold[i] - got));
        }
        for (size_t i = 0; i < 4; i++) if (raw[i] != 0xdeadbeefu) return 14;
        for (size_t i = count + 4; i < count + 12; i++) if (raw[i] != 0xdeadbeefu) return 15;
    }
    for (size_t i = 0; i < attn.length / 4; i++) if (!isfinite(((float *)attn.contents)[i]) || fabsf(((float *)attn.contents)[i] - 1.25f) > 1e-5f) return 16;
    if(in==3584 && out==1024 && rows>=5 && rows<=7) {
        const size_t prior_bytes=8*rows*out*4;
        const size_t saved_capacity=r->attention_decode_gqa_split_scratch_capacity;
        id<MTLBuffer> saved_base=r->attention_decode_gqa_split_scratch_buffer;
        id<MTLBuffer> saved_alt=r->attention_decode_gqa_split_scratch_buffer_alt;
        id<MTLBuffer> bounded=[r->device newBufferWithLength:prior_bytes options:MTLResourceStorageModePrivate];
        if(!bounded)return 21;
        for(size_t mode=0;mode<3;mode++) {
            if((termite_metal_decode_runtime_begin_frame(r) || (coalesced && termite_metal_decode_runtime_set_active_frame_regime(r, TERMITE_METAL_FRAME_REGIME_DECODE))))return 22;
            if(mode==0)r->attention_decode_gqa_split_scratch_capacity=prior_bytes;
            if(mode==1){r->attention_decode_gqa_split_scratch_buffer=bounded;r->attention_decode_gqa_split_scratch_buffer_alt=bounded;}
            if(mode==2)setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PARTIAL_DOWN_SPLIT","1",1);
            if(termite_metal_try_lightonocr3_down_split(r,0,x,16,rows,in,out,y[0],16)!=1)return 23;
            if(termite_metal_decode_runtime_submit_frame(r)||termite_metal_decode_runtime_wait_frame(r))return 24;
            r->attention_decode_gqa_split_scratch_capacity=saved_capacity;
            r->attention_decode_gqa_split_scratch_buffer=saved_base;
            r->attention_decode_gqa_split_scratch_buffer_alt=saved_alt;
            unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PARTIAL_DOWN_SPLIT");
            for(size_t i=0;i<count;i++)if(!isfinite(((float*)y[0].contents)[i+4])||fabsf(((float*)y[0].contents)[i+4]-gold[i])>2e-4f)return 25;
        }
    }
    if(in==1024 && out==6144 && rows>=5 && rows<=7) {
        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PARTIAL_QKV_SPLIT","1",1);
        if(termite_metal_try_lightonocr3_down_split(r,0,x,16,rows,in,out,y[0],16)!=0)return 26;
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PARTIAL_QKV_SPLIT");
    }
    // Every optional dependency must decline before encoding any GPU work.
    id<MTLComputePipelineState> saved_stage = r->lightonocr3_down_split32_pipeline;
    id<MTLComputePipelineState> saved_stage64 = r->lightonocr3_down_split64_pipeline;
    id<MTLComputePipelineState> saved_reduce = r->lightonocr3_down_split_reduce_pipeline;
    id<MTLBuffer> saved_scratch = r->attention_decode_gqa_split_scratch_buffer;
    for (size_t mode = 0; mode < 4; mode++) {
        if (mode == 0) {r->lightonocr3_down_split32_pipeline = nil; r->lightonocr3_down_split64_pipeline = nil;}
        if (mode == 1) r->lightonocr3_down_split_reduce_pipeline = nil;
        if (mode == 2) r->attention_decode_gqa_split_scratch_buffer = nil;
        if (mode == 3) setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_DOWN_SPLIT","1",1);
        if (termite_metal_try_lightonocr3_down_split(r,0,x,16,rows,in,out,y[0],16) != 0) return 17;
        r->lightonocr3_down_split32_pipeline = saved_stage; r->lightonocr3_down_split64_pipeline = saved_stage64;
        r->lightonocr3_down_split_reduce_pipeline = saved_reduce; r->attention_decode_gqa_split_scratch_buffer = saved_scratch;
    }
    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_DOWN_SPLIT");
    // The tail rollback must retain a numerically correct public fallback.
    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_TAIL_SPLIT", "1", 1);
    int rollback_rc = termite_metal_decode_runtime_apply_linear_multi_row_device(r, 0, (__bridge void *)x, 16, rows, in, out, (__bridge void *)y[0], 16);
    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_TAIL_SPLIT");
    if (rollback_rc) return 45;
    for (size_t i = 0; i < count; i++) {
        float got = ((float *)y[0].contents)[i + 4];
        if (!isfinite(got) || fabsf(got - gold[i]) > 2e-4f) return 46;
    }
    fprintf(stderr,"down_split_frame_check in=%zu out=%zu planned_producer_transition=pass partial_scratch_interleaving=pass overlapping_frames=pass max_abs=%g\n",in,out,worst);
    if (coalesced) unsetenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING");
    free(gold);
    return worst > 2e-4f ? 18 : 0;
}

// A norm producer opens the decoder scope before registering its accesses.
// The next norm and copy must consume it in that same encoder; prefill and
// the global coalescing rollback retain ordinary encoder ownership.
static int decode_autoscope_check(termite_metal_decode_runtime *r) {
    const size_t rows = 8, dim = 1024, count = rows * dim;
    id<MTLBuffer> x = [r->device newBufferWithLength:(count + 8) * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> w = [r->device newBufferWithLength:dim * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> y = [r->device newBufferWithLength:x.length options:MTLResourceStorageModeShared];
    id<MTLBuffer> z = [r->device newBufferWithLength:x.length options:MTLResourceStorageModeShared];
    id<MTLBuffer> consumer = [r->device newBufferWithLength:count * 4 options:MTLResourceStorageModeShared];
    float *gold = malloc(count * 4);
    if (!x || !w || !y || !z || !consumer || !gold)
        return 1;
    for (size_t i = 0; i < count; i++)
        ((float *)x.contents)[i + 4] = sinf(i * .013f) * .7f;
    for (size_t i = 0; i < dim; i++)
        ((float *)w.contents)[i] = .5f + (i % 7) * .1f;
    const antfly_norm_clamp_params p = {.rows = rows, .dim = dim, .eps = 1e-6f};
    for (size_t mode = 0; mode < 4; mode++) {
        if (mode)
            setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING", "1", 1);
        else
            unsetenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING");
        if (mode == 3)
            setenv("TERMITE_METAL_DISABLE_PLANNED_ENCODER_COALESCING", "1", 1);
        for (size_t i = 0; i < count + 8; i++)
            ((uint32_t *)y.contents)[i] = ((uint32_t *)z.contents)[i] = 0xdeadbeef;
        if (termite_metal_decode_runtime_begin_frame(r) ||
            termite_metal_decode_runtime_set_active_frame_regime(
                r, mode == 1 ? TERMITE_METAL_FRAME_REGIME_PREFILL : TERMITE_METAL_FRAME_REGIME_DECODE))
            return 2;
        if (antfly_metal_norm_clamp(r, (__bridge void *)x, 16, (__bridge void *)w, 0, &p, (__bridge void *)y,
                                    16))
            return 3;
        const bool planned = mode == 2;
        id<MTLComputeCommandEncoder> encoder = r->active_planned_compute_encoder;
        if ((encoder != nil) != planned || (planned && r->active_planned_compute_range_count < 3))
            return 4;
        if (antfly_metal_norm_clamp(r, (__bridge void *)y, 16, (__bridge void *)w, 0, &p, (__bridge void *)z,
                                    16) ||
            termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)z, 16, rows, dim, 0, dim,
                                                   (__bridge void *)consumer, 0))
            return 5;
        if (planned &&
            (r->active_planned_compute_encoder != encoder || r->active_frame_compute_encoder_count != 1))
            return 6;
        if (termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r))
            return 7;
        if (mode == 0)
            memcpy(gold, (char *)z.contents + 16, count * 4);
        if (memcmp(gold, (char *)z.contents + 16, count * 4) || memcmp(gold, consumer.contents, count * 4))
            return 8;
        for (size_t i = 0; i < 4; i++)
            if (((uint32_t *)y.contents)[i] != 0xdeadbeef ||
                ((uint32_t *)y.contents)[count + 4 + i] != 0xdeadbeef ||
                ((uint32_t *)z.contents)[i] != 0xdeadbeef ||
                ((uint32_t *)z.contents)[count + 4 + i] != 0xdeadbeef)
                return 9;
        unsetenv("TERMITE_METAL_DISABLE_PLANNED_ENCODER_COALESCING");
    }
    unsetenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING");
    free(gold);
    fprintf(stderr, "decode_autoscope_check producer_consumer=pass single_encoder=pass "
                    "prefill_unchanged=pass rollback=pass\n");
    return 0;
}
static int gate_split_frame_check(termite_metal_decode_runtime *r, size_t in, size_t out, size_t rows) {
    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_LOCAL_KERNELS", "1", 1);
    const size_t count = rows * out;
    uint16_t *weights = malloc(in * out * 2);
    float *bias = calloc(out, 4), *gold = malloc(count * 4);
    id<MTLBuffer> x = [r->device newBufferWithLength:(rows * in + 4) * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> y[3];
    for (size_t j = 0; j < 3; j++) {
        y[j] = [r->device newBufferWithLength:(count + 12) * 4 options:MTLResourceStorageModeShared];
        uint32_t *raw = y[j].contents;
        for (size_t i = 0; i < count + 12; i++) raw[i] = 0xdeadbeefu;
    }
    if (!weights || !bias || !gold || !x || !y[0] || !y[1] || !y[2]) return 1;
    uint32_t rng = 123;
    for (size_t i = 0; i < in * out; i++) {
        rng = rng * 1664525u + 1013904223u;
        weights[i] = round_bf16(((int)(rng >> 16) - 32768) * .000001f);
    }
    for (size_t i = 0; i < rows * in; i++) ((float *)x.contents)[i + 4] = sinf(i * .01f);
    if (termite_metal_decode_runtime_prepare_linear_bf16(r, 0, (uint8_t *)weights, in * out * 2, bias, in, out)) return 2;
    for(size_t i=0;i<in*out;i++) weights[i]=round_bf16(cosf(i*.023f)*.03f);
    for(size_t i=0;i<out;i++) bias[i]=sinf(i*.01f)*.1f;
    if(termite_metal_decode_runtime_prepare_linear_bf16(r,1,(uint8_t*)weights,in*out*2,bias,in,out))return 2;
    free(weights); free(bias);
    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_GATE_UP_SPLIT", "1", 1);
    if(termite_metal_decode_runtime_can_apply_bf16_gate_up(r,0,1,rows,in,out)) {
    if (termite_metal_decode_runtime_apply_bf16_gate_up_device(r, 0, 1, (__bridge void *)x, 16, rows, in, out, (__bridge void *)y[0], 16)) return 3;
    } else {
        id<MTLBuffer> gate=[r->device newBufferWithLength:count*4 options:MTLResourceStorageModePrivate];
        id<MTLBuffer> up=[r->device newBufferWithLength:count*4 options:MTLResourceStorageModePrivate];
        if(!gate||!up||termite_metal_decode_runtime_apply_linear_multi_row_device(r,0,(__bridge void*)x,16,rows,in,out,(__bridge void*)gate,0)||
            termite_metal_decode_runtime_apply_linear_multi_row_device(r,1,(__bridge void*)x,16,rows,in,out,(__bridge void*)up,0)||
            termite_metal_decode_runtime_apply_activation_multiply_device(r,2,(__bridge void*)gate,0,(__bridge void*)up,0,rows,out,(__bridge void*)y[0],16))return 3;
    }
    memcpy(gold, (float *)y[0].contents + 4, count * 4);
    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_GATE_UP_SPLIT");
    // Constant attention has an independent exact result, while using the
    // same physical split scratch between two down-projection dispatches.
    id<MTLBuffer> q = [r->device newBufferWithLength:8 * 256 * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> k = [r->device newBufferWithLength:256 * 2 * 256 * 2 options:MTLResourceStorageModeShared];
    id<MTLBuffer> v = [r->device newBufferWithLength:k.length options:MTLResourceStorageModeShared];
    id<MTLBuffer> blocks = [r->device newBufferWithLength:4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> attn = [r->device newBufferWithLength:q.length options:MTLResourceStorageModeShared];
    memset(q.contents, 0, q.length); memset(k.contents, 0, k.length); *(uint32_t *)blocks.contents = 0;
    for (size_t i = 0; i < v.length / 2; i++) ((uint16_t *)v.contents)[i] = 0x3d00;
    termite_metal_paged_attention_params p = {.q_len=1,.kv_tokens=192,.num_heads=8,.num_kv_heads=2,.head_dim=256,
        .key_row_bytes=1024,.base_key_row_bytes=1024,.query_position_offset=191,.v_row_stride=512,
        .page_size=256,.block_count=1,.format=3,.v_element_bytes=2};
    // Produce the input in a planned encoder, then consume it in a split
    // encoder. This catches real reader dispatch and transition ordering.
    id<MTLBuffer> source = [r->device newBufferWithBytes:x.contents length:x.length options:MTLResourceStorageModeShared];
    id<MTLBuffer> zero = [r->device newBufferWithLength:x.length options:MTLResourceStorageModeShared];
    if (!source || !zero) return 40;
    memset(zero.contents, 0, zero.length);
    memset(x.contents, 0, x.length);
    if (termite_metal_decode_runtime_begin_frame(r)) return 4;
    if (termite_metal_decode_runtime_begin_planned_compute_scope(r, TERMITE_METAL_COMPUTE_SOURCE_FFN, TERMITE_METAL_COMPUTE_REGION_FFN)) return 41;
    if (termite_metal_decode_runtime_apply_add_device(r, (__bridge void *)source, 16, (__bridge void *)zero, 16, rows * in, (__bridge void *)x, 16)) return 42;
    if (r->active_planned_compute_encoder == nil) return 43;
    if (termite_metal_try_lightonocr3_gate_split(r, 0, 1, x, 16, rows, in, out, y[0], 16) != 1) return 5;
    if (r->active_planned_compute_encoder != nil || termite_metal_decode_runtime_end_planned_compute_scope(r)) return 44;
    termite_metal_decode_gqa_split_launch launch;
    id<MTLBuffer> scratch = nil; bool strict = false;
    if (!termite_metal_decode_gqa_split_eligible(r,r,3,1,192,0,8,2,256,1024,512,256,0,0,NULL,&scratch,&launch,&strict) || strict) return 6;
    id<MTLComputeCommandEncoder> enc = termite_metal_tracked_compute_command_encoder_for(r->active_frame_cb, TERMITE_METAL_COMPUTE_SOURCE_ATTENTION);
    [enc setBuffer:q offset:0 atIndex:0]; [enc setBuffer:k offset:0 atIndex:1]; [enc setBuffer:v offset:0 atIndex:2];
    [enc setBuffer:blocks offset:0 atIndex:3]; [enc setBuffer:q offset:0 atIndex:4];
    if (termite_metal_encode_decode_gqa_split_dispatch(r,r,enc,&p,scratch,&launch,attn,0,attn.length,-1)) return 7;
    [enc endEncoding];
    if (termite_metal_try_lightonocr3_gate_split(r,0,1,x,16,rows,in,out,y[1],16) != 1) return 8;
    if (termite_metal_decode_runtime_submit_frame(r)) return 9;
    id<MTLBuffer> submitted_scratch = r->submitted_frame_decode_gqa_split_scratch_slot == 0
        ? r->attention_decode_gqa_split_scratch_buffer : r->attention_decode_gqa_split_scratch_buffer_alt;
    if (termite_metal_decode_runtime_begin_frame(r)) return 10;
    if (termite_metal_decode_gqa_split_scratch_for_encoding(r) == submitted_scratch) return 11;
    if (termite_metal_try_lightonocr3_gate_split(r,0,1,x,16,rows,in,out,y[2],16) != 1) return 12;
    if (termite_metal_decode_runtime_wait_frame(r) || termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r)) return 13;
    float worst = 0;
    for (size_t j = 0; j < 3; j++) {
        uint32_t *raw = y[j].contents;
        for (size_t i = 0; i < count; i++) {
            const float got = ((float *)y[j].contents)[i + 4];
            if (!isfinite(got)) return 19;
            worst = fmaxf(worst,fabsf(gold[i] - got));
        }
        for (size_t i = 0; i < 4; i++) if (raw[i] != 0xdeadbeefu) return 14;
        for (size_t i = count + 4; i < count + 12; i++) if (raw[i] != 0xdeadbeefu) return 15;
    }
    for (size_t i = 0; i < attn.length / 4; i++) if (!isfinite(((float *)attn.contents)[i]) || fabsf(((float *)attn.contents)[i] - 1.25f) > 1e-5f) return 16;
    // Dependency failures decline while a valid frame is being encoded.
    if(termite_metal_decode_runtime_begin_frame(r))return 17;
    id<MTLComputePipelineState> saved_stage=r->lightonocr3_gate_up_split16_pipeline;
    id<MTLComputePipelineState> saved_reduce=r->lightonocr3_gate_up_split_reduce_pipeline;
    size_t capacity=r->attention_decode_gqa_split_scratch_capacity;
    id<MTLBuffer> saved_scratch=r->attention_decode_gqa_split_scratch_buffer;
    id<MTLBuffer> saved_alt=r->attention_decode_gqa_split_scratch_buffer_alt;
    for(size_t mode=0;mode<6;mode++) {
        if(mode==0)r->lightonocr3_gate_up_split16_pipeline=nil;
        if(mode==1)r->lightonocr3_gate_up_split_reduce_pipeline=nil;
        if(mode==2)r->attention_decode_gqa_split_scratch_capacity=1;
        if(mode==3)setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_GATE_UP_SPLIT","1",1);
        if(mode==4){r->attention_decode_gqa_split_scratch_buffer=x;r->attention_decode_gqa_split_scratch_buffer_alt=x;}
        if(mode==5){r->attention_decode_gqa_split_scratch_buffer=y[0];r->attention_decode_gqa_split_scratch_buffer_alt=y[0];}
        if(termite_metal_try_lightonocr3_gate_split(r,0,1,x,16,rows,in,out,y[0],16)!=0)return 18;
        r->lightonocr3_gate_up_split16_pipeline=saved_stage;
        r->lightonocr3_gate_up_split_reduce_pipeline=saved_reduce;
        r->attention_decode_gqa_split_scratch_capacity=capacity;
        r->attention_decode_gqa_split_scratch_buffer=saved_scratch;
        r->attention_decode_gqa_split_scratch_buffer_alt=saved_alt;
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_GATE_UP_SPLIT");
    }
    if(termite_metal_decode_runtime_submit_frame(r)||termite_metal_decode_runtime_wait_frame(r))return 20;
    if(termite_metal_try_lightonocr3_gate_split(r,0,1,x,16,rows,in,out,y[0],16)!=0)return 21;
    fprintf(stderr,"gate_split_frame_check in=%zu out=%zu planned_producer_transition=pass partial_scratch_interleaving=pass overlapping_frames=pass max_abs=%g\n",in,out,worst);
    free(gold);
    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_LOCAL_KERNELS");
    return worst > 2e-4f ? 18 : 0;
}

static int prefill_conv_check(termite_metal_decode_runtime *r, size_t seq, size_t value_heads, bool initialized, bool benchmark) {
    const size_t channels = (32 + value_heads) * 128, count = seq * channels;
    id<MTLBuffer> x = [r->device newBufferWithLength:(count + 4) * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> w = [r->device newBufferWithLength:(channels * 4 + 4) * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> h = [r->device newBufferWithLength:w.length options:MTLResourceStorageModeShared];
    id<MTLBuffer> y = [r->device newBufferWithLength:(count + 12) * 4 options:MTLResourceStorageModeShared];
    float *gold = malloc(count * 4);
    if (!x || !w || !h || !y || !gold) return 1;
    float *xs = (float *)x.contents + 4, *ws = (float *)w.contents + 4, *hs = (float *)h.contents + 4;
    for (size_t i = 0; i < count; i++) xs[i] = sinf(i * .01f);
    for (size_t i = 0; i < channels * 4; i++) {ws[i] = cosf(i * .01f) * .1f; hs[i] = sinf(i * .03f);}
    size_t offsets[15] = {0}; offsets[0] = offsets[1] = offsets[6] = offsets[9] = 16;
    uint32_t p[6] = {(uint32_t)seq,16,(uint32_t)value_heads,128,4,initialized};
    double gpu[2] = {0};
    for (size_t trial = 0; trial < (benchmark ? 30 : 1); trial++)
        for (size_t mode = 0; mode < 2; mode++) {
            if (mode == 0) setenv("TERMITE_METAL_DISABLE_QWEN35_PREFILL_CONV_VEC4","1",1);
            else unsetenv("TERMITE_METAL_DISABLE_QWEN35_PREFILL_CONV_VEC4");
            const bool vector = termite_metal_qwen35_prefill_conv_vec4_available(r,seq,128,4,offsets);
            if (vector != (mode == 1)) return 2;
            uint32_t *raw = y.contents;
            for (size_t i = 0; i < 4; i++) raw[i] = 0xdeadbeefu;
            for (size_t i = count + 4; i < count + 12; i++) raw[i] = 0xcafebabeu;
            if (termite_metal_decode_runtime_begin_frame(r)) return 3;
            id<MTLComputePipelineState> kernel = vector ? r->qwen35_prefill_conv_vec4_pipeline : r->qwen35_conv_pipeline;
            id<MTLComputeCommandEncoder> enc = termite_metal_tracked_compute_command_encoder_for(r->active_frame_cb,TERMITE_METAL_COMPUTE_SOURCE_OTHER);
            [enc setComputePipelineState:kernel]; [enc setBuffer:x offset:16 atIndex:0]; [enc setBuffer:w offset:16 atIndex:1];
            [enc setBuffer:h offset:16 atIndex:6]; [enc setBuffer:y offset:16 atIndex:9]; [enc setBytes:p length:sizeof(p) atIndex:15];
            [enc dispatchThreads:MTLSizeMake(count / (vector ? 4 : 1),1,1) threadsPerThreadgroup:MTLSizeMake(256,1,1)]; [enc endEncoding];
            if (termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r)) return 4;
            if (benchmark && trial >= 10) gpu[mode] += r->last_frame_gpu_nanos;
            float *got = (float *)y.contents + 4;
            if (mode == 0) memcpy(gold,got,count * 4);
            else if (memcmp(gold,got,count * 4) != 0) return 5;
            for (size_t i = 0; i < 4; i++) if (raw[i] != 0xdeadbeefu) return 6;
            for (size_t i = count + 4; i < count + 12; i++) if (raw[i] != 0xcafebabeu) return 7;
            for (size_t t = 0; t < seq; t += seq - 1)
                for (size_t c = 0; c < channels; c += channels - 1) {
                    double sum = 0;
                    for (size_t k = 0; k < 4; k++) {
                        const int src = (int)t + (int)k - 3;
                        const float value = src >= 0 ? xs[(size_t)src * channels + c] : initialized ? hs[c * 4 + (size_t)(4 + src)] : 0;
                        sum += (double)value * ws[c * 4 + k];
                    }
                    if (fabs(got[t * channels + c] - sum / (1 + exp(-sum))) > 2e-6) return 8;
                }
        }
    id<MTLComputePipelineState> saved = r->qwen35_prefill_conv_vec4_pipeline;
    r->qwen35_prefill_conv_vec4_pipeline = nil;
    if (termite_metal_qwen35_prefill_conv_vec4_available(r,seq,128,4,offsets)) return 9;
    r->qwen35_prefill_conv_vec4_pipeline = saved;
    offsets[0] = 4;
    if (termite_metal_qwen35_prefill_conv_vec4_available(r,seq,128,4,offsets)) return 10;
    offsets[0] = 16;
    if (termite_metal_qwen35_prefill_conv_vec4_available(r,1,128,4,offsets) || termite_metal_qwen35_prefill_conv_vec4_available(r,seq,64,4,offsets)) return 11;
    fprintf(stderr,"prefill_conv_check seq=%zu value_heads=%zu initialized=%d offset=16 bit_exact=pass cpu_samples=pass\n",seq,value_heads,initialized);
    if (benchmark) fprintf(stderr,"prefill_conv_benchmark seq=%zu value_heads=%zu rollback_gpu_us=%.2f vector_gpu_us=%.2f\n",seq,value_heads,gpu[0]/20000,gpu[1]/20000);
    free(gold);
    return 0;
}

static float expand_f16(uint16_t bits) { __fp16 value; memcpy(&value,&bits,2); return (float)value; }
static int f16_encoder_check(termite_metal_decode_runtime *r,size_t rows,size_t in,size_t out,size_t offset,bool benchmark) {
    uint16_t *weights=malloc(in*out*2); float *bias=malloc(out*4),*gold=malloc(rows*out*4);
    id<MTLBuffer>x=[r->device newBufferWithLength:rows*in*4+offset options:MTLResourceStorageModeShared];
    id<MTLBuffer>y=[r->device newBufferWithLength:(rows*out+12)*4 options:MTLResourceStorageModeShared];
    if(!weights||!bias||!gold||!x||!y)return 1;
    uint32_t rng=123;for(size_t i=0;i<in*out;i++){rng=rng*1664525u+1013904223u;__fp16 value=(__fp16)(((int)(rng>>16)-32768)*.000001f);memcpy(weights+i,&value,2);}
    float*xs=(float*)((uint8_t*)x.contents+offset);
    for(size_t i=0;i<rows*in;i++)xs[i]=sinf(i*.01f);
    for(size_t i=0;i<out;i++)bias[i]=cosf(i*.01f)*.1f;
    double expected[4];for(size_t a=0;a<2;a++)for(size_t b=0;b<2;b++){size_t row=a?rows-1:0,col=b?out-1:0;double sum=bias[col];for(size_t k=0;k<in;k++)sum+=(double)xs[row*in+k]*expand_f16(weights[col*in+k]);expected[a*2+b]=sum;}
    if(termite_metal_decode_runtime_prepare_linear_f16(r,0,(uint8_t*)weights,in*out*2,bias,in,out))return 2;
    free(weights);free(bias);
    id<MTLComputePipelineState>saved=r->f16_encoder_mpp_pipeline;double gpu[2]={0};float worst=0;double cpu_error=0;
    for(size_t trial=0;trial<(benchmark?30:1);trial++)for(size_t mode=0;mode<(benchmark?2:3);mode++){
        if(mode==0)setenv("TERMITE_METAL_DISABLE_F16_ENCODER_MPP","1",1);else unsetenv("TERMITE_METAL_DISABLE_F16_ENCODER_MPP");
        if(mode==2)r->f16_encoder_mpp_pipeline=nil;
        uint32_t*raw=y.contents;for(size_t i=0;i<4;i++)raw[i]=0xdeadbeefu;for(size_t i=rows*out+4;i<rows*out+12;i++)raw[i]=0xcafebabeu;
        if(termite_metal_decode_runtime_begin_frame(r))return 3;
        const uint64_t mps_before=r->mps_dense_linear_active_frame_calls;
        if(termite_metal_decode_runtime_apply_linear_multi_row_device(r,0,(__bridge void*)x,offset,rows,in,out,(__bridge void*)y,16))return 4;
        bool optimized=mode==1&&offset%16==0&&termite_metal_f16_encoder_mpp_shape(rows,in,out);
        if((r->mps_dense_linear_active_frame_calls==mps_before)!=optimized)return 5;
        if(termite_metal_decode_runtime_submit_frame(r)||termite_metal_decode_runtime_wait_frame(r))return 6;
        r->f16_encoder_mpp_pipeline=saved;
        float*got=(float*)y.contents+4;
        if(mode==0)memcpy(gold,got,rows*out*4);
        for(size_t i=0;i<rows*out;i++){if(!isfinite(got[i]))return 7;worst=fmaxf(worst,fabsf(got[i]-gold[i]));}
        for(size_t a=0;a<2;a++)for(size_t b=0;b<2;b++)cpu_error=fmax(cpu_error,fabs(got[(a?rows-1:0)*out+(b?out-1:0)]-expected[a*2+b]));
        for(size_t i=0;i<4;i++)if(raw[i]!=0xdeadbeefu)return 8;
        for(size_t i=rows*out+4;i<rows*out+12;i++)if(raw[i]!=0xcafebabeu)return 9;
        if(benchmark&&trial>=10)gpu[mode]+=r->last_frame_gpu_nanos;
    }
    unsetenv("TERMITE_METAL_DISABLE_F16_ENCODER_MPP");
    fprintf(stderr,"f16_encoder_check rows=%zu in=%zu out=%zu offset=%zu max_abs=%g cpu_max_abs=%g\n",rows,in,out,offset,worst,cpu_error);
    if(benchmark)fprintf(stderr,"f16_encoder_benchmark rows=%zu in=%zu out=%zu mps_gpu_us=%.2f tensor_gpu_us=%.2f\n",rows,in,out,gpu[0]/20000,gpu[1]/20000);
    free(gold);return worst>2e-4f||cpu_error>2e-4?10:0;
}

static int ocr_residency_check(termite_metal_decode_runtime *r) {
    const char *previous=getenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_WEIGHT_RESIDENCY");
    char *saved=previous?strdup(previous):NULL;
    setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_WEIGHT_RESIDENCY","1",1);
    uint16_t weights[128*64]={0};float bias[64]={0};
    if(termite_metal_decode_runtime_prepare_linear_bf16(r,0,(uint8_t*)weights,sizeof(weights),bias,128,64))return 1;
    id<MTLBuffer> original=r->linear_weight_buffers[0];
    if(r->ocr_residency_placements.count!=1||r->ocr_residency_total_bytes!=original.length)return 2;
    const size_t bytes=r->ocr_residency_total_bytes;
    r->linear_weight_buffers[1]=original;
    termite_metal_ocr_residency_track(r,1,original);
    NSDictionary *record=[r->ocr_residency_placements objectForKey:original];
    if([record[@"refs"] unsignedLongLongValue]!=2||r->ocr_residency_total_bytes!=bytes)return 3;
    if(termite_metal_decode_runtime_clear_linear_slot(r,0))return 4;
    record=[r->ocr_residency_placements objectForKey:original];
    if([record[@"refs"] unsignedLongLongValue]!=1||r->ocr_residency_total_bytes!=bytes)return 5;
    if(termite_metal_decode_runtime_clear_linear_slot(r,1)||r->ocr_residency_total_bytes!=0||r->ocr_residency_placements.count!=0)return 6;
    if (@available(macOS 15.0,*))for(id<MTLResidencySet>set in r->ocr_residency_sets)if(set.allAllocations.count!=0)return 7;
    // Safetensors use no-copy shared allocations; test that exact resource
    // type as well as owned buffers, with lifetime tied to the Metal object.
    void *mapped=NULL;
    if(posix_memalign(&mapped,16384,16384)!=0)return 15;
    memset(mapped,0,16384);
    id<MTLBuffer>alias=[r->device newBufferWithBytesNoCopy:mapped length:16384 options:MTLResourceStorageModeShared deallocator:^(void *pointer,NSUInteger length){(void)length;free(pointer);}];
    if(alias==nil){free(mapped);return 16;}
    termite_metal_ocr_residency_track(r,3,alias);
    if(r->ocr_residency_slots[3]!=alias||r->ocr_residency_total_bytes!=alias.length)return 17;
    termite_metal_ocr_residency_track(r,3,nil);
    if(r->ocr_residency_total_bytes!=0||r->ocr_residency_placements.count!=0)return 18;
    // Refuse an additional allocation when the model-owned soft cap is full.
    id<MTLBuffer> small=[r->device newBufferWithLength:4096 options:MTLResourceStorageModeShared];
    r->ocr_residency_total_bytes=(size_t)12*1024*1024*1024;
    termite_metal_ocr_residency_track(r,2,small);
    if(r->ocr_residency_slots[2]!=nil||r->ocr_residency_placements.count!=0)return 8;
    r->ocr_residency_total_bytes=0;
    if(termite_metal_decode_runtime_prepare_linear_bf16(r,0,(uint8_t*)weights,sizeof(weights),bias,128,64))return 9;
    id<MTLBuffer> first=r->linear_weight_buffers[0];
    if(termite_metal_decode_runtime_prepare_linear_bf16(r,0,(uint8_t*)weights,sizeof(weights),bias,128,64))return 10;
    if(r->ocr_residency_slots[0]!=r->linear_weight_buffers[0]||r->ocr_residency_placements.count!=1)return 11;
    if(first!=r->linear_weight_buffers[0]&&[r->ocr_residency_placements objectForKey:first]!=nil)return 12;
    unsetenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_WEIGHT_RESIDENCY");
    termite_metal_ocr_residency_track(r,0,r->linear_weight_buffers[0]);
    if(r->ocr_residency_total_bytes!=0||r->ocr_residency_slots[0]!=nil)return 13;
    termite_metal_ocr_residency_shutdown(r);
    if(r->ocr_residency_sets!=nil||r->ocr_residency_placements!=nil||r->ocr_residency_total_bytes!=0)return 14;
    if(saved){setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_WEIGHT_RESIDENCY",saved,1);free(saved);}
    fprintf(stderr,"ocr_weight_residency_check tied_buffers=pass mapped_alias=pass clear_replace=pass cap=pass disable=pass release=pass\n");
    return 0;
}

static int batch_gate_up_check(termite_metal_decode_runtime *r, size_t rows, size_t in, size_t out,
                               bool benchmark) {
    @autoreleasepool {
        uint16_t *w = malloc(in * out * 2);
        float *bias = malloc(out * 4), *gold = malloc(rows * out * 4);
        if (!w || !bias || !gold)
            return 1;
        uint32_t rng = 123;
        double cpu_dots[2][4];
        for (size_t slot = 0; slot < 2; slot++) {
            for (size_t i = 0; i < in * out; i++) {
                rng = rng * 1664525u + 1013904223u;
                w[i] = round_bf16(((int)(rng >> 16) - 32768) * .000001f);
            }
            for (size_t i = 0; i < out; i++)
                bias[i] = cosf(i * .01f + slot) * .1f;
            for (size_t corner = 0; corner < 4; corner++) {
                size_t row = corner / 2 ? rows - 1 : 0, col = corner % 2 ? out - 1 : 0;
                double sum = bias[col];
                for (size_t k = 0; k < in; k++)
                    sum += (double)sinf((row * in + k) * .01f) * expand_bf16(w[col * in + k]);
                cpu_dots[slot][corner] = sum;
            }
            if (termite_metal_decode_runtime_prepare_linear_bf16(r, slot, (uint8_t *)w, in * out * 2, bias,
                                                                 in, out))
                return 2;
        }
        free(w);
        free(bias);
        id<MTLBuffer> x = [r->device newBufferWithLength:(rows * in + 8) * 4
                                                 options:MTLResourceStorageModeShared],
                      g = [r->device newBufferWithLength:rows * out * 4 options:MTLResourceStorageModeShared],
                      u = [r->device newBufferWithLength:rows * out * 4 options:MTLResourceStorageModeShared],
                      y = [r->device newBufferWithLength:(rows * out + 12) * 4
                                                 options:MTLResourceStorageModeShared];
        if (!x || !g || !u || !y)
            return 3;
        float *xs = (float *)x.contents + 4;
        for (size_t i = 0; i < rows * in; i++)
            xs[i] = sinf(i * .01f);
        bool qualified = rows >= 2 && rows <= 16 && (in == 1024 || in == 2560);
        if ((termite_metal_decode_runtime_should_apply_bf16_gate_up(r, 0, 1, 16, rows, in, out) != 0) !=
            qualified)
            return 4;
        if (termite_metal_decode_runtime_should_apply_bf16_gate_up(r, 0, 1, 4, rows, in, out))
            return 5;
        if (!qualified) {
            free(gold);
            return 0;
        }
        for (const char **flag =
                 (const char *[]){"TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_GATE_UP",
                                  "TERMITE_METAL_DISABLE_BF16_FUSED_GATE_UP",
                                  "TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_PROJECTIONS", NULL};
             *flag; flag++) {
            setenv(*flag, "1", 1);
            if (termite_metal_decode_runtime_should_apply_bf16_gate_up(r, 0, 1, 16, rows, in, out))
                return 6;
            unsetenv(*flag);
        }
        id<MTLComputePipelineState> p16 = r->lightonocr3_gate_up_small16_pipeline,
                                    p32 = r->lightonocr3_gate_up_small32_pipeline;
        id<MTLComputePipelineState> local[4] = {
            r->lightonocr3_local_gate_vectors[0], r->lightonocr3_local_gate_vectors[1],
            r->lightonocr3_local_gate_tg16, r->lightonocr3_local_gate_tg32};
        r->lightonocr3_local_gate_vectors[0] = nil;
        r->lightonocr3_local_gate_vectors[1] = nil;
        r->lightonocr3_local_gate_tg16 = nil;
        r->lightonocr3_local_gate_tg32 = nil;
        id<MTLComputePipelineState> wide = r->lightonocr3_gate_up_wide16_pipeline;
        r->lightonocr3_gate_up_wide16_pipeline = nil;
        r->lightonocr3_gate_up_small16_pipeline = nil;
        r->lightonocr3_gate_up_small32_pipeline = nil;
        if (termite_metal_decode_runtime_should_apply_bf16_gate_up(r, 0, 1, 16, rows, in, out))
            return 7;
        r->lightonocr3_gate_up_wide16_pipeline = wide;
        r->lightonocr3_gate_up_small16_pipeline = p16;
        r->lightonocr3_gate_up_small32_pipeline = p32;
        r->lightonocr3_local_gate_vectors[0] = local[0];
        r->lightonocr3_local_gate_vectors[1] = local[1];
        r->lightonocr3_local_gate_tg16 = local[2];
        r->lightonocr3_local_gate_tg32 = local[3];
        if (termite_metal_lightonocr3_local_pipeline_valid(r, local[0], SIZE_MAX, 0) ||
            termite_metal_lightonocr3_local_pipeline_valid(r, local[2], 128, SIZE_MAX))
            return 70;
        id<MTLBuffer> source = [r->device newBufferWithBytes:x.contents
                                                      length:x.length
                                                     options:MTLResourceStorageModeShared];
        id<MTLBuffer> zero = [r->device newBufferWithLength:x.length options:MTLResourceStorageModeShared];
        if (!source || !zero)
            return 71;
        memset(zero.contents, 0, zero.length);

        double gpu[3] = {0};
        float worst = 0;
        double cpu = 0;
        for (size_t trial = 0; trial < (benchmark ? 30 : 1); trial++)
            for (size_t mode = 0; mode < 3; mode++) {
                uint32_t *raw = y.contents;
                for (size_t i = 0; i < 4; i++)
                    raw[i] = 0xdeadbeef;
                for (size_t i = rows * out + 4; i < rows * out + 12; i++)
                    raw[i] = 0xcafebabe;
                if (termite_metal_decode_runtime_begin_frame(r))
                    return 8;
                if (mode == 0) {
                    if (termite_metal_decode_runtime_apply_linear_multi_row_device(
                            r, 0, (__bridge void *)x, 16, rows, in, out, (__bridge void *)g, 0) ||
                        termite_metal_decode_runtime_apply_linear_multi_row_device(
                            r, 1, (__bridge void *)x, 16, rows, in, out, (__bridge void *)u, 0) ||
                        termite_metal_decode_runtime_apply_activation_multiply_device(
                            r, 2, (__bridge void *)g, 0, (__bridge void *)u, 0, rows, out, (__bridge void *)y,
                            16))
                        return 9;
                } else {
                    if (mode == 1) {
                        memset(x.contents, 0, x.length);
                        if (termite_metal_decode_runtime_begin_planned_compute_scope(
                                r, TERMITE_METAL_COMPUTE_SOURCE_FFN, TERMITE_METAL_COMPUTE_REGION_FFN) ||
                            termite_metal_decode_runtime_apply_add_device(r, (__bridge void *)source, 16,
                                                                          (__bridge void *)zero, 16,
                                                                          rows * in, (__bridge void *)x, 16))
                            return 72;
                    } else
                        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_LOCAL_KERNELS", "1", 1);
                    if (termite_metal_decode_runtime_apply_bf16_gate_up_device(
                            r, 0, 1, (__bridge void *)x, 16, rows, in, out, (__bridge void *)y, 16))
                        return 10;
                    if (mode == 1 && termite_metal_decode_runtime_end_planned_compute_scope(r))
                        return 73;
                    if (mode == 2)
                        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_LOCAL_KERNELS");
                }
                if (termite_metal_decode_runtime_submit_frame(r) ||
                    termite_metal_decode_runtime_wait_frame(r))
                    return 11;
                if (benchmark && trial >= 10)
                    gpu[mode] += r->last_frame_gpu_nanos;
                float *ys = (float *)y.contents + 4;
                if (mode == 0)
                    memcpy(gold, ys, rows * out * 4);
                else
                    for (size_t i = 0; i < rows * out; i++) {
                        if (!isfinite(ys[i]))
                            return 12;
                        worst = fmaxf(worst, fabsf(ys[i] - gold[i]));
                    }
                for (size_t corner = 0; corner < 4; corner++) {
                    size_t row = corner / 2 ? rows - 1 : 0, col = corner % 2 ? out - 1 : 0;
                    double gate = cpu_dots[0][corner], up = cpu_dots[1][corner],
                           want = gate / (1 + exp(-gate)) * up;
                    cpu = fmax(cpu, fabs(want - ys[row * out + col]));
                }
                for (size_t i = 0; i < 4; i++)
                    if (raw[i] != 0xdeadbeef)
                        return 13;
                for (size_t i = rows * out + 4; i < rows * out + 12; i++)
                    if (raw[i] != 0xcafebabe)
                        return 14;
            }
        fprintf(stderr,
                "batch_gate_up rows=%zu in=%zu out=%zu offset=16 max_abs=%g cpu_samples=%g fallback=pass\n",
                rows, in, out, worst, cpu);
        if (benchmark)
            fprintf(
                stderr,
                "batch_gate_up_benchmark rows=%zu in=%zu out=%zu separate_gpu_us=%.2f fused_gpu_us=%.2f\n",
                rows, in, out, gpu[0] / 20000, gpu[1] / 20000);
        free(gold);
        return worst > 2e-4f || cpu > 2e-4 ? 15 : 0;
    }
}

static int wide_gate_up_check(termite_metal_decode_runtime *r, size_t rows, size_t in, size_t out,
                               bool benchmark) {
    @autoreleasepool {
        uint16_t *w = malloc(in * out * 2);
        float *bias = malloc(out * 4), *gold = malloc(rows * out * 4);
        if (!w || !bias || !gold)
            return 1;
        uint32_t rng = 123;
        double cpu_dots[2][4];
        for (size_t slot = 0; slot < 2; slot++) {
            for (size_t i = 0; i < in * out; i++) {
                rng = rng * 1664525u + 1013904223u;
                w[i] = round_bf16(((int)(rng >> 16) - 32768) * .000001f);
            }
            for (size_t i = 0; i < out; i++)
                bias[i] = cosf(i * .01f + slot) * .1f;
            for (size_t corner = 0; corner < 4; corner++) {
                size_t row = corner / 2 ? rows - 1 : 0, col = corner % 2 ? out - 1 : 0;
                double sum = bias[col];
                for (size_t k = 0; k < in; k++)
                    sum += (double)sinf((row * in + k) * .01f) * expand_bf16(w[col * in + k]);
                cpu_dots[slot][corner] = sum;
            }
            if (termite_metal_decode_runtime_prepare_linear_bf16(r, slot, (uint8_t *)w, in * out * 2, bias,
                                                                 in, out))
                return 2;
        }
        free(w);
        free(bias);
        id<MTLBuffer> x = [r->device newBufferWithLength:(rows * in + 8) * 4
                                                 options:MTLResourceStorageModeShared],
                      g = [r->device newBufferWithLength:rows * out * 4 options:MTLResourceStorageModeShared],
                      u = [r->device newBufferWithLength:rows * out * 4 options:MTLResourceStorageModeShared],
                      y = [r->device newBufferWithLength:(rows * out + 12) * 4
                                                 options:MTLResourceStorageModeShared];
        if (!x || !g || !u || !y)
            return 3;
        float *xs = (float *)x.contents + 4;
        for (size_t i = 0; i < rows * in; i++)
            xs[i] = sinf(i * .01f);
        bool qualified = (rows >= 32 && rows <= 64 && in == 1024) ||
            (rows >= 17 && rows <= 64 && in == 2560 &&
                (rows < 64 || termite_metal_env_flag_enabled(getenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_FULL64_GATE_UP"))) &&
                termite_metal_env_flag_enabled(getenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_WIDE64_GATE_UP")));
        if ((termite_metal_decode_runtime_should_apply_bf16_gate_up(r, 0, 1, 16, rows, in, out) != 0) !=
            qualified)
            return 4;
        if (termite_metal_decode_runtime_should_apply_bf16_gate_up(r, 0, 1, 4, rows, in, out))
            return 5;
        for (const char **flag =
                 (const char *[]){"TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_GATE_UP",
                                  "TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_GATE_UP",
                                  "TERMITE_METAL_DISABLE_BF16_FUSED_GATE_UP",
                                  "TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_PROJECTIONS", NULL};
             *flag; flag++) {
            setenv(*flag, "1", 1);
            if (termite_metal_decode_runtime_should_apply_bf16_gate_up(r, 0, 1, 16, rows, in, out))
                return 6;
            unsetenv(*flag);
        }
        id<MTLComputePipelineState> p16 = r->lightonocr3_gate_up_small16_pipeline,
                                    p32 = r->lightonocr3_gate_up_small32_pipeline;
        id<MTLComputePipelineState> local[4] = {
            r->lightonocr3_local_gate_vectors[0], r->lightonocr3_local_gate_vectors[1],
            r->lightonocr3_local_gate_tg16, r->lightonocr3_local_gate_tg32};
        r->lightonocr3_local_gate_vectors[0] = nil;
        r->lightonocr3_local_gate_vectors[1] = nil;
        r->lightonocr3_local_gate_tg16 = nil;
        r->lightonocr3_local_gate_tg32 = nil;
        id<MTLComputePipelineState> wide = r->lightonocr3_gate_up_wide16_pipeline;
        id<MTLComputePipelineState> wide32 = r->lightonocr3_gate_up_wide32_pipeline;
        id<MTLComputePipelineState> wide64 = r->lightonocr3_gate_up_wide64_pipeline;
        r->lightonocr3_gate_up_wide64_pipeline = nil;
        r->lightonocr3_gate_up_wide32_pipeline = nil;
        r->lightonocr3_gate_up_wide16_pipeline = nil;
        r->lightonocr3_gate_up_small16_pipeline = nil;
        r->lightonocr3_gate_up_small32_pipeline = nil;
        if (termite_metal_decode_runtime_should_apply_bf16_gate_up(r, 0, 1, 16, rows, in, out))
            return 7;
        // Missing specialized and generic PSOs must decline before dispatch.
        id<MTLComputePipelineState> generic = r->bf16_gate_up_mm_pipeline;
        r->bf16_gate_up_mm_pipeline = nil;
        if (termite_metal_decode_runtime_can_apply_bf16_gate_up(r, 0, 1, rows, in, out)) return 74;
        r->bf16_gate_up_mm_pipeline = generic;
        r->lightonocr3_gate_up_wide32_pipeline = wide32;
        r->lightonocr3_gate_up_wide64_pipeline = wide64;
        r->lightonocr3_gate_up_wide16_pipeline = wide;
        r->lightonocr3_gate_up_small16_pipeline = p16;
        r->lightonocr3_gate_up_small32_pipeline = p32;
        r->lightonocr3_local_gate_vectors[0] = local[0];
        r->lightonocr3_local_gate_vectors[1] = local[1];
        r->lightonocr3_local_gate_tg16 = local[2];
        r->lightonocr3_local_gate_tg32 = local[3];
        if (termite_metal_lightonocr3_local_pipeline_valid(r, local[0], SIZE_MAX, 0) ||
            termite_metal_lightonocr3_local_pipeline_valid(r, local[2], 128, SIZE_MAX))
            return 70;
        id<MTLBuffer> source = [r->device newBufferWithBytes:x.contents
                                                      length:x.length
                                                     options:MTLResourceStorageModeShared];
        id<MTLBuffer> zero = [r->device newBufferWithLength:x.length options:MTLResourceStorageModeShared];
        if (!source || !zero)
            return 71;
        memset(zero.contents, 0, zero.length);

        double gpu[3] = {0};
        float worst = 0;
        double cpu = 0;
        for (size_t trial = 0; trial < (benchmark ? 30 : 3); trial++)
            for (size_t mode = 0; mode < 3; mode++) {
                uint32_t *raw = y.contents;
                for (size_t i = 0; i < 4; i++)
                    raw[i] = 0xdeadbeef;
                for (size_t i = rows * out + 4; i < rows * out + 12; i++)
                    raw[i] = 0xcafebabe;
                const uint64_t before_wide = r->lightonocr3_wide_gate_up_calls;
                const uint64_t before_full64 = r->lightonocr3_full64_gate_up_calls;
                if (termite_metal_decode_runtime_begin_frame(r) ||
                    termite_metal_decode_runtime_set_active_frame_regime(r, TERMITE_METAL_FRAME_REGIME_DECODE))
                    return 8;
                if (mode == 1) {
                    memset(x.contents, 0, x.length);
                    if (termite_metal_decode_runtime_begin_planned_compute_scope(
                            r, TERMITE_METAL_COMPUTE_SOURCE_FFN, TERMITE_METAL_COMPUTE_REGION_FFN) ||
                        termite_metal_decode_runtime_apply_add_device(r, (__bridge void *)source, 16,
                                                                      (__bridge void *)zero, 16,
                                                                      rows * in, (__bridge void *)x, 16))
                        return 72;
                } else if (mode == 2)
                    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_GATE_UP", "1", 1);
                // Intermediate rows have no generic fused kernel. Its rollback
                // must use the actual separate-projection production route.
                const bool separate = mode == 0 || (rows < 32 && (mode == 2 || !qualified));
                if (separate) {
                    if (termite_metal_decode_runtime_apply_linear_multi_row_device(
                            r, 0, (__bridge void *)x, 16, rows, in, out, (__bridge void *)g, 0) ||
                        termite_metal_decode_runtime_apply_linear_multi_row_device(
                            r, 1, (__bridge void *)x, 16, rows, in, out, (__bridge void *)u, 0) ||
                        termite_metal_decode_runtime_apply_activation_multiply_device(
                            r, 2, (__bridge void *)g, 0, (__bridge void *)u, 0, rows, out, (__bridge void *)y, 16))
                        return 9;
                } else if (termite_metal_decode_runtime_apply_bf16_gate_up_device(
                            r, 0, 1, (__bridge void *)x, 16, rows, in, out, (__bridge void *)y, 16))
                    return 10;
                if (mode == 1 && termite_metal_decode_runtime_end_planned_compute_scope(r)) return 73;
                if (mode == 2) unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_GATE_UP");
                if (termite_metal_decode_runtime_submit_frame(r) ||
                    termite_metal_decode_runtime_wait_frame(r))
                    return 11;
                if (benchmark && trial >= 10)
                    gpu[mode] += r->last_frame_gpu_nanos;
                if (r->lightonocr3_wide_gate_up_calls - before_wide != (mode == 1 && qualified ? 1u : 0u)) return 75;
                if (r->lightonocr3_full64_gate_up_calls - before_full64 !=
                    (mode == 1 && qualified && rows == 64 && in == 2560 ? 1u : 0u)) return 76;
                float *ys = (float *)y.contents + 4;
                if (mode == 0)
                    memcpy(gold, ys, rows * out * 4);
                else
                    for (size_t i = 0; i < rows * out; i++) {
                        if (!isfinite(ys[i]))
                            return 12;
                        worst = fmaxf(worst, fabsf(ys[i] - gold[i]));
                    }
                for (size_t corner = 0; corner < 4; corner++) {
                    size_t row = corner / 2 ? rows - 1 : 0, col = corner % 2 ? out - 1 : 0;
                    double gate = cpu_dots[0][corner], up = cpu_dots[1][corner],
                           want = gate / (1 + exp(-gate)) * up;
                    cpu = fmax(cpu, fabs(want - ys[row * out + col]));
                }
                for (size_t i = 0; i < 4; i++)
                    if (raw[i] != 0xdeadbeef)
                        return 13;
                for (size_t i = rows * out + 4; i < rows * out + 12; i++)
                    if (raw[i] != 0xcafebabe)
                        return 14;
            }
        fprintf(stderr,
                "wide_gate_up rows=%zu in=%zu out=%zu offset=16 max_abs=%g cpu_samples=%g fallback=pass\n",
                rows, in, out, worst, cpu);
        if (benchmark)
            fprintf(
                stderr,
                "wide_gate_up_benchmark rows=%zu in=%zu out=%zu separate_gpu_us=%.2f fused_gpu_us=%.2f\n",
                rows, in, out, gpu[0] / 20000, gpu[1] / 20000);
        free(gold);
        return worst > 2e-4f || cpu > 2e-4 ? 15 : 0;
    }
}

static int prefill_columns_check(termite_metal_decode_runtime *r, size_t seq, size_t vh) {
    @autoreleasepool {
        size_t channels = (32 + vh) * 128;
        size_t counts[15] = {
            seq * channels, channels * 4, seq * vh,       seq * vh,       vh,           vh,
            channels * 4,   channels * 4, vh * 128 * 128, seq * channels, seq * vh * 2, seq * vh * 128,
            seq * vh * 128, 128,          seq * vh * 128};
        id<MTLBuffer> b[2][15];
        void *handles[2][15];
        size_t offsets[15];
        for (size_t j = 0; j < 15; j++)
            offsets[j] = 16;
        for (size_t mode = 0; mode < 2; mode++)
            for (size_t j = 0; j < 15; j++) {
                b[mode][j] = [r->device newBufferWithLength:(counts[j] + 8) * 4
                                                    options:MTLResourceStorageModeShared];
                if (!b[mode][j])
                    return 1;
                handles[mode][j] = (__bridge void *)b[mode][j];
                uint32_t *raw = b[mode][j].contents;
                for (size_t i = 0; i < 4; i++)
                    raw[i] = raw[counts[j] + 4 + i] = 0xdeadbeef;
                float *xs = (float *)raw + 4;
                for (size_t i = 0; i < counts[j]; i++)
                    xs[i] = j == 13 ? .5f + (i % 7) * .1f : sinf(i * .013f + j) * .1f;
            }
        float worst = 0;
        for (size_t step = 0; step < 3; step++) {
            for (size_t mode = 0; mode < 2; mode++) {
                if (mode == 0)
                    setenv("TERMITE_METAL_DISABLE_QWEN35_PREFILL_COLUMNS", "1", 1);
                else
                    unsetenv("TERMITE_METAL_DISABLE_QWEN35_PREFILL_COLUMNS");
                id<MTLComputePipelineState> saved = r->qwen35_recurrent_columns4_pipeline;
                if (mode == 1 && step == 2)
                    r->qwen35_recurrent_columns4_pipeline = nil;
                if (termite_metal_decode_runtime_begin_frame(r) ||
                    termite_metal_decode_runtime_qwen35_core_device(r, handles[mode], offsets, seq, 16, vh,
                                                                    128, 4, step != 0) ||
                    termite_metal_decode_runtime_submit_frame(r) ||
                    termite_metal_decode_runtime_wait_frame(r))
                    return 2;
                r->qwen35_recurrent_columns4_pipeline = saved;
            }
            const size_t changed[] = {7, 8, 9, 10, 11, 14};
            for (size_t k = 0; k < 6; k++) {
                size_t j = changed[k];
                float *gold = (float *)b[0][j].contents + 4, *got = (float *)b[1][j].contents + 4;
                for (size_t i = 0; i < counts[j]; i++) {
                    if (!isfinite(got[i]))
                        return 3;
                    worst = fmaxf(worst, fabsf(got[i] - gold[i]));
                }
            }
            for (size_t mode = 0; mode < 2; mode++)
                for (size_t j = 0; j < 15; j++) {
                    uint32_t *raw = b[mode][j].contents;
                    for (size_t i = 0; i < 4; i++)
                        if (raw[i] != 0xdeadbeef || raw[counts[j] + 4 + i] != 0xdeadbeef)
                            return 4;
                }
            for (size_t mode = 0; mode < 2; mode++) {
                id<MTLBuffer> swap = b[mode][6];
                b[mode][6] = b[mode][7];
                b[mode][7] = swap;
                handles[mode][6] = (__bridge void *)b[mode][6];
                handles[mode][7] = (__bridge void *)b[mode][7];
            }
        }
        unsetenv("TERMITE_METAL_DISABLE_QWEN35_PREFILL_COLUMNS");
        fprintf(stderr,
                "prefill_columns seq=%zu value_heads=%zu cold_continue_missing=pass canaries=pass offset=16 "
                "max_abs=%g\n",
                seq, vh, worst);
        return worst > 2e-5f ? 5 : 0;
    }
}

static int activation_pool_check(termite_metal_decode_runtime *r) {
    const size_t bytes = 4096;
    const bool pipelined = r->whisper_pipelined_frames;
    r->whisper_pipelined_frames = true;
    if (termite_metal_decode_runtime_set_lighton_activation_pool(r, 65536) ||
        termite_metal_decode_runtime_set_lighton_activation_pool(r, 8u * 1024u * 1024u + 1) != -3) return 1;
    id<MTLBuffer> source[2], output[2];
    for (size_t i = 0; i < 2; i++) {
        source[i] = [r->device newBufferWithLength:bytes options:MTLResourceStorageModeShared];
        output[i] = [r->device newBufferWithLength:bytes options:MTLResourceStorageModeShared];
        if (!source[i] || !output[i]) return 2;
        for (size_t j = 0; j < bytes / 4; j++) ((uint32_t *)source[i].contents)[j] = (uint32_t)(j * 1021 + 37 * i);
    }
    if (termite_metal_decode_runtime_begin_frame(r) ||
        termite_metal_decode_runtime_set_active_frame_regime(r, TERMITE_METAL_FRAME_REGIME_DECODE)) return 3;
    void *first = termite_metal_buffer_alloc(r, bytes, 1);
    if (!first || termite_metal_buffer_copy(r, (__bridge void *)source[0], 0, first, 0, bytes) ||
        termite_metal_buffer_copy(r, first, 0, (__bridge void *)output[0], 0, bytes)) return 4;
    const uintptr_t first_identity = (uintptr_t)first;
    void *alias = (__bridge_retained void *)((__bridge id<MTLBuffer>)first);
    termite_metal_decode_runtime_release_buffer(r, first);
    termite_metal_decode_runtime_release_buffer(r, alias);
    if (termite_metal_decode_runtime_submit_frame(r) || r->lighton_activation_bytes == 0 ||
        termite_metal_decode_runtime_set_lighton_activation_pool(r, 0) != -2 ||
        termite_metal_lighton_activation_take(r, bytes) != NULL) return 5;
    // Encode another producer while the first frame's cache entry is fenced.
    if (termite_metal_decode_runtime_begin_frame(r) ||
        termite_metal_decode_runtime_set_active_frame_regime(r, TERMITE_METAL_FRAME_REGIME_DECODE)) return 6;
    void *inflight = termite_metal_buffer_alloc(r, bytes, 1);
    if (!inflight || (uintptr_t)inflight == first_identity ||
        termite_metal_buffer_copy(r, (__bridge void *)source[1], 0, inflight, 0, bytes)) return 7;
    if (termite_metal_decode_runtime_wait_frame(r)) return 8;
    void *reused = termite_metal_buffer_alloc(r, bytes, 1);
    void *distinct = termite_metal_buffer_alloc(r, bytes, 1);
    if (!distinct || distinct == reused) return 21;
    if ((uintptr_t)reused != first_identity || r->lighton_activation_hits != 1 ||
        termite_metal_buffer_copy(r, inflight, 0, reused, 0, bytes) ||
        termite_metal_buffer_copy(r, reused, 0, (__bridge void *)output[1], 0, bytes)) return 9;
    termite_metal_decode_runtime_release_buffer(r, inflight);
    termite_metal_decode_runtime_release_buffer(r, reused);
    termite_metal_decode_runtime_release_buffer(r, distinct);
    if (termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r)) return 10;
    for (size_t i = 0; i < 2; i++) if (memcmp(source[i].contents, output[i].contents, bytes)) return 11;
    if (r->lighton_activation_bytes > 65536 || r->lighton_activation_peak > 65536 ||
        termite_metal_decode_runtime_set_lighton_activation_pool(r, 0) || r->lighton_activation_bytes != 0) return 12;
    for (size_t i = 0; i < 128; i++) if (r->lighton_activation_buffers[i] != nil || r->lighton_activation_fences[i] != nil) return 13;
    // Prefill stays outside the retained decode cache; cancelled buffers never
    // acquire a completed-frame lease.
    if (termite_metal_decode_runtime_set_lighton_activation_pool(r, 65536)) return 14;
    for (size_t mode = 0; mode < 2; mode++) {
        if (termite_metal_decode_runtime_begin_frame(r) || termite_metal_decode_runtime_set_active_frame_regime(r,
            mode == 0 ? TERMITE_METAL_FRAME_REGIME_PREFILL : TERMITE_METAL_FRAME_REGIME_DECODE)) return 15;
        void *buffer = termite_metal_buffer_alloc(r, bytes, 1);
        if (!buffer || termite_metal_buffer_copy(r, (__bridge void *)source[0], 0, buffer, 0, bytes)) return 16;
        termite_metal_decode_runtime_release_buffer(r, buffer);
        if (mode == 0) {
            if (termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r)) return 17;
        } else if (termite_metal_decode_runtime_cancel_frame(r)) return 18;
        if (r->lighton_activation_bytes != 0 || r->lighton_activation_hits != 0) return 19;
    }
    if (termite_metal_decode_runtime_set_lighton_activation_pool(r, 0)) return 20;
    r->whisper_pipelined_frames = pipelined;
    fprintf(stderr, "activation_pool_check completed_reuse=pass distinct_owners=pass inflight_isolation=pass producer_consumer=pass bounds=pass prefill=pass cancellation=pass teardown=pass\n");
    return 0;
}

static int wide_projection_tiles_check(termite_metal_decode_runtime *r,size_t rows,size_t in,size_t out,bool benchmark) {
  setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_MPP","1",1);
  id<MTLBuffer> x=[r->device newBufferWithLength:(rows*in+4)*4 options:MTLResourceStorageModeShared];
  id<MTLBuffer> y=[r->device newBufferWithLength:(rows*out+12)*4 options:MTLResourceStorageModeShared];
  float *v=(float*)x.contents+4,*got=(float*)y.contents+4;
  uint16_t *w=malloc(in*out*2);float *b=malloc(out*4),*gold=malloc(rows*out*4);
  if(!x||!y||!w||!b||!gold)return 1;
  uint32_t state=123;for(size_t i=0;i<in*out;i++){state=state*1664525u+1013904223u;w[i]=round_bf16(((int)(state>>16)-32768)*0.000001f);}
  for(size_t i=0;i<rows*in;i++)v[i]=sinf(i*.013f)*.7f;
  for(size_t i=0;i<out;i++)b[i]=cosf(i*.019f)*.2f;
  if(termite_metal_decode_runtime_prepare_linear_bf16(r,0,(uint8_t*)w,in*out*2,b,in,out))return 2;
  setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_PROJECTION_TILES","1",1);
  if(apply_projection(r,rows,in,out,x,4,y))return 3;
  memcpy(gold,got,rows*out*4);
  for(size_t round=0;round<4;round++){
    bool on=round==1||round==2;setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_PROJECTION_TILES",on?"0":"1",1);
    uint64_t before=r->frame_gpu_nanos;size_t calls=r->lightonocr3_wide_projection_tile_calls;
    float worst=0;
    for(size_t trial=0;trial<(benchmark?20:1);trial++){
      if(apply_projection(r,rows,in,out,x,4,y))return 4;
      for(size_t i=0;i<rows*out;i++){if(!isfinite(got[i]))return 5;worst=fmaxf(worst,fabsf(got[i]-gold[i]));}
    }
    if(worst>2e-4f || ((r->lightonocr3_wide_projection_tile_calls>calls)!=on))return 6;
    fprintf(stderr,"WIDE_PROJECTION_TILES rows=%zu in=%zu out=%zu on=%d round=%zu gpu_us=%.2f max_abs=%g calls=%zu\n",rows,in,out,on,round,(r->frame_gpu_nanos-before)/(benchmark?20000.0:1000.0),worst,(size_t)(r->lightonocr3_wide_projection_tile_calls-calls));
  }
  // A GPU producer feeds the full wide input through the same planned
  // encoder. The consumer spans both tiles and their shared scratch hazards.
  id<MTLBuffer> source=[r->device newBufferWithBytes:x.contents length:x.length options:MTLResourceStorageModeShared];
  id<MTLBuffer> zero=[r->device newBufferWithLength:x.length options:MTLResourceStorageModeShared];
  if(!source||!zero)return 7;
  memset(zero.contents,0,zero.length);memset(x.contents,0,x.length);
  unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_PROJECTION_TILES");
  if(termite_metal_decode_runtime_begin_frame(r)||
     termite_metal_decode_runtime_set_active_frame_regime(r,TERMITE_METAL_FRAME_REGIME_DECODE)||
     termite_metal_decode_runtime_begin_planned_compute_scope(r,TERMITE_METAL_COMPUTE_SOURCE_DENSE_LINEAR,TERMITE_METAL_COMPUTE_REGION_FFN)||
     termite_metal_decode_runtime_apply_add_device(r,(__bridge void*)source,16,(__bridge void*)zero,16,rows*in,(__bridge void*)x,16)||
     termite_metal_decode_runtime_apply_linear_multi_row_device(r,0,(__bridge void*)x,16,rows,in,out,(__bridge void*)y,16)||
     termite_metal_decode_runtime_end_planned_compute_scope(r)||
     termite_metal_decode_runtime_submit_frame(r)||termite_metal_decode_runtime_wait_frame(r))return 8;
  for(size_t i=0;i<rows*out;i++)if(!isfinite(got[i])||fabsf(got[i]-gold[i])>2e-4f)return 9;
  uint32_t*raw=y.contents;
  for(size_t i=0;i<4;i++)if(raw[i]!=0xdeadbeefu)return 10;
  for(size_t i=rows*out+4;i<rows*out+12;i++)if(raw[i]!=0xcafebabeu)return 11;
  unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_MPP");
  free(w);free(b);free(gold);return 0;
}
static int wide_mpp_check(termite_metal_decode_runtime *r,size_t rows,size_t in,size_t out,bool benchmark) {
  uint64_t *counter=out==248320?&r->lightonocr3_wide_mpp_head_calls:&r->lightonocr3_wide_mpp_projection_calls;
  id<MTLBuffer> x=[r->device newBufferWithLength:(rows*in+4)*4 options:MTLResourceStorageModeShared];
  id<MTLBuffer> y=[r->device newBufferWithLength:(rows*out+12)*4 options:MTLResourceStorageModeShared];
  float *v=(float*)x.contents+4,*got=(float*)y.contents+4;
  uint16_t *w=malloc(in*out*2);float *b=malloc(out*4),*gold=malloc(rows*out*4);
  if(!x||!y||!w||!b||!gold)return 1;
  uint32_t state=123;for(size_t i=0;i<in*out;i++){state=state*1664525u+1013904223u;w[i]=round_bf16(((int)(state>>16)-32768)*0.000001f);}
  for(size_t i=0;i<rows*in;i++)v[i]=sinf(i*.013f)*.7f;
  for(size_t i=0;i<out;i++)b[i]=cosf(i*.019f)*.2f;
  if(termite_metal_decode_runtime_prepare_linear_bf16(r,0,(uint8_t*)w,in*out*2,b,in,out))return 2;
  setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_MPP","1",1);
  if(apply_projection(r,rows,in,out,x,4,y))return 3;
  memcpy(gold,got,rows*out*4);
  for(size_t row=0;row<rows;row+=rows>1?rows-1:1)for(size_t col=0;col<out;col+=out>1?out-1:1){
    double want=b[col];for(size_t k=0;k<in;k++)want+=(double)v[row*in+k]*expand_bf16(w[col*in+k]);
    if(fabs(want-gold[row*out+col])>2e-4)return 12;
  }
  // A prefill-only precision setting must leave decoder frames and owned
  // vocabulary heads in F32, including the new thirty-two-row boundary.
  if(rows>16)setenv("TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS","1",1);
  for(size_t round=0;round<4;round++){
    bool on=round==1||round==2;setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_MPP",on?"0":"1",1);
    uint64_t before=r->frame_gpu_nanos;size_t calls=(*counter);
    float worst=0;
    for(size_t trial=0;trial<(benchmark?20:1);trial++){
      if(apply_projection(r,rows,in,out,x,4,y))return 4;
      for(size_t i=0;i<rows*out;i++){if(!isfinite(got[i]))return 5;worst=fmaxf(worst,fabsf(got[i]-gold[i]));}
    }
    if(worst>2e-4f || (((*counter)>calls)!=on))return 6;
    fprintf(stderr,"WIDE_MPP_CHECK rows=%zu in=%zu out=%zu on=%d round=%zu gpu_us=%.2f max_abs=%g calls=%zu\n",rows,in,out,on,round,(r->frame_gpu_nanos-before)/(benchmark?20000.0:1000.0),worst,(size_t)((*counter)-calls));
  }
  // A GPU producer feeds the matrix head/projection through the same
  // planned encoder. The input must remain on device across the dependency.
  id<MTLBuffer> source=[r->device newBufferWithBytes:x.contents length:x.length options:MTLResourceStorageModeShared];
  id<MTLBuffer> zero=[r->device newBufferWithLength:x.length options:MTLResourceStorageModeShared];
  if(!source||!zero)return 7;
  memset(zero.contents,0,zero.length);memset(x.contents,0,x.length);
  unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_MPP");
  if(termite_metal_decode_runtime_begin_frame(r)||
     termite_metal_decode_runtime_set_active_frame_regime(r,TERMITE_METAL_FRAME_REGIME_DECODE)||
     termite_metal_decode_runtime_begin_planned_compute_scope(r,TERMITE_METAL_COMPUTE_SOURCE_DENSE_LINEAR,TERMITE_METAL_COMPUTE_REGION_FFN)||
     termite_metal_decode_runtime_apply_add_device(r,(__bridge void*)source,16,(__bridge void*)zero,16,rows*in,(__bridge void*)x,16)||
     termite_metal_decode_runtime_apply_linear_multi_row_device(r,0,(__bridge void*)x,16,rows,in,out,(__bridge void*)y,16)||
     termite_metal_decode_runtime_end_planned_compute_scope(r)||
     termite_metal_decode_runtime_submit_frame(r)||termite_metal_decode_runtime_wait_frame(r))return 8;
  for(size_t i=0;i<rows*out;i++)if(!isfinite(got[i])||fabsf(got[i]-gold[i])>2e-4f)return 9;
  uint32_t*raw=y.contents;
  for(size_t i=0;i<4;i++)if(raw[i]!=0xdeadbeefu)return 10;
  for(size_t i=rows*out+4;i<rows*out+12;i++)if(raw[i]!=0xcafebabeu)return 11;
  // Production greedy heads consume a GPU-produced hidden tensor after its
  // decoder frame closes. Exercise that owned-CB route without beginning a
  // replacement frame, checking every value and both canary regions.
  if(rows>16&&out==248320){
    if(r->active_frame_cb!=nil)return 16;
    size_t before=(*counter);
    uint64_t standalone_before=termite_metal_decode_runtime_lighton_standalone_wide_head_calls(r);
    if(termite_metal_decode_runtime_apply_linear_multi_row_device(r,0,(__bridge void*)x,16,rows,in,out,(__bridge void*)y,16))return 17;
    float worst=0;
    for(size_t i=0;i<rows*out;i++){
      if(!isfinite(got[i]))return 18;
      worst=fmaxf(worst,fabsf(got[i]-gold[i]));
    }
    if(worst>2e-4f||(*counter)!=before+1||
       termite_metal_decode_runtime_lighton_standalone_wide_head_calls(r)!=standalone_before+1)return 19;
    for(size_t i=0;i<4;i++)if(raw[i]!=0xdeadbeefu)return 20;
    for(size_t i=rows*out+4;i<rows*out+12;i++)if(raw[i]!=0xcafebabeu)return 21;
    fprintf(stderr,"WIDE_MPP_STANDALONE rows=%zu in=%zu out=%zu max_abs=%g calls=%zu\n",rows,in,out,worst,(size_t)((*counter)-before));
    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_MPP","1",1);
    if(termite_metal_decode_runtime_apply_linear_multi_row_device(r,0,(__bridge void*)x,16,rows,in,out,(__bridge void*)y,16))return 22;
    if((*counter)!=before+1||termite_metal_decode_runtime_lighton_standalone_wide_head_calls(r)!=standalone_before+1)return 23;
    for(size_t i=0;i<rows*out;i++)if(!isfinite(got[i])||fabsf(got[i]-gold[i])>2e-4f)return 24;
    for(size_t i=0;i<4;i++)if(raw[i]!=0xdeadbeefu)return 25;
    for(size_t i=rows*out+4;i<rows*out+12;i++)if(raw[i]!=0xcafebabeu)return 26;
    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_WIDE_MPP");
    // Explicit BF16 decode inputs use the same owned-CB head route. Compare
    // GPU packing of the original GPU-produced input against a separately
    // rounded host reference through the canonical linear operator.
    float *rounded=(float*)source.contents+4;
    for(size_t i=0;i<rows*in;i++)rounded[i]=expand_bf16(round_bf16(rounded[i]));
    setenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS","1",1);
    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PROJECTIONS","1",1);
    if(termite_metal_decode_runtime_apply_linear_multi_row_device(r,0,(__bridge void*)source,16,rows,in,out,(__bridge void*)y,16))return 27;
    memcpy(gold,got,rows*out*4);
    unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PROJECTIONS");
    before=(*counter);standalone_before=termite_metal_decode_runtime_lighton_standalone_wide_head_calls(r);
    if(termite_metal_decode_runtime_apply_linear_multi_row_device(r,0,(__bridge void*)x,16,rows,in,out,(__bridge void*)y,16))return 28;
    worst=0;
    for(size_t i=0;i<rows*out;i++){
      if(!isfinite(got[i]))return 29;
      worst=fmaxf(worst,fabsf(got[i]-gold[i]));
    }
    if(worst>2e-4f||(*counter)!=before+1||termite_metal_decode_runtime_lighton_standalone_wide_head_calls(r)!=standalone_before+1)return 30;
    for(size_t i=0;i<4;i++)if(raw[i]!=0xdeadbeefu)return 31;
    for(size_t i=rows*out+4;i<rows*out+12;i++)if(raw[i]!=0xcafebabeu)return 32;
    fprintf(stderr,"WIDE_MPP_STANDALONE_BF16 rows=%zu in=%zu out=%zu max_abs=%g calls=%zu\n",rows,in,out,worst,(size_t)((*counter)-before));
    unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS");
  }
  // Missing optional pipelines must decline before encoding anything.
  id<MTLComputePipelineState> saved[3];
  for(size_t i=0;i<3;i++){saved[i]=r->lightonocr3_wide_mpp_pipelines[i];r->lightonocr3_wide_mpp_pipelines[i]=nil;}
  if(termite_metal_try_lightonocr3_wide_mpp(r,0,x,16,rows,in,out,y,16)!=0)return 13;
  for(size_t i=0;i<3;i++)r->lightonocr3_wide_mpp_pipelines[i]=saved[i];
  if(rows>16){
    id<MTLComputePipelineState> matrix=r->lightonocr3_mpp_f32_pipeline;
    r->lightonocr3_mpp_f32_pipeline=nil;
    if(termite_metal_decode_runtime_begin_frame(r)||
       termite_metal_decode_runtime_set_active_frame_regime(r,TERMITE_METAL_FRAME_REGIME_DECODE))return 14;
    int declined=termite_metal_try_lightonocr3_mpp(r,0,x,16,rows,in,out,y,16);
    r->lightonocr3_mpp_f32_pipeline=matrix;
    termite_metal_decode_runtime_cancel_frame(r);
    if(declined!=0)return 15;
  }
  if(rows>16)unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS");
  free(w);free(b);free(gold);return 0;
}

// Compare one concurrent encoder against canonical per-page attention and a
// double-precision CPU softmax. Query production occurs on the GPU in the same
// planned frame. Pages have distinct logical lengths and permuted block tables.
static int ocr_attention_batch_check(termite_metal_decode_runtime *r,
                                     size_t rows, size_t heads,
                                     bool benchmark) {
  const size_t dim = 256, kv_heads = heads / 4, physical = 768, page = 128,
               width = heads * dim;
  id<MTLBuffer> source =
      [r->device newBufferWithLength:rows * width * 4
                             options:MTLResourceStorageModeShared];
  id<MTLBuffer> zero =
      [r->device newBufferWithLength:rows * width * 4
                             options:MTLResourceStorageModeShared];
  id<MTLBuffer> q =
      [r->device newBufferWithLength:rows * width * 4
                             options:MTLResourceStorageModePrivate];
  id<MTLBuffer> k =
      [r->device newBufferWithLength:physical * kv_heads * dim * 2
                             options:MTLResourceStorageModeShared];
  id<MTLBuffer> v =
      [r->device newBufferWithLength:k.length
                             options:MTLResourceStorageModeShared];
  id<MTLBuffer> result[2];
  for (size_t m = 0; m < 2; m++)
    result[m] = [r->device newBufferWithLength:(rows * width + 8) * 4
                                       options:MTLResourceStorageModeShared];
  if (!source || !zero || !q || !k || !v || !result[0] || !result[1])
    return 1;
  memset(zero.contents, 0, zero.length);
  float *qs = source.contents;
  __fp16 *ks = k.contents, *vs = v.contents;
  for (size_t j = 0; j < rows * width; j++)
    qs[j] = sinf((float)(j % 1049) * .31f) * .6f;
  for (size_t j = 0; j < physical * kv_heads * dim; j++) {
    ks[j] = cosf((float)(j % 997) * .17f) * .5f;
    vs[j] = sinf((float)(j % 991) * .29f) * .8f;
  }
  r->attention_span_encoded_key_buffers[0] = k;
  r->attention_span_v_buffers[0] = v;
  r->attention_span_slot_key_row_bytes[0] = kv_heads * dim * 2;
  r->attention_span_slot_v_row_stride[0] = kv_heads * dim;
  uint32_t blocks[6] = {640, 384, 128, 512, 256, 0};
  double elapsed[2] = {0};
  const size_t trials = benchmark ? 16 : 1;
  for (size_t trial = 0; trial < trials; trial++)
    for (size_t m = 0; m < (benchmark ? 2u : 5u); m++) {
      const size_t variant = (trial & 1) ? 1 - m : m;
      const size_t mode = variant ? 1 : 0;
      const uint64_t batch_kernel_before =
          r->lighton_attention_batch_kernel_calls;
      if (variant == 4)
        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCHED_GQA_KERNEL", "1", 1);
      id<MTLComputePipelineState> saved_stage =
          r->lightonocr3_gqa_split_stage_pipeline;
      if (variant == 2)
        r->lightonocr3_gqa_split_stage_pipeline = nil;
      if (variant == 3)
        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_GQA_SPLIT", "1", 1);
      uint32_t *out = result[mode].contents;
      for (size_t j = 0; j < rows * width + 8; j++)
        out[j] = 0xdeadbeef;
      if (termite_metal_decode_runtime_begin_frame(r) ||
          termite_metal_decode_runtime_begin_planned_compute_scope(
              r, TERMITE_METAL_COMPUTE_SOURCE_ATTENTION,
              TERMITE_METAL_COMPUTE_REGION_ATTENTION) ||
          termite_metal_decode_runtime_apply_add_device(
              r, (__bridge void *)source, 0, (__bridge void *)zero, 0,
              rows * width, (__bridge void *)q, 0))
        return 2;
      if (mode && termite_metal_decode_runtime_lighton_attention_batch_begin(r))
        return 3;
      for (size_t row = 0; row < rows; row++) {
        const size_t tokens = 432 + row * 13;
        if (termite_metal_decode_runtime_encode_paged_attention_slot_ex(
                r, r, r->active_frame_cb, 0, 3, q, row * width * 4, blocks, 6,
                page, 1, tokens, heads, kv_heads, dim, kv_heads * dim * 2,
                kv_heads * dim * 2, tokens - 1, 0, 0, 0, NULL, 0, result[mode],
                16 + row * width * 4))
          return 4;
      }
      if (mode) {
        if (r->lighton_attention_batch->count !=
                ((variant == 2 || variant == 3) ? 0 : rows) ||
            termite_metal_decode_runtime_lighton_attention_batch_end(r))
          return 5;
      }
      id<MTLCommandBuffer> cb = r->active_frame_cb;
      if (termite_metal_decode_runtime_end_planned_compute_scope(r) ||
          termite_metal_decode_runtime_submit_frame(r) ||
          termite_metal_decode_runtime_wait_frame(r))
        return 6;
      if (variant == 1 &&
          r->lighton_attention_batch_kernel_calls != batch_kernel_before + 1)
        return 10;
      if (variant != 1 &&
          r->lighton_attention_batch_kernel_calls != batch_kernel_before)
        return 11;
      if (variant == 4)
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCHED_GQA_KERNEL");
      r->lightonocr3_gqa_split_stage_pipeline = saved_stage;
      if (variant == 3)
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_GQA_SPLIT");
      elapsed[mode] += (cb.GPUEndTime - cb.GPUStartTime) * 1e6;
      for (size_t j = 0; j < 4; j++)
        if (out[j] != 0xdeadbeef || out[rows * width + 4 + j] != 0xdeadbeef)
          return 7;
    }
  double worst = 0, cpu_worst = 0;
  for (size_t row = 0; row < rows; row++) {
    const size_t tokens = 432 + row * 13;
    for (size_t j = 0; j < width; j++) {
      float a = ((float *)result[0].contents)[4 + row * width + j],
            b = ((float *)result[1].contents)[4 + row * width + j];
      if (!isfinite(a) || !isfinite(b))
        return 8;
      worst = fmax(worst, fabs(a - b));
    }
    for (size_t corner = 0; corner < 4; corner++) {
      size_t h = (corner & 1) ? heads - 1 : 0, d = (corner & 2) ? 255 : 0,
             kh = h / 4;
      double best = -INFINITY, denom = 0, numer = 0;
      for (size_t t = 0; t < tokens; t++) {
        size_t phys = blocks[t / page] + t % page;
        double dot = 0;
        for (size_t d1 = 0; d1 < dim; d1++)
          dot += (double)qs[row * width + h * dim + d1] *
                 ks[(phys * kv_heads + kh) * dim + d1];
        dot *= .0625;
        double next = fmax(best, dot), old = exp(best - next),
               weight = exp(dot - next);
        denom = denom * old + weight;
        numer = numer * old + weight * vs[(phys * kv_heads + kh) * dim + d];
        best = next;
      }
      cpu_worst = fmax(
          cpu_worst,
          fabs(numer / denom -
               ((float *)result[1].contents)[4 + row * width + h * dim + d]));
    }
  }
  fprintf(stderr,
          "attention_batch rows=%zu heads=%zu max_abs=%g cpu_max_abs=%g "
          "canonical_gpu_us=%.2f batch_gpu_us=%.2f\n",
          rows, heads, worst, cpu_worst, elapsed[0] / trials,
          elapsed[1] / trials);
  if (worst > 2e-5 || cpu_worst > 2e-5)
    return 9;
  return 0;
}


// Six distinct GPU-produced inputs share one prefill frame. Reuse must not
// preserve old input contents or leak across a submitted frame or standalone
// call. Full outputs are byte-exact to allocation-per-projection; CPU corners
// use explicitly rounded BF16 inputs and original BF16 checkpoint weights.
static int prefill_pack_reuse_check(termite_metal_decode_runtime *r,
                                    size_t rows, size_t in, size_t out,
                                    bool benchmark) {
  const size_t steps = 6, count = rows * in, outputs = rows * out,
               modes = benchmark ? 2 : 6;
  id<MTLBuffer> source =
      [r->device newBufferWithLength:steps * count * 4
                             options:MTLResourceStorageModeShared];
  id<MTLBuffer> zero =
      [r->device newBufferWithLength:count * 4
                             options:MTLResourceStorageModeShared];
  id<MTLBuffer> input =
      [r->device newBufferWithLength:count * 4
                             options:MTLResourceStorageModePrivate];
  id<MTLBuffer> results[6][6] = {{nil}};
  id<MTLBuffer> prefix_result = [r->device newBufferWithLength:(outputs + 8) * 4 options:MTLResourceStorageModeShared];
  if (!prefix_result) return 1;
  uint16_t *weights = malloc(in * out * 2);
  float *bias = calloc(out, 4);
  if (!source || !zero || !input || !weights || !bias)
    return 1;
  memset(zero.contents, 0, zero.length);
  float *xs = source.contents;
  for (size_t j = 0; j < steps * count; j++)
    xs[j] = sinf((float)(j % 1069) * .113f + (float)(j / count) * .7f) * .5f;
  for (size_t j = 0; j < in * out; j++)
    weights[j] = round_bf16(cosf((float)(j % 997) * .127f) * .015f);
  if (termite_metal_decode_runtime_prepare_linear_bf16(
          r, 0, (uint8_t *)weights, in * out * 2, bias, in, out))
    return 2;
  for (size_t m = 0; m < modes; m++)
    for (size_t step = 0; step < steps; step++) {
      results[m][step] =
          [r->device newBufferWithLength:(outputs + 8) * 4
                                 options:MTLResourceStorageModeShared];
      if (!results[m][step])
        return 3;
    }
  setenv("TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS", "1", 1);
  const size_t trials = benchmark ? 12 : 1;
  double gpu[2] = {0}, wall[2] = {0}, cpu_worst = 0;
  for (size_t trial = 0; trial < trials; trial++)
    for (size_t mi = 0; mi < modes; mi++) {
      const size_t mode = benchmark && (trial & 1) ? 1 - mi : mi;
      if (mode == 0)
        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_PACK_REUSE", "1", 1);
      else
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_PACK_REUSE");
      if (antfly_metal_lighton_prefill_pack_streaming(r, mode == 5)) return 15;
      if (mode == 5) {
        uint32_t *ys = prefix_result.contents;
        for (size_t j = 0; j < outputs + 8; j++) ys[j] = 0xdeadbeef;
      }
      const int old_reuse = r->buffer_reuse_enabled;
      if (mode == 2)
        r->buffer_reuse_enabled = 0;
      uint64_t calls = r->lighton_prefill_pack_calls,
               hits = r->lighton_prefill_pack_reuse_hits;
      const double begin = (double)termite_metal_clock_monotonic_nanos() * 1e-9;
      if (mode != 3) {
        if (termite_metal_decode_runtime_begin_frame(r) ||
            termite_metal_decode_runtime_set_active_frame_regime(
                r, TERMITE_METAL_FRAME_REGIME_PREFILL))
          return 4;
        if (mode == 4 || mode == 5) {
          if (termite_metal_decode_runtime_apply_add_device(
                  r, (__bridge void *)source, 0, (__bridge void *)zero, 0,
                  count, (__bridge void *)input, 0))
            return 5;
          termite_metal_decode_runtime_close_planned_compute_encoder_for_transition(r);
          if (mode == 5) {
            if (termite_metal_try_lightonocr3_mpp(r, 0, input, 0, rows, in, out, prefix_result, 16) != 1) return 16;
            if (antfly_metal_lighton_prefill_pack_streaming(r, 0) == 0 || !r->lighton_prefill_stream_pack_reuse) return 17;
          }
          if (termite_metal_decode_runtime_submit_frame(r) ||
              termite_metal_decode_runtime_begin_frame(r) ||
              termite_metal_decode_runtime_set_active_frame_regime(
                  r, TERMITE_METAL_FRAME_REGIME_PREFILL))
            return 5;
          if (mode == 5 && (antfly_metal_lighton_prefill_pack_streaming(r, 0) == 0 || !r->lighton_prefill_stream_pack_reuse)) return 17;
        }
      }
      for (size_t step = 0; step < steps; step++) {
        uint32_t *ys = results[mode][step].contents;
        for (size_t j = 0; j < outputs + 8; j++)
          ys[j] = 0xdeadbeef;
        if (termite_metal_decode_runtime_apply_add_device(
                r, (__bridge void *)source, step * count * 4,
                (__bridge void *)zero, 0, count, (__bridge void *)input, 0))
          return 6;
        termite_metal_decode_runtime_close_planned_compute_encoder_for_transition(
            r);
        if (termite_metal_try_lightonocr3_mpp(r, 0, input, 0, rows, in, out,
                                              results[mode][step], 16) != 1)
          return 7;
      }
      id<MTLCommandBuffer> cb = r->active_frame_cb;
      if (mode != 3) {
        if ((mode == 4 || mode == 5) && termite_metal_decode_runtime_wait_frame(r))
          return 8;
        if (termite_metal_decode_runtime_submit_frame(r) ||
            termite_metal_decode_runtime_wait_frame(r))
          return 9;
      }
      if (mode < 2) {
        gpu[mode] += (cb.GPUEndTime - cb.GPUStartTime) * 1e6;
        wall[mode] +=
            ((double)termite_metal_clock_monotonic_nanos() * 1e-9 - begin) *
            1e6;
      }
      const uint64_t call_delta = r->lighton_prefill_pack_calls - calls,
                     hit_delta = r->lighton_prefill_pack_reuse_hits - hits;
      const size_t expected_calls = rows <= 4096 && (mode == 1 || mode == 2 || mode == 5) ? steps + (mode == 5) : 0;
      if (call_delta != expected_calls) return 10;
      if (((mode == 1 || mode == 5) && rows <= 4096) ? hit_delta < steps - 1 : hit_delta != 0) return 11;
      if (mode == 5) {
        const uint32_t *ys = prefix_result.contents;
        for (size_t j = 0; j < 4; j++) if (ys[j] != 0xdeadbeef || ys[outputs + 4 + j] != 0xdeadbeef) return 18;
        if (memcmp((float *)prefix_result.contents + 4, (float *)results[0][0].contents + 4, outputs * 4)) return 19;
      }
      if (antfly_metal_lighton_prefill_pack_streaming(r, 0)) return 20;
      r->buffer_reuse_enabled = old_reuse;
      for (size_t step = 0; step < steps; step++) {
        uint32_t *ys = results[mode][step].contents;
        for (size_t j = 0; j < 4; j++)
          if (ys[j] != 0xdeadbeef || ys[outputs + 4 + j] != 0xdeadbeef)
            return 12;
        const float *got = (float *)ys + 4;
        if (mode &&
            memcmp((float *)results[0][step].contents + 4, got, outputs * 4))
          return 13;
        for (size_t j = 0; j < outputs; j++)
          if (!isfinite(got[j]))
            return 14;
        for (size_t corner = 0; corner < 4; corner++) {
          size_t row = (corner & 1) ? rows - 1 : 0,
                 col = (corner & 2) ? out - 1 : 0;
          double expected = 0;
          for (size_t k = 0; k < in; k++)
            expected += (double)expand_bf16(
                            round_bf16(xs[step * count + row * in + k])) *
                        expand_bf16(weights[col * in + k]);
          cpu_worst = fmax(cpu_worst, fabs(expected - got[row * out + col]));
        }
      }
    }
  if (!benchmark) {
    if (antfly_metal_lighton_prefill_pack_streaming(r, 1) || termite_metal_decode_runtime_begin_frame(r) ||
        termite_metal_decode_runtime_set_active_frame_regime(r, TERMITE_METAL_FRAME_REGIME_PREFILL)) return 21;
    if (termite_metal_decode_runtime_apply_add_device(r, (__bridge void *)source, 0, (__bridge void *)zero, 0, count, (__bridge void *)input, 0)) return 22;
    if (termite_metal_try_lightonocr3_mpp(r, 0, input, 0, rows, in, out, prefix_result, 16) != 1 ||
        termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_begin_frame(r) ||
        termite_metal_decode_runtime_set_active_frame_regime(r, TERMITE_METAL_FRAME_REGIME_PREFILL)) return 23;
    uint32_t *cancelled = results[5][0].contents;
    for (size_t j = 0; j < outputs + 8; j++) cancelled[j] = 0xdeadbeef;
    for (size_t step = 0; step < steps; step++) {
      if (termite_metal_try_lightonocr3_mpp(r, 0, input, 0, rows, in, out, results[5][0], 16) != 1) return 24;
    }
    if (termite_metal_decode_runtime_cancel_frame(r) || termite_metal_decode_runtime_wait_frame(r)) return 25;
    for (size_t j = 0; j < outputs + 8; j++) if (cancelled[j] != 0xdeadbeef) return 26;
    if (memcmp((float *)prefix_result.contents + 4, (float *)results[0][0].contents + 4, outputs * 4)) return 27;
    if (antfly_metal_lighton_prefill_pack_streaming(r, 0)) return 28;
  }
  unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_PACK_REUSE");
  unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS");
  fprintf(
      stderr,
      "prefill_pack_reuse rows=%zu in=%zu steps=%zu cpu_max_abs=%g "
      "old_gpu_us=%.2f reuse_gpu_us=%.2f old_wall_us=%.2f reuse_wall_us=%.2f\n",
      rows, in, steps, cpu_worst, gpu[0] / trials, gpu[1] / trials,
      wall[0] / trials, wall[1] / trials);
  free(weights);
  free(bias);
  return cpu_worst > 2e-4 ? 15 : 0;
}

static int decode_pack_reuse_check(termite_metal_decode_runtime *r, size_t rows,
                                   size_t in, size_t out, bool benchmark,
                                   bool activation_pool) {
  if (termite_metal_decode_runtime_set_lighton_activation_pool(
          r, activation_pool ? 8 * 1024 * 1024 : 0))
    return 16;
  const uint64_t prefill_before = r->lighton_prefill_pack_calls;
  const size_t steps = out == 248320 ? 2 : 6, count = rows * in,
               outputs = rows * out, modes = benchmark ? 2 : 6;
  id<MTLBuffer> source =
      [r->device newBufferWithLength:steps * count * 4
                             options:MTLResourceStorageModeShared];
  id<MTLBuffer> zero =
      [r->device newBufferWithLength:count * 4
                             options:MTLResourceStorageModeShared];
  id<MTLBuffer> input =
      [r->device newBufferWithLength:count * 4
                             options:MTLResourceStorageModePrivate];
  id<MTLBuffer> results[6][6] = {{nil}};
  uint16_t *weights = malloc(in * out * 2);
  float *bias = calloc(out, 4);
  if (!source || !zero || !input || !weights || !bias)
    return 1;
  memset(zero.contents, 0, zero.length);
  float *xs = source.contents;
  for (size_t j = 0; j < steps * count; j++)
    xs[j] = sinf((float)(j % 1069) * .113f + (float)(j / count) * .7f) * .5f;
  uint16_t pattern[997];
  for (size_t j = 0; j < 997; j++)
    pattern[j] = round_bf16(cosf((float)j * .127f) * .015f);
  for (size_t j = 0; j < in * out; j++)
    weights[j] = pattern[j % 997];
  if (termite_metal_decode_runtime_prepare_linear_bf16(
          r, 0, (uint8_t *)weights, in * out * 2, bias, in, out))
    return 2;
  for (size_t m = 0; m < modes; m++)
    for (size_t step = 0; step < steps; step++) {
      results[m][step] =
          [r->device newBufferWithLength:(outputs + 8) * 4
                                 options:MTLResourceStorageModeShared];
      if (!results[m][step])
        return 3;
    }
  setenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS", "1", 1);
  const size_t trials = benchmark ? 12 : 1;
  double gpu[2] = {0}, wall[2] = {0}, cpu_worst = 0;
  for (size_t trial = 0; trial < trials; trial++)
    for (size_t mi = 0; mi < modes; mi++) {
      const size_t mode = benchmark && (trial & 1) ? 1 - mi : mi;
      // Standalone skinny non-head projections have a different legacy
      // precision fallback. Owned-frame rollback is tested at32 and the head.
      if (mode == 3 && rows < 32 && out != 248320)
        continue;
      if (mode == 0)
        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_DECODE_PACK_REUSE", "1", 1);
      else
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_DECODE_PACK_REUSE");
      const bool old_concurrent = r->concurrent_planned_dispatch_enabled;
      if (mode == 5)
        r->concurrent_planned_dispatch_enabled = true;
      const int old_reuse = r->buffer_reuse_enabled;
      if (mode == 2) {
        r->buffer_reuse_enabled = 0;
        if (activation_pool &&
            termite_metal_decode_runtime_set_lighton_activation_pool(r, 0))
          return 17;
      }
      uint64_t calls = r->lighton_decode_pack_calls,
               hits = r->lighton_decode_pack_reuse_hits;
      const double begin = (double)termite_metal_clock_monotonic_nanos() * 1e-9;
      if (mode != 3) {
        if (termite_metal_decode_runtime_begin_frame(r) ||
            termite_metal_decode_runtime_set_active_frame_regime(
                r, TERMITE_METAL_FRAME_REGIME_DECODE))
          return 4;
        if (mode == 4) {
          if (termite_metal_decode_runtime_apply_add_device(
                  r, (__bridge void *)source, 0, (__bridge void *)zero, 0,
                  count, (__bridge void *)input, 0))
            return 5;
          termite_metal_decode_runtime_close_planned_compute_encoder_for_transition(
              r);
          if (termite_metal_decode_runtime_submit_frame(r) ||
              termite_metal_decode_runtime_begin_frame(r) ||
              termite_metal_decode_runtime_set_active_frame_regime(
                  r, TERMITE_METAL_FRAME_REGIME_DECODE))
            return 5;
        }
      }
      for (size_t step = 0; step < steps; step++) {
        uint32_t *ys = results[mode][step].contents;
        for (size_t j = 0; j < outputs + 8; j++)
          ys[j] = 0xdeadbeef;
        if (termite_metal_decode_runtime_apply_add_device(
                r, (__bridge void *)source, step * count * 4,
                (__bridge void *)zero, 0, count, (__bridge void *)input, 0))
          return 6;
        termite_metal_decode_runtime_close_planned_compute_encoder_for_transition(
            r);
        if (termite_metal_try_lightonocr3_mpp(r, 0, input, 0, rows, in, out,
                                              results[mode][step], 16) != 1)
          return 7;
      }
      id<MTLCommandBuffer> cb = r->active_frame_cb;
      if (mode != 3) {
        if (mode == 4 && termite_metal_decode_runtime_wait_frame(r))
          return 8;
        if (termite_metal_decode_runtime_submit_frame(r) ||
            termite_metal_decode_runtime_wait_frame(r))
          return 9;
      }
      if (mode < 2) {
        gpu[mode] += (cb.GPUEndTime - cb.GPUStartTime) * 1e6;
        wall[mode] +=
            ((double)termite_metal_clock_monotonic_nanos() * 1e-9 - begin) *
            1e6;
      }
      const uint64_t call_delta = r->lighton_decode_pack_calls - calls,
                     hit_delta = r->lighton_decode_pack_reuse_hits - hits;
      if (((mode == 1 || mode == 2) && rows <= 64) ? call_delta != steps
                                                   : call_delta != 0)
        return 10;
      if ((mode == 1 && rows <= 64) ? hit_delta < steps - 1 : hit_delta != 0)
        return 11;
      r->concurrent_planned_dispatch_enabled = old_concurrent;
      r->buffer_reuse_enabled = old_reuse;
      if (mode == 2 && activation_pool &&
          termite_metal_decode_runtime_set_lighton_activation_pool(r, 8 * 1024 *
                                                                          1024))
        return 18;
      for (size_t step = 0; step < steps; step++) {
        uint32_t *ys = results[mode][step].contents;
        for (size_t j = 0; j < 4; j++)
          if (ys[j] != 0xdeadbeef || ys[outputs + 4 + j] != 0xdeadbeef)
            return 12;
        const float *got = (float *)ys + 4;
        if (mode &&
            memcmp((float *)results[0][step].contents + 4, got, outputs * 4))
          return 13;
        for (size_t j = 0; j < outputs; j++)
          if (!isfinite(got[j]))
            return 14;
        for (size_t corner = 0; corner < 4; corner++) {
          size_t row = (corner & 1) ? rows - 1 : 0,
                 col = (corner & 2) ? out - 1 : 0;
          double expected = 0;
          for (size_t k = 0; k < in; k++)
            expected += (double)expand_bf16(
                            round_bf16(xs[step * count + row * in + k])) *
                        expand_bf16(weights[col * in + k]);
          cpu_worst = fmax(cpu_worst, fabs(expected - got[row * out + col]));
        }
      }
    }
  unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_DECODE_PACK_REUSE");
  unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS");
  fprintf(
      stderr,
      "decode_pack_reuse rows=%zu in=%zu out=%zu activation_pool=%d steps=%zu "
      "cpu_max_abs=%g "
      "old_gpu_us=%.2f reuse_gpu_us=%.2f old_wall_us=%.2f reuse_wall_us=%.2f\n",
      rows, in, out, activation_pool ? 1 : 0, steps, cpu_worst, gpu[0] / trials,
      gpu[1] / trials, wall[0] / trials, wall[1] / trials);
  if (r->lighton_prefill_pack_calls != prefill_before)
    return 19;
  uint64_t stats[4] = {0};
  termite_metal_decode_runtime_lighton_activation_pool_stats(r, stats);
  if (stats[1] > 8 * 1024 * 1024 || stats[3] != 0)
    return 20;
  if (termite_metal_decode_runtime_set_lighton_activation_pool(r, 0))
    return 21;
  free(weights);
  free(bias);
  return cpu_worst > 2e-4 ? 15 : 0;
}

static int vision_pack_reuse_check(termite_metal_decode_runtime *r, size_t rows,
                                   size_t in, size_t out, bool benchmark) {
  const uint64_t prefill_before = r->lighton_prefill_pack_calls,
                 decode_before = r->lighton_decode_pack_calls;
  const bool large_scope = rows > 4096 && rows <= 16384 && termite_metal_env_flag_enabled(getenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_LARGE_VISION_PACK_REUSE"));
  const bool scope_eligible = rows <= 4096 || large_scope;
  const size_t steps = large_scope ? 2 : 6, count = rows * in, outputs = rows * out,
               modes = benchmark ? 2 : 6;
  id<MTLBuffer> source =
      [r->device newBufferWithLength:steps * count * 4
                             options:MTLResourceStorageModeShared];
  id<MTLBuffer> zero =
      [r->device newBufferWithLength:count * 4
                             options:MTLResourceStorageModeShared];
  id<MTLBuffer> input =
      [r->device newBufferWithLength:count * 4
                             options:MTLResourceStorageModePrivate];
  id<MTLBuffer> results[6][6] = {{nil}};
  uint16_t *weights = malloc(in * out * 2);
  float *bias = calloc(out, 4);
  if (!source || !zero || !input || !weights || !bias)
    return 1;
  memset(zero.contents, 0, zero.length);
  float *xs = source.contents;
  for (size_t j = 0; j < steps * count; j++)
    xs[j] = sinf((float)(j % 1069) * .113f + (float)(j / count) * .7f) * .5f;
  for (size_t j = 0; j < in * out; j++)
    weights[j] = round_bf16(cosf((float)(j % 997) * .127f) * .015f);
  if (termite_metal_decode_runtime_prepare_linear_bf16(
          r, 0, (uint8_t *)weights, in * out * 2, bias, in, out))
    return 2;
  for (size_t m = 0; m < modes; m++)
    for (size_t step = 0; step < steps; step++) {
      results[m][step] =
          [r->device newBufferWithLength:(outputs + 8) * 4
                                 options:MTLResourceStorageModeShared];
      if (!results[m][step])
        return 3;
    }
  setenv("TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS", "1", 1);
  const size_t trials = benchmark ? 12 : 1;
  double gpu[2] = {0}, wall[2] = {0}, cpu_worst = 0;
  for (size_t trial = 0; trial < trials; trial++)
    for (size_t mi = 0; mi < modes; mi++) {
      const size_t mode = benchmark && (trial & 1) ? 1 - mi : mi;
      if (mode == 0)
        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_VISION_PACK_REUSE", "1", 1);
      else
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_VISION_PACK_REUSE");
      const bool old_concurrent = r->concurrent_planned_dispatch_enabled;
      if (mode == 5)
        r->concurrent_planned_dispatch_enabled = true;
      const int old_reuse = r->buffer_reuse_enabled;
      if (mode == 2)
        r->buffer_reuse_enabled = 0;
      uint64_t calls = r->lighton_vision_pack_calls,
               hits = r->lighton_vision_pack_reuse_hits;
      const double begin = (double)termite_metal_clock_monotonic_nanos() * 1e-9;
      if (mode != 3) {
        if (termite_metal_decode_runtime_begin_frame(r) ||
            termite_metal_decode_runtime_set_active_frame_regime(
                r, TERMITE_METAL_FRAME_REGIME_NONE))
          return 4;
        if (mode == 4) {
          if (termite_metal_decode_runtime_apply_add_device(
                  r, (__bridge void *)source, 0, (__bridge void *)zero, 0,
                  count, (__bridge void *)input, 0))
            return 5;
          termite_metal_decode_runtime_close_planned_compute_encoder_for_transition(
              r);
          if (termite_metal_decode_runtime_submit_frame(r) ||
              termite_metal_decode_runtime_begin_frame(r) ||
              termite_metal_decode_runtime_set_active_frame_regime(
                  r, TERMITE_METAL_FRAME_REGIME_NONE))
            return 5;
        }
      }
      for (size_t step = 0; step < steps; step++) {
        uint32_t *ys = results[mode][step].contents;
        for (size_t j = 0; j < outputs + 8; j++)
          ys[j] = 0xdeadbeef;
        if (termite_metal_decode_runtime_apply_add_device(
                r, (__bridge void *)source, step * count * 4,
                (__bridge void *)zero, 0, count, (__bridge void *)input, 0))
          return 6;
        termite_metal_decode_runtime_close_planned_compute_encoder_for_transition(
            r);
        if (termite_metal_try_lightonocr3_mpp(r, 0, input, 0, rows, in, out,
                                              results[mode][step], 16) != 1)
          return 7;
      }
      id<MTLCommandBuffer> cb = r->active_frame_cb;
      if (mode != 3) {
        if (mode == 4 && termite_metal_decode_runtime_wait_frame(r))
          return 8;
        if (termite_metal_decode_runtime_submit_frame(r) ||
            termite_metal_decode_runtime_wait_frame(r))
          return 9;
      }
      if (mode < 2) {
        gpu[mode] += (cb.GPUEndTime - cb.GPUStartTime) * 1e6;
        wall[mode] +=
            ((double)termite_metal_clock_monotonic_nanos() * 1e-9 - begin) *
            1e6;
      }
      const uint64_t call_delta = r->lighton_vision_pack_calls - calls,
                     hit_delta = r->lighton_vision_pack_reuse_hits - hits;
      if (((mode == 1 || mode == 2) && scope_eligible) ? call_delta != steps
                                                     : call_delta != 0)
        return 10;
      if ((mode == 1 && scope_eligible) ? hit_delta < steps - 1 : hit_delta != 0)
        return 11;
      r->concurrent_planned_dispatch_enabled = old_concurrent;
      r->buffer_reuse_enabled = old_reuse;
      for (size_t step = 0; step < steps; step++) {
        uint32_t *ys = results[mode][step].contents;
        for (size_t j = 0; j < 4; j++)
          if (ys[j] != 0xdeadbeef || ys[outputs + 4 + j] != 0xdeadbeef)
            return 12;
        const float *got = (float *)ys + 4;
        if (mode &&
            memcmp((float *)results[0][step].contents + 4, got, outputs * 4))
          return 13;
        for (size_t j = 0; j < outputs; j++)
          if (!isfinite(got[j]))
            return 14;
        for (size_t corner = 0; corner < 4; corner++) {
          size_t row = (corner & 1) ? rows - 1 : 0,
                 col = (corner & 2) ? out - 1 : 0;
          double expected = 0;
          for (size_t k = 0; k < in; k++)
            expected += (double)expand_bf16(
                            round_bf16(xs[step * count + row * in + k])) *
                        expand_bf16(weights[col * in + k]);
          cpu_worst = fmax(cpu_worst, fabs(expected - got[row * out + col]));
        }
      }
    }
  unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_VISION_PACK_REUSE");
  unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS");
  fprintf(
      stderr,
      "vision_pack_reuse rows=%zu in=%zu out=%zu steps=%zu cpu_max_abs=%g "
      "old_gpu_us=%.2f reuse_gpu_us=%.2f old_wall_us=%.2f reuse_wall_us=%.2f\n",
      rows, in, out, steps, cpu_worst, gpu[0] / trials, gpu[1] / trials,
      wall[0] / trials, wall[1] / trials);
  if (r->lighton_prefill_pack_calls != prefill_before ||
      r->lighton_decode_pack_calls != decode_before)
    return 16;
  free(weights);
  free(bias);
  return cpu_worst > 2e-4 ? 15 : 0;
}

static int planned_mpp_check(termite_metal_decode_runtime *r, size_t rows,
                             size_t in, size_t out, bool bf16, bool pool) {
  const size_t steps = out == 248320 ? 2 : 6, count = rows * in,
               outputs = rows * out;
  if (termite_metal_decode_runtime_set_lighton_activation_pool(
          r, pool ? 8 * 1024 * 1024 : 0))
    return 1;
  id<MTLBuffer> source =
      [r->device newBufferWithLength:steps * count * 4
                             options:MTLResourceStorageModeShared];
  id<MTLBuffer> zero =
      [r->device newBufferWithLength:count * 4
                             options:MTLResourceStorageModeShared];
  id<MTLBuffer> input =
      [r->device newBufferWithLength:count * 4
                             options:MTLResourceStorageModePrivate];
  id<MTLBuffer> results[4][6] = {{nil}};
  uint16_t *weights = malloc(in * out * 2);
  float *bias = malloc(out * 4);
  if (!source || !zero || !input || !weights || !bias)
    return 2;
  memset(zero.contents, 0, zero.length);
  float *xs = source.contents, input_pattern[6][1069];
  uint16_t weight_pattern[997];
  for (size_t step = 0; step < steps; step++) {
    for (size_t j = 0; j < 1069; j++)
      input_pattern[step][j] = sinf((float)j * .113f + (float)step * .7f) * .5f;
    for (size_t j = 0; j < count; j++)
      xs[step * count + j] = input_pattern[step][j % 1069];
  }
  for (size_t j = 0; j < 997; j++)
    weight_pattern[j] = round_bf16(cosf((float)j * .127f) * .015f);
  for (size_t j = 0; j < in * out; j++)
    weights[j] = weight_pattern[j % 997];
  for (size_t j = 0; j < out; j++)
    bias[j] = sinf((float)(j % 257) * .11f) * .001f;
  if (termite_metal_decode_runtime_prepare_linear_bf16(
          r, 0, (uint8_t *)weights, in * out * 2, bias, in, out))
    return 3;
  for (size_t mode = 0; mode < 4; mode++)
    for (size_t step = 0; step < steps; step++) {
      results[mode][step] =
          [r->device newBufferWithLength:(outputs + 8) * 4
                                 options:MTLResourceStorageModeShared];
      if (!results[mode][step])
        return 4;
    }
  setenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS", bf16 ? "1" : "0", 1);
  unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS");
  const bool old_concurrent = r->concurrent_planned_dispatch_enabled;
  double worst = 0;
  uint64_t encoders[4] = {0};
  for (size_t mode = 0; mode < 4; mode++) {
    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PLANNED_MPP",
           (mode == 0 || mode == 2) ? "1" : "0", 1);
    uint64_t planned_before = r->lighton_mpp_planned_calls,
             dedicated_before = r->lighton_mpp_dedicated_calls;
    if (termite_metal_decode_runtime_begin_frame(r) ||
        termite_metal_decode_runtime_set_active_frame_regime(
            r, rows <= 32 ? TERMITE_METAL_FRAME_REGIME_DECODE
                          : TERMITE_METAL_FRAME_REGIME_PREFILL))
      return 5;
    r->concurrent_planned_dispatch_enabled = mode == 3;
    for (size_t step = 0; step < steps; step++) {
      if (mode != 0 && r->active_planned_compute_encoder == nil &&
          termite_metal_decode_runtime_begin_planned_compute_scope(
              r, TERMITE_METAL_COMPUTE_SOURCE_LAYER,
              TERMITE_METAL_COMPUTE_REGION_ATTENTION))
        return 6;
      if (mode == 3 && !termite_metal_planned_compute_barriers_enabled(r))
        return 7;
      uint32_t *ys = results[mode][step].contents;
      for (size_t j = 0; j < outputs + 8; j++)
        ys[j] = 0xdeadbeef;
      if (termite_metal_decode_runtime_apply_add_device(
              r, (__bridge void *)source, step * count * 4,
              (__bridge void *)zero, 0, count, (__bridge void *)input, 0))
        return 8;
      if (termite_metal_try_lightonocr3_mpp(r, 0, input, 0, rows, in, out,
                                            results[mode][step], 16) != 1)
        return 9;
      const bool expected_planned = mode == 1 || mode == 3;
      if ((r->active_planned_compute_encoder != nil) != expected_planned)
        return 10;
    }
    encoders[mode] = r->active_frame_compute_encoder_count;
    if ((mode == 1 || mode == 3) && encoders[mode] != 1)
      return 11;
    if (termite_metal_decode_runtime_submit_frame(r) ||
        termite_metal_decode_runtime_wait_frame(r))
      return 12;
    r->concurrent_planned_dispatch_enabled = old_concurrent;
    uint64_t planned = r->lighton_mpp_planned_calls - planned_before,
             dedicated = r->lighton_mpp_dedicated_calls - dedicated_before;
    if (planned != ((mode == 1 || mode == 3) ? steps : 0) ||
        dedicated != ((mode == 0 || mode == 2) ? steps : 0))
      return 13;
    for (size_t step = 0; step < steps; step++) {
      uint32_t *ys = results[mode][step].contents;
      for (size_t j = 0; j < 4; j++)
        if (ys[j] != 0xdeadbeef || ys[outputs + 4 + j] != 0xdeadbeef)
          return 14;
      const float *got = (float *)ys + 4;
      if (mode &&
          memcmp((float *)results[0][step].contents + 4, got, outputs * 4))
        return 15;
      for (size_t j = 0; j < outputs; j++)
        if (!isfinite(got[j]))
          return 16;
      for (size_t corner = 0; corner < 4; corner++) {
        size_t row = (corner & 1) ? rows - 1 : 0,
               col = (corner & 2) ? out - 1 : 0;
        double expected = bias[col];
        for (size_t k = 0; k < in; k++) {
          float x = xs[step * count + row * in + k];
          expected += (double)(bf16 ? expand_bf16(round_bf16(x)) : x) *
                      expand_bf16(weights[col * in + k]);
        }
        worst = fmax(worst, fabs(expected - got[row * out + col]));
      }
    }
  }
  uint64_t stats[4] = {0};
  termite_metal_decode_runtime_lighton_activation_pool_stats(r, stats);
  if (stats[1] > 8 * 1024 * 1024 || stats[3] != 0)
    return 17;
  if (termite_metal_decode_runtime_set_lighton_activation_pool(r, 0))
    return 18;
  unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PLANNED_MPP");
  unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS");
  fprintf(stderr,
          "planned_mpp rows=%zu in=%zu out=%zu bf16=%d pool=%d steps=%zu "
          "encoders=%llu/%llu/%llu/%llu cpu_max_abs=%g\n",
          rows, in, out, bf16, pool, steps, (unsigned long long)encoders[0],
          (unsigned long long)encoders[1], (unsigned long long)encoders[2],
          (unsigned long long)encoders[3], worst);
  free(weights);
  free(bias);
  return worst > 2e-4 ? 19 : 0;
}

static int planned_wide64_check(termite_metal_decode_runtime *r, size_t rows,
                             size_t in, size_t out, bool bf16, bool pool) {
  const size_t steps = out == 248320 ? 2 : 6, count = rows * in,
               outputs = rows * out;
  if (termite_metal_decode_runtime_set_lighton_activation_pool(
          r, pool ? 8 * 1024 * 1024 : 0))
    return 1;
  id<MTLBuffer> source =
      [r->device newBufferWithLength:steps * count * 4
                             options:MTLResourceStorageModeShared];
  id<MTLBuffer> zero =
      [r->device newBufferWithLength:count * 4
                             options:MTLResourceStorageModeShared];
  id<MTLBuffer> input =
      [r->device newBufferWithLength:count * 4
                             options:MTLResourceStorageModePrivate];
  id<MTLBuffer> results[5][6] = {{nil}};
  uint16_t *weights = malloc(in * out * 2);
  float *bias = malloc(out * 4);
  if (!source || !zero || !input || !weights || !bias)
    return 2;
  memset(zero.contents, 0, zero.length);
  float *xs = source.contents, input_pattern[6][1069];
  uint16_t weight_pattern[997];
  for (size_t step = 0; step < steps; step++) {
    for (size_t j = 0; j < 1069; j++)
      input_pattern[step][j] = sinf((float)j * .113f + (float)step * .7f) * .5f;
    for (size_t j = 0; j < count; j++)
      xs[step * count + j] = input_pattern[step][j % 1069];
  }
  for (size_t j = 0; j < 997; j++)
    weight_pattern[j] = round_bf16(cosf((float)j * .127f) * .015f);
  for (size_t j = 0; j < in * out; j++)
    weights[j] = weight_pattern[j % 997];
  for (size_t j = 0; j < out; j++)
    bias[j] = sinf((float)(j % 257) * .11f) * .001f;
  if (termite_metal_decode_runtime_prepare_linear_bf16(
          r, 0, (uint8_t *)weights, in * out * 2, bias, in, out))
    return 3;
  for (size_t mode = 0; mode < 5; mode++)
    for (size_t step = 0; step < steps; step++) {
      results[mode][step] =
          [r->device newBufferWithLength:(outputs + 8) * 4
                                 options:MTLResourceStorageModeShared];
      if (!results[mode][step])
        return 4;
    }
  setenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS", bf16 ? "1" : "0", 1);
  unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS");
  const bool old_concurrent = r->concurrent_planned_dispatch_enabled;
  double worst = 0;
  uint64_t encoders[5] = {0};
  const bool gate = out == 16 || out == 32;
  double cross_tile_worst = 0;
  for (size_t mode = 0; mode < 5; mode++) {
    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PLANNED_MPP",
           (mode == 0 || mode == 2) ? "1" : "0", 1);
    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_64_ROW_TILE",
           (mode == 0 || mode == 4) ? "1" : "0", 1);
    uint64_t planned_before = r->lighton_mpp_planned_calls,
             dedicated_before = r->lighton_mpp_dedicated_calls;
    if (termite_metal_decode_runtime_begin_frame(r) ||
        termite_metal_decode_runtime_set_active_frame_regime(
            r, TERMITE_METAL_FRAME_REGIME_DECODE))
      return 5;
    r->concurrent_planned_dispatch_enabled = mode == 3;
    for (size_t step = 0; step < steps; step++) {
      if (mode != 0 && r->active_planned_compute_encoder == nil &&
          termite_metal_decode_runtime_begin_planned_compute_scope(
              r, TERMITE_METAL_COMPUTE_SOURCE_LAYER,
              TERMITE_METAL_COMPUTE_REGION_ATTENTION))
        return 6;
      if (mode == 3 && !termite_metal_planned_compute_barriers_enabled(r))
        return 7;
      uint32_t *ys = results[mode][step].contents;
      for (size_t j = 0; j < outputs + 8; j++)
        ys[j] = 0xdeadbeef;
      if (termite_metal_decode_runtime_apply_add_device(
              r, (__bridge void *)source, step * count * 4,
              (__bridge void *)zero, 0, count, (__bridge void *)input, 0))
        return 8;
      if (termite_metal_try_lightonocr3_mpp(r, 0, input, 0, rows, in, out,
                                            results[mode][step], 16) != 1)
        return 9;
      const bool expected_planned = mode == 1 || mode == 3 || mode == 4;
      if ((r->active_planned_compute_encoder != nil) != expected_planned)
        return 10;
    }
    encoders[mode] = r->active_frame_compute_encoder_count;
    if ((mode == 1 || mode == 3 || mode == 4) && encoders[mode] != 1)
      return 11;
    if (termite_metal_decode_runtime_submit_frame(r) ||
        termite_metal_decode_runtime_wait_frame(r))
      return 12;
    r->concurrent_planned_dispatch_enabled = old_concurrent;
    uint64_t planned = r->lighton_mpp_planned_calls - planned_before,
             dedicated = r->lighton_mpp_dedicated_calls - dedicated_before;
    if (!gate && (planned != ((mode == 1 || mode == 3 || mode == 4) ? steps : 0) ||
        dedicated != ((mode == 0 || mode == 2) ? steps : 0)))
      return 13;
    if (gate && (planned != 0 || dedicated != 0))
      return 20;
    for (size_t step = 0; step < steps; step++) {
      uint32_t *ys = results[mode][step].contents;
      for (size_t j = 0; j < 4; j++)
        if (ys[j] != 0xdeadbeef || ys[outputs + 4 + j] != 0xdeadbeef)
          return 14;
      const float *got = (float *)ys + 4;
      const size_t reference = gate || mode == 4 ? 0 : 1;
      if (mode > 1 && memcmp((float *)results[reference][step].contents + 4,
                            got, outputs * 4))
        return 15;
      if (mode == 1) {
        const float *baseline = (float *)results[0][step].contents + 4;
        for (size_t j = 0; j < outputs; j++)
          cross_tile_worst = fmax(cross_tile_worst, fabs((double)baseline[j] - got[j]));
      }
      for (size_t j = 0; j < outputs; j++)
        if (!isfinite(got[j]))
          return 16;
      for (size_t corner = 0; corner < 4; corner++) {
        size_t row = (corner & 1) ? rows - 1 : 0,
               col = (corner & 2) ? out - 1 : 0;
        double expected = bias[col];
        for (size_t k = 0; k < in; k++) {
          float x = xs[step * count + row * in + k];
          expected += (double)(bf16 ? expand_bf16(round_bf16(x)) : x) *
                      expand_bf16(weights[col * in + k]);
        }
        worst = fmax(worst, fabs(expected - got[row * out + col]));
      }
    }
  }
  uint64_t stats[4] = {0};
  termite_metal_decode_runtime_lighton_activation_pool_stats(r, stats);
  if (stats[1] > 8 * 1024 * 1024 || stats[3] != 0)
    return 17;
  if (termite_metal_decode_runtime_set_lighton_activation_pool(r, 0))
    return 18;
  unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_64_ROW_TILE");
  unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PLANNED_MPP");
  unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS");
  fprintf(stderr,
          "planned_wide64 rows=%zu in=%zu out=%zu bf16=%d pool=%d steps=%zu "
          "encoders=%llu/%llu/%llu/%llu/%llu cpu_max_abs=%g cross_tile_max_abs=%g\n",
          rows, in, out, bf16, pool, steps, (unsigned long long)encoders[0],
          (unsigned long long)encoders[1], (unsigned long long)encoders[2],
          (unsigned long long)encoders[3], (unsigned long long)encoders[4], worst, cross_tile_worst);
  free(weights);
  free(bias);
  return worst > 2e-4 || cross_tile_worst > 2e-4 ? 19 : 0;
}

static int rolled_frame_check(termite_metal_decode_runtime *r) {
    const size_t count = 32;
    float initial[32], changed[32], got[32];
    for (size_t i = 0; i < count; ++i) { initial[i] = (float)i + .25f; changed[i] = initial[i] + 100; }
    id<MTLBuffer> source = [r->device newBufferWithBytes:initial length:sizeof(initial) options:MTLResourceStorageModeShared];
    id<MTLBuffer> a = [r->device newBufferWithLength:sizeof(initial) options:MTLResourceStorageModePrivate];
    id<MTLBuffer> b = [r->device newBufferWithLength:sizeof(initial) options:MTLResourceStorageModePrivate];
    if (!source || !a || !b) return 20;
    if (termite_metal_decode_runtime_begin_frame(r)) return 21;
    r->active_stage_sample.frame_id = 100;
    if (termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)source, 0, 1, count, 0, count, (__bridge void *)a, 0) ||
        termite_metal_decode_runtime_submit_frame(r)) return 22;
    id<MTLBuffer> previous = a;
    for (uint64_t frame = 1; frame < 4; ++frame) {
        id<MTLBuffer> next = previous == a ? b : a;
        if (termite_metal_decode_runtime_begin_frame(r)) return 23;
        r->active_stage_sample.frame_id = 100 + frame;
        if (termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)previous, 0, 1, count, 0, count, (__bridge void *)next, 0)) return 24;
        if (r->active_frame_cb == nil || r->submitted_frame_cb == nil) return 25;
        if (termite_metal_decode_runtime_wait_frame(r) ||
            r->active_frame_cb == nil || r->submitted_frame_cb != nil ||
            r->active_stage_sample.frame_id != 100 + frame) return 26;
        if (termite_metal_decode_runtime_submit_frame(r) ||
            r->submitted_stage_sample.frame_id != 100 + frame) return 27;
        previous = next;
    }
    if (termite_metal_decode_runtime_wait_frame(r) ||
        termite_metal_buffer_download(r, (__bridge void *)previous, 0, got, sizeof(got)) ||
        memcmp(initial, got, sizeof(got))) return 28;
    // A canceled suffix must leave its destination unchanged while the
    // submitted prefix still completes. Never discard the in-flight owner.
    memcpy(source.contents, changed, sizeof(changed));
    if (termite_metal_decode_runtime_begin_frame(r) ||
        termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)source, 0, 1, count, 0, count, (__bridge void *)a, 0) ||
        termite_metal_decode_runtime_submit_frame(r) ||
        termite_metal_decode_runtime_begin_frame(r) ||
        termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)a, 0, 1, count, 0, count, (__bridge void *)b, 0) ||
        termite_metal_decode_runtime_cancel_frame(r) ||
        r->active_frame_cb != nil || r->submitted_frame_cb == nil ||
        termite_metal_decode_runtime_wait_frame(r)) return 29;
    if (termite_metal_buffer_download(r, (__bridge void *)a, 0, got, sizeof(got)) ||
        memcmp(changed, got, sizeof(got)) ||
        termite_metal_buffer_download(r, (__bridge void *)b, 0, got, sizeof(got)) ||
        memcmp(initial, got, sizeof(got))) return 30;
    fprintf(stderr, "rolled_frame_check four_frames=pass two_in_flight=bounded metadata=pass canceled_suffix=pass\n");
    return 0;
}

static int frame_policy_refresh_check(termite_metal_decode_runtime *r) {
    const char *names[] = {"TERMITE_METAL_ENABLE_LIGHTONOCR3_FRAME_POLICY",
        "TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_GATE_UP",
        "TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_PARALLEL",
        "TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING"};
    char *saved[4] = {0};
    for (size_t i = 0; i < 4; ++i) {
        const char *value = getenv(names[i]);
        if (value != NULL) saved[i] = strdup(value);
    }
    int rc = 0;
    id<MTLComputePipelineState> before = nil;
    setenv(names[0], "1", 1);
    setenv(names[1], "0", 1);
    unsetenv(names[2]); // Default-on remains enabled when absent.
    setenv(names[3], "1", 1);
    if (termite_metal_decode_runtime_begin_frame(r)) { rc = 1; goto done; }
    before = termite_metal_lightonocr3_batch_gate_up_pipeline(r, 16, 2560, 9216);
    if (before == nil || !termite_metal_frame_env_enabled(r, TERMITE_METAL_FRAME_ENABLE_LIGHTONOCR3_RECURRENT_PARALLEL) ||
        !termite_metal_frame_env_enabled(r, TERMITE_METAL_FRAME_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING)) { rc = 2; goto done; }
    setenv(names[1], "1", 1); setenv(names[2], "0", 1); setenv(names[3], "0", 1);
    if (termite_metal_lightonocr3_batch_gate_up_pipeline(r, 16, 2560, 9216) != before ||
        !termite_metal_frame_env_enabled(r, TERMITE_METAL_FRAME_ENABLE_LIGHTONOCR3_RECURRENT_PARALLEL) ||
        !termite_metal_frame_env_enabled(r, TERMITE_METAL_FRAME_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING)) { rc = 3; goto done; }
    if (termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_begin_frame(r)) { rc = 4; goto done; }
    if (termite_metal_lightonocr3_batch_gate_up_pipeline(r, 16, 2560, 9216) != nil ||
        termite_metal_frame_env_enabled(r, TERMITE_METAL_FRAME_ENABLE_LIGHTONOCR3_RECURRENT_PARALLEL) ||
        termite_metal_frame_env_enabled(r, TERMITE_METAL_FRAME_ENABLE_LIGHTONOCR3_RECURRENT_COALESCING)) { rc = 5; goto done; }
    if (termite_metal_decode_runtime_cancel_frame(r) || termite_metal_decode_runtime_wait_frame(r)) { rc = 6; goto done; }
    setenv(names[0], "0", 1); setenv(names[1], "0", 1);
    if (termite_metal_decode_runtime_begin_frame(r)) { rc = 7; goto done; }
    if (termite_metal_frame_env_enabled(r, TERMITE_METAL_FRAME_DISABLE_LIGHTONOCR3_BATCH_GATE_UP)) { rc = 8; goto done; }
    setenv(names[1], "1", 1);
    if (!termite_metal_frame_env_enabled(r, TERMITE_METAL_FRAME_DISABLE_LIGHTONOCR3_BATCH_GATE_UP)) rc = 9;
done:
    if (r->active_frame_cb != nil) termite_metal_decode_runtime_cancel_frame(r);
    if (r->submitted_frame_cb != nil) termite_metal_decode_runtime_wait_frame(r);
    for (size_t i = 0; i < 4; ++i) {
        if (saved[i] != NULL) { setenv(names[i], saved[i], 1); free(saved[i]); }
        else unsetenv(names[i]);
    }
    fprintf(stderr, "frame_policy_refresh same_frame=stable next_frame=refreshed rollback=live rc=%d\n", rc);
    return rc;
}

static int frame_metadata_check(termite_metal_decode_runtime *r) {
  const uint64_t ids[] = {17, 401, UINT64_MAX - 1};
  for (size_t roofline = 0; roofline < 2; roofline++)
    for (size_t i = 0; i < sizeof(ids) / sizeof(ids[0]); i++) {
      if (termite_metal_decode_runtime_begin_frame(r)) return 1;
      // Exercise both submission-copy paths without requiring optional GPU
      // timestamp support or changing the numerical kernels under test.
      r->active_stage_sample.frame_id = ids[i];
      r->active_stage_sample.roofline_enabled = roofline != 0;
      if (termite_metal_decode_runtime_submit_frame(r)) return 2;
      if (r->submitted_stage_sample.frame_id != ids[i]) return 3;
      if (termite_metal_decode_runtime_wait_frame(r)) return 4;
    }
  fprintf(stderr, "frame_metadata normal_and_roofline_ids=pass\n");
  return 0;
}

static int batch_argmax_check(termite_metal_decode_runtime *r) {
    const size_t dim = 248320;
    const size_t row_cases[] = {1, 2, 16, 33, 64, 65};
    const int32_t suppress_ids[] = {-1, 2, 1024};
    id<MTLBuffer> suppress = [r->device newBufferWithBytes:suppress_ids length:sizeof(suppress_ids) options:MTLResourceStorageModeShared];
    if (suppress == nil) return 70;
    for (size_t ri = 0; ri < 6; ++ri) {
        const size_t rows = row_cases[ri], bytes = (rows * dim + 8) * sizeof(float);
        id<MTLBuffer> source = [r->device newBufferWithLength:bytes options:MTLResourceStorageModeShared];
        id<MTLBuffer> input = [r->device newBufferWithLength:bytes options:MTLResourceStorageModePrivate];
        if (source == nil || input == nil || termite_metal_decode_runtime_reserve_argmax_rows(r, rows + 4, dim)) return 71;
        float *values = (float *)source.contents + 4;
        for (size_t row = 0; row < rows; ++row) {
            float *x = values + row * dim;
            for (size_t i = 0; i < dim; ++i) x[i] = sinf((float)(i % 10007) * .031f);
            x[0] = NAN; x[2] = 100; x[3] = 100;
            x[7] = INFINITY; x[1024] = INFINITY; x[1025] = INFINITY;
            if (row % 3 == 1) { for (size_t i = 0; i < dim; ++i) x[i] = -INFINITY; }
            if (row % 3 == 2) { for (size_t i = 0; i < dim; ++i) x[i] = NAN; }
        }
        for (size_t count = 0; count <= 3; count += 3) {
            uint32_t baseline[65];
            for (int mode = 0; mode < 5; ++mode) {
                setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_BATCH_ARGMAX", mode == 0 ? "0" : "1", 1);
                id<MTLComputePipelineState> saved = r->lighton_batch_argmax_partials_pipeline;
                id<MTLComputePipelineState> saved_reduce = r->lighton_batch_argmax_reduce_pipeline;
                if (mode == 2) r->lighton_batch_argmax_partials_pipeline = nil;
                if (mode == 4) r->lighton_batch_argmax_reduce_pipeline = nil;
                uint64_t before = r->lighton_batch_argmax_calls;
                uint32_t *storage = r->token_buffer.contents;
                for (size_t i = 0; i < rows + 4; ++i) storage[i] = 0x1234abcdu;
                if (termite_metal_decode_runtime_begin_frame(r) ||
                    termite_metal_decode_runtime_begin_planned_compute_scope(r, TERMITE_METAL_COMPUTE_SOURCE_OTHER, TERMITE_METAL_COMPUTE_REGION_OTHER) ||
                    termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)source, 16, 1, rows * dim, 0, rows * dim, (__bridge void *)input, 16)) return 72;
                if (mode == 3 && (termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r))) return 81;
                uint32_t got[65];
                int rc = termite_metal_decode_runtime_argmax_from_logits_rows_suppress_device(r, (__bridge void *)input, 16, rows, dim,
                    count ? suppress_ids : NULL, count, got, mode == 3 ? 0 : 1);
                r->lighton_batch_argmax_partials_pipeline = saved;
                r->lighton_batch_argmax_reduce_pipeline = saved_reduce;
                if (rc != 0 || r->active_frame_cb != nil || r->submitted_frame_cb != nil) return 73;
                const bool selected = (mode == 1 || mode == 3) && rows > 1 && rows <= 64;
                if (r->lighton_batch_argmax_calls - before != (selected ? 1u : 0u)) return 74;
                for (size_t row = 0; row < rows; ++row) {
                    uint32_t expected = row % 3 == 0 ? 7u : 0u;
                    if (got[row] != expected || (mode != 0 && got[row] != baseline[row])) return 75;
                    if (mode == 0) baseline[row] = got[row];
                }
                for (size_t i = rows; i < rows + 4; ++i) if (storage[i] != 0x1234abcdu) return 76;
            }
        }
        if (rows >= 2) {
        // A different vocabulary keeps the original dispatcher even when enabled.
        uint64_t previous_calls = r->lighton_batch_argmax_calls;
        uint32_t other[2];
        if (termite_metal_decode_runtime_argmax_from_logits_rows_suppress_device(r, (__bridge void *)input, 16, 2, 1025,
                suppress_ids, 3, other, 0) || r->lighton_batch_argmax_calls != previous_calls ||
                other[0] != 7u || other[1] != 0u) return 82;
        }
        // Cancel only unsubmitted work: output tokens must retain the canary.
        uint32_t *tokens = r->token_buffer.contents;
        for (size_t i = 0; i < rows + 4; ++i) tokens[i] = 0x4321dcabu;
        if (termite_metal_decode_runtime_begin_frame(r) ||
            termite_metal_decode_runtime_begin_planned_compute_scope(r, TERMITE_METAL_COMPUTE_SOURCE_OTHER, TERMITE_METAL_COMPUTE_REGION_OTHER)) return 77;
        if (rows > 1 && rows <= 64 && termite_metal_encode_lighton_batch_argmax(r, r->active_planned_compute_encoder, input, 16,
                rows, dim, suppress, 3, -78)) return 78;
        if (termite_metal_decode_runtime_cancel_frame(r)) return 79;
        for (size_t i = 0; i < rows + 4; ++i) if (tokens[i] != 0x4321dcabu) return 80;
        fprintf(stderr, "batch_argmax_check rows=%zu cpu=pass legacy=pass private_producer=pass token_canary=pass missing_pipeline=pass standalone=pass cancel=pass\n", rows);
    }
    unsetenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_BATCH_ARGMAX");
    return 0;
}

static int vision_workspace_cache_check(termite_metal_decode_runtime *r) {
    float values[32], downloaded[32];
    for (size_t i = 0; i < 32; ++i) values[i] = (float)i + .25f;
    id<MTLBuffer> source = [r->device newBufferWithBytes:values length:sizeof(values) options:MTLResourceStorageModeShared];
    id<MTLBuffer> output = [r->device newBufferWithLength:40 * sizeof(float) options:MTLResourceStorageModeShared];
    if (source == nil || output == nil) return 90;
    for (int enabled = 0; enabled <= 1; ++enabled) {
        if (termite_metal_decode_runtime_set_lighton_vision_workspace_cache(r, enabled)) return 91;
        uint64_t before[3]; termite_metal_decode_runtime_lighton_vision_workspace_stats(r, before);
        for (size_t step = 0; step < 3; ++step) {
            @autoreleasepool {
                uint32_t *guard = output.contents;
                for (size_t i = 0; i < 40; ++i) guard[i] = 0xdeadbeefu;
                if (termite_metal_decode_runtime_begin_frame(r)) return 92;
                size_t bytes = step == 2 ? 256 : 128;
                id<MTLBuffer> workspace = termite_metal_lightonocr3_vision_workspace(r, r->active_frame_cb, bytes);
                if (workspace == nil || termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)source, 0, 1, 32, 0, 32, (__bridge void *)workspace, 0) ||
                    termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)workspace, 0, 1, 32, 0, 32, (__bridge void *)output, 16) ||
                    termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r)) return 93;
                if (memcmp(values, (char *)output.contents + 16, sizeof(values))) return 94;
                for (size_t i = 0; i < 4; ++i) if (guard[i] != 0xdeadbeefu || guard[36 + i] != 0xdeadbeefu) return 95;
                uint64_t stats[3]; termite_metal_decode_runtime_lighton_vision_workspace_stats(r, stats);
                if (stats[2] != (enabled ? bytes : 0)) return 96;
            }
        }
        uint64_t after[3]; termite_metal_decode_runtime_lighton_vision_workspace_stats(r, after);
        if (after[0] - before[0] != (enabled ? 2u : 3u) || after[1] - before[1] != (enabled ? 1u : 0u)) return 97;
    }
    if (termite_metal_decode_runtime_set_lighton_vision_workspace_cache(r, 1) || termite_metal_decode_runtime_begin_frame(r)) return 98;
    id<MTLBuffer> a = termite_metal_lightonocr3_vision_workspace(r, r->active_frame_cb, 128);
    if (a == nil || termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)source, 0, 1, 32, 0, 32, (__bridge void *)a, 0) ||
        termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_begin_frame(r)) return 99;
    id<MTLBuffer> b = termite_metal_lightonocr3_vision_workspace(r, r->active_frame_cb, 128);
    if (b == nil || a == b || termite_metal_decode_runtime_set_lighton_vision_workspace_cache(r, 0) == 0 ||
        termite_metal_decode_runtime_clear_lightonocr3_vision_workspace(r) == 0) return 100;
    uint32_t *guard = output.contents;
    for (size_t i = 0; i < 40; ++i) guard[i] = 0x1234abcdu;
    if (termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)a, 0, 1, 32, 0, 32, (__bridge void *)b, 0) ||
        termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)b, 0, 1, 32, 0, 32, (__bridge void *)output, 16) ||
        termite_metal_decode_runtime_wait_frame(r) || termite_metal_decode_runtime_cancel_frame(r)) return 101;
    if (termite_metal_buffer_download(r, (__bridge void *)a, 0, downloaded, sizeof(downloaded)) || memcmp(values, downloaded, sizeof(values))) return 102;
    for (size_t i = 0; i < 40; ++i) if (guard[i] != 0x1234abcdu) return 103;
    a = nil; b = nil;
    if (termite_metal_decode_runtime_begin_frame(r)) return 104;
    id<MTLBuffer> grown = termite_metal_lightonocr3_vision_workspace(r, r->active_frame_cb, 256);
    if (grown == nil || termite_metal_decode_runtime_cancel_frame(r)) return 105;
    grown = nil;
    uint64_t stats[3]; termite_metal_decode_runtime_lighton_vision_workspace_stats(r, stats);
    if (stats[2] != 384 || termite_metal_decode_runtime_set_lighton_vision_workspace_cache(r, 0)) return 106;
    termite_metal_decode_runtime_lighton_vision_workspace_stats(r, stats);
    if (stats[2] != 0 || r->lighton_vision_workspace_keep) return 107;
    fprintf(stderr, "vision_workspace_cache_check default_off=pass reuse=pass grow=pass two_banks=pass prefix=pass cancel=pass canaries=pass clear_before_lease_release=pass\n");
    return 0;
}

static int resident_vision_dispatch_check(termite_metal_decode_runtime *r) {
    const size_t rows = 128, in = 768, out = 768, count = rows * in, outputs = rows * out;
    id<MTLBuffer> source = [r->device newBufferWithLength:count * 4 options:MTLResourceStorageModeShared];
    id<MTLBuffer> input = [r->device newBufferWithLength:count * 4 options:MTLResourceStorageModePrivate];
    id<MTLBuffer> output = [r->device newBufferWithLength:(outputs + 8) * 4 options:MTLResourceStorageModeShared];
    uint16_t *weights = malloc(in * out * 2);
    float *bias = calloc(out, 4);
    if (!source || !input || !output || !weights || !bias) return 140;
    for (size_t i = 0; i < count; ++i) ((float *)source.contents)[i] = 1;
    for (size_t i = 0; i < in * out; ++i) weights[i] = round_bf16(1);
    if (termite_metal_decode_runtime_prepare_linear_bf16(r, 0, (uint8_t *)weights, in * out * 2, bias, in, out)) return 141;
    setenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS", "0", 1);
    setenv("TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS", "0", 1);
    if (termite_metal_decode_runtime_require_lighton_vision_mpp(r, 1) != 0) return 142;
    uint32_t *guard = output.contents;
    for (size_t i = 0; i < outputs + 8; ++i) guard[i] = 0xdeadbeefu;
    if (termite_metal_decode_runtime_begin_frame(r) ||
        termite_metal_buffer_slice_last_dim_2d(r, (__bridge void *)source, 0, rows, in, 0, in, (__bridge void *)input, 0) ||
        termite_metal_decode_runtime_apply_linear_multi_row_device(r, 0, (__bridge void *)input, 0, rows, in, out, (__bridge void *)output, 16) ||
        termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r)) return 143;
    for (size_t i = 0; i < outputs; ++i) if (((float *)output.contents)[i + 4] != 768) return 144;
    for (size_t i = 0; i < 4; ++i) if (guard[i] != 0xdeadbeefu || guard[outputs + 4 + i] != 0xdeadbeefu) return 145;
    for (size_t mode = 0; mode < 5; ++mode) {
        for (size_t i = 0; i < outputs + 8; ++i) guard[i] = 0x1234abcdu;
        id<MTLComputePipelineState> saved = r->lightonocr3_mpp_f32_pipeline;
        const uint8_t dtype = r->linear_weight_dtypes[0];
        if (mode == 0) r->lightonocr3_mpp_f32_pipeline = nil;
        if (mode == 1) r->linear_weight_dtypes[0] = TERMITE_METAL_DENSE_LINEAR_DTYPE_F16;
        if (mode == 2) setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_MPP", "1", 1);
        if (termite_metal_decode_runtime_begin_frame(r)) return 146;
        const int rc = mode == 4
            ? termite_metal_decode_runtime_apply_linear_device(r, 0, (__bridge void *)input, 0, in, out, (__bridge void *)output, 16)
            : termite_metal_decode_runtime_apply_linear_multi_row_device(r, 0, (__bridge void *)input, 0, mode == 3 ? 16 : rows, in, out, (__bridge void *)output, 16);
        if (termite_metal_decode_runtime_cancel_frame(r)) return 147;
        r->lightonocr3_mpp_f32_pipeline = saved;
        r->linear_weight_dtypes[0] = dtype;
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_MPP");
        if (rc != -65) { fprintf(stderr, "resident_vision_dispatch mode=%zu rc=%d expected=-65\n", mode, rc); return 148; }
        for (size_t i = 0; i < outputs + 8; ++i) if (guard[i] != 0x1234abcdu) return 149;
    }
    if (termite_metal_decode_runtime_require_lighton_vision_mpp(r, 0) != 1) return 150;
    if (termite_metal_decode_runtime_begin_frame(r) ||
        termite_metal_decode_runtime_apply_linear_multi_row_device(r, 0, (__bridge void *)input, 0, 16, in, out, (__bridge void *)output, 16) ||
        termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r)) return 151;
    for (size_t i = 0; i < 16 * out; ++i) if (((float *)output.contents)[i + 4] != 768) return 152;
    unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS");
    unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS");
    free(weights); free(bias);
    fprintf(stderr, "resident_vision_dispatch positive=pass gpu_producer=pass missing_pipeline=pass dtype=pass disable=pass shape=pass scalar=pass no_fallback_writes=pass legacy_restored=pass\n");
    return 0;
}

static int gpu_token_burst_check(termite_metal_decode_runtime *r, size_t width) {
    setenv("TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME", "1", 1);
    setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_BATCH_ARGMAX", "1", 1);
    const size_t vocab = 248320, slot = TERMITE_METAL_LINEAR_SLOT_CAPACITY - 1;
    const size_t dims[] = {1024,2560}, counts[] = {2,16,64};
    for (size_t di=0;di<2;di++) for(size_t ci=0;ci<3;ci++) {
        @autoreleasepool {
            const size_t dim=dims[di],rows=counts[ci],bytes=rows*vocab*sizeof(float);
            id<MTLBuffer> weight=[r->device newBufferWithLength:vocab*dim*2 options:MTLResourceStorageModeShared];
            id<MTLBuffer> source=[r->device newBufferWithLength:bytes options:MTLResourceStorageModeShared];
            id<MTLBuffer> later=[r->device newBufferWithLength:bytes options:MTLResourceStorageModeShared];
            id<MTLBuffer> logits=[r->device newBufferWithLength:bytes options:MTLResourceStorageModePrivate];
            id<MTLBuffer> embed=[r->device newBufferWithLength:(rows*dim+8)*4 options:MTLResourceStorageModeShared];
            if(!weight||!source||!later||!logits||!embed)return 150;
            uint16_t *w=weight.contents;
            for(size_t t=7;t<=23;t++)for(size_t d=0;d<dim;d++) {float f=(float)(t*2+(d%17))/8;uint32_t b;memcpy(&b,&f,4);w[t*dim+d]=(uint16_t)(b>>16);}
            float *a=source.contents,*b=later.contents,*out=embed.contents;
            for(size_t i=0;i<rows*vocab;i++){a[i]=-1;b[i]=-1;}
            for(size_t row=0;row<rows;row++){a[row*vocab+(row%2?13:7)]=9;b[row*vocab+(row%2?23:21)]=9;}
            for(size_t i=0;i<rows*dim+8;i++)out[i]=-999;
            r->linear_weight_buffers[slot]=weight;r->linear_in_dims[slot]=dim;r->linear_out_dims[slot]=vocab;
            r->linear_weight_dtypes[slot]=TERMITE_METAL_DENSE_LINEAR_DTYPE_BF16;r->linear_slot_prepared[slot]=1;
            if(termite_metal_decode_runtime_reserve_argmax_rows(r,rows+4,vocab))return 151;
            uint32_t *tokens=r->token_buffer.contents;for(size_t i=0;i<rows+4;i++)tokens[i]=0x1234abcd;
            uint32_t unused=77,first[64],second[64];
            if(termite_metal_decode_runtime_argmax_from_logits_rows_suppress_device(r,(__bridge void*)logits,0,rows,vocab,NULL,0,&unused,2)!=-18)return 152;
            if(termite_metal_decode_runtime_begin_frame(r)||termite_metal_decode_runtime_begin_planned_compute_scope(r,TERMITE_METAL_COMPUTE_SOURCE_OTHER,TERMITE_METAL_COMPUTE_REGION_OTHER)||
                termite_metal_buffer_slice_last_dim_2d(r,(__bridge void*)source,0,1,rows*vocab,0,rows*vocab,(__bridge void*)logits,0)||
                termite_metal_decode_runtime_argmax_from_logits_rows_suppress_device(r,(__bridge void*)logits,0,rows,vocab,NULL,0,&unused,2))return 153;
            if(unused!=77||!r->active_frame_cb||r->submitted_frame_cb)return 154;
            if(termite_metal_decode_runtime_submit_frame(r))return 155;
            first[0]=0x55;
            if(antfly_metal_lighton_read_tokens(r,first,rows)!=-1||first[0]!=0x55)return 156;
            if(termite_metal_decode_runtime_begin_frame(r)||termite_metal_decode_runtime_begin_planned_compute_scope(r,TERMITE_METAL_COMPUTE_SOURCE_OTHER,TERMITE_METAL_COMPUTE_REGION_OTHER))return 157;
            // Rejected geometry/PSO/dtype must encode no fallback or output.
            if(antfly_metal_lighton_embed_tokens(r,slot,1,dim,(__bridge void*)embed,16)!=-1)return 158;
            id<MTLComputePipelineState> saved=r->embedding_lookup_bf16_pipeline;r->embedding_lookup_bf16_pipeline=nil;
            int rc=antfly_metal_lighton_embed_tokens(r,slot,rows,dim,(__bridge void*)embed,16);r->embedding_lookup_bf16_pipeline=saved;
            if(rc!=-1)return 159;
            r->linear_weight_dtypes[slot]=TERMITE_METAL_DENSE_LINEAR_DTYPE_F16;
            rc=antfly_metal_lighton_embed_tokens(r,slot,rows,dim,(__bridge void*)embed,16);r->linear_weight_dtypes[slot]=TERMITE_METAL_DENSE_LINEAR_DTYPE_BF16;
            if(rc!=-1)return 160;
            for(size_t step=1;step<width;step++) {
                // Step one already owns an active frame. Thereafter retain one
                // submitted producer while encoding its successor.
                if(step>1 && (termite_metal_decode_runtime_begin_frame(r)||termite_metal_decode_runtime_begin_planned_compute_scope(r,TERMITE_METAL_COMPUTE_SOURCE_OTHER,TERMITE_METAL_COMPUTE_REGION_OTHER)))return 170;
                id<MTLBuffer> next=(step%2)?later:source;
                if(antfly_metal_lighton_embed_tokens(r,slot,rows,dim,(__bridge void*)embed,16)||
                    termite_metal_buffer_slice_last_dim_2d(r,(__bridge void*)next,0,1,rows*vocab,0,rows*vocab,(__bridge void*)logits,0)||
                    termite_metal_decode_runtime_argmax_from_logits_rows_suppress_device(r,(__bridge void*)logits,0,rows,vocab,NULL,0,&unused,2)||
                    termite_metal_decode_runtime_wait_frame(r)||antfly_metal_lighton_read_tokens(r,first,rows))return 161;
                for(size_t row=0;row<rows;row++) {
                    uint32_t previous=((step-1)%2)?(row%2?23:21):(row%2?13:7);
                    if(first[row]!=previous)return 162;
                    if(step>1)for(size_t d=0;d<dim;d++) {
                        uint32_t input=((step-2)%2)?(row%2?23:21):(row%2?13:7);
                        float expected=(float)(input*2+(d%17))/8;
                        if(out[4+row*dim+d]!=expected)return 165;
                    }
                }
                if(termite_metal_decode_runtime_submit_frame(r))return 163;
            }
            if(termite_metal_decode_runtime_wait_frame(r)||antfly_metal_lighton_read_tokens(r,second,rows))return 163;
            for(size_t row=0;row<rows;row++) {
                uint32_t final=((width-1)%2)?(row%2?23:21):(row%2?13:7);
                uint32_t input=((width-2)%2)?(row%2?23:21):(row%2?13:7);
                if(second[row]!=final)return 164;
                for(size_t d=0;d<dim;d++){float expected=(float)(input*2+(d%17))/8;if(out[4+row*dim+d]!=expected)return 165;}
            }
            for(size_t i=0;i<4;i++)if(out[i]!=-999||out[4+rows*dim+i]!=-999)return 166;
            for(size_t i=rows;i<rows+4;i++)if(tokens[i]!=0x1234abcd)return 167;
            // Cancel an encoded, unsubmitted argmax; tokens keep the last
            // completed result and callers can drain every frame borrow.
            if(termite_metal_decode_runtime_begin_frame(r)||termite_metal_decode_runtime_begin_planned_compute_scope(r,TERMITE_METAL_COMPUTE_SOURCE_OTHER,TERMITE_METAL_COMPUTE_REGION_OTHER)||
                termite_metal_decode_runtime_argmax_from_logits_rows_suppress_device(r,(__bridge void*)logits,0,rows,vocab,NULL,0,&unused,2)||termite_metal_decode_runtime_cancel_frame(r))return 168;
            for(size_t row=0;row<rows;row++)if(tokens[row]!=second[row])return 169;
            r->linear_weight_buffers[slot]=nil;r->linear_slot_prepared[slot]=0;
            fprintf(stderr,"gpu_token_burst dim=%zu rows=%zu frames=%zu gpu_tokens=pass exact_bf16_embedding=pass ordered_readback=pass canaries=pass fail_closed=pass cancel=pass\n",dim,rows,width);
        }
    }
    unsetenv("TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME");unsetenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_BATCH_ARGMAX");
    return 0;
}

static int frame_staging_lifetime_check(void) {
    for (int mode = 0; mode < 3; mode++) {
        __weak id<MTLBuffer> slots[2] = {nil, nil};
        __weak id<MTLComputePipelineState> bias_pipeline = nil;
        __weak MPSMatrixMultiplication *dot_plan = nil;
        __weak MPSMatrixMultiplication *gemm_plan = nil;
        size_t retained_after_destroy = 0;
        @autoreleasepool {
            termite_metal_decode_runtime *runtime = termite_metal_decode_runtime_create();
            if (!runtime) return 1;
            bias_pipeline = runtime->linear_bias_pipeline;
            dot_plan = termite_metal_decode_runtime_cached_dot_general_mps_mm(runtime, 4, 4, 4, 1);
            gemm_plan = termite_metal_decode_runtime_cached_gemm_mps_mm(runtime, 4, 4, 4, 0, 1, 1.0f, 0.0f);
            if (!dot_plan || !gemm_plan) return 13;
            float zeros[4] = {0};
            id<MTLBuffer> zero = [runtime->device newBufferWithBytes:zeros length:sizeof(zeros) options:MTLResourceStorageModeShared];
            id<MTLBuffer> output = [runtime->device newBufferWithLength:sizeof(zeros) options:MTLResourceStorageModeShared];
            if (!zero || !output || !bias_pipeline) return 2;
            for (int frame = 0; frame < 2; frame++) {
                if (termite_metal_decode_runtime_begin_frame(runtime)) return 3;
                if (termite_metal_decode_runtime_ensure_host_staging_buffer(runtime, false, 8u * 1024u * 1024u)) return 4;
                size_t index = runtime->frame_host_staging_index & 1u;
                slots[index] = runtime->active_frame_host_staging_buffer;
                if (!slots[index]) return 5;
                memset(runtime->active_frame_host_staging_buffer.contents, 0, sizeof(zeros));
                if (mode == 0) {
                    if (termite_metal_decode_runtime_cancel_frame(runtime)) return 6;
                } else {
                    if (termite_metal_decode_runtime_apply_add_device(runtime, (__bridge void *)runtime->active_frame_host_staging_buffer, 0,
                        (__bridge void *)zero, 0, 4, (__bridge void *)output, 0)) return 7;
                    // Mode1 leaves the final producer active; mode2 submits it.
                    if (frame == 0 || mode == 2) {
                        if (termite_metal_decode_runtime_submit_frame(runtime)) return 8;
                        if (frame == 0 && termite_metal_decode_runtime_wait_frame(runtime)) return 9;
                    }
                }
            }
            if (!slots[0] || !slots[1]) return 10;
            termite_metal_decode_runtime_destroy(runtime);
            if (mode != 0) for (size_t i = 0; i < 4; i++) if (((float *)output.contents)[i] != 0) return 11;
        }
        for (size_t i = 0; i < 2; i++) retained_after_destroy += slots[i] != nil;
        fprintf(stderr, "FRAME_STAGING_LIFETIME mode=%d retained_buffers=%zu bias_pipeline_retained=%d mps_plans_retained=%d\n", mode, retained_after_destroy, bias_pipeline != nil, (dot_plan != nil) + (gemm_plan != nil));
        if (retained_after_destroy || bias_pipeline != nil || dot_plan != nil || gemm_plan != nil) return 12;
    }
    return 0;
}

int main(int argc, const char **argv) {
    @autoreleasepool {
        if (argc == 2 && strcmp(argv[1], "--check-frame-staging-lifetime") == 0) return frame_staging_lifetime_check();
        termite_metal_decode_runtime *r = termite_metal_decode_runtime_create();
        if (!r)
            return 1;
        termite_metal_decode_runtime_set_lightonocr3_projections(r, 1);
        if (!r->lightonocr3_projections_enabled) {
            fprintf(stderr, "SKIP: dispatch qualification is M4 Max\n");
            return 77;
        }
        if (!r->lightonocr3_mpp_f32_pipeline || !r->lightonocr3_mpp_bf16_pipeline || !r->lightonocr3_gemv_pipeline) {
            fprintf(stderr, "Missing qualified shader pipeline\n");
            return 2;
        }
        termite_metal_decode_runtime_set_qwen35_ocr_vector_loads(r, 1);
        if (argc == 2 && strcmp(argv[1], "--check-gpu-token-burst-long") == 0) {
            int rc=gpu_token_burst_check(r,3);
            if(!rc)rc=gpu_token_burst_check(r,4);
            termite_metal_decode_runtime_destroy(r);
            return rc;
        }
        if (argc == 2 && strcmp(argv[1], "--check-gpu-token-burst") == 0) {
            int rc=gpu_token_burst_check(r,2);
            termite_metal_decode_runtime_destroy(r);
            return rc;
        }
        if (argc == 2 && strcmp(argv[1], "--check-resident-vision-dispatch") == 0) {
            const int rc = resident_vision_dispatch_check(r);
            termite_metal_decode_runtime_destroy(r);
            return rc;
        }
        if (argc == 2 && strcmp(argv[1], "--check-vision-workspace-cache") == 0) {
            int rc = vision_workspace_cache_check(r);
            termite_metal_decode_runtime_destroy(r);
            return rc;
        }
        if (argc == 2 && strcmp(argv[1], "--check-large-vision-pack-reuse") == 0) {
            setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_LARGE_VISION_PACK_REUSE", "1", 1);
            const size_t shapes[][3] = {{4097,1024,1024},{8192,4096,1024},{16384,768,768}};
            for (size_t i = 0; i < 3; ++i) {
                @autoreleasepool {
                    int rc = vision_pack_reuse_check(r, shapes[i][0], shapes[i][1], shapes[i][2], false);
                    if (rc) { fprintf(stderr,"LARGE_VISION_PACK_FAIL rows=%zu rc=%d\n",shapes[i][0],rc); return rc; }
                }
            }
            unsetenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_LARGE_VISION_PACK_REUSE");
            termite_metal_decode_runtime_destroy(r);
            return 0;
        }
        if (argc == 2 && strcmp(argv[1], "--check-batch-argmax") == 0) {
            int rc = batch_argmax_check(r);
            termite_metal_decode_runtime_destroy(r);
            return rc;
        }
        if (argc == 2 && strcmp(argv[1], "--check-frame-metadata") == 0) {
          int rc = frame_policy_refresh_check(r);
          if (rc == 0) rc = frame_metadata_check(r);
          if (rc == 0) rc = rolled_frame_check(r);
          termite_metal_decode_runtime_destroy(r);
          return rc;
        }
        if (argc == 2 && (strcmp(argv[1], "--check-wide-gate-up") == 0 ||
                          strcmp(argv[1], "--check-wide-gate-up-large64") == 0 ||
                          strcmp(argv[1], "--check-wide-gate-up-full64") == 0)) {
            const bool full64 = strcmp(argv[1], "--check-wide-gate-up-full64") == 0;
            setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_FULL64_GATE_UP", full64 ? "1" : "0", 1);
            if (strcmp(argv[1], "--check-wide-gate-up-large64") == 0 || full64)
                setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_WIDE64_GATE_UP", "1", 1);
            else
                unsetenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_WIDE64_GATE_UP");
            const size_t rows[] = {17, 19, 24, 31, 32, 33, 48, 56, 63, 64};
            const bool hybrid = termite_metal_env_flag_enabled(getenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_WIDE64_GATE_UP"));
            if ((termite_metal_lightonocr3_batch_gate_up_pipeline(r, 31, 2560, 9216) != nil) != hybrid ||
                ((termite_metal_lightonocr3_batch_gate_up_pipeline(r, 64, 2560, 9216) != nil) != (hybrid && full64)) ||
                termite_metal_lightonocr3_batch_gate_up_pipeline(r, 65, 2560, 9216) != nil ||
                termite_metal_lightonocr3_batch_gate_up_pipeline(r, 64, 1536, 4096) != nil) return 136;
            for (size_t i = 0; i < sizeof(rows)/sizeof(rows[0]); ++i)
                for (size_t model = 0; model < 2; ++model) {
                    int rc = wide_gate_up_check(r, rows[i], model ? 2560 : 1024,
                                                model ? 9216 : 3584, false);
                    if (rc) {
                        fprintf(stderr, "WIDE_GATE_UP_FAIL rows=%zu model=%zu rc=%d\n", rows[i], model, rc);
                        return 136;
                    }
                }
            termite_metal_decode_runtime_destroy(r);
            return 0;
        }
        if (argc == 2 && strcmp(argv[1], "--check-wide64") == 0) {
          const size_t rows[] = {33, 48, 56, 63, 64};
          const size_t shapes[][2] = {{1024, 6144}, {3584, 1024},
              {2560, 8192}, {9216, 2560}, {1024, 248320}, {2560, 248320},
              {1024, 16}, {2560, 32}};
          for (size_t pool = 0; pool < 2; pool++)
            for (size_t bf16 = 0; bf16 < 2; bf16++)
              for (size_t i = 0; i < sizeof(shapes) / sizeof(shapes[0]); i++) {
                if (bf16 && shapes[i][1] <= 32) continue;
                for (size_t j = 0; j < sizeof(rows) / sizeof(rows[0]); j++) {
                  @autoreleasepool {
                    int rc = planned_wide64_check(r, rows[j], shapes[i][0],
                                                  shapes[i][1], bf16 != 0, pool != 0);
                    if (rc) {
                      fprintf(stderr, "WIDE64_FAIL rows=%zu in=%zu out=%zu bf16=%zu pool=%zu rc=%d\n",
                              rows[j], shapes[i][0], shapes[i][1], bf16, pool, rc);
                      return 136;
                    }
                  }
                }
              }
          termite_metal_decode_runtime_destroy(r);
          return 0;
        }
        if (argc == 2 && strcmp(argv[1], "--check-planned-mpp") == 0) {
          const size_t shapes[][3] = {{17, 1024, 4096},   {24, 2560, 4096},
                                      {32, 2560, 8192},   {32, 2560, 9216},
                                      {32, 9216, 2560},   {128, 768, 3072},
                                      {2304, 1024, 4096}, {432, 2560, 4096},
                                      {2648, 9216, 2560}, {32, 2560, 248320}};
          for (size_t pool = 0; pool < 2; pool++)
            for (size_t bf16 = 0; bf16 < 2; bf16++)
              for (size_t i = 0; i < sizeof(shapes) / sizeof(shapes[0]); i++) {
                @autoreleasepool {
                  int rc = planned_mpp_check(r, shapes[i][0], shapes[i][1],
                                             shapes[i][2], bf16 != 0, pool != 0);
                  if (rc) {
                    fprintf(stderr,
                            "PLANNED_MPP_FAIL rows=%zu in=%zu out=%zu bf16=%zu "
                            "pool=%zu rc=%d\n",
                            shapes[i][0], shapes[i][1], shapes[i][2], bf16, pool, rc);
                    return 135;
                  }
                }
              }
          termite_metal_decode_runtime_destroy(r);
          return 0;
        }
        if (argc == 2 && (strcmp(argv[1], "--check-decode-pack-reuse") == 0 ||
                          strcmp(argv[1], "--benchmark-decode-pack-reuse") == 0)) {
          const bool benchmark = strstr(argv[1], "benchmark") != NULL;
          const size_t shapes[][3] = {
              {32, 2560, 4096}, {32, 9216, 2560}, {17, 1024, 4096},  {24, 1024, 4096},
              {31, 1024, 4096}, {32, 1024, 4096}, {17, 2560, 4096},  {24, 2560, 4096},
              {31, 2560, 4096}, {33, 2560, 4096}, {48, 2560, 4096}, {64, 2560, 4096},
              {65, 2560, 4096}, {32, 2560, 248320}};
          const size_t count = benchmark ? 2 : sizeof(shapes) / sizeof(shapes[0]);
          for (size_t pool = 0; pool < 2; pool++)
            for (size_t i = 0; i < count; i++) {
              @autoreleasepool {
                int rc = decode_pack_reuse_check(r, shapes[i][0], shapes[i][1],
                                                 shapes[i][2], benchmark, pool != 0);
                if (rc) {
                  fprintf(
                      stderr,
                      "DECODE_PACK_REUSE_FAIL rows=%zu in=%zu out=%zu pool=%zu rc=%d\n",
                      shapes[i][0], shapes[i][1], shapes[i][2], pool, rc);
                  return 133;
                }
              }
            }
          termite_metal_decode_runtime_destroy(r);
          return 0;
        }
        if (argc == 2 && (strcmp(argv[1], "--check-vision-pack-reuse") == 0 ||
                          strcmp(argv[1], "--benchmark-vision-pack-reuse") == 0)) {
          const bool benchmark = strstr(argv[1], "benchmark") != NULL;
          const size_t shapes[][3] = {{2304, 1024, 4096}, {2304, 4096, 1024},
                                      {128, 768, 768},    {432, 768, 3072},
                                      {2304, 768, 2304},  {2648, 1024, 4096},
                                      {4096, 1024, 1024}, {4097, 1024, 1024}};
          const size_t count = benchmark ? 2 : sizeof(shapes) / sizeof(shapes[0]);
          for (size_t i = 0; i < count; i++) {
            @autoreleasepool {
              int rc = vision_pack_reuse_check(r, shapes[i][0], shapes[i][1],
                                               shapes[i][2], benchmark);
              if (rc) {
                fprintf(stderr,
                        "VISION_PACK_REUSE_FAIL rows=%zu in=%zu out=%zu rc=%d\n",
                        shapes[i][0], shapes[i][1], shapes[i][2], rc);
                return 134;
              }
            }
          }
          termite_metal_decode_runtime_destroy(r);
          return 0;
        }
        if (argc == 2 && (strcmp(argv[1], "--check-prefill-pack-reuse") == 0 || strcmp(argv[1], "--benchmark-prefill-pack-reuse") == 0)) {
            const size_t sizes[]={128,432,864};
            for(size_t in=1024;in<=2560;in+=1536)for(size_t i=0;i<3;i++){
                int rc=prefill_pack_reuse_check(r,sizes[i],in,4096,strstr(argv[1],"benchmark")!=NULL);
                if(rc){fprintf(stderr,"PREFILL_PACK_REUSE_FAIL rows=%zu in=%zu rc=%d\n",sizes[i],in,rc);return 131;}
            }
            termite_metal_decode_runtime_destroy(r);return 0;
        }
        if (argc == 2 &&
            (strcmp(argv[1], "--check-prefill-pack-reuse-large") == 0 ||
             strcmp(argv[1], "--benchmark-prefill-pack-reuse-large") == 0)) {
          const bool benchmark = strstr(argv[1], "benchmark") != NULL;
          const size_t shapes[][3] = {
              {2648, 2560, 4096}, {2648, 9216, 2560}, {2047, 1024, 4096},
              {2048, 1024, 4096}, {2049, 1024, 4096}, {4096, 1024, 4096},
              {4097, 1024, 4096}, {2049, 2560, 4096}, {4096, 2560, 4096}};
          const size_t count = benchmark ? 2 : sizeof(shapes) / sizeof(shapes[0]);
          for (size_t i = 0; i < count; i++) {
            @autoreleasepool {
              int rc = prefill_pack_reuse_check(r, shapes[i][0], shapes[i][1],
                                                shapes[i][2], benchmark);
              if (rc) {
                fprintf(stderr,
                        "PREFILL_PACK_REUSE_LARGE_FAIL rows=%zu in=%zu out=%zu rc=%d\n",
                        shapes[i][0], shapes[i][1], shapes[i][2], rc);
                return 132;
              }
            }
          }
          termite_metal_decode_runtime_destroy(r);
          return 0;
        }
        if (argc == 2 && (strcmp(argv[1], "--check-batch-attention") == 0 || strcmp(argv[1], "--benchmark-batch-attention") == 0)) {
            for(size_t heads=8;heads<=16;heads+=8)for(size_t rows=2;rows<=16;rows*=2){
                int rc=ocr_attention_batch_check(r,rows,heads,strstr(argv[1],"benchmark")!=NULL);
                if(rc){fprintf(stderr,"ATTENTION_BATCH_FAIL rc=%d\n",rc);return 125;}
            }
            if (r->lighton_attention_batch_scratch[0] == nil && r->lighton_attention_batch_scratch[1] == nil) return 126;
            if (termite_metal_decode_runtime_begin_frame(r) || termite_metal_decode_runtime_lighton_attention_batch_begin(r)) return 127;
            if (termite_metal_decode_runtime_lighton_attention_batch_release(r) != -2 || r->lighton_attention_batch == NULL) return 128;
            if (termite_metal_decode_runtime_cancel_frame(r) || termite_metal_decode_runtime_lighton_attention_batch_release(r)) return 129;
            if (r->lighton_attention_batch != NULL || r->lighton_attention_batch_scratch[0] != nil || r->lighton_attention_batch_scratch[1] != nil) return 130;
            termite_metal_decode_runtime_destroy(r);return 0;
        }
        if (argc == 2 && strcmp(argv[1], "--check-activation-pool") == 0) {
            const int rc = activation_pool_check(r);
            termite_metal_decode_runtime_destroy(r);
            return rc;
        }
        if (argc == 2 && strcmp(argv[1], "--check-wide-mpp") == 0) {
            const size_t cases[][3]={{9,1024,6144},{12,1024,4096},{13,1024,2048},{16,2048,1024},
                {9,2560,4096},{12,2560,8192},{9,1024,3584},{9,2560,9216},{13,4096,2560},
                {5,1024,248320},{8,1024,248320},{9,1024,248320},{12,1024,248320},{16,1024,248320},
                {7,2560,248320},{8,2560,248320},{9,2560,248320},{12,2560,248320},{16,2560,248320},
                {17,1024,6144},{24,1024,3584},{31,3584,1024},{32,1024,2048},
                {17,2560,8192},{24,2560,9216},{31,9216,2560},{32,4096,2560},
                {17,1024,248320},{24,2560,248320},{31,1024,248320},{32,2560,248320}};
            for(size_t i=0;i<sizeof(cases)/sizeof(cases[0]);i++){
                int rc=wide_mpp_check(r,cases[i][0],cases[i][1],cases[i][2],false);
                if(rc){fprintf(stderr,"WIDE_MPP_FAIL case=%zu rc=%d\n",i,rc);return 124;}
                if(cases[i][0]>16){
                    rc=projection(r,cases[i][0],cases[i][1],cases[i][2],4);
                    if(rc){fprintf(stderr,"WIDE_PRECISION_FAIL case=%zu rc=%d\n",i,rc);return 125;}
                }
            }
            termite_metal_decode_runtime_destroy(r);return 0;
        }
        const bool wide_tiles_benchmark = argc == 2 && strcmp(argv[1], "--benchmark-wide-projection-tiles") == 0;
        if (wide_tiles_benchmark || (argc == 2 && strcmp(argv[1], "--check-wide-projection-tiles") == 0)) {
            const size_t shapes[][2] = {{1024,6144},{1024,2048},{2048,1024},{1024,512},{1024,4096},
                {2560,8192},{2560,4096},{4096,2560},{2560,1024},{3584,1024},{9216,2560},{1024,3584},{2560,9216}};
            const size_t batches[] = {9,12,16};
            for (size_t b = 0; b < 3; b++) for (size_t shape = 0; shape < sizeof(shapes)/sizeof(shapes[0]); shape++) {
                if (projection(r,batches[b],shapes[shape][0],shapes[shape][1],4) ||
                    wide_projection_tiles_check(r,batches[b],shapes[shape][0],shapes[shape][1],wide_tiles_benchmark)) return 123;
            }
            termite_metal_decode_runtime_destroy(r);
            return 0;
        }
        const bool wide_benchmark = argc == 2 && strcmp(argv[1], "--benchmark-wide-gate-up") == 0;
        if (wide_benchmark || (argc == 2 && strcmp(argv[1], "--check-wide") == 0)) {
            setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_PARALLEL", "1", 1);
            if (!r->lightonocr3_recurrent_parallel_pipeline) return 102;
            const size_t batches[] = {9, 12, 16};
            for (size_t i = 0; i < 3; i++) {
                const size_t rows = batches[i];
                if (batch_gate_up_check(r, rows, 1024, 3584, wide_benchmark) ||
                    batch_gate_up_check(r, rows, 2560, 9216, wide_benchmark)) return 122;
                if (!wide_benchmark &&
                    (projection(r, rows, 1024, 512, 4) || projection(r, rows, 2560, 1024, 4) ||
                     projection(r, rows, 1024, 257, 4) || fused_batch_core_check(r, 16, 16, rows, false) ||
                     fused_batch_core_check(r, 16, 32, rows, false))) return 101;
            }
            termite_metal_decode_runtime_destroy(r);
            return 0;
        }
        if (argc == 2 && strcmp(argv[1], "--benchmark-prefill-core") == 0) {
            const size_t lengths[] = {128,432,662,2048};
            for (size_t h = 16; h <= 32; h += 16) for (size_t j = 0; j < 4; j++)
                if (prefill_core_check(r,lengths[j],h,true,true)) return 104;
            termite_metal_decode_runtime_destroy(r);
            return 0;
        }
        if (argc == 2 && strcmp(argv[1], "--benchmark-recurrent") == 0) {
            setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_PARALLEL", "1", 1);
            if (!r->lightonocr3_recurrent_parallel_pipeline) return 102;
            for (size_t pages = 1; pages <= 8; pages++)
                if (fused_batch_core_check(r,16,16,pages,true) || fused_batch_core_check(r,16,32,pages,true)) return 101;
            termite_metal_decode_runtime_destroy(r);
            return 0;
        }
        if (!termite_metal_decode_runtime_lightonocr3_batch_projection_available(r, 8, 2560) ||
            termite_metal_decode_runtime_lightonocr3_batch_projection_available(r, 1, 2560) ||
            !termite_metal_decode_runtime_lightonocr3_batch_projection_available(r, 9, 2560) ||
            !termite_metal_decode_runtime_lightonocr3_batch_projection_available(r, 16, 2560) ||
            termite_metal_decode_runtime_lightonocr3_batch_projection_available(r, 17, 2560) ||
            termite_metal_decode_runtime_lightonocr3_batch_projection_available(r, 8, 2561)) return 60;
        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_PROJECTIONS", "1", 1);
        if (termite_metal_decode_runtime_lightonocr3_batch_projection_available(r, 8, 2560)) return 61;
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_PROJECTIONS");
        id<MTLComputePipelineState> saved_batch8 = r->lightonocr3_batch8_pipeline;
        r->lightonocr3_batch8_pipeline = nil;
        if (termite_metal_decode_runtime_lightonocr3_batch_projection_available(r, 8, 2560)) return 62;
        r->lightonocr3_batch8_pipeline = saved_batch8;
        const bool batch_gate_benchmark=argc==2 && strcmp(argv[1],"--benchmark-batch-gate-up")==0;
        for(size_t rows=2;rows<=8;rows++)if(batch_gate_up_check(r,rows,1024,3584,batch_gate_benchmark&&(rows==7||rows==8)) || batch_gate_up_check(r,rows,2560,9216,batch_gate_benchmark&&(rows==4||rows==8)))return 122;
        if(batch_gate_benchmark)return 0;
        const size_t prefill_seqs[]={127,128,432,554,662};
        for(size_t i=0;i<5;i++)for(size_t vh=16;vh<=32;vh+=16)if(prefill_columns_check(r,prefill_seqs[i],vh))return 123;
        if(ocr_residency_check(r))return 121;
        if (!r->f16_encoder_mpp_pipeline) return 116;
        if (termite_metal_f16_encoder_mpp_shape(127,128,512) || termite_metal_f16_encoder_mpp_shape(524289,128,512) || termite_metal_f16_encoder_mpp_shape(128,129,512)) return 117;
        const bool f16_benchmark = argc == 2 && strcmp(argv[1],"--benchmark-f16-encoder") == 0;
        for (size_t width=128;width<=1024;width*=2) {
            if(f16_encoder_check(r,129,width,width,16,false) || f16_encoder_check(r,129,width,width*3,16,false) || f16_encoder_check(r,129,width,width*4,16,false) || f16_encoder_check(r,129,width*4,width,16,false)) return 118;
        }
        if(f16_encoder_check(r,578,768,768,16,false) || f16_encoder_check(r,578,3072,768,16,false) || f16_encoder_check(r,129,1024,768,16,false) || f16_encoder_check(r,127,128,512,16,false) || f16_encoder_check(r,129,128,512,4,false)) return 119;
        if(f16_encoder_check(r,25088,128,512,16,f16_benchmark) || f16_encoder_check(r,578,768,3072,16,f16_benchmark) || f16_encoder_check(r,2304,512,2048,16,f16_benchmark)) return 120;
        if(f16_benchmark)return 0;
        if (!r->lightonocr3_four_row_segment_pipeline || !r->lightonocr3_mpp_four_row_pipeline) return 114;
        id<MTLComputePipelineState> saved_four_segment = r->lightonocr3_four_row_segment_pipeline;
        id<MTLComputePipelineState> saved_four_mpp = r->lightonocr3_mpp_four_row_pipeline;
        for (size_t mode = 0; mode < 3; mode++) {
            if (mode == 1) {r->lightonocr3_four_row_segment_pipeline = nil; r->lightonocr3_mpp_four_row_pipeline = nil;}
            if (mode == 2) setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_FOUR_ROW_TILE","1",1);
            if (projection(r,4,1024,3584,4) || projection(r,4,2560,8192,4) || projection(r,4,2560,9216,4)) return 115;
            r->lightonocr3_four_row_segment_pipeline = saved_four_segment;
            r->lightonocr3_mpp_four_row_pipeline = saved_four_mpp;
        }
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_FOUR_ROW_TILE");
        if (!r->qwen35_prefill_conv_vec4_pipeline) return 111;
        const bool conv_benchmark = argc == 2 && strcmp(argv[1],"--benchmark-prefill-conv") == 0;
        const size_t conv_seqs[] = {2,3,4,17,432,554};
        for (size_t i = 0; i < 6; i++) for (size_t vh = 16; vh <= 32; vh += 16) for (size_t init = 0; init < 2; init++)
            if (prefill_conv_check(r,conv_seqs[i],vh,init != 0,conv_benchmark && conv_seqs[i] == 554)) return 112;
        if (conv_benchmark) return 0;
        if (!r->lightonocr3_down_split32_pipeline || !r->lightonocr3_down_split64_pipeline || !r->lightonocr3_down_split_reduce_pipeline) return 107;
        if(gate_split_frame_check(r,1024,3584,2)||gate_split_frame_check(r,1024,3584,3)||gate_split_frame_check(r,1024,3584,4)||gate_split_frame_check(r,2560,9216,2)||gate_split_frame_check(r,2560,9216,3)||gate_split_frame_check(r,1024,3584,5)||gate_split_frame_check(r,1024,3584,6)||gate_split_frame_check(r,1024,3584,7)||gate_split_frame_check(r,1024,3584,8)||gate_split_frame_check(r,2560,9216,4)||gate_split_frame_check(r,2560,9216,5)||gate_split_frame_check(r,2560,9216,6)||gate_split_frame_check(r,2560,9216,7))return 123;
        if (down_split_frame_check(r,3584,1024,5,false) || down_split_frame_check(r,3584,1024,6,false) || down_split_frame_check(r,3584,1024,7,false) || down_split_frame_check(r,1024,6144,5,false) || down_split_frame_check(r,1024,6144,6,false) || down_split_frame_check(r,1024,6144,7,false) || down_split_frame_check(r,3584,1024,8,false) || down_split_frame_check(r,9216,2560,8,false) || down_split_frame_check(r,1024,6144,4,false) || down_split_frame_check(r,1024,3584,4,false) || down_split_frame_check(r,2560,8192,4,false) || down_split_frame_check(r,2560,9216,4,false) || down_split_frame_check(r,9216,2560,4,false)) return 108;
        const size_t qualified_tails[][3] = {
            {1024,6144,3}, {1024,3584,3}, {1024,3584,5}, {1024,3584,6}, {1024,3584,7},
            {2560,8192,2}, {2560,8192,3}, {2560,8192,5}, {2560,8192,6}, {2560,8192,7},
            {2560,9216,2}, {2560,9216,3}, {2560,9216,5}, {2560,9216,6}, {2560,9216,7},
            {9216,2560,3}, {9216,2560,5}, {9216,2560,6}, {9216,2560,7},
        };
        for (size_t i = 0; i < sizeof(qualified_tails) / sizeof(qualified_tails[0]); i++)
            if (down_split_frame_check(r, qualified_tails[i][0], qualified_tails[i][1], qualified_tails[i][2],false)) return 124;
        for (size_t rows = 2; rows <= 8; rows++) {
            if (projection(r,rows,3584,1024,4) || projection(r,rows,9216,2560,4)) return 109;
        }
        if (!r->lightonocr3_gqa_split_stage_pipeline || !r->lightonocr3_gqa_split_reduce_pipeline) return 102;
        const bool ocr_gqa_benchmark=argc==2 && strcmp(argv[1],"--benchmark-ocr-gqa")==0;
        const size_t ocr_contexts[]={128,192,432,554,662,1024,2048,2056};
        for(size_t i=0;i<8;i++)for(size_t shape=0;shape<2;shape++)
            if(ocr_gqa_check(r,ocr_contexts[i],shape?16:8,shape?4:2,i%2?8:1,ocr_gqa_benchmark && (ocr_contexts[i]==432 || ocr_contexts[i]==662)))return 103;
        if (ocr_gqa_benchmark) return 0;
        if (argc == 2 && strcmp(argv[1], "--benchmark-down-split") == 0) {
            for (size_t rows = 2; rows <= 8; rows++)
                if (small_projection_timing(r,3584,1024,rows) || small_projection_timing(r,9216,2560,rows)) return 110;
            return 0;
        }
        if (argc == 2 && strcmp(argv[1], "--benchmark-projection-parity") == 0) {
            const size_t shapes[][2] = {{1024,6144},{1024,3584},{3584,1024},{2560,8192},{2560,9216},{9216,2560}};
            const size_t batch_rows[] = {1,4,8,128,512};
            for (size_t shape = 0; shape < 6; shape++) for (size_t row = 0; row < 5; row++)
                if (small_projection_timing(r,shapes[shape][0],shapes[shape][1],batch_rows[row])) return 113;
            return 0;
        }
        if (!r->lightonocr3_mpp_small_tile_pipeline) return 97;
        if (argc==2 && strcmp(argv[1],"--benchmark-ffn-tile")==0) {
            for (size_t rows=2;rows<=8;rows++)
                if(small_projection_timing(r,1024,3584,rows) || small_projection_timing(r,2560,9216,rows)) return 100;
            return 0;
        }
        if (argc == 2 && strcmp(argv[1], "--benchmark-qkv-tile") == 0) {
            for (size_t rows = 4; rows <= 8; rows++)
                if (small_projection_timing(r, 1024, 6144, rows) || small_projection_timing(r, 2560, 8192, rows)) return 104;
            return 0;
        }
        for (size_t rows = 4; rows <= 8; rows++) {
            if (projection(r, rows, 1024, 6144, 0) || projection(r, rows, 2560, 8192, 0)) return 105;
        }
        id<MTLComputePipelineState> saved_qkv_tile = r->lightonocr3_mpp_small_tile_pipeline;
        for (int mode = 0; mode < 2; mode++) {
            if (mode == 0) r->lightonocr3_mpp_small_tile_pipeline = nil;
            else setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_QKV_SMALL_TILE", "1", 1);
            if (projection(r, 4, 1024, 6144, 0) || projection(r, 8, 2560, 8192, 0)) return 106;
            r->lightonocr3_mpp_small_tile_pipeline = saved_qkv_tile;
        }
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_QKV_SMALL_TILE");
        id<MTLComputePipelineState> saved_small_ffn = r->lightonocr3_mpp_small_tile_pipeline;
        r->lightonocr3_mpp_small_tile_pipeline = nil;
        if (projection(r,8,1024,3584,0) || projection(r,4,2560,9216,0)) return 98;
        r->lightonocr3_mpp_small_tile_pipeline = saved_small_ffn;
        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_FFN_SMALL_TILE","1",1);
        if (projection(r,8,1024,3584,0) || projection(r,4,2560,9216,0)) return 99;
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_FFN_SMALL_TILE");
        if (!r->sdpa_f32_florence_window_parallel_pipeline) return 94;
        const bool window_benchmark=argc==2 && strcmp(argv[1],"--benchmark-florence-window")==0;
        const size_t windows[]={1,31,49,64,128,256};
        for(size_t i=0;i<6;i++)for(int layout=0;layout<2;layout++)
            if(florence_window_check(r,2,windows[i],3,layout,false))return 95;
        if(window_benchmark){if(florence_window_check(r,64,49,4,1,true))return 96;return 0;}
        if (!r->florence_channel_scores_parallel_pipeline) return 92;
        const bool florence_benchmark=argc==2 && strcmp(argv[1],"--benchmark-florence-channel")==0;
        if (florence_channel_check(r,1,1,64,2,false) || florence_channel_check(r,1,65,48,3,false) || florence_channel_check(r,2,49,96,3,false) ||
            florence_channel_check(r,1,196,192,6,florence_benchmark) || florence_channel_check(r,2,784,384,12,florence_benchmark) ||
            florence_channel_check(r,1,3136,192,6,florence_benchmark)) return 93;
        if (florence_benchmark) return 0;
        if (decode_autoscope_check(r) || down_split_frame_check(r,3584,1024,6,true) ||
            down_split_frame_check(r,9216,2560,8,true) || down_split_frame_check(r,1024,6144,4,true)) return 105;
        if (!r->qwen35_fused_decode_pipeline) return 90;
        const bool core_benchmark=argc==2 && strcmp(argv[1],"--benchmark-core")==0;
        if (fused_core_check(r,1,2,core_benchmark) || fused_core_check(r,16,16,core_benchmark) || fused_core_check(r,16,32,core_benchmark)) return 91;
        if (core_benchmark) return 0;
        setenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_PARALLEL", "1", 1);
        if (!r->lightonocr3_recurrent_parallel_pipeline) return 102;
        for (size_t pages = 1; pages <= 8; pages++)
            if (fused_batch_core_check(r,16,16,pages,false) || fused_batch_core_check(r,16,32,pages,false)) return 101;
        unsetenv("TERMITE_METAL_ENABLE_LIGHTONOCR3_RECURRENT_PARALLEL");
        const size_t prefill_lengths[] = {8,128,129,432,662,2048};
        for (size_t h = 16; h <= 32; h += 16) for (size_t j = 0; j < 6; j++)
            if (prefill_core_check(r,prefill_lengths[j],h,j%2,false)) return 104;

        if (argc == 2 && (strcmp(argv[1], "--benchmark-lifetime") == 0 || strcmp(argv[1], "--benchmark-small-rows") == 0)) {
            int rc = strcmp(argv[1], "--benchmark-lifetime") == 0 ? benchmark_lifetime(r) : benchmark_small_rows(r);
            termite_metal_decode_runtime_destroy(r);
            return rc;
        }
        const size_t cases[][4] = {{2216,768,2304,0},{4096,1024,4096,0},{8192,768,768,4},{16384,768,768,0},{5,1024,3584,0},{6,1024,3584,0},{7,1024,3584,0},{4,2560,9216,0},{5,2560,9216,0},{6,2560,9216,0},{7,2560,9216,0},{6, 1024, 6144, 0}, {7, 2560, 8192, 0}, {2, 9216, 2560, 0}, {3, 9216, 2560, 0}, {4, 2560, 9216, 0}, {5, 1024, 6144, 0}, {5, 2560, 248320, 0}, {8, 1024, 248320, 0}, {8, 1024, 3584, 0}, {8, 1024, 6144, 0}, {8, 2560, 8192, 0}, {8, 2560, 9216, 0}, {8, 9216, 2560, 0}, {1, 1024, 16, 0}, {1, 1024, 512, 0}, {1, 1024, 6144, 0}, {1, 3584, 1024, 0}, {1, 2048, 1024, 0}, {1, 1024, 248320, 0}, {33, 768, 2304, 0}, {33, 768, 3072, 0}, {33, 3072, 768, 0}, {33, 3072, 3072, 0}, {33, 3072, 1024, 0}, {33, 1024, 3584, 0}, {33, 3584, 1024, 0}, {8, 2560, 248320, 0}, {2, 1024, 3584, 0}, {3, 1024, 128, 4}, {4, 3584, 1024, 0},
                                   {5, 2560, 1024, 0}, {8, 4096, 2560, 4}, {4, 2560, 64, 0},
                                   {4, 1024, 3584, 1}, {1, 2560, 32, 0},    {1, 2560, 64, 0},    {1, 2560, 1024, 0},  {1, 4096, 2560, 4},
                                   {1, 9216, 2560, 0},  {1, 2560, 1024, 1},  {33, 2560, 1024, 0}, {65, 4096, 2560, 4},
                                   {33, 1024, 3072, 0}, {31, 2560, 1024, 0}, {33, 2560, 1024, 1}};
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            int rc = projection(r, cases[i][0], cases[i][1], cases[i][2], cases[i][3]);
            if (rc) {
                fprintf(stderr, "projection case %zu failed %d\n", i, rc);
                return 10 + rc;
            }
        }
        int rc = gate_up(r, 2560, 9216);
        if (!rc) rc = gate_up(r, 1024, 3584);
        if (rc) {
            fprintf(stderr, "gate_up failed %d\n", rc);
            return 30 + rc;
        }
        if (pack_edges(r))
            return 40;
        for(size_t row=1;row<=8;row++)
            if(projection(r,row,1024,16,4)||projection(r,row,2560,32,4))return 144;
        for(size_t model=0;model<2;model++){
            id<MTLComputePipelineState> saved=r->lightonocr3_recurrent_gate_vectors[model][7];
            r->lightonocr3_recurrent_gate_vectors[model][7]=nil;
            if(projection(r,8,model?2560:1024,model?32:16,4))return 145;
            r->lightonocr3_recurrent_gate_vectors[model][7]=saved;
        }
        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_LOCAL_KERNELS","1",1);
        if(projection(r,1,1024,16,4)||projection(r,8,2560,32,4))return 146;
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_LOCAL_KERNELS");
        if(projection(r,8,1024,16,1)||projection(r,9,2560,32,4))return 147;
        const size_t decode_shapes[][2] = {{2048, 1024}, {1024, 2048}, {1024, 512}, {1024, 4096}, {4096, 2560}, {2560, 4096}, {2560, 1024}};
        for(size_t shape=0;shape<7;shape++)for(size_t row=2;row<=8;row++)
            if(projection(r,row,decode_shapes[shape][0],decode_shapes[shape][1],4))return 148;
        id<MTLComputePipelineState> saved_chunks[4][4],saved_tails[6][3];
        for(size_t tile=0;tile<4;tile++)for(size_t row=0;row<4;row++){
            saved_chunks[tile][row]=r->lightonocr3_decode_chunks[tile][row];r->lightonocr3_decode_chunks[tile][row]=nil;
        }
        for(size_t tile=0;tile<6;tile++)for(size_t row=0;row<3;row++){
            saved_tails[tile][row]=r->lightonocr3_decode_tails[tile][row];r->lightonocr3_decode_tails[tile][row]=nil;
        }
        if(projection(r,5,1024,512,4)||projection(r,8,4096,2560,4))return 149;
        for(size_t tile=0;tile<4;tile++)for(size_t row=0;row<4;row++)r->lightonocr3_decode_chunks[tile][row]=saved_chunks[tile][row];
        for(size_t tile=0;tile<6;tile++)for(size_t row=0;row<3;row++)r->lightonocr3_decode_tails[tile][row]=saved_tails[tile][row];
        const size_t prefill_rows[] = {32, 33, 129, 432, 554, 662, 2048};
        for (size_t i = 0; i < sizeof(prefill_rows) / sizeof(prefill_rows[0]); i++) {
            if (projection(r, prefill_rows[i], 1024, 16, 4) || projection(r, prefill_rows[i], 2560, 32, 4) ||
                projection(r, prefill_rows[i], 2048, 1024, 4))
                return 140;
        }
        if(projection(r,31,1024,16,4)||projection(r,2049,2560,32,4)||projection(r,129,1024,16,1))return 143;
        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_PROJECTIONS", "1", 1);
        if (projection(r, 129, 1024, 16, 4) || projection(r, 129, 2560, 32, 4) ||
            projection(r, 129, 2048, 1024, 4))
            return 141;
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_PROJECTIONS");
        id<MTLComputePipelineState> gate08 = r->lightonocr3_prefill_gate8_16,
                                    gate4b = r->lightonocr3_prefill_gate16_32;
        r->lightonocr3_prefill_gate8_16 = nil;
        r->lightonocr3_prefill_gate16_32 = nil;
        if (projection(r, 129, 1024, 16, 4) || projection(r, 129, 2560, 32, 4))
            return 142;
        r->lightonocr3_prefill_gate8_16 = gate08;
        r->lightonocr3_prefill_gate16_32 = gate4b;
        if (cached_norm_views_check(r, 1024) || cached_norm_views_check(r, 2560))
            return 124;
        const size_t norms[][2] = {{1, 1024}, {1, 128}, {2, 1024}, {8, 1024}, {4, 128}, {1, 2560}, {4, 256}, {16, 256}, {1, 512}, {33, 2560}, {33, 256}, {9, 1024}, {12, 1024}, {16, 1024}, {17, 1024}, {16, 128}, {17, 128}, {33, 128}, {128, 1024}, {432, 1024}, {554, 2560}, {662, 2560}, {554, 512}};
        for (size_t i = 0; i < sizeof(norms) / sizeof(norms[0]); i++) {
            int nrc = norm_rows(r, norms[i][0], norms[i][1]);
            if (nrc) {
                fprintf(stderr, "norm check failed %d\n", nrc);
                return 60 + nrc;
            }
        }
        // Pipeline absence declines cleanly to the original projection implementation.
        r->lightonocr3_mpp_f32_pipeline = nil;
        if (projection(r, 33, 2560, 1024, 0))
            return 50;
        r->lightonocr3_batch4_pipeline = nil;
        if (projection(r, 4, 1024, 3584, 0))
            return 51;
        id<MTLComputePipelineState> saved_tiled = r->lightonocr3_tiled_pipelines[8];
        r->lightonocr3_tiled_pipelines[8] = nil;
        if (projection(r, 8, 1024, 248320, 0)) return 52;
        r->lightonocr3_tiled_pipelines[8] = saved_tiled;
        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_TILED_PROJECTIONS", "1", 1);
        if (projection(r, 5, 1024, 6144, 0)) return 53;
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_TILED_PROJECTIONS");
        if (compute_copy_check(r)) return 105;
        termite_metal_decode_runtime_destroy(r);
        fprintf(stderr, "All OCR Metal kernel checks passed\n");
        return 0;
    }
}
