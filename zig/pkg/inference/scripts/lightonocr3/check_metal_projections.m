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
    const int rc = rows == 1 ? termite_metal_decode_runtime_apply_linear_device(r, 0, (__bridge void *)input,
                                                                                offset_floats * 4, in, out,
                                                                                (__bridge void *)result, 16)
                             : termite_metal_decode_runtime_apply_linear_multi_row_device(
                                   r, 0, (__bridge void *)input, offset_floats * 4, rows, in, out,
                                   (__bridge void *)result, 16);
    if (rc)
        return rc;
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
        if (bf16 && rows < 32)
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
        (rows >= 1 && rows <= 8 && (hidden == 1024 || hidden == 128));
    if (termite_metal_a4b_simd4_rms_norm_rows_preferred(r, rows, hidden) != expected)
        return 4;
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
                if(mode>0) {
                    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_SMALL_MPP","1",1);
                    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_FFN_SMALL_TILE","1",1);
                    setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_QKV_SMALL_TILE","1",1);
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
            if (mode == 1 && step == 4)
                r->qwen35_fused_decode_pipeline = nil;
            int rc = termite_metal_decode_runtime_qwen35_core_device(r, handles[mode], offsets, 1, kh, vh,
                                                                     128, 4, step != 0);
            r->qwen35_fused_decode_pipeline = saved;
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
static int fused_batch_core_check(termite_metal_decode_runtime *r, size_t kh, size_t vh) {
    const size_t channels = (2 * kh + vh) * 128;
    const size_t counts[15] = {
        channels,       channels * 4, vh,     vh,       vh,       vh,  channels * 4, channels * 4,
        vh * 128 * 128, channels,     vh * 2, vh * 128, vh * 128, 128, vh * 128};
    id<MTLBuffer> buffers[2][2][15];
    void *handles[2][2][15];
    size_t offsets[15];
    for (size_t j = 0; j < 15; j++)
        offsets[j] = 16;
    for (size_t mode = 0; mode < 2; mode++)
        for (size_t page = 0; page < 2; page++)
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
    for (size_t step = 0; step < 4; step++) {
        setenv("TERMITE_METAL_DISABLE_QWEN35_FUSED_DECODE", "1", 1);
        for (size_t page = 0; page < 2; page++)
            if (termite_metal_decode_runtime_qwen35_core_device(r, handles[0][page], offsets, 1, kh, vh, 128,
                                                                4, step != 0))
                return 2;
        unsetenv("TERMITE_METAL_DISABLE_QWEN35_FUSED_DECODE");
        id<MTLComputePipelineState> saved = r->qwen35_fused_decode_pipeline;
        if (step == 3)
            r->qwen35_fused_decode_pipeline = nil;
        if (termite_metal_decode_runtime_begin_frame(r) ||
            termite_metal_decode_runtime_qwen35_core_batch_begin(r, 2) != 1)
            return 3;
        for (size_t page = 0; page < 2; page++)
            if (termite_metal_decode_runtime_qwen35_core_device(r, handles[1][page], offsets, 1, kh, vh, 128,
                                                                4, step != 0))
                return 4;
        if (termite_metal_decode_runtime_qwen35_core_batch_end(r) ||
            termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r))
            return 5;
        r->qwen35_fused_decode_pipeline = saved;
        const size_t outputs[] = {7, 8, 9, 10, 11, 14};
        float worst = 0;
        for (size_t page = 0; page < 2; page++)
            for (size_t k = 0; k < 6; k++) {
                const size_t j = outputs[k];
                const float *gold = (float *)buffers[0][page][j].contents + 4,
                            *got = (float *)buffers[1][page][j].contents + 4;
                for (size_t i = 0; i < counts[j]; i++) {
                    if (!isfinite(got[i]) || !isfinite(gold[i]))
                        return 6;
                    worst = fmaxf(worst, fabsf(got[i] - gold[i]));
                }
            }
        for (size_t mode = 0; mode < 2; mode++)
            for (size_t page = 0; page < 2; page++)
                for (size_t j = 0; j < 15; j++) {
                    const uint32_t *raw = buffers[mode][page][j].contents;
                    for (size_t i = 0; i < 4; i++)
                        if (raw[i] != 0xdeadbeefu || raw[counts[j] + 4 + i] != 0xdeadbeefu)
                            return 7;
                }
        fprintf(stderr,
                "fused_batch_core_check pages=2 key_heads=%zu value_heads=%zu offset=16 step=%zu fallback=%d "
                "max_abs=%g\n",
                kh, vh, step, step == 3, worst);
        if (worst > 2e-4f)
            return 8;
        for (size_t mode = 0; mode < 2; mode++)
            for (size_t page = 0; page < 2; page++) {
                id<MTLBuffer> swap = buffers[mode][page][6];
                buffers[mode][page][6] = buffers[mode][page][7];
                buffers[mode][page][7] = swap;
                handles[mode][page][6] = (__bridge void *)buffers[mode][page][6];
                handles[mode][page][7] = (__bridge void *)buffers[mode][page][7];
            }
    }
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
    const char *rollback = (out == 6144 || out == 8192) ? "TERMITE_METAL_DISABLE_LIGHTONOCR3_QKV_SMALL_TILE" : "TERMITE_METAL_DISABLE_LIGHTONOCR3_FFN_SMALL_TILE";
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
                termite_metal_decode_runtime_apply_linear_multi_row_device(
                    r, 0, (__bridge void *)input, 0, rows, in, out, (__bridge void *)output, 0) ||
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

int main(int argc, const char **argv) {
    @autoreleasepool {
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
        if (!termite_metal_decode_runtime_lightonocr3_batch_projection_available(r, 8, 2560) ||
            termite_metal_decode_runtime_lightonocr3_batch_projection_available(r, 1, 2560) ||
            termite_metal_decode_runtime_lightonocr3_batch_projection_available(r, 9, 2560) ||
            termite_metal_decode_runtime_lightonocr3_batch_projection_available(r, 8, 2561)) return 60;
        setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_PROJECTIONS", "1", 1);
        if (termite_metal_decode_runtime_lightonocr3_batch_projection_available(r, 8, 2560)) return 61;
        unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_PROJECTIONS");
        id<MTLComputePipelineState> saved_batch8 = r->lightonocr3_batch8_pipeline;
        r->lightonocr3_batch8_pipeline = nil;
        if (termite_metal_decode_runtime_lightonocr3_batch_projection_available(r, 8, 2560)) return 62;
        r->lightonocr3_batch8_pipeline = saved_batch8;
        if (!r->lightonocr3_gqa_split_stage_pipeline || !r->lightonocr3_gqa_split_reduce_pipeline) return 102;
        const bool ocr_gqa_benchmark=argc==2 && strcmp(argv[1],"--benchmark-ocr-gqa")==0;
        const size_t ocr_contexts[]={128,192,432,554,662,1024,2048,2056};
        for(size_t i=0;i<8;i++)for(size_t shape=0;shape<2;shape++)
            if(ocr_gqa_check(r,ocr_contexts[i],shape?16:8,shape?4:2,i%2?8:1,ocr_gqa_benchmark && (ocr_contexts[i]==432 || ocr_contexts[i]==662)))return 103;
        if (ocr_gqa_benchmark) return 0;
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
        if (!r->qwen35_fused_decode_pipeline) return 90;
        const bool core_benchmark=argc==2 && strcmp(argv[1],"--benchmark-core")==0;
        if (fused_core_check(r,1,2,core_benchmark) || fused_core_check(r,16,16,core_benchmark) || fused_core_check(r,16,32,core_benchmark)) return 91;
        if (core_benchmark) return 0;
        if (fused_batch_core_check(r,16,16) || fused_batch_core_check(r,16,32)) return 101;
        if (argc == 2 && (strcmp(argv[1], "--benchmark-lifetime") == 0 || strcmp(argv[1], "--benchmark-small-rows") == 0)) {
            int rc = strcmp(argv[1], "--benchmark-lifetime") == 0 ? benchmark_lifetime(r) : benchmark_small_rows(r);
            termite_metal_decode_runtime_destroy(r);
            return rc;
        }
        const size_t cases[][4] = {{5,1024,3584,0},{6,1024,3584,0},{7,1024,3584,0},{4,2560,9216,0},{5,2560,9216,0},{6,2560,9216,0},{7,2560,9216,0},{6, 1024, 6144, 0}, {7, 2560, 8192, 0}, {2, 9216, 2560, 0}, {3, 9216, 2560, 0}, {4, 2560, 9216, 0}, {5, 1024, 6144, 0}, {5, 2560, 248320, 0}, {8, 1024, 248320, 0}, {8, 1024, 3584, 0}, {8, 1024, 6144, 0}, {8, 2560, 8192, 0}, {8, 2560, 9216, 0}, {8, 9216, 2560, 0}, {1, 1024, 16, 0}, {1, 1024, 512, 0}, {1, 1024, 6144, 0}, {1, 3584, 1024, 0}, {1, 2048, 1024, 0}, {1, 1024, 248320, 0}, {33, 768, 2304, 0}, {33, 768, 3072, 0}, {33, 3072, 768, 0}, {33, 3072, 3072, 0}, {33, 3072, 1024, 0}, {33, 1024, 3584, 0}, {33, 3584, 1024, 0}, {8, 2560, 248320, 0}, {2, 1024, 3584, 0}, {3, 1024, 128, 4}, {4, 3584, 1024, 0},
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
        const size_t norms[][2] = {{1, 1024}, {1, 128}, {2, 1024}, {8, 1024}, {4, 128}, {1, 2560}, {4, 256}, {16, 256}, {1, 512}, {33, 2560}};
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
        termite_metal_decode_runtime_destroy(r);
        fprintf(stderr, "All OCR Metal kernel checks passed\n");
        return 0;
    }
}
