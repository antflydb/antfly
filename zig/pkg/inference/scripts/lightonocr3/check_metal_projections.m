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
static int apply_projection(termite_metal_decode_runtime *r, size_t rows, size_t in, size_t out, id<MTLBuffer> input,
                            size_t offset_floats, id<MTLBuffer> result) {
    if (rows == 1)
        return termite_metal_decode_runtime_apply_linear_device(r, 0, (__bridge void *)input, offset_floats * 4, in,
                                                                out, (__bridge void *)result, 16);
    return termite_metal_decode_runtime_apply_linear_multi_row_device(r, 0, (__bridge void *)input, offset_floats * 4,
                                                                      rows, in, out, (__bridge void *)result, 16);
}
static int projection(termite_metal_decode_runtime *r, size_t rows, size_t in, size_t out, size_t offset_floats) {
    id<MTLBuffer> input = [r->device newBufferWithLength:(rows * in + offset_floats) * 4
                                                 options:MTLResourceStorageModeShared];
    id<MTLBuffer> result = [r->device newBufferWithLength:(rows * out + 4) * 4 options:MTLResourceStorageModeShared];
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
    for (int bf16 = 0; bf16 < 2; bf16++) {
        if (bf16 && rows < 32)
            continue;
        if (bf16) {
            setenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS", "1", 1);
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
    const char *modes[] = {"tiled", "vec4", "generic", "tensorops_f32", "tensorops_bf16"};
    for (size_t shape = 0; shape < sizeof(shapes)/sizeof(shapes[0]); shape++) {
        const size_t in = shapes[shape][0], out = shapes[shape][1];
        uint16_t *weights = malloc(in*out*2);
        float *bias = calloc(out, 4);
        if (!weights || !bias) return 1;
        for (size_t i=0;i<in*out;i++) weights[i]=0x3c80u+(uint16_t)(i%17);
        if (termite_metal_decode_runtime_prepare_linear_bf16(r, 0, (uint8_t *)weights, in*out*2, bias, in, out)) return 2;
        free(weights); free(bias);
        for (size_t b=0;b<3;b++) {
            const size_t rows=batch_sizes[b];
            id<MTLBuffer> input = [r->device newBufferWithLength:rows*in*4 options:MTLResourceStorageModeShared];
            id<MTLBuffer> output = [r->device newBufferWithLength:rows*out*4 options:MTLResourceStorageModePrivate];
            if (!input || !output) return 3;
            float *values=input.contents;
            for(size_t i=0;i<rows*in;i++) values[i]=sinf((float)i*.01f);
            for(size_t mode=0;mode<5;mode++) {
                unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_PROJECTIONS");
                unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_TILED_PROJECTIONS");
                unsetenv("TERMITE_METAL_LIGHTONOCR3_SMALL_MPP");
                unsetenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS");
                setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_SMALL_MPP","1",1);
                if(mode==1)setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_TILED_PROJECTIONS","1",1);
                if(mode==2)setenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_BATCH_PROJECTIONS","1",1);
                if(mode>=3)unsetenv("TERMITE_METAL_DISABLE_LIGHTONOCR3_SMALL_MPP");
                if(mode>=3)setenv("TERMITE_METAL_LIGHTONOCR3_SMALL_MPP","1",1);
                if(mode==4)setenv("TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS","1",1);
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
        if (argc == 2 && (strcmp(argv[1], "--benchmark-lifetime") == 0 || strcmp(argv[1], "--benchmark-small-rows") == 0)) {
            int rc = strcmp(argv[1], "--benchmark-lifetime") == 0 ? benchmark_lifetime(r) : benchmark_small_rows(r);
            termite_metal_decode_runtime_destroy(r);
            return rc;
        }
        const size_t cases[][4] = {{6, 1024, 6144, 0}, {7, 2560, 8192, 0}, {2, 9216, 2560, 0}, {3, 9216, 2560, 0}, {4, 2560, 9216, 0}, {5, 1024, 6144, 0}, {5, 2560, 248320, 0}, {8, 1024, 248320, 0}, {8, 1024, 3584, 0}, {8, 1024, 6144, 0}, {8, 2560, 8192, 0}, {8, 2560, 9216, 0}, {8, 9216, 2560, 0}, {1, 1024, 16, 0}, {1, 1024, 512, 0}, {1, 1024, 6144, 0}, {1, 3584, 1024, 0}, {1, 2048, 1024, 0}, {1, 1024, 248320, 0}, {33, 768, 2304, 0}, {33, 768, 3072, 0}, {33, 3072, 768, 0}, {33, 3072, 3072, 0}, {33, 3072, 1024, 0}, {33, 1024, 3584, 0}, {33, 3584, 1024, 0}, {8, 2560, 248320, 0}, {2, 1024, 3584, 0}, {3, 1024, 128, 4}, {4, 3584, 1024, 0},
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
        fprintf(stderr, "All projection checks passed\n");
        return 0;
    }
}
