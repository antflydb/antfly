// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
    termite_metal_decode_runtime_set_lightonocr3_4b_projections(r, 0);
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
    termite_metal_decode_runtime_set_lightonocr3_4b_projections(r, 1);
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
static int gate_up(termite_metal_decode_runtime *r) {
    const size_t in = 2560, out = 9216;
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
    termite_metal_decode_runtime_set_lightonocr3_4b_projections(r, 0);
    if (termite_metal_a4b_simd4_rms_norm_rows_preferred(r, rows, hidden))
        return 2;
    if (termite_metal_decode_runtime_apply_rms_norm_rows_device(r, 0, (__bridge void *)input, 0, rows, hidden, 1e-6f,
                                                                (__bridge void *)output, 0))
        return 3;
    memcpy(gold, output.contents, count * 4);
    termite_metal_decode_runtime_set_lightonocr3_4b_projections(r, 1);
    bool expected = rows <= 32 && (hidden == 256 || hidden == 2560);
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
int main(void) {
    @autoreleasepool {
        termite_metal_decode_runtime *r = termite_metal_decode_runtime_create();
        if (!r)
            return 1;
        termite_metal_decode_runtime_set_lightonocr3_4b_projections(r, 1);
        if (!r->lightonocr3_4b_projections_enabled) {
            fprintf(stderr, "SKIP: dispatch qualification is M4 Max\n");
            return 77;
        }
        if (!r->lightonocr3_mpp_f32_pipeline || !r->lightonocr3_mpp_bf16_pipeline || !r->lightonocr3_gemv_pipeline) {
            fprintf(stderr, "Missing qualified shader pipeline\n");
            return 2;
        }
        const size_t cases[][4] = {{1, 2560, 32, 0},    {1, 2560, 64, 0},    {1, 2560, 1024, 0},  {1, 4096, 2560, 4},
                                   {1, 9216, 2560, 0},  {1, 2560, 1024, 1},  {33, 2560, 1024, 0}, {65, 4096, 2560, 4},
                                   {33, 1024, 3072, 0}, {31, 2560, 1024, 0}, {33, 2560, 1024, 1}};
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            int rc = projection(r, cases[i][0], cases[i][1], cases[i][2], cases[i][3]);
            if (rc) {
                fprintf(stderr, "projection case %zu failed %d\n", i, rc);
                return 10 + rc;
            }
        }
        int rc = gate_up(r);
        if (rc) {
            fprintf(stderr, "gate_up failed %d\n", rc);
            return 30 + rc;
        }
        if (pack_edges(r))
            return 40;
        const size_t norms[][2] = {{1, 2560}, {4, 256}, {16, 256}, {1, 512}, {33, 2560}};
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
        termite_metal_decode_runtime_destroy(r);
        fprintf(stderr, "All projection checks passed\n");
        return 0;
    }
}
