// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Standalone checks for the host-backed K-block FFN execution family.
// clang -O3 -fobjc-arc -framework Foundation -framework Metal \
//   -framework MetalPerformanceShaders -framework Accelerate check_metal_host_ffn.m -o check-host-ffn
#include "../src/backends/metal_kernels.m"
#include "metal_host_ffn_trace.h"
#include "metal_host_ternary_ffn_check.h"

typedef int (*HostFfn)(termite_metal_decode_runtime *, const float *, const float *, size_t,
    size_t, size_t, uint32_t, size_t, size_t, size_t, size_t, size_t, float *);
#ifdef ANTFLY_HOST_FFN_BASELINE_HEADER
#include ANTFLY_HOST_FFN_BASELINE_HEADER
#endif

static const HostFfn host_ffn_candidates[] = {
    termite_metal_decode_runtime_apply_gated_ffn_residual_q4_k_slots,
    termite_metal_decode_runtime_apply_gated_ffn_residual_q6_k_slots,
    termite_metal_decode_runtime_apply_gated_ffn_residual_q4_k_pair_q5_k_down_slots,
    termite_metal_decode_runtime_apply_gated_ffn_residual_q4_k_pair_q6_k_down_slots,
};
#ifdef ANTFLY_HOST_FFN_BASELINE_HEADER
static const HostFfn host_ffn_baselines[] = {
    as_before_q4_k_slots, as_before_q6_k_slots,
    as_before_q4_k_pair_q5_k_down_slots, as_before_q4_k_pair_q6_k_down_slots,
};
#else
#define host_ffn_baselines host_ffn_candidates
#endif
static const uint32_t host_gate_formats[] = {
    TERMITE_METAL_QUANT_FORMAT_Q4_K, TERMITE_METAL_QUANT_FORMAT_Q6_K,
    TERMITE_METAL_QUANT_FORMAT_Q4_K, TERMITE_METAL_QUANT_FORMAT_Q4_K,
};
static const uint32_t host_down_formats[] = {
    TERMITE_METAL_QUANT_FORMAT_Q4_K, TERMITE_METAL_QUANT_FORMAT_Q6_K,
    TERMITE_METAL_QUANT_FORMAT_Q5_K, TERMITE_METAL_QUANT_FORMAT_Q6_K,
};

// Independently construct blocks with one known value per 256-value block.
// Scale/code patterns vary by plane, row and block; no production dequantizer
// participates in the reference. All scales are exactly representable halves.
static uint8_t *host_weights(uint32_t format, size_t in, size_t out, size_t plane, float *dense, size_t *bytes) {
    const size_t block_bytes = format == TERMITE_METAL_QUANT_FORMAT_Q4_K ? 144 : format == TERMITE_METAL_QUANT_FORMAT_Q5_K ? 176 : 210;
    const size_t blocks = in / 256;
    *bytes = out * blocks * block_bytes;
    uint8_t *raw = calloc(1, *bytes);
    if (raw == NULL) return NULL;
    for (size_t row = 0; row < out; ++row) for (size_t b = 0; b < blocks; ++b) {
        const size_t seed = row * 7 + b * 3 + plane * 11;
        const unsigned nibble = 1 + seed % 14;
        const unsigned high = (seed / 3) % 4;
        const _Float16 scale = (_Float16)((1 + seed % 11) / 2048.0f);
        uint8_t *block = raw + (row * blocks + b) * block_bytes;
        int code;
        if (format == TERMITE_METAL_QUANT_FORMAT_Q6_K) {
            memset(block, nibble * 17, 128);
            memset(block + 128, high * 85, 64);
            memset(block + 192, 1, 16);
            memcpy(block + 208, &scale, 2);
            code = (int)nibble + 16 * (int)high - 32;
        } else {
            memcpy(block, &scale, 2);
            memset(block + 4, 1, 12);
            const bool q5 = format == TERMITE_METAL_QUANT_FORMAT_Q5_K;
            if (q5) memset(block + 16, seed % 2 ? 255 : 0, 32);
            memset(block + (q5 ? 48 : 16), nibble * 17, 128);
            code = (int)nibble + (q5 && seed % 2 ? 16 : 0);
        }
        for (size_t k = 0; k < 256; ++k) dense[row * in + b * 256 + k] = (float)scale * code;
    }
    return raw;
}

static void host_reference(const float *input, const float *residual, const float *gate_weight,
    const float *up_weight, const float *down_weight, const float *gate_norm, const float *down_norm,
    size_t rows, size_t h, size_t d, uint32_t activation, unsigned norms, float *output) {
    float *product = malloc(d * sizeof(float));
    for (size_t row = 0; row < rows; ++row) {
        for (size_t j = 0; j < d; ++j) {
            double g = 0, u = 0;
            for (size_t k = 0; k < h; ++k) {
                g += (double)input[row * h + k] * gate_weight[j * h + k];
                u += (double)input[row * h + k] * up_weight[j * h + k];
            }
            const float gf = (float)g, uf = (float)u;
            const double activated = activation == 2 ? gf / (1.0 + exp(-(double)gf)) :
                0.5 * gf * (1.0 + tanh(0.7978845608028654 * (gf + 0.044715 * gf * gf * gf)));
            product[j] = (float)activated * uf;
        }
        if (norms & 1) {
            double squares = 0;
            for (size_t j = 0; j < d; ++j) squares += (double)product[j] * product[j];
            const double scale = 1.0 / sqrt(squares / d);
            for (size_t j = 0; j < d; ++j) product[j] = (float)(product[j] * scale * gate_norm[j]);
        }
        for (size_t j = 0; j < h; ++j) {
            double value = 0;
            for (size_t k = 0; k < d; ++k) value += (double)product[k] * down_weight[j * d + k];
            output[row * h + j] = (float)value;
        }
        if (norms & 2) {
            double squares = 0;
            for (size_t j = 0; j < h; ++j) squares += (double)output[row * h + j] * output[row * h + j];
            const double scale = 1.0 / sqrt(squares / h);
            for (size_t j = 0; j < h; ++j) output[row * h + j] = (float)(output[row * h + j] * scale * down_norm[j]);
        }
        for (size_t j = 0; j < h; ++j) output[row * h + j] += residual[row * h + j];
    }
    free(product);
}

static HostFfnTrace *host_trace_call(HostFfn fn, termite_metal_decode_runtime *runtime,
    const float *input, const float *residual, size_t rows, size_t h, size_t d,
    unsigned activation, unsigned norms, float *output, BOOL forceCopy, NSUInteger failEncoder, BOOL failCommand, BOOL failCompletion, NSUInteger failAllocation, int *status) {
    HostFfnTrace *trace = [HostFfnTrace new];
    trace.failAllocation = failAllocation; trace.failEncoder = failEncoder; trace.failCommand = failCommand; trace.failCompletion = failCompletion; trace.output = output; trace.forceCopiedOutput = forceCopy;
    id<MTLCommandQueue> queue = runtime->queue;
    id<MTLDevice> device = runtime->device;
    runtime->device = [HostFfnTraceProxy wrap:device trace:trace];
    runtime->queue = [HostFfnTraceProxy wrap:queue trace:trace];
    *status = fn(runtime,input,residual,rows,h,d,activation,0,1,norms & 1 ? 0 : SIZE_MAX,norms & 2 ? 1 : SIZE_MAX,2,output);
    runtime->queue = queue;
    runtime->device = device;
    return trace;
}

static bool host_weight_unchanged(termite_metal_decode_runtime *runtime, uint32_t format,
    size_t slot, size_t in, size_t out, const void *bytes, size_t length) {
    termite_metal_quant_linear_slot_view view;
    if (termite_metal_decode_runtime_require_quant_linear_slot(runtime,format,slot,in,out,&view)) return false;
    id<MTLBuffer> copy = [runtime->device newBufferWithLength:length options:MTLResourceStorageModeShared];
    id<MTLCommandBuffer> command = [runtime->queue commandBuffer];
    id<MTLBlitCommandEncoder> blit = [command blitCommandEncoder];
    [blit copyFromBuffer:view.weight_buffer sourceOffset:view.weight_offset toBuffer:copy destinationOffset:0 size:length];
    [blit endEncoding]; [command commit]; [command waitUntilCompleted];
    return command.status == MTLCommandBufferStatusCompleted && memcmp(copy.contents,bytes,length)==0;
}

static int host_admission(HostFfn fn, termite_metal_decode_runtime *runtime, const float *input, const float *residual, float *output) {
    for (unsigned which = 0; which < 20; ++which) {
        termite_metal_decode_runtime *rt = runtime;
        const float *x = input, *r = residual; float *y = output;
        size_t rows = 1, h = 256, d = 512, g = 0, u = 1, down = 2, gn = SIZE_MAX, dn = SIZE_MAX;
        int expected;
        switch (which) {
            case 0: rt = NULL; expected = -1; break;
            case 1: x = NULL; expected = -1; break;
            case 2: r = NULL; expected = -1; break;
            case 3: y = NULL; expected = -1; break;
            case 4: rows = 0; expected = -3; break;
            case 5: rows = (size_t)UINT32_MAX + 1; expected = -3; break;
            case 6: h = 0; expected = -3; break;
            case 7: d = 513; expected = -4; break;
            case 8: g = TERMITE_METAL_LINEAR_SLOT_CAPACITY; expected = -5; break;
            case 9: u = TERMITE_METAL_LINEAR_SLOT_CAPACITY; expected = -5; break;
            case 10: down = TERMITE_METAL_LINEAR_SLOT_CAPACITY; expected = -5; break;
            case 11: g = 99; expected = -6; break;
            case 12: u = 99; expected = -7; break;
            case 13: down = 99; expected = -8; break;
            case 14: gn = TERMITE_METAL_RMS_NORM_SLOT_CAPACITY; expected = -9; break;
            case 15: gn = 99; expected = -10; break;
            case 16: gn = 1; expected = -10; break;
            case 17: dn = TERMITE_METAL_RMS_NORM_SLOT_CAPACITY; expected = -11; break;
            case 18: dn = 99; expected = -12; break;
            default: dn = 0; expected = -12; break;
        }
        if (fn(rt,x,r,rows,h,d,0,g,u,gn,dn,down,y) != expected) return 13;
    }
    id<MTLComputePipelineState> activation = runtime->activation_pipeline;
    runtime->activation_pipeline = nil;
    int result = fn(runtime,input,residual,0,0,0,0,0,1,SIZE_MAX,SIZE_MAX,2,output);
    runtime->activation_pipeline = activation;
    return result == -2 ? 0 : 14;
}

static uint64_t host_now(void) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC_RAW, &t);
    return (uint64_t)t.tv_sec * 1000000000ull + t.tv_nsec;
}
static uint64_t host_duration(HostFfn fn, termite_metal_decode_runtime *runtime,
    const float *input, const float *residual, size_t rows, size_t h, size_t d, unsigned norms, float *output, size_t reps) {
    const uint64_t start = host_now();
    for (size_t i = 0; i < reps; ++i) if (fn(runtime, input, residual, rows, h, d, 0, 0, 1,
        norms & 1 ? 0 : SIZE_MAX, norms & 2 ? 1 : SIZE_MAX, 2, output) != 0) return 0;
    return host_now() - start;
}

// GPU timestamps exclude host allocation/encoding/wait scheduling. The proxy
// captures the real completed command buffer; it adds no device commands.
static uint64_t host_gpu_duration(HostFfn fn, termite_metal_decode_runtime *runtime,
    const float *input, const float *residual, size_t rows, size_t h, size_t d, unsigned norms, float *output, size_t reps) {
    uint64_t total = 0;
    for (size_t i = 0; i < reps; ++i) {
        int status;
        HostFfnTrace *trace = host_trace_call(fn,runtime,input,residual,rows,h,d,0,norms,output,
            ((uintptr_t)output & 4095) != 0,0,NO,NO,0,&status);
        if (status || trace.command == nil) return 0;
        const double start = trace.command.GPUStartTime, end = trace.command.GPUEndTime;
        if (!(start > 0 && end > start)) return 0;
        total += (uint64_t)llround((end-start)*1e9);
    }
    return total;
}

int main(int argc, const char **argv) { @autoreleasepool {
    const bool gpu_timestamps = argc == 2 && strcmp(argv[1], "--benchmark-gpu") == 0;
    const bool benchmark = gpu_timestamps || (argc == 2 && strcmp(argv[1], "--benchmark") == 0);
#ifndef ANTFLY_HOST_FFN_BASELINE_HEADER
    if (benchmark) { fprintf(stderr,"Benchmarking requires a frozen original-function header.\n"); return 2; }
#endif
    termite_metal_decode_runtime *runtime = termite_metal_decode_runtime_create();
    if (runtime == NULL) return 1;
    if (argc == 2 && (strcmp(argv[1], "--benchmark-ternary-gpu") == 0 || strcmp(argv[1], "--benchmark-ternary-host") == 0)) {
        ternary_ffn_host_timing = strcmp(argv[1], "--benchmark-ternary-host") == 0;
        const int rc = check_host_ternary_ffn(runtime, true);
        termite_metal_decode_runtime_destroy(runtime);
        return rc;
    }
    size_t checked = 0, failures_checked = 0, alias_outputs = 0, copied_outputs = 0;
    for (size_t family = 0; family < 4; ++family) {
        const size_t h = 256, d = 512, capacity = 16 * h;
        float *gw = malloc(d*h*sizeof(float)), *uw = malloc(d*h*sizeof(float)), *dw = malloc(h*d*sizeof(float));
        size_t gb, ub, db;
        uint8_t *gr = host_weights(host_gate_formats[family], h, d, 0, gw, &gb);
        uint8_t *ur = host_weights(host_gate_formats[family], h, d, 1, uw, &ub);
        uint8_t *dr = host_weights(host_down_formats[family], d, h, 2, dw, &db);
        float *input, *residual, *output_base, *reference, *before, *gn, *dn;
        if (posix_memalign((void **)&input, 16384, capacity*sizeof(float)) || posix_memalign((void **)&residual, 16384, capacity*sizeof(float)) ||
            posix_memalign((void **)&output_base, 16384, capacity*sizeof(float)+16384)) return 2;
        reference = malloc(capacity*sizeof(float)); before = malloc(capacity*sizeof(float)); gn = malloc(d*sizeof(float)); dn = malloc(h*sizeof(float));
        for (size_t i = 0; i < capacity; ++i) { input[i] = sinf(i * 0.17f) * 0.1f + 0.02f; residual[i] = cosf(i * 0.23f) * 0.2f; }
        for (size_t i = 0; i < d; ++i) gn[i] = 0.8f + (i % 7) * 0.03f;
        for (size_t i = 0; i < h; ++i) dn[i] = 0.7f + (i % 11) * 0.02f;
        if (gr == NULL || ur == NULL || dr == NULL ||
            termite_metal_decode_runtime_prepare_quantized_linear_slot(runtime, host_gate_formats[family], 0, gr, gb, h, d) ||
            termite_metal_decode_runtime_prepare_quantized_linear_slot(runtime, host_gate_formats[family], 1, ur, ub, h, d) ||
            termite_metal_decode_runtime_prepare_quantized_linear_slot(runtime, host_down_formats[family], 2, dr, db, d, h) ||
            termite_metal_decode_runtime_prepare_rms_norm(runtime, 0, gn, d) || termite_metal_decode_runtime_prepare_rms_norm(runtime, 1, dn, h)) return 3;
        if (host_admission(host_ffn_baselines[family],runtime,input,residual,before) ||
            host_admission(host_ffn_candidates[family],runtime,input,residual,output_base)) return 13;
        NSData *input_snapshot = [NSData dataWithBytes:input length:capacity*sizeof(float)];
        NSData *residual_snapshot = [NSData dataWithBytes:residual length:capacity*sizeof(float)];
        for (size_t rows = 1; rows <= 16; rows = rows == 1 ? 3 : rows == 3 ? 8 : 16) {
            for (unsigned norms = 0; norms < 4; ++norms) for (unsigned unaligned = 0; unaligned < 2; ++unaligned) for (unsigned activation = 0; activation <= 2; activation += 2) {
                float *output = output_base + (unaligned ? 1 : 0);
                output[rows*h] = 98765.0f;
                if (unaligned) output[-1] = -98765.0f;
                host_reference(input, residual, gw, uw, dw, gn, dn, rows, h, d, activation, norms, reference);
                const size_t gate_norm = norms & 1 ? 0 : SIZE_MAX, down_norm = norms & 2 ? 1 : SIZE_MAX;
                if (host_ffn_baselines[family](runtime, input, residual, rows, h, d, activation, 0, 1, gate_norm, down_norm, 2, before) ||
                    host_ffn_candidates[family](runtime, input, residual, rows, h, d, activation, 0, 1, gate_norm, down_norm, 2, output)) return 4;
                int before_status, after_status;
                HostFfnTrace *old_trace = host_trace_call(host_ffn_baselines[family],runtime,input,residual,rows,h,d,activation,norms,before,unaligned,0,NO,NO,0,&before_status);
                HostFfnTrace *new_trace = host_trace_call(host_ffn_candidates[family],runtime,input,residual,rows,h,d,activation,norms,output,unaligned,0,NO,NO,0,&after_status);
                if (before_status || after_status || new_trace.encoders != 5 + !!(norms & 1) + !!(norms & 2) || ![old_trace.events isEqualToArray:new_trace.events]) {
                    fprintf(stderr,"HOST_FFN_TRACE_FAIL family=%zu rows=%zu norms=%u\nOLD %s\nNEW %s\n",family,rows,norms,old_trace.events.description.UTF8String,new_trace.events.description.UTF8String);return 8;
                }
                if (new_trace.outputAliased) ++alias_outputs; else ++copied_outputs;
                if (rows == 1 && activation == 0 && unaligned == 1) {
                    int errors[7] = {-16,-17,-18}; size_t error_count = 3;
                    if (norms & 1) errors[error_count++] = -19;
                    errors[error_count++] = -20;
                    if (norms & 2) errors[error_count++] = -21;
                    errors[error_count++] = -22;
                    for (NSUInteger fail = 0; fail <= new_trace.encoders + 1; ++fail) {
                        HostFfnTrace *old_fail = host_trace_call(host_ffn_baselines[family],runtime,input,residual,rows,h,d,activation,norms,before,YES,fail,fail==0,fail==new_trace.encoders+1,0,&before_status);
                        HostFfnTrace *new_fail = host_trace_call(host_ffn_candidates[family],runtime,input,residual,rows,h,d,activation,norms,output,YES,fail,fail==0,fail==new_trace.encoders+1,0,&after_status);
                        const int expected = fail == 0 ? -15 : fail == error_count+1 ? -23 : errors[fail-1];
                        if (before_status != expected || before_status != after_status || ![old_fail.events isEqualToArray:new_fail.events]) return 9;
                        ++failures_checked;
                    }
                    for (NSUInteger allocation = 1; allocation <= 3; ++allocation) {
                        HostFfnTrace *old_fail = host_trace_call(host_ffn_baselines[family],runtime,input,residual,rows,h,d,activation,norms,before,YES,0,NO,NO,allocation,&before_status);
                        HostFfnTrace *new_fail = host_trace_call(host_ffn_candidates[family],runtime,input,residual,rows,h,d,activation,norms,output,YES,0,NO,NO,allocation,&after_status);
                        if (before_status != -13 || before_status != after_status || ![old_fail.events isEqualToArray:new_fail.events]) return 12;
                        ++failures_checked;
                    }
                    // Failure injection may submit the final command before
                    // reporting a completion error. Restore the success output.
                    if (host_ffn_candidates[family](runtime,input,residual,rows,h,d,activation,0,1,gate_norm,down_norm,2,output)) return 4;
                }
                for (size_t i = 0; i < rows*h; ++i) {
                    const float bound = 0.0002f + 0.0002f * fabsf(reference[i]);
                    if (!isfinite(output[i]) || fabsf(output[i]-reference[i]) > bound || output[i] != before[i]) {
                        fprintf(stderr,"HOST_FFN_FAIL family=%zu rows=%zu norms=%u unaligned=%u activation=%u index=%zu actual=%g reference=%g before=%g\n",family,rows,norms,unaligned,activation,i,output[i],reference[i],before[i]); return 5;
                    }
                }
                if (output[rows*h] != 98765.0f || (unaligned && output[-1] != -98765.0f)) return 6;
                ++checked;
                if (benchmark && (rows == 1 || rows == 8) && (norms == 0 || norms == 3) && activation == 0) {
                    for (unsigned order = 0; order < 3; ++order) for (unsigned sample = 0; sample < 64; ++sample) {
                        HostFfn first = order == 2 ? host_ffn_candidates[family] : host_ffn_baselines[family];
                        HostFfn second = order == 0 ? host_ffn_baselines[family] : order == 1 ? host_ffn_candidates[family] : host_ffn_baselines[family];
                        const uint64_t a = (gpu_timestamps ? host_gpu_duration : host_duration)(first,runtime,input,residual,rows,h,d,norms,output,4);
                        const uint64_t b = (gpu_timestamps ? host_gpu_duration : host_duration)(second,runtime,input,residual,rows,h,d,norms,output,4);
                        if (!a || !b) return 7;
                        printf("HOST_FFN_%sTIMING %zu %zu %u %u %u %u %llu %llu\n",gpu_timestamps ? "GPU_" : "",family,rows,norms,unaligned,order,sample,(unsigned long long)a,(unsigned long long)b); fflush(stdout);
                    }
                }
            }
            if (rows == 16) break;
        }
        if (memcmp(input,input_snapshot.bytes,input_snapshot.length) || memcmp(residual,residual_snapshot.bytes,residual_snapshot.length) ||
            !host_weight_unchanged(runtime,host_gate_formats[family],0,h,d,gr,gb) ||
            !host_weight_unchanged(runtime,host_gate_formats[family],1,h,d,ur,ub) ||
            !host_weight_unchanged(runtime,host_down_formats[family],2,d,h,dr,db)) return 10;
        free(gw);free(uw);free(dw);free(gr);free(ur);free(dr);free(input);free(residual);free(output_base);free(reference);free(before);free(gn);free(dn);
    }
    if (!benchmark) { const int rc = check_host_ternary_ffn(runtime, false); if (rc) return rc; }
    termite_metal_decode_runtime_destroy(runtime);
    if (alias_outputs == 0 || copied_outputs == 0) { fprintf(stderr,"HOST_COPY_COVERAGE alias=%zu copied=%zu checked=%zu failures=%zu\n",alias_outputs,copied_outputs,checked,failures_checked); return 11; }
    fprintf(stderr,"Host FFN oracle/trace/copy/canary/read-only checks passed: %zu cases; %zu failures; %zu aliased / %zu copied outputs\n",checked,failures_checked,alias_outputs,copied_outputs);
    return 0;
} }
