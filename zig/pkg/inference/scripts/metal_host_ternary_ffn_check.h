// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Included by check_metal_host_ffn.m. Uniform ternary codes give an independent
// reference without using the production packing or dequantization routines.
#ifdef ANTFLY_TERNARY_FFN_BASELINE_HEADER
#include ANTFLY_TERNARY_FFN_BASELINE_HEADER
#else
#define at_before_tl1 termite_metal_decode_runtime_apply_gated_ffn_residual_tl1
#define at_before_tl2 termite_metal_decode_runtime_apply_gated_ffn_residual_tl2
#endif

typedef struct {
    uint8_t *raw;
    size_t bytes;
    uint32_t scale_off, values, signs, three_cols, two_cols;
} TernaryFfnWeight;

static TernaryFfnWeight ternary_ffn_weight(size_t in, size_t out, bool tl2, float scale) {
    TernaryFfnWeight w = {0};
    if (tl2) {
        w.three_cols = (uint32_t)(in - in % 96);
        w.two_cols = (uint32_t)(in % 96);
        w.values = (uint32_t)(out * w.three_cols / 6);
        w.signs = (uint32_t)(out * w.three_cols / 24);
        w.scale_off = w.values + w.signs + (uint32_t)(out * w.two_cols / 4);
    } else w.scale_off = (uint32_t)(out * in / 4);
    w.bytes = w.scale_off + 32;
    w.raw = calloc(1, w.bytes);
    if (w.raw) {
        memset(w.raw, tl2 ? 0xdd : 0x88, tl2 ? w.values : w.scale_off);
        if (tl2) memset(w.raw + w.values + w.signs, 0x88, out * w.two_cols / 4);
        memcpy(w.raw + w.scale_off, &scale, sizeof(scale));
    }
    return w;
}

static int ternary_ffn_call(bool before, bool tl2, termite_metal_decode_runtime *runtime,
    const float *input, const float *residual, size_t h, size_t d, unsigned activation,
    size_t norm, TernaryFfnWeight g, TernaryFfnWeight u, TernaryFfnWeight down, float *output) {
#define TL1_WEIGHT(W) (W).raw, (W).bytes, (W).scale_off, 128, 64, 32
#define TL2_WEIGHT(W) (W).raw, (W).bytes, (W).scale_off, (W).values, (W).signs, 128, 96, 32, (W).three_cols, (W).two_cols
#define TL1_CALL(Fn) Fn(runtime,input,residual,h,d,activation,TL1_WEIGHT(g),TL1_WEIGHT(u),norm,TL1_WEIGHT(down),output)
#define TL2_CALL(Fn) Fn(runtime,input,residual,h,d,activation,TL2_WEIGHT(g),TL2_WEIGHT(u),norm,TL2_WEIGHT(down),output)
    if (tl2) return before ? TL2_CALL(at_before_tl2) : TL2_CALL(termite_metal_decode_runtime_apply_gated_ffn_residual_tl2);
    return before ? TL1_CALL(at_before_tl1) : TL1_CALL(termite_metal_decode_runtime_apply_gated_ffn_residual_tl1);
#undef TL1_CALL
#undef TL2_CALL
#undef TL1_WEIGHT
#undef TL2_WEIGHT
}

// Capture only the existing command buffer timestamps: no extra GPU work.
static uint64_t ternary_ffn_gpu_duration(bool before, bool tl2, termite_metal_decode_runtime *runtime,
    const float *x, const float *r, size_t h, size_t d, size_t norm,
    TernaryFfnWeight g, TernaryFfnWeight u, TernaryFfnWeight down, float *out, bool copy) {
    uint64_t ns=0;
    for (unsigned rep=0;rep<4;++rep) {
        HostFfnTrace *trace=[HostFfnTrace new]; trace.recordInitializedBytes=YES; trace.output=out; trace.forceCopiedOutput=copy;
        id<MTLDevice> device=runtime->device; id<MTLCommandQueue> queue=runtime->queue;
        runtime->device=[HostFfnTraceProxy wrap:device trace:trace]; runtime->queue=[HostFfnTraceProxy wrap:queue trace:trace];
        int rc=ternary_ffn_call(before,tl2,runtime,x,r,h,d,0,norm,g,u,down,out);
        runtime->device=device; runtime->queue=queue;
        const double start=trace.command.GPUStartTime,end=trace.command.GPUEndTime;
        if (rc || !(start>0 && end>start)) return 0;
        ns+=(uint64_t)llround((end-start)*1e9);
    }
    return ns;
}

static int check_host_ternary_ffn(termite_metal_decode_runtime *runtime, bool benchmark) {
#ifndef ANTFLY_TERNARY_FFN_BASELINE_HEADER
    if (benchmark) { fprintf(stderr,"Ternary timing requires a frozen original-function header.\n"); return 2; }
#endif
    size_t cases = 0, failures = 0;
    for (unsigned tl2 = 0; tl2 < 2; ++tl2) for (unsigned shape = 0; shape < 2; ++shape) {
        const size_t h = 1536, d = shape ? 4096 : 1536;
        TernaryFfnWeight g = ternary_ffn_weight(h,d,tl2,1.0f/128), u = ternary_ffn_weight(h,d,tl2,1.0f/64), down = ternary_ffn_weight(d,h,tl2,1.0f/256);
        float *x, *r, *y;
        if (posix_memalign((void **)&x,16384,h*sizeof(float)) || posix_memalign((void **)&r,16384,h*sizeof(float)) ||
            posix_memalign((void **)&y,16384,h*sizeof(float)+16384) || !g.raw || !u.raw || !down.raw) return 20;
        float *original = malloc(h*sizeof(float)), *norm = malloc(d*sizeof(float));
        if (!original || !norm) return 20;
        for (size_t i=0;i<h;++i) { x[i]=1.0f/64; r[i]=(float)(i%17)/32; }
        for (size_t i=0;i<d;++i) norm[i]=1;
        if (termite_metal_decode_runtime_prepare_rms_norm(runtime,0,norm,d)) return 21;
        NSData *gs=[NSData dataWithBytes:g.raw length:g.bytes], *us=[NSData dataWithBytes:u.raw length:u.bytes], *ds=[NSData dataWithBytes:down.raw length:down.bytes];
        for (unsigned normalized=0;normalized<2;++normalized) for (unsigned copy=0;copy<2;++copy) for (unsigned activation=0;activation<=2;activation+=2) {
            float *out=y+1; out[-1]=-98765; out[h]=98765;
            const double gate=(double)h/8192, up=(double)h/4096;
            const double activated=activation==2 ? gate/(1+exp(-gate)) : .5*gate*(1+tanh(.7978845608028654*(gate+.044715*gate*gate*gate)));
            const double projected=(normalized ? 1 : (float)activated*(float)up)*(double)d/256;
            NSArray *events=nil;
            for (unsigned before=0;before<2;++before) {
                HostFfnTrace *trace=[HostFfnTrace new]; trace.recordInitializedBytes=YES; trace.output=out; trace.forceCopiedOutput=copy;
                id<MTLDevice> device=runtime->device; id<MTLCommandQueue> queue=runtime->queue;
                runtime->device=[HostFfnTraceProxy wrap:device trace:trace]; runtime->queue=[HostFfnTraceProxy wrap:queue trace:trace];
                const int rc=ternary_ffn_call(before,tl2,runtime,x,r,h,d,activation,normalized?0:SIZE_MAX,g,u,down,out);
                runtime->device=device; runtime->queue=queue;
                if (rc || trace.encoders!=6+normalized || trace.outputAliased==copy || out[-1]!=-98765 || out[h]!=98765) return 22;
                if (before && (![events isEqualToArray:trace.events] || memcmp(original,out,h*sizeof(float)))) return 23;
                if (!before) { events=[trace.events copy]; memcpy(original,out,h*sizeof(float)); }
                for (size_t i=0;i<h;++i) if (!isfinite(out[i]) || fabs(out[i]-(projected+r[i]))>3e-4*fmax(1,fabs(projected+r[i])) || x[i]!=1.0f/64 || r[i]!=(float)(i%17)/32) return 24;
            }
            if (benchmark && shape==1 && activation==0) {
                for (unsigned order=0;order<3;++order) {
                    // Warm both implementations before each AA/AB/BA sequence.
                    for (unsigned warm=0;warm<8;++warm) if (!ternary_ffn_gpu_duration(warm&1,tl2,runtime,x,r,h,d,normalized?0:SIZE_MAX,g,u,down,out,copy)) return 27;
                    for (unsigned sample=0;sample<64;++sample) {
                        uint64_t a=ternary_ffn_gpu_duration(order!=2,tl2,runtime,x,r,h,d,normalized?0:SIZE_MAX,g,u,down,out,copy);
                        uint64_t b=ternary_ffn_gpu_duration(order!=1,tl2,runtime,x,r,h,d,normalized?0:SIZE_MAX,g,u,down,out,copy);
                        if (!a || !b) return 27;
                        printf("TERNARY_GPU_TIMING %u %u %u %u %u %llu %llu\n",tl2,normalized,copy,order,sample,(unsigned long long)a,(unsigned long long)b); fflush(stdout);
                    }
                }
            }
            ++cases;
            if (copy || activation) continue;
            // Decline every intermediate/parameter allocation, each encoder,
            // command creation, and completion; retain original error precedence.
            for (unsigned mode=0;mode<4;++mode) {
                const size_t count=mode==0 ? 17+2*normalized : mode==1 ? 6+normalized : 1;
                for (size_t n=1;n<=count;++n) {
                    NSArray *first=nil;
                    int expected=mode==0 ? (n<=11+normalized ? -6 : -7) : mode==1 ? -(int)(n+8+(normalized==0 && n>=5)) : mode==2 ? -8 : -16;
                    for (unsigned before=0;before<2;++before) {
                        HostFfnTrace *trace=[HostFfnTrace new]; trace.recordInitializedBytes=YES; trace.output=out;
                        trace.failAllocation=mode==0?n:0; trace.failEncoder=mode==1?n:0; trace.failCommand=mode==2; trace.failCompletion=mode==3;
                        id<MTLDevice> device=runtime->device; id<MTLCommandQueue> queue=runtime->queue;
                        runtime->device=[HostFfnTraceProxy wrap:device trace:trace]; runtime->queue=[HostFfnTraceProxy wrap:queue trace:trace];
                        const int rc=ternary_ffn_call(before,tl2,runtime,x,r,h,d,activation,normalized?0:SIZE_MAX,g,u,down,out);
                        runtime->device=device; runtime->queue=queue;
                        if (rc!=expected || (before && ![first isEqualToArray:trace.events])) { fprintf(stderr,"Ternary failure codec=%u norm=%u mode=%u n=%zu rc=%d expected=%d\n",tl2,normalized,mode,n,rc,expected); return 25; }
                        first=[trace.events copy];
                    }
                    ++failures;
                }
            }
        }
        if (memcmp(g.raw,gs.bytes,g.bytes) || memcmp(u.raw,us.bytes,u.bytes) || memcmp(down.raw,ds.bytes,down.bytes)) return 26;
        free(g.raw);free(u.raw);free(down.raw);free(x);free(r);free(y);free(original);free(norm);
    }
    fprintf(stderr,"Ternary host FFN oracle/trace/copy/read-only checks passed: %zu cases; %zu failures\n",cases,failures);
    return 0;
}
