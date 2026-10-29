// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#include "../src/backends/metal_kernels.m"
#include "metal_host_ffn_trace.h"
#ifdef ANTFLY_TERNARY_ATTENTION_BASELINE
#include ANTFLY_TERNARY_ATTENTION_BASELINE
#endif

typedef struct {
    bool tl2, span;
    size_t in, out, bytes, heads;
    uint32_t scale, values, signs, three, two, bm, by, bmm;
    uint8_t *raw, *keys;
    float *input, *residual, *q, *v, *pre, *post, *output;
} AttentionCase;

static int attention_call(AttentionCase *c, termite_metal_decode_runtime *runtime, bool before, bool unfixed, unsigned norms, float eps) {
    size_t pre=norms&1?0:SIZE_MAX,post=norms&2?1:SIZE_MAX;
#ifdef ANTFLY_TERNARY_ATTENTION_BASELINE
#define TL1 c->raw,c->bytes,c->scale,c->bm,c->by,c->bmm
#define TL2 c->raw,c->bytes,c->scale,c->values,c->signs,c->bm,c->by,c->bmm,c->three,c->two
#define SPAN 0,c->q,c->keys,c->v,3,c->heads,2,256,256,512,256,2,0,0
    if(unfixed && !c->span) return c->tl2 ? av_unfixed_apply_attention_residual_tl2(runtime,c->input,pre,TL2,post,c->residual,c->in,c->out,eps,c->output) : av_unfixed_apply_attention_residual_tl1(runtime,c->input,pre,TL1,post,c->residual,c->in,c->out,eps,c->output);
    if(before) {
        if(c->span) return c->tl2 ? av_before_apply_attention_span_residual_tl2(runtime,SPAN,pre,TL2,post,c->residual,c->in,c->out,eps,c->output) : av_before_apply_attention_span_residual_tl1(runtime,SPAN,pre,TL1,post,c->residual,c->in,c->out,eps,c->output);
        return c->tl2 ? av_before_apply_attention_residual_tl2(runtime,c->input,pre,TL2,post,c->residual,c->in,c->out,eps,c->output) : av_before_apply_attention_residual_tl1(runtime,c->input,pre,TL1,post,c->residual,c->in,c->out,eps,c->output);
    }
#undef TL1
#undef TL2
#undef SPAN
#endif
    if(c->span) return termite_metal_decode_runtime_apply_attention_span_residual_ternary_slot(runtime,0,c->q,c->keys,c->v,3,c->heads,2,256,256,512,256,2,0,0,pre,0,post,c->residual,c->in,c->out,eps,c->output);
    return termite_metal_decode_runtime_apply_attention_residual_ternary_slot(runtime,c->input,pre,0,post,c->residual,c->in,c->out,eps,c->output);
}

static double attention_reference(AttentionCase *c, unsigned norms, float eps, float *reference) {
    float *x=malloc(c->in*sizeof(float));double squares=0;
    for(size_t i=0;i<c->in;++i) squares+=(double)c->input[i]*c->input[i];
    double scale=norms&1?1/sqrt(squares/c->in+eps):1;
    float maximum=0;
    for(size_t i=0;i<c->in;++i) {x[i]=norms&1?(float)(c->input[i]*scale*c->pre[i]):c->input[i];maximum=fmaxf(maximum,fabsf(x[i]));}
    const float step=maximum==0?1:maximum/127;
    double dot=0;for(size_t i=0;i<c->in;++i)dot+=(double)(roundf(x[i]/step)*step)/128;
    const float projected=(float)dot;
    const double post_scale=norms&2?1/sqrt((double)projected*projected+eps):1;
    for(size_t i=0;i<c->out;++i)reference[i]=(norms&2?(float)(projected*post_scale*c->post[i]):projected)+c->residual[i];
    free(x);return dot;
}

static HostFfnTrace *attention_trace(AttentionCase *c, termite_metal_decode_runtime *runtime, bool before, bool unfixed, unsigned norms, float eps, bool copied, size_t failure, size_t encoder, bool command, bool completion, int *rc) {
    HostFfnTrace *trace=[HostFfnTrace new];trace.output=c->output;trace.forceCopiedOutput=copied;trace.recordInitializedBytes=YES;
    trace.failAllocation=failure;trace.failEncoder=encoder;trace.failCommand=command;trace.failCompletion=completion;
    id<MTLDevice> device=runtime->device;id<MTLCommandQueue> queue=runtime->queue;
    runtime->device=[HostFfnTraceProxy wrap:device trace:trace];runtime->queue=[HostFfnTraceProxy wrap:queue trace:trace];
    *rc=attention_call(c,runtime,before,unfixed,norms,eps);
    runtime->device=device;runtime->queue=queue;return trace;
}

static NSArray *attention_commands(HostFfnTrace *trace) {
    NSMutableArray *events=[NSMutableArray new];
    for(NSString *event in trace.events) if(![event hasPrefix:@"alias "] && ![event hasPrefix:@"copy "] && ![event hasPrefix:@"allocate "]) [events addObject:event];
    return events;
}

static uint64_t attention_now(void) {
    struct timespec t;clock_gettime(CLOCK_MONOTONIC_RAW,&t);return (uint64_t)t.tv_sec*1000000000ull+t.tv_nsec;
}
static uint64_t attention_duration(AttentionCase *c, termite_metal_decode_runtime *runtime, bool before, unsigned norms, bool gpu, bool copied) {
    const uint64_t start=attention_now();uint64_t total=0;
    for(size_t rep=0;rep<4;++rep) {
        int rc;
        if(gpu) {
            HostFfnTrace *trace=attention_trace(c,runtime,before,false,norms,1e-5f,copied,0,0,false,false,&rc);
            const double begin=trace.command.GPUStartTime,end=trace.command.GPUEndTime;
            if(rc || !(begin>0 && end>begin))return 0;
            total+=(uint64_t)llround((end-begin)*1e9);
        } else if(attention_call(c,runtime,before,false,norms,1e-5f))return 0;
    }
    return gpu?total:attention_now()-start;
}

static int prepare_attention(AttentionCase *c, termite_metal_decode_runtime *runtime) {
    c->out=1536;c->heads=c->in/256;c->bm=c->in==1536?128:256;c->by=c->tl2?(c->in==1536?192:96):(c->in==1536?64:128);c->bmm=c->tl2?32:(c->in==1536?64:32);
    c->three=(uint32_t)(c->in-c->in%c->by);c->two=(uint32_t)(c->in%c->by);c->values=(uint32_t)(c->out*c->three/6);c->signs=(uint32_t)(c->out*c->three/24);
    c->scale=c->tl2?c->values+c->signs+(uint32_t)(c->out*c->two/4):(uint32_t)(c->in*c->out/4);c->bytes=c->scale+32;
    c->raw=calloc(1,c->bytes);c->keys=calloc(1,3*256);c->q=calloc(c->in,sizeof(float));c->v=calloc(3*512,sizeof(float));
    if(!c->raw||!c->keys||!c->q||!c->v||posix_memalign((void **)&c->input,16384,c->in*4)||posix_memalign((void **)&c->residual,16384,c->out*4))return 1;
    memset(c->raw,c->tl2?0xdd:0x88,c->tl2?c->values:c->scale);
    if(c->tl2)memset(c->raw+c->values+c->signs,0x88,c->out*c->two/4);
    const float weight_scale=1.0f/128;memcpy(c->raw+c->scale,&weight_scale,4);
    c->pre=malloc(c->in*4);c->post=malloc(c->out*4);if(!c->pre||!c->post)return 1;
    for(size_t i=0;i<c->in;++i) {
        const size_t kv=(i/256)/(c->heads/2),d=i%256;
        c->input[i]=(float)(16+(d*7+kv*13)%9)/1024;
        c->pre[i]=.75f+(i%3)*.0625f;
    }
    for(size_t token=0;token<3;++token)for(size_t i=0;i<512;++i)c->v[token*512+i]=(float)(16+((i%256)*7+(i/256)*13)%9)/1024;
    for(size_t i=0;i<c->out;++i){c->residual[i]=(float)(i%17)/32;c->post[i]=.625f+(i%5)*.0625f;}
    const uint64_t prep_start=attention_now();
    int rc=c->tl2?termite_metal_decode_runtime_prepare_bitnet_tl2_linear_slot(runtime,0,c->raw,c->bytes,c->in,c->out,c->scale,c->values,c->signs,c->bm,c->by,c->bmm,c->three,c->two):termite_metal_decode_runtime_prepare_bitnet_tl1_linear_slot(runtime,0,c->raw,c->bytes,c->in,c->out,c->scale,c->bm,c->by,c->bmm);
    fprintf(stderr,"TERNARY_PREP %u %zu %zu %llu %zu\n",c->tl2,c->in,c->out,(unsigned long long)(attention_now()-prep_start),c->bytes);
    return rc||termite_metal_decode_runtime_prepare_rms_norm(runtime,0,c->pre,c->in)||termite_metal_decode_runtime_prepare_rms_norm(runtime,1,c->post,c->out);
}

static int attention_failures(AttentionCase *c, termite_metal_decode_runtime *runtime, size_t *checked) {
    c->span=false;
    for(unsigned norms=0;norms<4;++norms) {
        const size_t pre=!!(norms&1),post=!!(norms&2),allocations=8+pre+post,encoders=2+pre+post;
        for(unsigned mode=0;mode<4;++mode)for(size_t n=1;n<=(mode==0?allocations:mode==1?encoders:1);++n) {
            int expected;
            if(mode==0) expected=n<=3?-2:n<=6?-8:pre&&n==7?-9:n==7+pre?-11:post&&n==8+pre?-13:-15;
            else if(mode==1)expected=pre&&n==1?-10:n==1+pre?-12:post&&n==2+pre?-14:-16;
            else expected=mode==2?-3:-4;
            for(size_t i=0;i<c->out;++i)c->output[i]=-12345;
            int rc;HostFfnTrace *trace=attention_trace(c,runtime,false,false,norms,1e-5f,true,mode==0?n:0,mode==1?n:0,mode==2,mode==3,&rc);
            if(rc!=expected){fprintf(stderr,"failure codec=%u norms=%u mode=%u n=%zu actual=%d expected=%d allocations=%lu encoders=%lu\n",c->tl2,norms,mode,n,rc,expected,(unsigned long)trace.allocations,(unsigned long)trace.encoders);return 8;}
            for(size_t i=0;i<c->out;++i)if(c->output[i]!=-12345)return 9;
            ++*checked;
        }
    }
    const uint32_t format=runtime->quant_linear_formats[0];runtime->quant_linear_formats[0]=TERMITE_METAL_QUANT_FORMAT_Q8_0;
    int rc=attention_call(c,runtime,false,false,0,0);runtime->quant_linear_formats[0]=format;if(rc!=-5)return 10;
    const size_t offset=runtime->quant_linear_weight_offsets[0];runtime->quant_linear_weight_offsets[0]=SIZE_MAX;
    rc=attention_call(c,runtime,false,false,0,0);runtime->quant_linear_weight_offsets[0]=offset;if(rc!=-5)return 10;
    uint32_t *geometry=c->tl2?&runtime->quant_linear_tl2_bmm[0]:&runtime->quant_linear_tl1_bmm[0];
    const uint32_t saved=*geometry;*geometry=0;rc=attention_call(c,runtime,false,false,0,0);*geometry=saved;if(rc!=-5)return 10;
    uint32_t *length=c->tl2?&runtime->quant_linear_tl2_three_value_len[0]:&runtime->quant_linear_tl1_packed_len[0];
    const uint32_t saved_length=*length;*length=1;rc=attention_call(c,runtime,false,false,0,0);*length=saved_length;if(rc!=-5)return 10;
    *checked+=4;return 0;
}

int main(int argc,const char **argv) { @autoreleasepool {
    const bool gpu=argc==2 && strcmp(argv[1],"--benchmark-gpu")==0;
    const bool benchmark=gpu || (argc==2 && strcmp(argv[1],"--benchmark-host")==0);
#ifndef ANTFLY_TERNARY_ATTENTION_BASELINE
    if(benchmark){fprintf(stderr,"Timing requires frozen original functions.\n");return 2;}
#endif
    termite_metal_decode_runtime *runtime=termite_metal_decode_runtime_create();if(!runtime)return 1;
    size_t checked=0,failures=0,proven_bugs=0;
    for(unsigned codec=0;codec<2;++codec)for(unsigned shape=0;shape<2;++shape) {
        AttentionCase c={.tl2=codec,.in=shape?4096:1536};if(prepare_attention(&c,runtime))return 2;
        float *owner;if(posix_memalign((void **)&owner,16384,(c.out+4096)*4))return 2;c.output=owner+1;
        float *reference=malloc(c.out*4),*before=malloc(c.out*4);if(!reference||!before)return 2;
        NSData *raw=[NSData dataWithBytes:c.raw length:c.bytes],*input=[NSData dataWithBytes:c.input length:c.in*4],*residual=[NSData dataWithBytes:c.residual length:c.out*4],*queries=[NSData dataWithBytes:c.q length:c.in*4],*keys=[NSData dataWithBytes:c.keys length:768],*values=[NSData dataWithBytes:c.v length:3*512*4];
#ifdef ANTFLY_TERNARY_ATTENTION_BASELINE
        for(size_t i=0;i<c.out;++i)c.output[i]=-12345;
        int bug_status;attention_trace(&c,runtime,true,true,0,0,true,0,0,false,false,&bug_status);
        if(bug_status)return 3;for(size_t i=0;i<c.out;++i)if(c.output[i]!=-12345)return 3;++proven_bugs;
#endif
        for(unsigned span=0;span<2;++span)for(unsigned norms=0;norms<4;++norms)for(unsigned copied=0;copied<2;++copied)for(unsigned epsilon=0;epsilon<2;++epsilon) {
            c.span=span;const float eps=epsilon?1e-5f:0;attention_reference(&c,norms,eps,reference);
            NSArray *commands=nil;
            for(unsigned side=0;side<2;++side) {
                c.output[-1]=-98765;c.output[c.out]=98765;int rc;
                HostFfnTrace *trace=attention_trace(&c,runtime,side==0,false,norms,eps,copied,0,0,false,false,&rc);
                if(rc){fprintf(stderr,"case codec=%u shape=%u span=%u norms=%u copy=%u eps=%u rc=%d\n",codec,shape,span,norms,copied,epsilon,rc);return 4;}
                if(trace.outputAliased==copied || c.output[-1]!=-98765 || c.output[c.out]!=98765)return 5;
                for(size_t i=0;i<c.out;++i)if(!isfinite(c.output[i])||fabs(c.output[i]-reference[i])>3e-4*fmax(1,fabs(reference[i]))){fprintf(stderr,"oracle codec=%u shape=%u span=%u norms=%u i=%zu actual=%g reference=%g\n",codec,shape,span,norms,i,c.output[i],reference[i]);return 6;}
                if(side==0){memcpy(before,c.output,c.out*4);commands=attention_commands(trace);}else if(memcmp(before,c.output,c.out*4)||![commands isEqualToArray:attention_commands(trace)]){fprintf(stderr,"parity codec=%u span=%u norms=%u\nBEFORE %s\nAFTER %s\n",codec,span,norms,commands.description.UTF8String,attention_commands(trace).description.UTF8String);return 7;}
            }
            if(benchmark && epsilon && (norms==0 || norms==3) && (gpu || !copied)) {
                for(unsigned order=0;order<3;++order) {
                    for(size_t warm=0;warm<8;++warm) if(!attention_duration(&c,runtime,warm&1,norms,gpu,copied))return 12;
                    for(unsigned sample=0;sample<64;++sample) {
                        const uint64_t a=attention_duration(&c,runtime,order!=2,norms,gpu,copied);
                        const uint64_t b=attention_duration(&c,runtime,order!=1,norms,gpu,copied);
                        if(!a || !b)return 12;
                        printf("TERNARY_ATTENTION_TIMING %u %u %u %u %u %u %u %llu %llu\n",codec,shape,span,norms,copied,order,sample,(unsigned long long)a,(unsigned long long)b);fflush(stdout);
                    }
                }
            }
            ++checked;
        }
        if(attention_failures(&c,runtime,&failures))return 8;
        if(memcmp(c.raw,raw.bytes,c.bytes)||memcmp(c.input,input.bytes,c.in*4)||memcmp(c.residual,residual.bytes,c.out*4)||memcmp(c.q,queries.bytes,c.in*4)||memcmp(c.keys,keys.bytes,768)||memcmp(c.v,values.bytes,3*512*4))return 11;
        id<MTLBuffer> readback=[runtime->device newBufferWithLength:c.bytes options:MTLResourceStorageModeShared];id<MTLCommandBuffer> cb=[runtime->queue commandBuffer];id<MTLBlitCommandEncoder> blit=[cb blitCommandEncoder];
        [blit copyFromBuffer:runtime->quant_linear_weight_buffers[0] sourceOffset:0 toBuffer:readback destinationOffset:0 size:c.bytes];[blit endEncoding];[cb commit];[cb waitUntilCompleted];if(cb.status!=MTLCommandBufferStatusCompleted||memcmp(readback.contents,c.raw,c.bytes))return 11;
        free(owner);free(reference);free(before);free(c.raw);free(c.keys);free(c.q);free(c.v);free(c.input);free(c.residual);free(c.pre);free(c.post);
    }
    termite_metal_decode_runtime_destroy(runtime);
    fprintf(stderr,"Ternary attention: %zu oracle/trace/copy/guard/read-only cases; %zu failure cases; %zu original copyback bugs reproduced\n",checked,failures,proven_bugs);
    return 0;
} }
