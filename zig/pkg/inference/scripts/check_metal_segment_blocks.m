// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Original/candidate and independent F64 qualification for segmented block MMA.
// Usage: check_metal_segment_blocks --dump-source FROZEN_MAIN.metal
//        check_metal_segment_blocks FROZEN_MAIN.metal [--bench|--require-source-identity]
// The required source snapshot must be emitted before changing the program.
#define main antfly_existing_model_family_main
#include "check_metal_model_families.m"
#undef main

static NSData *segment_reference(NSArray<id<MTLBuffer>> *buffers,
    const termite_metal_sdpa_segments_f32_params *p, bool grouped) {
    unsigned d=p->head_dim, heads=p->num_heads, kv_heads=grouped?p->reserved0:heads;
    unsigned hidden=heads*d, kv_hidden=kv_heads*d;
    float scale=1.0f/sqrtf((float)d);if(grouped)memcpy(&scale,&p->reserved1,4);
    const float *q=buffers[0].contents,*k=buffers[1].contents,*v=buffers[2].contents;
    const uint32_t *ranges=buffers[3].contents;
    const int32_t *qp=buffers[4].contents,*kp=buffers[5].contents;
    NSMutableData *result=[NSMutableData dataWithLength:(size_t)p->queries*hidden*4];
    float *out=result.mutableBytes;double *weights=calloc(p->keys,sizeof(double));
    require_family(weights!=NULL,"reference scratch");
    for(unsigned r=0;r<p->queries;r++)for(unsigned h=0;h<heads;h++){
        unsigned kh=h/(heads/kv_heads);double peak=-INFINITY,total=0;
        for(unsigned c=0;c<p->keys;c++){
            const uint32_t *x=ranges+6*r;
            uint32_t distance=qp[r]>=kp[c]?(uint32_t)qp[r]-(uint32_t)kp[c]:(uint32_t)kp[c]-(uint32_t)qp[r];
            bool visible=((c>=x[0]&&c<x[1])||(c>=x[2]&&c<x[3])||(c>=x[4]&&c<x[5]))&&(p->window==UINT32_MAX||distance<=p->window);
            double score=-INFINITY;
            if(visible){score=0;for(unsigned i=0;i<d;i++)score+=(double)q[(r*heads+h)*d+i]*k[(c*kv_heads+kh)*d+i];score*=scale;}
            weights[c]=score;peak=fmax(peak,score);
        }
        for(unsigned c=0;c<p->keys;c++){weights[c]=weights[c]==-INFINITY?0:exp(weights[c]-peak);total+=weights[c];}
        if(total>0)for(unsigned i=0;i<d;i++){double sum=0;for(unsigned c=0;c<p->keys;c++)sum+=weights[c]*v[c*kv_hidden+kh*d+i];out[r*hidden+h*d+i]=(float)(sum/total);}
    }
    free(weights);return result;
}

int main(int argc,const char **argv){@autoreleasepool{
    if(argc==3&&!strcmp(argv[1],"--dump-source")){
        NSError *failure=nil;bool ok=[termite_metal_shader_source() writeToFile:[NSString stringWithUTF8String:argv[2]] atomically:YES encoding:NSUTF8StringEncoding error:&failure];
        if(!ok)fprintf(stderr,"%s\n",failure.description.UTF8String);require_family(ok,"write source snapshot");return 0;
    }
    require_family(argc==2||(argc==3&&(!strcmp(argv[2],"--bench")||!strcmp(argv[2],"--require-source-identity"))),"frozen source path [--bench|--require-source-identity]");
    bool bench=argc==3&&!strcmp(argv[2],"--bench");NSError *error=nil;
    NSString *baseline=[NSString stringWithContentsOfFile:[NSString stringWithUTF8String:argv[1]] encoding:NSUTF8StringEncoding error:&error];
    require_family(baseline!=nil,"read frozen original source");
    if(argc==3&&!strcmp(argv[2],"--require-source-identity")){
        require_family([baseline isEqualToString:termite_metal_shader_source()],"full shader source identity");
        printf("{\"kind\":\"source_identity\",\"pass\":true}\n");
    }
    id<MTLDevice>d=MTLCreateSystemDefaultDevice();require_family(d!=nil,"device");
    id<MTLCommandQueue>queue=[d newCommandQueue];
    NSArray<NSString*>*names=@[@"termite_sdpa_f32_segments_blocks",@"termite_sdpa_f32_grouped_blocks",@"termite_sdpa_f32_grouped_blocks256",@"termite_sdpa_f32_grouped_blocks256x16",@"termite_sdpa_f32_segments_wide"];
    for(unsigned mode=0;mode<2;mode++){
        MTLCompileOptions *options=[MTLCompileOptions new];if(mode)options.mathMode=MTLMathModeSafe;
        id<MTLLibrary>a=[d newLibraryWithSource:baseline options:options error:&error];
        if(!a)fprintf(stderr,"baseline: %s\n",error.description.UTF8String);require_family(a!=nil,"baseline library");
        id<MTLLibrary>b=[d newLibraryWithSource:termite_metal_shader_source() options:options error:&error];
        if(!b)fprintf(stderr,"candidate: %s\n",error.description.UTF8String);require_family(b!=nil,"candidate library");
        for(NSString *name in names){
            id<MTLComputePipelineState>pa=[d newComputePipelineStateWithFunction:[a newFunctionWithName:name] error:&error];
            id<MTLComputePipelineState>pb=[d newComputePipelineStateWithFunction:[b newFunctionWithName:name] error:&error];
            require_family(pa&&pb,"resource pipeline");
            require_family(pa.staticThreadgroupMemoryLength==pb.staticThreadgroupMemoryLength&&pa.threadExecutionWidth==pb.threadExecutionWidth&&pa.maxTotalThreadsPerThreadgroup==pb.maxTotalThreadsPerThreadgroup,"native pipeline resources");
            printf("{\"kind\":\"resources\",\"case\":\"%s\",\"mode\":%u,\"shared_bytes\":%lu,\"simd_width\":%lu,\"max_threads\":%lu,\"pass\":true}\n",name.UTF8String,mode,pa.staticThreadgroupMemoryLength,pa.threadExecutionWidth,pa.maxTotalThreadsPerThreadgroup);
        }
        for(unsigned shape=0;shape<5;shape++)for(unsigned threads=128;threads<=512;threads*=2){
            unsigned dim=(unsigned[]){64,128,256,512,256}[shape],queries=(unsigned[]){7,17,65,129,1}[shape],keys=(unsigned[]){7,33,257,513,17}[shape];
            for(unsigned family=0;family<names.count;family++){
                if(family>=2&&family<=3&&dim>256)continue;
                if(family==4&&threads!=128)continue;
                bool grouped=family!=0&&family!=4;unsigned heads=4,kv_heads=grouped?(shape&1?1:2):heads;
                id<MTLBuffer>q=floats(d,(size_t)queries*heads*dim,.4f),k=floats(d,(size_t)keys*kv_heads*dim,.3f),v=floats(d,(size_t)keys*kv_heads*dim,.7f);
                id<MTLBuffer>ranges=family_buffer(d,queries*6*4),qp=family_buffer(d,queries*4),kp=family_buffer(d,keys*4),out=floats(d,(size_t)queries*heads*dim+8,0);
                // Keep a nonzero canary and nonzero destinations for empty rows.
                for(size_t i=0;i<out.length/4;i++)((float*)out.contents)[i]=123.0f;
                for(unsigned r=0;r<queries;r++){
                    uint32_t *x=(uint32_t*)ranges.contents+r*6;
                    x[0]=0;x[1]=keys/3;x[2]=keys/3+1;x[3]=2*keys/3;x[4]=2*keys/3+1;x[5]=keys;
                    if(r%11==5)memset(x,0,6*4);
                    ((int32_t*)qp.contents)[r]=(int32_t)(r*2)-11;
                }
                for(unsigned c=0;c<keys;c++)((int32_t*)kp.contents)[c]=(int32_t)(c*3)-17;
                float scale=.071f;uint32_t scale_bits;memcpy(&scale_bits,&scale,4);
                termite_metal_sdpa_segments_f32_params p={.queries=queries,.keys=keys,.num_heads=heads,.head_dim=dim,.window=shape&1?13:UINT32_MAX,.reserved0=kv_heads,.reserved1=scale_bits};
                NSArray*buffers=@[q,k,v,ranges,qp,kp,out];
                NSData *reference=segment_reference(buffers,&p,grouped);
                unsigned rows=threads/16;
                unsigned case_id=shape*15+(threads==128?0:threads==256?1:2)*5+family;
                qualify_family(d,queue,a,b,names[family],buffers,&p,sizeof p,7,MTLSizeMake((queries+rows-1)/rows,heads,1),threads,[NSIndexSet indexSetWithIndex:6],bench,mode,case_id,0,@{@6:reference});
            }
        }
    }
    return 0;
}}
