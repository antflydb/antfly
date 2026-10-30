// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Frozen-source parity, pipeline resources and independent F64 attention checks.
// Usage: check_metal_dense_tiles --dump-source ORIGINAL.metal
//        check_metal_dense_tiles ORIGINAL.metal [--require-source-identity|--bench]
#define main antfly_existing_model_family_main
#include "check_metal_model_families.m"
#undef main

struct dense_reference_shape {
    unsigned batch, queries, keys, heads, kv_heads, dim, query_offset, key_offset, window;
    bool causal, narrow;
};

// F64 softmax of the admitted staged operands. Half variants round scaled Q,
// K and V at acquisition; their half probability tiles are covered by the
// declared 2e-3*(1+abs(reference)) bound. Full F32 uses 3e-4 instead.
static NSData *dense_tile_reference(NSArray<id<MTLBuffer>> *inputs,struct dense_reference_shape p) {
    const float *q=inputs[0].contents,*k=inputs[1].contents,*v=inputs[2].contents;
    float scale=1.0f/sqrtf((float)p.dim);
    NSMutableData *result=[NSMutableData dataWithLength:(size_t)p.batch*p.queries*p.heads*p.dim*4];
    float *out=result.mutableBytes;double *weights=calloc(p.keys,sizeof(double));
    require_family(weights!=NULL,"reference weights");
    for(unsigned b=0;b<p.batch;b++)for(unsigned row=0;row<p.queries;row++)for(unsigned h=0;h<p.heads;h++){
        unsigned kh=h/(p.heads/p.kv_heads);double peak=-INFINITY,total=0;
        uint32_t query_position=p.query_offset+row;
        for(unsigned key=0;key<p.keys;key++){
            uint32_t key_position=p.key_offset+key;
            bool allowed=!p.causal||(key_position<=query_position&&(!p.window||query_position-key_position<p.window));
            double score=-INFINITY;
            if(allowed){score=0;for(unsigned d=0;d<p.dim;d++){
                float x=q[((b*p.queries+row)*p.heads+h)*p.dim+d]*scale;
                float y=k[((b*p.keys+key)*p.kv_heads+kh)*p.dim+d];
                if(p.narrow){x=(float)(__fp16)x;y=(float)(__fp16)y;}
                score+=(double)x*y;
            }}
            weights[key]=score;peak=fmax(peak,score);
        }
        for(unsigned key=0;key<p.keys;key++){weights[key]=weights[key]==-INFINITY?0:exp(weights[key]-peak);total+=weights[key];}
        if(total>0)for(unsigned d=0;d<p.dim;d++){
            double sum=0;for(unsigned key=0;key<p.keys;key++){
                float x=v[((b*p.keys+key)*p.kv_heads+kh)*p.dim+d];if(p.narrow)x=(float)(__fp16)x;sum+=weights[key]*x;
            }
            out[((b*p.queries+row)*p.heads+h)*p.dim+d]=(float)(sum/total);
        }
    }
    free(weights);return result;
}

static id<MTLBuffer> dense_half_buffer(id<MTLDevice>d,id<MTLBuffer>input){
    id<MTLBuffer>out=family_buffer(d,input.length/2);
    for(size_t i=0;i<input.length/4;i++)((__fp16*)out.contents)[i]=(__fp16)((float*)input.contents)[i];return out;
}

static void dense_tile_case(id<MTLDevice>d,id<MTLCommandQueue>queue,id<MTLLibrary>original,id<MTLLibrary>candidate,
    NSString*name,NSArray<id<MTLBuffer>>*buffers,NSArray<id<MTLBuffer>>*reference_inputs,
    const void*params,size_t params_size,unsigned parameter_slot,unsigned output_slot,MTLSize groups,
    unsigned threads,unsigned scratch,struct dense_reference_shape shape,bool bench,unsigned mode,unsigned case_id){
    qualify_family(d,queue,original,candidate,name,buffers,params,params_size,parameter_slot,groups,threads,[NSIndexSet indexSetWithIndex:output_slot],bench,mode,case_id,scratch,nil);
    NSData *reference=dense_tile_reference(reference_inputs,shape);NSError*error=nil;
    id<MTLComputePipelineState>pipeline=[d newComputePipelineStateWithFunction:[candidate newFunctionWithName:name] error:&error];require_family(pipeline!=nil,"reference pipeline");
    dispatch_family(queue,pipeline,buffers,params,params_size,parameter_slot,groups,threads,1,scratch);
    const float *want=reference.bytes,*got=buffers[output_slot].contents;double tolerance=shape.narrow?2e-3:3e-4,worst=0;
    for(size_t i=0;i<reference.length/4;i++){
        worst=fmax(worst,fabs(got[i]-want[i]));require_family(isfinite(got[i])&&fabs(got[i]-want[i])<=tolerance*(1+fabs(want[i])),"independent dense tile reference");
    }
    for(size_t i=reference.length/4;i<reference.length/4+8;i++)require_family(got[i]==123.0f,"reference output canary");
    printf("{\"kind\":\"reference\",\"case\":\"%s\",\"mode\":%u,\"shape\":%u,\"max_abs\":%.9g,\"pass\":true}\n",name.UTF8String,mode,case_id,worst);fflush(stdout);
}

int main(int argc,const char**argv){@autoreleasepool{
    if(argc==3&&!strcmp(argv[1],"--dump-source")){
        NSError*error=nil;bool ok=[termite_metal_shader_source() writeToFile:[NSString stringWithUTF8String:argv[2]] atomically:YES encoding:NSUTF8StringEncoding error:&error];require_family(ok,"freeze main shader");return 0;
    }
    require_family(argc==2||(argc==3&&(!strcmp(argv[2],"--bench")||!strcmp(argv[2],"--require-source-identity"))),"frozen source [--bench|--require-source-identity]");
    bool bench=argc==3&&!strcmp(argv[2],"--bench");NSError*error=nil;
    NSString*source=[NSString stringWithContentsOfFile:[NSString stringWithUTF8String:argv[1]] encoding:NSUTF8StringEncoding error:&error];require_family(source!=nil,"frozen source");
    if(argc==3&&!strcmp(argv[2],"--require-source-identity")){
        require_family([source isEqualToString:termite_metal_shader_source()],"entire shader identity");printf("{\"kind\":\"source_identity\",\"pass\":true}\n");
    }
    id<MTLDevice>d=MTLCreateSystemDefaultDevice();require_family(d!=nil,"device");id<MTLCommandQueue>queue=[d newCommandQueue];
    NSArray<NSString*>*dense=@[@"termite_attention_f32_dense_causal_sg_q16",@"termite_attention_f32_dense_causal_sg_q16_f16kv",@"termite_attention_f32_dense_causal_sg_q16_f16kv_hd128",@"termite_attention_f32_dense_causal_sg_q16_f16kv_gqa2"];
    NSArray<NSString*>*vision=@[@"termite_sdpa_f32_vision_hd64_flash_q16",@"termite_sdpa_f32_vision_hd64_flash_q32",@"termite_sdpa_f32_vision_hd64_flash_q32_full"];
    for(unsigned mode=0;mode<2;mode++){
        MTLCompileOptions*options=[MTLCompileOptions new];if(mode)options.mathMode=MTLMathModeSafe;
        id<MTLLibrary>a=[d newLibraryWithSource:source options:options error:&error];if(!a)fprintf(stderr,"original: %s\n",error.description.UTF8String);require_family(a!=nil,"original library");
        id<MTLLibrary>b=[d newLibraryWithSource:termite_metal_shader_source() options:options error:&error];if(!b)fprintf(stderr,"candidate: %s\n",error.description.UTF8String);require_family(b!=nil,"candidate library");
        for(NSString*name in [dense arrayByAddingObjectsFromArray:vision]){
            id<MTLComputePipelineState>pa=[d newComputePipelineStateWithFunction:[a newFunctionWithName:name] error:&error],pb=[d newComputePipelineStateWithFunction:[b newFunctionWithName:name] error:&error];require_family(pa&&pb,"pipeline resources");
            require_family(pa.staticThreadgroupMemoryLength==pb.staticThreadgroupMemoryLength&&pa.threadExecutionWidth==pb.threadExecutionWidth&&pa.maxTotalThreadsPerThreadgroup==pb.maxTotalThreadsPerThreadgroup,"native resource parity");
            printf("{\"kind\":\"resources\",\"case\":\"%s\",\"mode\":%u,\"shared_bytes\":%lu,\"simd_width\":%lu,\"max_threads\":%lu,\"pass\":true}\n",name.UTF8String,mode,pa.staticThreadgroupMemoryLength,pa.threadExecutionWidth,pa.maxTotalThreadsPerThreadgroup);
        }
        for(unsigned width=0;width<4;width++)for(unsigned shape=0;shape<6;shape++)for(unsigned family=0;family<dense.count;family++){
            unsigned dim=(unsigned[]){32,64,96,128}[width];if(family==2&&dim!=128)continue;
            unsigned rows=(unsigned[]){1,15,16,17,33,65}[shape],keys=(unsigned[]){1,31,32,33,64,257}[shape];
            unsigned heads=4,kv_heads=family==3?2:(shape&1?1:2);
            id<MTLBuffer>q=floats(d,(size_t)rows*heads*dim,.4f),k=floats(d,(size_t)keys*kv_heads*dim,.3f),v=floats(d,(size_t)keys*kv_heads*dim,.7f),out=floats(d,(size_t)rows*heads*dim+8,0);
            for(size_t i=0;i<out.length/4;i++)((float*)out.contents)[i]=123;
            termite_metal_attention_f32_params p={.q_len=rows,.kv_len=keys,.num_heads=heads,.num_kv_heads=kv_heads,.head_dim=dim,.query_position_offset=(unsigned[]){0,32,32,3,48,250}[shape],.kv_position_offset=(unsigned[]){0,0,4,7,0,0}[shape],.sliding_window=shape>=4?(shape==4?7:35):0,.total_sequence_len=keys,.batch=1};
            NSArray*inputs=@[q,k,v];NSArray*buffers=@[q,family==0?k:dense_half_buffer(d,k),family==0?v:dense_half_buffer(d,v),out];
            unsigned threads=family==3?256:128,scratch=family==3?64*dim*2+8192:48*dim*2+4096;
            struct dense_reference_shape r={1,rows,keys,heads,kv_heads,dim,p.query_position_offset,p.kv_position_offset,p.sliding_window,true,true};
            dense_tile_case(d,queue,a,b,dense[family],buffers,inputs,&p,sizeof p,4,3,MTLSizeMake((rows+15)/16,family==3?kv_heads:heads,1),threads,scratch,r,bench,mode,width*24+shape*4+family);
        }
        for(unsigned shape=0;shape<10;shape++)for(unsigned family=0;family<vision.count;family++){
            unsigned rows=(unsigned[]){1,15,16,17,31,32,33,65,129,257}[shape],batch=shape%3==2?2:1,heads=(unsigned[]){1,2,4}[shape%3];
            size_t n=(size_t)batch*rows*heads*64;
            id<MTLBuffer>q=floats(d,n,.4f),k=floats(d,n,.3f),v=floats(d,n,.7f),dummy=floats(d,1,0),out=floats(d,n+8,0);
            for(size_t i=0;i<out.length/4;i++)((float*)out.contents)[i]=123;
            termite_metal_sdpa_f32_params p={.batch=batch,.seq_len=rows,.num_heads=heads,.head_dim=64,.layout=1};
            unsigned tile=family==0?16:32;struct dense_reference_shape r={batch,rows,rows,heads,heads,64,0,0,0,false,family!=2};
            dense_tile_case(d,queue,a,b,vision[family],@[q,k,v,dummy,dummy,out],@[q,k,v],&p,sizeof p,6,5,MTLSizeMake(((rows+tile-1)/tile)*heads*batch,1,1),256,0,r,bench,mode,shape*3+family);
        }
    }
    return 0;
}}
