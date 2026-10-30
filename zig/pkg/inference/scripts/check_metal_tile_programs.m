// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Compile every prototype Metal kernel and qualify the opt-in logits ABI.
// Run from the inference package root: check_metal_tile_programs [--bench]
#define main antfly_existing_model_family_main
#include "check_metal_model_families.m"
#undef main

static void logits_reference(const float *values, const int32_t *suppress,
    const uint32_t *grammar, const termite_metal_whisper_logits_params_host *p,
    float *output) {
    unsigned blocks=(p->out_dim+1023)/1024;
    unsigned text=(p->mode&1)?grammar[0]:p->text_allowed;
    unsigned lo=(p->mode&1)?grammar[1]:p->ts_min,hi=(p->mode&1)?grammar[2]:p->ts_max;
    for(unsigned block=0;block<blocks;block++){
        float *record=output+16*block;
        double maximum[7],sum[7]={0};uint32_t ids[4];
        for(unsigned c=0;c<7;c++)maximum[c]=-INFINITY;
        for(unsigned c=0;c<4;c++)ids[c]=UINT32_MAX;
        unsigned end=MIN(p->out_dim,(block+1)*1024);
        for(unsigned pass=0;pass<2;pass++)for(unsigned i=block*1024;i<end;i++){
            bool timestamp=i>=p->ts_begin;
            bool allowed=i==p->eot||(timestamp?(i>=lo&&i<hi):text!=0);
            for(unsigned j=0;j<p->suppress_count;j++)if(suppress[j]>=0&&(uint32_t)suppress[j]==i)allowed=false;
            bool selected[7]={allowed,allowed&&!timestamp,allowed&&timestamp,true,allowed,allowed&&timestamp,true};
            double value=values[i];
            for(unsigned c=0;c<7;c++)if(selected[c]){
                if(!pass){if(c<4&&(ids[c]==UINT32_MAX||value>maximum[c]))ids[c]=i;maximum[c]=fmax(maximum[c],value);}
                else if(c>=4&&value!=-INFINITY)sum[c]+=exp(value-maximum[c]);
            }
        }
        for(unsigned c=0;c<7;c++){record[2*c]=(float)maximum[c];if(c<4)memcpy(record+2*c+1,ids+c,4);else record[2*c+1]=(float)sum[c];}
        record[14]=record[15]=0;
    }
}
static void check_records(const float *expected,const float *actual,unsigned blocks){
    for(unsigned block=0;block<blocks;block++)for(unsigned c=0;c<16;c++){
        float a=actual[block*16+c],e=expected[block*16+c];
        if(c<8){require_family(!memcmp(&a,&e,4),"argmax value/id parity");}
        else require_family(a==e||fabs(a-e)<=2e-4*(1+fabs(e)),"independent F64 logits reference");
    }
}
int main(int argc,const char **argv){@autoreleasepool{
    require_family(argc==1||(argc==2&&!strcmp(argv[1],"--bench")),"[--bench]");bool bench=argc==2;NSError *error=nil;
    id<MTLDevice>d=MTLCreateSystemDefaultDevice();require_family(d!=nil,"device");id<MTLCommandQueue>q=[d newCommandQueue];
    NSString*model=[NSString stringWithContentsOfFile:@"src/ops/metal/generated/tile_whisper_model.metal" encoding:NSUTF8StringEncoding error:&error];
    for(unsigned mode=0;mode<2;mode++){
        MTLCompileOptions*options=[MTLCompileOptions new];if(mode)options.mathMode=MTLMathModeSafe;
        id<MTLLibrary>prototype=[d newLibraryWithSource:model options:options error:&error];if(!prototype)fprintf(stderr,"%s\n",error.description.UTF8String);require_family(prototype!=nil,"prototype model library");
        for(NSString*name in prototype.functionNames)require_family([d newComputePipelineStateWithFunction:[prototype newFunctionWithName:name] error:&error]!=nil,"prototype pipeline");
        id<MTLLibrary>old=[d newLibraryWithSource:termite_metal_shader_source() options:options error:&error];require_family(old!=nil,"production library");
        id<MTLLibrary>tile=[d newLibraryWithSource:[NSString stringWithUTF8String:antfly_tile_whisper_logits_partials_msl] options:options error:&error];if(!tile)fprintf(stderr,"%s\n",error.description.UTF8String);require_family(tile!=nil,"tile logits library");
        id<MTLComputePipelineState>pipelines[2]={[d newComputePipelineStateWithFunction:[old newFunctionWithName:@"termite_whisper_logits_partials"] error:&error],[d newComputePipelineStateWithFunction:[tile newFunctionWithName:@"antfly_tile_whisper_logits_partials"] error:&error]};require_family(pipelines[0]&&pipelines[1],"logits pipelines");
        // Exercise the real catalog hook and its typed field publication.
        termite_metal_decode_runtime *catalog=calloc(1,sizeof(*catalog));require_family(catalog!=NULL,"catalog allocation");
        setenv("ANTFLY_TILE_WHISPER_LOGITS","1",1);
        termite_metal_load_pipeline_catalog(catalog,d,old,old,antfly_metal_index_whisper_logits_partials_pipeline,1);
        unsetenv("ANTFLY_TILE_WHISPER_LOGITS");
        require_family(catalog->whisper_logits_partials_pipeline!=nil,"catalog Tile hook");
        pipelines[1]=catalog->whisper_logits_partials_pipeline;
        catalog->whisper_logits_partials_pipeline=nil;free(catalog);
        for(unsigned shape=0;shape<8;shape++){
            unsigned n=(unsigned[]){1,7,1023,1024,1025,4099,51865,51865}[shape],blocks=(n+1023)/1024;
            id<MTLBuffer>x=floats(d,n,7),s=family_buffer(d,16),g=family_buffer(d,32),output[2]={floats(d,16*blocks+8,0),floats(d,16*blocks+8,0)};
            int32_t suppressed[4]={-1,1,7,33};memcpy(s.contents,suppressed,16);
            uint32_t grammar[8]={shape&1,n/2,n-1};memcpy(g.contents,grammar,32);
            if(shape==1)for(unsigned i=0;i<n;i++)((float*)x.contents)[i]=2; // ties
            if(shape==7)for(unsigned i=0;i<n;i++)((float*)x.contents)[i]=-INFINITY;
            termite_metal_whisper_logits_params_host p={.out_dim=n,.suppress_count=4,.ts_begin=n/2,.text_allowed=1,.ts_min=n/2,.ts_max=n,.eot=n-1,.mode=shape&1};
            float *reference=calloc(16*blocks,sizeof(float));require_family(reference!=NULL,"reference allocation");logits_reference(x.contents,suppressed,grammar,&p,reference);
            // Index 3 is replaced by the params bytes, grammar stays at 4.
            NSArray*buffers[2]={@[x,s,output[0],g,g],@[x,s,output[1],g,g]};
            for(unsigned side=0;side<2;side++){
                dispatch_family(q,pipelines[side],buffers[side],&p,sizeof p,3,MTLSizeMake(blocks,1,1),256,1,14*256*4);
                if(shape!=7||side)check_records(reference,output[side].contents,blocks);
                for(unsigned i=16*blocks;i<16*blocks+8;i++)require_family(((float*)output[side].contents)[i]==0,"output canary");
            }
            printf("{\"kind\":\"correctness\",\"case\":\"tile_logits\",\"mode\":%u,\"shape\":%u,\"pass\":true}\n",mode,shape);fflush(stdout);free(reference);
            if(!bench||shape!=6)continue;
            for(unsigned order=0;order<3;order++){
                unsigned ls=order==2?1:0,rs=order?1-ls:0;double times[2][31];
                for(unsigned sample=0;sample<35;sample++){
                    double left=dispatch_family(q,pipelines[ls],buffers[ls],&p,sizeof p,3,MTLSizeMake(blocks,1,1),256,32,14*256*4);
                    double right=dispatch_family(q,pipelines[rs],buffers[rs],&p,sizeof p,3,MTLSizeMake(blocks,1,1),256,32,14*256*4);
                    if(sample<4)continue;times[0][sample-4]=left;times[1][sample-4]=right;
                    printf("{\"kind\":\"sample\",\"mode\":%u,\"order\":%u,\"left_us\":%.9g,\"right_us\":%.9g}\n",mode,order,left,right);
                }
                qsort(times[0],31,sizeof(double),compare_sample);qsort(times[1],31,sizeof(double),compare_sample);
                double median=order==2?times[0][15]/times[1][15]:times[1][15]/times[0][15],tail=order==2?times[0][29]/times[1][29]:times[1][29]/times[0][29];
                printf("{\"kind\":\"timing\",\"mode\":%u,\"order\":%u,\"median_ratio\":%.9g,\"p95_ratio\":%.9g,\"pass\":%s}\n",mode,order,median,tail,median<=1.05&&tail<=1.10?"true":"false");
                require_family(median<=1.05&&tail<=1.10,"tile logits timing gate");
            }
        }
    }
    return 0;
}}
