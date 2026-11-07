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
static id<MTLComputePipelineState> portable_pipeline(id<MTLDevice>d,id<MTLLibrary>lib,NSString*name){NSError*error=nil;id<MTLComputePipelineState>p=[d newComputePipelineStateWithFunction:[lib newFunctionWithName:name] error:&error];if(!p)fprintf(stderr,"%s\n",error.description.UTF8String);require_family(p!=nil,"portable pipeline");return p;}
static void check_portable(id<MTLDevice>d,id<MTLCommandQueue>q,NSString*path){
 NSError*error=nil;NSString*source=[NSString stringWithContentsOfFile:path encoding:NSUTF8StringEncoding error:&error];
 id<MTLLibrary>lib=[d newLibraryWithSource:source options:nil error:&error];if(!lib)fprintf(stderr,"%s\n",error.description.UTF8String);require_family(lib!=nil,"portable compile");for(NSString*name in lib.functionNames)portable_pipeline(d,lib,name);
 // RMS tails and independent F64 oracle, including vector-width boundaries.
 for(unsigned dim=1;dim<=513;dim=dim==1?7:dim==7?33:dim==33?129:dim==129?513:514){unsigned rows=3;id<MTLBuffer>x=family_buffer(d,rows*dim*4),w=family_buffer(d,dim*4),o=family_buffer(d,(rows*dim+4)*4);for(unsigned i=0;i<rows*dim;i++)((float*)x.contents)[i]=((int)(i%31)-15)*.125f;for(unsigned c=0;c<dim;c++)((float*)w.contents)[c]=(c%7+1)*.1f;memset(o.contents,0,o.length);uint32_t params[]={rows,dim};dispatch_family(q,portable_pipeline(d,lib,@"contract_rms"),@[x,w,o],params,sizeof params,3,MTLSizeMake(rows,1,1),128,1,0);for(unsigned r=0;r<rows;r++){double sum=0;for(unsigned c=0;c<dim;c++){double v=((float*)x.contents)[r*dim+c];sum+=v*v;}double inv=1/sqrt(sum/dim+1e-5);for(unsigned c=0;c<dim;c++){double expected=((float*)x.contents)[r*dim+c]*inv*((float*)w.contents)[c];require_family(fabs(((float*)o.contents)[r*dim+c]-expected)<3e-6,"RMS f64");}}for(unsigned i=rows*dim;i<rows*dim+4;i++)require_family(((float*)o.contents)[i]==0,"RMS canary");}
 // Typed u32 gather, repeated ids, offset and padding.
 {id<MTLBuffer>x=family_buffer(d,24*4),ids=family_buffer(d,3*4),out=family_buffer(d,15*4);for(unsigned i=0;i<24;i++)((uint32_t*)x.contents)[i]=0x7fc00000u+i;uint32_t indices[]={2,0,2};memcpy(ids.contents,indices,sizeof indices);memset(out.contents,0,out.length);uint64_t p[]={3,3,1,8,2,2,4,1};dispatch_family(q,portable_pipeline(d,lib,@"contract_gather"),@[x,ids,out],p,sizeof p,3,MTLSizeMake(1,1,1),256,1,0);for(unsigned r=0;r<3;r++)for(unsigned c=0;c<3;c++)require_family(((uint32_t*)out.contents)[2+r*4+c]==0x7fc00000u+1+indices[r]*8+c*2,"typed strided gather");}
 // Fused projection with explicit half and bfloat16 rounding; independent dot.
 {unsigned m=3,n=7,k=33;id<MTLBuffer>x=family_buffer(d,m*k*4),w=family_buffer(d,n*k*4),b=family_buffer(d,n*4),res=family_buffer(d,m*n*4),skip=family_buffer(d,m*n*4),out=family_buffer(d,m*n*4);for(unsigned i=0;i<m*k;i++)((float*)x.contents)[i]=((int)(i%11)-5)*.125f;for(unsigned i=0;i<n*k;i++)((float*)w.contents)[i]=((int)(i%7)-3)*.125f;for(unsigned i=0;i<n;i++)((float*)b.contents)[i]=i*.125f;for(unsigned i=0;i<m*n;i++){((float*)res.contents)[i]=.5f;((float*)skip.contents)[i]=.25f;}uint32_t p[]={m,n,k};dispatch_family(q,portable_pipeline(d,lib,@"contract_projection"),@[x,w,b,res,skip,out],p,sizeof p,6,MTLSizeMake(1,1,1),256,1,0);for(unsigned r=0;r<m;r++)for(unsigned c=0;c<n;c++){double dot=0;for(unsigned j=0;j<k;j++)dot+=((float*)x.contents)[r*k+j]*((float*)w.contents)[c*k+j];float rounded=(float)(_Float16)(dot+((float*)b.contents)[c]);float y=(float)(rounded/(1+exp(-rounded))*.5+.25);uint32_t u;memcpy(&u,&y,4);u=(u+0x7fff+((u>>16)&1))&0xffff0000;memcpy(&y,&u,4);require_family(((float*)out.contents)[r*n+c]==y,"projection rounding");}}
 puts("portable Metal: RMS, gather, fused epilogue passed");
 // F16 movement preserves payload bits, with strided input and contiguous output.
 {id<MTLBuffer>x=family_buffer(d,16),out=family_buffer(d,16);uint16_t values[]={0x7c01,0x8000,0x3555,0xffff,0,0x7c00,0x3c00,0x0400};memcpy(x.contents,values,16);memset(out.contents,0,16);uint64_t p[]={2,3,1,4,1,0,3,1};dispatch_family(q,portable_pipeline(d,lib,@"contract_copy"),@[x,out],p,sizeof p,2,MTLSizeMake(1,1,1),256,1,0);uint16_t expected[]={0x8000,0x3555,0xffff,0x7c00,0x3c00,0x0400};require_family(!memcmp(out.contents,expected,sizeof expected),"half movement preserves bits");}
 // Masked padding may contain NaN; it must be excluded before accumulation.
 {id<MTLBuffer>x=family_buffer(d,9*4),mask=family_buffer(d,3*4),out=family_buffer(d,3*4);float values[]={2,4,6,NAN,NAN,NAN,4,6,8};int32_t selected[]={1,0,2};memcpy(x.contents,values,sizeof values);memcpy(mask.contents,selected,sizeof selected);uint32_t p[]={3,3};dispatch_family(q,portable_pipeline(d,lib,@"contract_masked_mean"),@[x,mask,out],p,sizeof p,3,MTLSizeMake(1,1,1),256,1,0);float expected[]={3,5,7};require_family(!memcmp(out.contents,expected,sizeof expected),"masked mean excludes padding NaNs");memset(mask.contents,0,12);dispatch_family(q,portable_pipeline(d,lib,@"contract_masked_mean"),@[x,mask,out],p,sizeof p,3,MTLSizeMake(1,1,1),256,1,0);for(unsigned c=0;c<3;c++)require_family(((float*)out.contents)[c]==0,"empty masked mean");}
}
int main(int argc,const char **argv){@autoreleasepool{
    require_family(argc==1||(argc==2&&!strcmp(argv[1],"--bench"))||(argc==3&&!strcmp(argv[1],"--portable")),"[--bench|--portable <source.metal>]");bool bench=argc==2;NSError *error=nil;
    id<MTLDevice>d=MTLCreateSystemDefaultDevice();require_family(d!=nil,"device");id<MTLCommandQueue>q=[d newCommandQueue];
    if(argc==3){check_portable(d,q,[NSString stringWithUTF8String:argv[2]]);return 0;}
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
