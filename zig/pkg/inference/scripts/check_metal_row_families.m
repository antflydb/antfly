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

#include "../src/backends/metal_kernels.m"
#ifdef ANTFLY_METAL_ROW_BASELINE_HEADER
#include ANTFLY_METAL_ROW_BASELINE_HEADER
#else
#define antfly_row_baseline_shader_source termite_metal_shader_source
#endif

static void require_row(bool ok, const char *what) {
    if (!ok) { fprintf(stderr,"row qualification failed: %s\n",what); exit(1); }
}
static id<MTLBuffer> row_buffer(id<MTLDevice> d, size_t count) {
    id<MTLBuffer> b=[d newBufferWithLength:MAX(count,1)*sizeof(float) options:MTLResourceStorageModeShared];
    require_row(b!=nil,"buffer allocation"); return b;
}
static float row_value(size_t i) { return ((int)((i*37+19)%521)-260)/83.0f; }
static double row_dispatch(id<MTLCommandQueue> queue,id<MTLComputePipelineState> pipeline,
    NSArray<id<MTLBuffer>> *buffers,NSUInteger parameter_index,const void *params,size_t bytes,
    NSUInteger groups,NSUInteger threads,unsigned repeats,NSUInteger scratch) {
    id<MTLCommandBuffer> cb=[queue commandBuffer];
    id<MTLComputeCommandEncoder> e=[cb computeCommandEncoder];
    [e setComputePipelineState:pipeline];
    if(scratch) [e setThreadgroupMemoryLength:scratch atIndex:0];
    for(NSUInteger j=0;j<buffers.count;j++) [e setBuffer:buffers[j] offset:0 atIndex:j];
    [e setBytes:params length:bytes atIndex:parameter_index];
    for(unsigned j=0;j<repeats;j++) [e dispatchThreadgroups:MTLSizeMake(groups,1,1) threadsPerThreadgroup:MTLSizeMake(threads,1,1)];
    [e endEncoding]; [cb commit]; [cb waitUntilCompleted];
    require_row(cb.status==MTLCommandBufferStatusCompleted,"GPU completion");
    return (cb.GPUEndTime-cb.GPUStartTime)*1e6/repeats;
}
static int compare_row_double(const void *a,const void *b) { double x=*(const double*)a,y=*(const double*)b;return (x>y)-(x<y); }
static void row_benchmark(id<MTLCommandQueue> queue,id<MTLComputePipelineState> a,id<MTLComputePipelineState> b,
    NSArray<id<MTLBuffer>> *buffers,NSUInteger pi,const void *p,size_t size,NSUInteger groups,NSUInteger threads,const char *name,NSUInteger scratch) {
    // Fixed before sampling: 31 paired samples, 32 dispatches, AA/AB/BA;
    // median <= 1.05, p95 <= 1.10. Retain raw samples, including control misses.
    for(unsigned order=0;order<3;order++) {
        double left[31],right[31];
        id<MTLComputePipelineState> l=order==2?b:a,r=order==0?a:order==1?b:a;
        for(unsigned warm=0;warm<4;warm++) { row_dispatch(queue,l,buffers,pi,p,size,groups,threads,32,scratch);row_dispatch(queue,r,buffers,pi,p,size,groups,threads,32,scratch); }
        for(unsigned i=0;i<31;i++) {
            left[i]=row_dispatch(queue,l,buffers,pi,p,size,groups,threads,32,scratch);
            right[i]=row_dispatch(queue,r,buffers,pi,p,size,groups,threads,32,scratch);
            printf("{\"kind\":\"sample\",\"case\":\"%s\",\"order\":\"%s\",\"i\":%u,\"left_us\":%.9g,\"right_us\":%.9g}\n",name,order==0?"AA":order==1?"AB":"BA",i,left[i],right[i]);
        }
        qsort(left,31,sizeof(double),compare_row_double); qsort(right,31,sizeof(double),compare_row_double);
        double median=order==2?left[15]/right[15]:right[15]/left[15];
        double tail=order==2?left[29]/right[29]:right[29]/left[29];
        printf("{\"kind\":\"timing\",\"case\":\"%s\",\"order\":\"%s\",\"median_ratio\":%.9g,\"p95_ratio\":%.9g,\"pass\":%s}\n",name,order==0?"AA":order==1?"AB":"BA",median,tail,median<=1.05&&tail<=1.10?"true":"false");
    }
}
// Shared-program migrations must execute the actual compatibility exports,
// comparing the frozen shader and an independent reference on tail widths.
static void qualify_shared_rows(id<MTLDevice> device,id<MTLCommandQueue> queue,
    id<MTLLibrary> old,id<MTLLibrary> current,unsigned precise,bool bench) {
    const char *names[]={"termite_slice_last_dim_f32_2d","termite_concat_lastdim_f32_2d",
        "termite_gliner_word_embeddings_f32","termite_apply_rms_norm_rows_reduce","termite_apply_layer_norm_rows"};
    for(unsigned family=0;family<5;family++) for(unsigned shape=0;shape<4;shape++) {
        unsigned width=(unsigned[]){1,3,33,257}[shape],rows=(unsigned[]){1,7,64,257}[shape];
        if(family==2) rows=12;
        unsigned outcols=family==1?2*width+1:width;
        size_t input_count=family==2?27*width:(size_t)rows*(width+2),out_count=(size_t)rows*outcols;
        id<MTLBuffer> input=row_buffer(device,input_count),second=row_buffer(device,(size_t)rows*(width+1));
        id<MTLBuffer> weight=row_buffer(device,width),bias=row_buffer(device,width),mask=row_buffer(device,27);
        id<MTLBuffer> outa=row_buffer(device,out_count),outb=row_buffer(device,out_count);
        float *x=input.contents,*right=second.contents,*w=weight.contents,*beta=bias.contents,*ids=mask.contents;
        for(size_t i=0;i<input_count;i++) x[i]=row_value(i);
        for(size_t i=0;i<second.length/4;i++) right[i]=row_value(i+101);
        for(unsigned i=0;i<width;i++){w[i]=0.5f+row_value(i+71)/8;beta[i]=row_value(i+19)/4;}
        for(unsigned b=0;b<3;b++) for(unsigned t=0;t<9;t++)
            ids[b*9+t]=shape==0?-1.0f:t==0?0.0f:t==8?8.0f:(float)((t+b)%3+1);
        unsigned char params[32]={0};size_t size;NSUInteger pi,groups,threads=256,scratch=0;
        NSArray<id<MTLBuffer>> *bufa,*bufb;
        if(family==0){
            termite_metal_slice_last_dim_f32_2d_params p={rows,width+2,1,width};
            memcpy(params,&p,sizeof p);size=sizeof p;pi=2;
            bufa=@[input,outa];bufb=@[input,outb];groups=(out_count+255)/256;
        }else if(family==1){
            termite_metal_concat_lastdim_f32_2d_params p={rows,width,width+1,0};
            memcpy(params,&p,sizeof p);size=sizeof p;pi=3;
            bufa=@[input,second,outa];bufb=@[input,second,outb];groups=(out_count+255)/256;
        }else if(family==2){
            termite_metal_gliner_word_embeddings_f32_params p={3,9,width,4};
            memcpy(params,&p,sizeof p);size=sizeof p;pi=3;
            bufa=@[input,mask,outa];bufb=@[input,mask,outb];groups=(out_count+255)/256;
        }else if(family==3){
            termite_metal_apply_row_norm_params p={rows,width,1e-5f};
            memcpy(params,&p,sizeof p);size=sizeof p;pi=3;scratch=256*4;groups=rows;
            bufa=@[input,weight,outa];bufb=@[input,weight,outb];
        }else{
            termite_metal_apply_layer_norm_params p={width,1e-5f};
            memcpy(params,&p,sizeof p);size=sizeof p;pi=4;scratch=512*4;groups=rows;
            bufa=@[input,weight,bias,outa];bufb=@[input,weight,bias,outb];
        }
        NSError *error=nil;NSString *name=[NSString stringWithUTF8String:names[family]];
        id<MTLComputePipelineState> a=[device newComputePipelineStateWithFunction:[old newFunctionWithName:name] error:&error];
        id<MTLComputePipelineState> b=[device newComputePipelineStateWithFunction:[current newFunctionWithName:name] error:&error];
        require_row(a&&b,"shared program pipelines");
        memset(outa.contents,0xa5,outa.length);memset(outb.contents,0xa5,outb.length);
        row_dispatch(queue,a,bufa,pi,params,size,groups,threads,1,scratch);
        row_dispatch(queue,b,bufb,pi,params,size,groups,threads,1,scratch);
        require_row(memcmp(outa.contents,outb.contents,outa.length)==0,"shared program bitwise baseline parity");
        float *y=outb.contents;
        for(unsigned row=0;row<rows;row++) {
            double mean=0,squares=0;
            if(family>=3){for(unsigned col=0;col<width;col++){double v=x[row*width+col];mean+=v;squares+=v*v;}mean/=width;squares/=width;}
            for(unsigned col=0;col<outcols;col++) {
                double expected;
                if(family==0) expected=x[row*(width+2)+1+col];
                else if(family==1) expected=col<width?x[row*width+col]:right[row*(width+1)+col-width];
                else if(family==2){expected=0;for(unsigned t=0;t<9;t++) if(ids[(row/4)*9+t]==(float)(row%4+1)){expected=x[((row/4)*9+t)*width+col];break;}}
                else if(family==3) expected=x[row*width+col]/sqrt(squares+1e-5)*w[col];
                else expected=(x[row*width+col]-mean)/sqrt(fmax(squares-mean*mean,0)+1e-5)*w[col]+beta[col];
                require_row(isfinite(y[row*outcols+col])&&fabs(y[row*outcols+col]-expected)<3e-5*(1+fabs(expected)),"shared program independent reference");
            }
        }
        char label[160];snprintf(label,sizeof label,"shared/%s/mode%u/shape%u",names[family],precise,shape);
        printf("{\"kind\":\"correctness\",\"case\":\"%s\",\"bitwise\":true,\"reference\":true,\"scratch_bytes\":%lu}\n",label,(unsigned long)(scratch+b.staticThreadgroupMemoryLength));
        if(bench) row_benchmark(queue,a,b,bufa,pi,params,size,groups,threads,label,scratch);
    }
}
int main(int argc,const char **argv) { @autoreleasepool {
    bool bench=argc==2&&strcmp(argv[1],"--bench")==0;
    id<MTLDevice> device=MTLCreateSystemDefaultDevice(); require_row(device!=nil,"Metal device");
    id<MTLCommandQueue> queue=[device newCommandQueue];
    printf("{\"kind\":\"device\",\"name\":\"%s\"}\n",device.name.UTF8String);
    for(unsigned precise=0;precise<2;precise++) {
        NSError *error=nil; MTLCompileOptions *opts=[MTLCompileOptions new];
        if(precise) opts.mathMode=MTLMathModeSafe;
        id<MTLLibrary> old=[device newLibraryWithSource:antfly_row_baseline_shader_source() options:opts error:&error];
        if(!old) fprintf(stderr,"%s\n",error.localizedDescription.UTF8String);
        require_row(old!=nil,"baseline library");
        id<MTLLibrary> current=[device newLibraryWithSource:termite_metal_shader_source() options:opts error:&error];
        if(!current) fprintf(stderr,"%s\n",error.localizedDescription.UTF8String);
        require_row(current!=nil,"candidate library");
        qualify_shared_rows(device,queue,old,current,precise,bench);
        const char *names[]={"antfly_audio_frontend","termite_gemma4_audio_channel_norm_relu","termite_embedding_head_prepare","termite_vision_norm_rope","termite_apply_head_rms_rope_hd128_sg"};
        for(unsigned family=0;family<5;family++) for(unsigned shape=0;shape<4;shape++) {
            if (precise*20+family*4+shape < (unsigned)(getenv("ANTFLY_ROW_START_CASE")?atoi(getenv("ANTFLY_ROW_START_CASE")):0)) continue;
            NSString *name=[NSString stringWithUTF8String:names[family]];
            id<MTLComputePipelineState> a=[device newComputePipelineStateWithFunction:[old newFunctionWithName:name] error:&error];
            id<MTLComputePipelineState> b=[device newComputePipelineStateWithFunction:[current newFunctionWithName:name] error:&error];
            require_row(a&&b,"native pipelines");
            unsigned width=family==4?128:(unsigned[]){32,64,128,256}[shape];
            unsigned rows=(unsigned[]){1,7,64,257}[shape],heads=4,stride=heads*width+13,start=3;
            size_t n=family<2?(size_t)width*rows*2:(size_t)rows*stride;
            size_t outn=family==0?n:family==1?(size_t)rows*width:(size_t)rows*heads*width;
            id<MTLBuffer> input=row_buffer(device,n),weight=row_buffer(device,width),indices=row_buffer(device,rows),rotary=row_buffer(device,(size_t)rows*width*2);
            id<MTLBuffer> outa=row_buffer(device,outn),outb=row_buffer(device,outn);
            float *x=input.contents,*w=weight.contents;
            for(size_t i=0;i<n;i++) x[i]=shape==0?0.0f:row_value(i);
            for(unsigned i=0;i<width;i++) w[i]=0.5f+row_value(i+71)/8;
            for(unsigned i=0;i<rows;i++) ((int32_t*)indices.contents)[i]=(int32_t)(rows-1-i);
            // Cache values deliberately differ from on-the-fly trig to verify reads.
            for(size_t i=0;i<rotary.length/sizeof(float);i++) ((float*)rotary.contents)[i]=row_value(i)/4;
            uint8_t params[128]={0};size_t size=0;NSUInteger pi=3,groups=(rows+63)/64,threads=64;
            NSArray<id<MTLBuffer>> *bufa=nil,*bufb=nil;
            if(family<2) {
                if(family==0) { antfly_audio_frontend_params p={3,rows,width,0,1e-5f,2};memcpy(params,&p,sizeof p);size=sizeof p;groups=(rows*2+63)/64; }
                else { termite_metal_gemma4_audio_channel_norm_params p={.channels=width,.positions=rows,.eps=1e-5f};memcpy(params,&p,sizeof p);size=sizeof p; }
                bufa=@[input,weight,outa];bufb=@[input,weight,outb];
            } else if(family==2) {
                termite_embedding_head_params p={rows,rows,heads/2,heads,width,rows,stride,start,shape>1,shape==3?1:0,shape!=1,1e-6f,10000,0.7f,shape==3};
                memcpy(params,&p,sizeof p);size=sizeof p;pi=4;groups=rows*heads;threads=32;
                bufa=@[input,weight,indices,outa,rotary,rotary];bufb=@[input,weight,indices,outb,rotary,rotary];
            } else if(family==3) {
                termite_vision_norm_rope_params p={rows,heads*width,width,rows,1,1e-6f,10000,0.7f,stride,start,shape>1,shape!=1,shape==3};
                memcpy(params,&p,sizeof p);size=sizeof p;groups=rows*heads;threads=32;
                bufa=@[input,weight,outa,rotary,rotary];bufb=@[input,weight,outb,rotary,rotary];
            } else {
                termite_metal_head_rms_rope_params p={rows*heads,width,width,3,10000,1,1e-6f,0.7f,0,heads,rows};
                memcpy(params,&p,sizeof p);size=sizeof p;groups=(rows*heads+3)/4;threads=128;
                bufa=@[input,weight,outa];bufb=@[input,weight,outb];
            }
            // Initialize sentinels and bind p after buffer bindings (p occupies
            // an ABI slot between output and optional rotary buffers).
            memset(outa.contents,0xa5,outa.length);memset(outb.contents,0xa5,outb.length);
            row_dispatch(queue,a,bufa,pi,params,size,groups,threads,1,0);
            row_dispatch(queue,b,bufb,pi,params,size,groups,threads,1,0);
            require_row(memcmp(outa.contents,outb.contents,outa.length)==0,"bitwise baseline parity");
            const float *y=outb.contents;
            for(size_t i=0;i<outn;i++) require_row(isfinite(y[i]),"finite output / full coverage");
            if(family<2) {
                for(unsigned batch=0;batch<(family==0?2:1);batch++) for(unsigned row=0;row<rows;row++) {
                    size_t base=(size_t)batch*rows*width+row;double mean=0,var=0;
                    for(unsigned c=0;c<width;c++) mean+=x[base+c*rows];mean/=width;
                    for(unsigned c=0;c<width;c++){double d=x[base+c*rows]-mean;var+=d*d;}var/=width;
                    for(unsigned c=0;c<width;c++){double expected=fmax((x[base+c*rows]-mean)/sqrt(var+1e-5)*w[c],0);require_row(fabs(y[base+c*rows]-expected)<2e-5*(1+fabs(expected)),"independent channel norm reference");}
                }
            }
            if(family>=2) {
                for(unsigned row=0;row<rows;row++) for(unsigned h=0;h<heads;h++) {
                    unsigned source_row=family==2&&shape==3?rows-1-row:row;
                    size_t base=family==4?((size_t)row*heads+h)*width:(size_t)source_row*stride+start+(family==2?h/2:h)*width;
                    double square=0;for(unsigned d=0;d<width;d++) square+=(double)x[base+d]*x[base+d];
                    double inverse=1.0/sqrt(square/width+1e-6);
                    for(unsigned d=0;d<width;d++) {
                        double expected=x[base+d]*inverse*(family==4||shape!=1?w[d]:1)*0.7f;
                        if(family==4||shape>1) {
                            unsigned span=family==3?width/4:width/2;
                            unsigned local=family==3?d%(2*span):d;
                            unsigned pair=local%span,left=family==3?(d/(2*span))*(2*span)+pair:pair,right=left+span;
                            double lo=x[base+left]*inverse*(family==4||shape!=1?w[left]:1);
                            double hi=x[base+right]*inverse*(family==4||shape!=1?w[right]:1);
                            double c,sn;
                            if(family!=4&&shape==3) {
                                unsigned pos=family==3?(d/(2*span)==0?row:rows):source_row;
                                size_t index=((size_t)pos*span+pair)*2;
                                c=((float*)rotary.contents)[index];sn=((float*)rotary.contents)[index+1];
                            } else {
                                double pos=family==4?3+row:family==3?(d/(2*span)==0?row:0):source_row;
                                double angle=pos/pow(10000.0,2.0*pair/(family==3?width/2:width));c=cos(angle);sn=sin(angle);
                            }
                            if(family==4){lo*=0.7f;hi*=0.7f;}
                            expected=(local<span?lo*c-hi*sn:lo*sn+hi*c)*(family==4?1.0:0.7f);
                        }
                        require_row(fabs(y[((size_t)row*heads+h)*width+d]-expected)<3e-4*(1+fabs(expected)),"independent head norm/rotary reference");
                    }
                }
            }
            char label[160];snprintf(label,sizeof label,"%s/mode%u/shape%u",names[family],precise,shape);
            printf("{\"kind\":\"correctness\",\"case\":\"%s\",\"bitwise\":true,\"reference\":%s,\"scratch_bytes\":%lu}\n",label,"true",(unsigned long)b.staticThreadgroupMemoryLength);
            if(bench) row_benchmark(queue,a,b,bufa,pi,params,size,groups,threads,label,0);
        }
    }
}return 0;}
