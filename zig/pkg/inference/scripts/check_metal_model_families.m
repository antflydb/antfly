// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Freeze both original shader factories in ANTFLY_METAL_FAMILY_BASELINE_HEADER.
// Without a baseline header this is an AA qualification. --bench retains all
// 31 paired AA/AB/BA samples (32 dispatches), median <=1.05 and p95 <=1.10.
#include "../src/backends/metal_kernels.m"
#ifdef ANTFLY_METAL_FAMILY_BASELINE_HEADER
#include ANTFLY_METAL_FAMILY_BASELINE_HEADER
#else
#define baseline_termite_metal_shader_source termite_metal_shader_source
#define baseline_termite_metal_lightonocr3_recurrent_source termite_metal_gated_delta_source
#endif
static void require_family(bool ok,const char *what){if(!ok){fprintf(stderr,"family qualification failed: %s\n",what);exit(1);}}
static float sample_value(size_t i){return ((int)((i*37+19)%521)-260)/283.0f;}
static id<MTLBuffer> family_buffer(id<MTLDevice>d,size_t bytes){id<MTLBuffer>b=[d newBufferWithLength:MAX(bytes,4) options:MTLResourceStorageModeShared];require_family(b!=nil,"allocation");return b;}
static id<MTLBuffer> floats(id<MTLDevice>d,size_t n,float scale){id<MTLBuffer>b=family_buffer(d,n*4);for(size_t i=0;i<n;i++)((float*)b.contents)[i]=sample_value(i)*scale;return b;}
static int compare_sample(const void*a,const void*b){double x=*(const double*)a,y=*(const double*)b;return(x>y)-(x<y);}
static double dispatch_family(id<MTLCommandQueue>q,id<MTLComputePipelineState>p,NSArray<id<MTLBuffer>>*buffers,
    const void*params,size_t size,unsigned pi,MTLSize groups,unsigned threads,unsigned repeats,unsigned scratch){
    id<MTLCommandBuffer>cb=[q commandBuffer];id<MTLComputeCommandEncoder>e=[cb computeCommandEncoder];[e setComputePipelineState:p];
    id<MTLBuffer>indirect=nil;
    if(pi==16){
        struct {uint64_t pointers[15];uint32_t params[6];} row={0};
        for(unsigned i=0;i<15;i++){row.pointers[i]=buffers[i].gpuAddress;[e useResource:buffers[i] usage:MTLResourceUsageRead|MTLResourceUsageWrite];}
        memcpy(row.params,params,sizeof row.params);indirect=family_buffer(q.device,sizeof row);memcpy(indirect.contents,&row,sizeof row);[e setBuffer:indirect offset:0 atIndex:0];
    }else{
        for(unsigned i=0;i<buffers.count;i++)[e setBuffer:buffers[i] offset:0 atIndex:i];
        [e setBytes:params length:size atIndex:pi];
    }
    if(scratch)[e setThreadgroupMemoryLength:scratch atIndex:0];
    for(unsigned i=0;i<repeats;i++)[e dispatchThreadgroups:groups threadsPerThreadgroup:MTLSizeMake(threads,1,1)];
    [e endEncoding];[cb commit];[cb waitUntilCompleted];require_family(cb.status==MTLCommandBufferStatusCompleted,"dispatch");
    return(cb.GPUEndTime-cb.GPUStartTime)*1e6/repeats;
}
static float audio_load(NSArray<id<MTLBuffer>>*b,unsigned part,unsigned row,unsigned col,const termite_metal_gemma4_audio_attention_params*p){
    unsigned stride=p->qkv_stride?p->qkv_stride:p->hidden;
    float x=((float*)b[part].contents)[row*stride+col+(p->qkv_stride?part*p->hidden:0)];
    if(p->qkv_flags&(1u<<(part*2)))x=fmaxf(x,p->qkv_bounds[part*2]);
    if(p->qkv_flags&(2u<<(part*2)))x=fminf(x,p->qkv_bounds[part*2+1]);return x;
}
static void audio_reference(NSArray<id<MTLBuffer>>*buffers,id<MTLBuffer>output,const termite_metal_gemma4_audio_attention_params*p){
    float*valid=buffers[5].contents,*scales=buffers[4].contents,*relative=buffers[3].contents,*actual=output.contents;
    for(unsigned q=0;q<p->rows;q++)for(unsigned head=0;head<p->heads;head++){
        bool active=(p->reserved&2)?valid[q]>0:valid[q]!=0;
        double scores[64],maximum=-INFINITY,denominator=0;int keys[64];
        if(active){
            unsigned clip=(p->reserved&2)?(unsigned)valid[q]-1:0,block=clip+((q-clip)/p->chunk)*p->chunk,offset=q-block;
            for(unsigned c=0;c<p->context;c++){
                int rel=(int)c-(int)offset,k=(int)(block+c)-(int)p->context_left+1;keys[c]=-1;scores[c]=-INFINITY;
                bool ok=rel>=0&&rel<(int)p->context_left&&k>=0&&k<(int)p->rows;
                if(ok)ok=((p->reserved&2)?valid[k]==valid[q]:valid[k]!=0)&&k<=(int)q&&(int)q-k<=(int)p->context_left-1-(int)(p->reserved&1);
                if(!ok)continue;keys[c]=k;double sum=0;
                for(unsigned d=0;d<p->head_dim;d++){
                    double x=(double)audio_load(buffers,0,q,head*p->head_dim+d,p)*scales[d];
                    sum+=x*(double)audio_load(buffers,1,k,head*p->head_dim+d,p)*p->k_scale;
                    sum+=x*relative[(unsigned)rel*p->hidden+head*p->head_dim+d];
                }
                scores[c]=tanh(sum/p->logit_cap)*p->logit_cap;maximum=fmax(maximum,scores[c]);
            }
            if(isfinite(maximum))for(unsigned c=0;c<p->context;c++){scores[c]=exp(scores[c]-maximum);denominator+=scores[c];}
        }
        for(unsigned d=0;d<p->head_dim;d++){
            double expected=0;if(active&&denominator)for(unsigned c=0;c<p->context;c++)if(keys[c]>=0)expected+=scores[c]/denominator*audio_load(buffers,2,keys[c],head*p->head_dim+d,p);
            require_family(fabs(actual[q*p->hidden+head*p->head_dim+d]-expected)<=2e-4*(1+fabs(expected)),"independent audio reference");
        }
    }
}
static double bf16_value(uint16_t x){uint32_t bits=(uint32_t)x<<16;float value;memcpy(&value,&bits,4);return value;}
static void projection_reference(NSArray<id<MTLBuffer>>*buffers,id<MTLBuffer>output,unsigned in,unsigned out,bool gated){
    float*x=buffers[0].contents,*y=output.contents,*b0=buffers[gated?3:2].contents;
    uint16_t*w0=buffers[1].contents,*w1=gated?buffers[2].contents:NULL;float*b1=gated?buffers[4].contents:NULL;
    for(unsigned r=0;r<out;r++){
        double g=0,u=0;for(unsigned i=0;i<in;i++){g+=(double)x[i]*bf16_value(w0[(size_t)r*in+i]);if(gated)u+=(double)x[i]*bf16_value(w1[(size_t)r*in+i]);}
        g+=b0[r];double expected=g;if(gated){u+=b1[r];expected=g/(1+exp(-g))*u;}
        require_family(fabs(y[r]-expected)<=2e-4*(1+fabs(expected)),"independent BF16 reference");
    }
}
static void qualify_family(id<MTLDevice>d,id<MTLCommandQueue>q,id<MTLLibrary>a,id<MTLLibrary>b,NSString*name,
    NSArray<id<MTLBuffer>>*input,const void*params,size_t size,unsigned pi,MTLSize groups,unsigned threads,
    NSIndexSet*changed,bool bench,unsigned mode,unsigned shape,unsigned scratch,NSDictionary<NSNumber*,NSData*>*references){
    const char*filter=getenv("ANTFLY_METAL_FAMILY_FILTER");if(filter&&!strstr(name.UTF8String,filter))return;
    NSError*error=nil;NSString*oldname=name;
    const char*modern[]={"antfly_mpp_split_affine_rows","antfly_mpp_split_gate","antfly_mpp_split_affine","antfly_segment_training_attention","antfly_causal_depthwise_segmented_f32","antfly_gdn_causal_conv_f32","antfly_gdn_causal_conv_vec4_f32","antfly_causal_history_f32"};
    const char*legacy[]={"termite_lightonocr3_prefill_gate","termite_lightonocr3_local_gate","termite_lightonocr3_local_down","termite_modernbert_training_attention","termite_gemma4_audio_dwconv_causal","termite_qwen35_conv","termite_qwen35_prefill_conv_vec4","termite_qwen35_history"};
    for(unsigned i=0;i<sizeof modern/sizeof modern[0];i++){NSString*prefix=[NSString stringWithUTF8String:modern[i]];if([name hasPrefix:prefix]){oldname=[[NSString stringWithUTF8String:legacy[i]] stringByAppendingString:[name substringFromIndex:prefix.length]];break;}}
    id<MTLFunction>old_function=[a newFunctionWithName:name];if(!old_function)old_function=[a newFunctionWithName:oldname];
    id<MTLComputePipelineState>pa=[d newComputePipelineStateWithFunction:old_function error:&error];
    id<MTLComputePipelineState>pb=[d newComputePipelineStateWithFunction:[b newFunctionWithName:name] error:&error];require_family(pa&&pb,"pipeline");
    NSMutableArray*copies[2]={[NSMutableArray new],[NSMutableArray new]};
    // Identical read-only allocations avoid measuring different cache residency
    // for baseline and candidate weights. Mutable outputs/state stay independent.
    for(unsigned side=0;side<2;side++)for(unsigned i=0;i<input.count;i++){
        id<MTLBuffer>src=input[i],dst=src;if([changed containsIndex:i]){dst=family_buffer(d,src.length);memcpy(dst.contents,src.contents,src.length);}[copies[side] addObject:dst];
    }
    dispatch_family(q,pa,copies[0],params,size,pi,groups,threads,1,scratch);dispatch_family(q,pb,copies[1],params,size,pi,groups,threads,1,scratch);
    for(NSUInteger index=changed.firstIndex;index!=NSNotFound;index=[changed indexGreaterThanIndex:index]){id<MTLBuffer>x=copies[0][index],y=copies[1][index];
        if(memcmp(x.contents,y.contents,x.length)){fprintf(stderr,"%s mode %u shape %u buffer %lu\n",name.UTF8String,mode,shape,index);require_family(false,"original bitwise parity");}
        for(size_t i=0;i<x.length/4;i++)require_family(isfinite(((float*)y.contents)[i]),"finite result");
    }
    bool gemv=strstr(name.UTF8String,"gemv")!=NULL,audio=strstr(name.UTF8String,"audio_local_attention")||[name isEqualToString:@"antfly_audio_attention_parallel"];
    unsigned guarded=gemv?(pi==4?3:5):audio?6:pi==15||pi==16?14:(unsigned)changed.lastIndex;
    id<MTLBuffer>result=copies[1][guarded],initial=input[guarded];
    require_family(!memcmp((char*)result.contents+result.length-32,(char*)initial.contents+initial.length-32,32),"output canary");
    if(audio)audio_reference(input,result,params);
    if(gemv&&pi==4){const termite_metal_apply_linear_params*p=params;projection_reference(input,result,p->in_dim,p->out_dim,false);}
    if(gemv&&pi==6){const termite_metal_linear_params*p=params;projection_reference(input,result,p->in_dim,p->out_dim,true);}
    for(NSNumber*index in references){NSData*ref=references[index];id<MTLBuffer>actual=copies[1][index.unsignedIntegerValue];
        const float*expected=ref.bytes;const float*observed=actual.contents;require_family(ref.length<=actual.length,"reference extent");
        for(size_t i=0;i<ref.length/4;i++)require_family(fabs(observed[i]-expected[i])<=3e-4*(1+fabs(expected[i])),"independent program reference");
    }
    printf("{\"kind\":\"correctness\",\"case\":\"%s\",\"mode\":%u,\"shape\":%u,\"pass\":true}\n",name.UTF8String,mode,shape);fflush(stdout);
    if(!bench)return;
    for(unsigned order=0;order<3;order++){
        unsigned ls=order==2?1:0,rs=order==0?0:order==1?1:0;double left[31],right[31];
        id<MTLComputePipelineState>pipelines[2]={pa,pb};
        for(unsigned sample=0;sample<35;sample++){
            // Reset mutable buffers before each timed command, outside GPU timing.
            for(unsigned side=0;side<2;side++)for(NSUInteger j=changed.firstIndex;j!=NSNotFound;j=[changed indexGreaterThanIndex:j])memcpy(((id<MTLBuffer>)copies[side][j]).contents,input[j].contents,input[j].length);
            double l=dispatch_family(q,pipelines[ls],copies[ls],params,size,pi,groups,threads,32,scratch);
            double r=dispatch_family(q,pipelines[rs],copies[rs],params,size,pi,groups,threads,32,scratch);
            if(sample<4)continue;left[sample-4]=l;right[sample-4]=r;
            printf("{\"kind\":\"sample\",\"case\":\"%s\",\"mode\":%u,\"shape\":%u,\"order\":%u,\"left_us\":%.9g,\"right_us\":%.9g}\n",name.UTF8String,mode,shape,order,l,r);
        }
        qsort(left,31,sizeof(double),compare_sample);qsort(right,31,sizeof(double),compare_sample);
        double med=order==2?left[15]/right[15]:right[15]/left[15],tail=order==2?left[29]/right[29]:right[29]/left[29];
        printf("{\"kind\":\"timing\",\"case\":\"%s\",\"mode\":%u,\"shape\":%u,\"order\":%u,\"median_ratio\":%.9g,\"p95_ratio\":%.9g,\"pass\":%s}\n",name.UTF8String,mode,shape,order,med,tail,med<=1.05&&tail<=1.10?"true":"false");fflush(stdout);
    }
}
int main(int argc,const char**argv){@autoreleasepool{
    bool bench=argc==2&&!strcmp(argv[1],"--bench");id<MTLDevice>d=MTLCreateSystemDefaultDevice();require_family(d!=nil,"device");id<MTLCommandQueue>q=[d newCommandQueue];
    for(unsigned mode=0;mode<2;mode++){
        NSError*error=nil;MTLCompileOptions*o=[MTLCompileOptions new];if(mode)o.mathMode=MTLMathModeSafe;
        id<MTLLibrary>a=[d newLibraryWithSource:baseline_termite_metal_shader_source() options:o error:&error];require_family(a!=nil,"baseline library");
        id<MTLLibrary>b=[d newLibraryWithSource:termite_metal_shader_source() options:o error:&error];require_family(b!=nil,"candidate library");
        id<MTLLibrary>ar=[d newLibraryWithSource:baseline_termite_metal_lightonocr3_recurrent_source() options:o error:&error];require_family(ar!=nil,"baseline recurrent library");
        id<MTLLibrary>br=[d newLibraryWithSource:termite_metal_gated_delta_source() options:o error:&error];require_family(br!=nil,"candidate recurrent library");
        for(unsigned shape=0;shape<4;shape++){
            unsigned in=(unsigned[]){128,1024,2560,3584}[shape],out=(unsigned[]){7,1024,8192,1024}[shape];
            id<MTLBuffer>x=floats(d,in,1),w=family_buffer(d,(size_t)in*out*2),u=family_buffer(d,w.length),bias=floats(d,out,.1f),ub=floats(d,out,.2f),y=floats(d,out+8,0);
            for(size_t i=0;i<w.length/2;i++){float f=sample_value(i)*.02f,g=sample_value(i+17)*.03f;uint32_t bits;memcpy(&bits,&f,4);((uint16_t*)w.contents)[i]=bits>>16;memcpy(&bits,&g,4);((uint16_t*)u.contents)[i]=bits>>16;}
            termite_metal_apply_linear_params p={.in_dim=in,.out_dim=out};
            for(unsigned cols=1;cols<=2;cols++)qualify_family(d,q,a,b,cols==1?@"termite_lightonocr3_gemv_vec4":@"termite_lightonocr3_gemv_vec4_rows2",@[x,w,bias,y],&p,sizeof p,4,MTLSizeMake((out+cols-1)/cols,1,1),32,[NSIndexSet indexSetWithIndex:3],bench,mode,shape,0,nil);
            termite_metal_linear_params gp={.rows=1,.in_dim=in,.out_dim=out};
            qualify_family(d,q,a,b,@"termite_lightonocr3_gate_up_gemv",@[x,w,u,bias,ub,y],&gp,sizeof gp,6,MTLSizeMake(out,1,1),32,[NSIndexSet indexSetWithIndex:5],bench,mode,shape,0,nil);
            // Exercise scalar and SIMD GDN phases, including grouped heads,
            // multiple tokens, epsilon-only rows and both gate saturation branches.
            uint32_t rp[6]={shape==0?1:7,shape<2?1:2,shape<2?2:4,shape==1?32:128,4,shape&1};
            unsigned seq=rp[0],kh=rp[1],vh=rp[2],dim=rp[3],channels=(2*kh+vh)*dim;
            size_t sizes[15]={seq*channels,channels*4,seq*vh,seq*vh,vh,vh,channels*4,channels*4,vh*dim*dim,seq*channels,seq*vh*2,seq*vh*dim,seq*vh*dim,dim,seq*vh*dim+8};
            NSMutableArray*rb=[NSMutableArray new];for(unsigned j=0;j<15;j++)[rb addObject:floats(d,sizes[j],j==1?.05f:.2f)];
            for(unsigned i=0;i<seq*vh;i++)((float*)((id<MTLBuffer>)rb[3]).contents)[i]=(float[]){-30,0,30}[i%3];
            if(shape==0)memset(((id<MTLBuffer>)rb[9]).contents,0,sizes[9]*4);
            for(unsigned parallel=0;parallel<2;parallel++){
                unsigned step=parallel?32:1,rows=parallel?vh:seq*vh;
                qualify_family(d,q,a,b,parallel?@"termite_qwen35_prepare_parallel":@"termite_qwen35_prepare",rb,rp,sizeof rp,15,MTLSizeMake((rows*step+31)/32,1,1),32,[NSIndexSet indexSetWithIndexesInRange:NSMakeRange(9,2)],bench,mode,shape,0,nil);
                qualify_family(d,q,a,b,parallel?@"termite_qwen35_norm_parallel":@"termite_qwen35_norm",rb,rp,sizeof rp,15,MTLSizeMake((rows*step+31)/32,1,1),32,[NSIndexSet indexSetWithIndex:14],bench,mode,shape,0,nil);
            }
            qualify_family(d,q,ar,br,@"termite_lightonocr3_prefill_prepare_parallel",rb,rp,sizeof rp,15,MTLSizeMake(seq*vh,1,1),32,[NSIndexSet indexSetWithIndexesInRange:NSMakeRange(9,2)],bench,mode,shape,0,nil);
            qualify_family(d,q,ar,br,@"termite_lightonocr3_prefill_norm_parallel",rb,rp,sizeof rp,15,MTLSizeMake(seq*vh,1,1),32,[NSIndexSet indexSetWithIndex:14],bench,mode,shape,0,nil);
            if(dim==128){rp[0]=1;qualify_family(d,q,a,b,@"termite_qwen35_fused_decode",rb,rp,sizeof rp,15,MTLSizeMake(vh,1,1),128,[NSIndexSet indexSetWithIndexesInRange:NSMakeRange(7,8)],bench,mode,shape,0,nil);}
            if(dim==128){rp[0]=1;qualify_family(d,q,ar,br,@"termite_lightonocr3_recurrent_parallel",rb,rp,sizeof rp,16,MTLSizeMake(vh,1,1),512,[NSIndexSet indexSetWithIndexesInRange:NSMakeRange(7,8)],bench,mode,shape,0,nil);}
            // Audio variants retain their distinct dot-product reductions.
            unsigned rows=(unsigned[]){1,7,17,65}[shape],hd=(unsigned[]){32,64,128,256}[shape],heads=2,hidden=hd*heads;
            termite_metal_gemma4_audio_attention_params ap={.rows=rows,.hidden=hidden,.heads=heads,.head_dim=hd,.chunk=4,.context_left=8,.context=12,.reserved=shape==3?3:0,.k_scale=.7f,.logit_cap=shape==2?.01f:30,.invalid_value=-1e9f,.qkv_stride=shape>1?hidden*3+7:0,.qkv_flags=shape>1?63:0,.qkv_bounds={-.5,.5,-.7,.7,-.9,.9}};
            unsigned stride=ap.qkv_stride?ap.qkv_stride:hidden;
            id<MTLBuffer>aq=floats(d,(size_t)rows*stride,1),ak=floats(d,(size_t)rows*stride,.5),av=floats(d,(size_t)rows*stride,.3),rel=floats(d,8*hidden,.2),sc=floats(d,hd,1),valid=floats(d,rows,0),ay=floats(d,rows*hidden+8,0);
            for(unsigned i=0;i<rows;i++)((float*)valid.contents)[i]=shape==0?0:shape==3?(i<33?1:34):(i%5?1:0);
            NSArray*ab=@[aq,ak,av,rel,sc,valid,ay];
            for(unsigned schedule=0;schedule<3;schedule++){
                NSString*name=@[@"termite_gemma4_audio_local_attention",@"termite_gemma4_audio_local_attention_tg",@"antfly_audio_attention_parallel"][schedule];
                MTLSize grid=schedule==0?MTLSizeMake((rows*heads+31)/32,1,1):MTLSizeMake(heads,rows,1);
                qualify_family(d,q,a,b,name,ab,&ap,sizeof ap,7,grid,schedule==0?32:schedule==1?hd:256,[NSIndexSet indexSetWithIndex:6],bench,mode,shape,0,nil);
            }
        }
    }
return 0;
}}
