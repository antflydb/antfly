// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Native MPP, segmented attention, depthwise and rotary qualification.
// The shared harness retains 31x32 AA/AB/BA samples and independent references.
#define main encoder_family_fixture_main
#include "check_metal_model_families.m"
#undef main
static NSData* reference_data(const float*x,size_t n){return[NSData dataWithBytes:x length:n*4];}
static uint64_t replay_mix(uint64_t x){x+=UINT64_C(0x9e3779b97f4a7c15);x=(x^(x>>30))*UINT64_C(0xbf58476d1ce4e5b9);x=(x^(x>>27))*UINT64_C(0x94d049bb133111eb);return x^(x>>31);}
static float replay_keep(const antfly_segment_training_attention_params*p,unsigned b,unsigned h,unsigned q,unsigned k){
    if(p->dropout_probability<=0)return 1;
    uint64_t index=(((uint64_t)b*p->num_heads+h)*p->seq_len+q)*p->seq_len+k;
    uint64_t bits=replay_mix(p->dropout_seed^replay_mix(index));
    uint64_t threshold=(uint64_t)(p->dropout_probability*4294967296.0f);
    return(bits>>32)<threshold?0:1.0f/(1.0f-p->dropout_probability);
}
static void segment_references(const antfly_segment_training_attention_params*p,const float*qkv,const int*ctrl,const float*dout,float*y,float*lse,float*delta,float*dq,float*dk,float*dv){
    unsigned hidden=p->num_heads*p->head_dim,n=p->tokens*hidden;const float*k=qkv+n,*v=k+n;
    const int*positions=ctrl+p->tokens*6;
    for(unsigned q=0;q<p->tokens;q++)for(unsigned h=0;h<p->num_heads;h++){
        double scores[p->seq_len],prob[p->seq_len],maximum=-INFINITY,denominator=0;unsigned batch=q/p->seq_len,start=batch*p->seq_len;
        for(unsigned j=0;j<p->seq_len;j++){
            unsigned key=start+j;bool visible=false;for(unsigned r=0;r<3;r++)visible|=key>=(unsigned)ctrl[q*6+2*r]&&key<(unsigned)ctrl[q*6+2*r+1];
            visible&=p->window==UINT32_MAX||(unsigned)abs(positions[q]-positions[key])<=p->window;scores[j]=-INFINITY;
            if(visible){double dot=0;for(unsigned d=0;d<p->head_dim;d++)dot+=(double)qkv[q*hidden+h*p->head_dim+d]*k[key*hidden+h*p->head_dim+d];scores[j]=dot*p->scale;maximum=fmax(maximum,scores[j]);}
        }
        for(unsigned j=0;j<p->seq_len;j++){prob[j]=isfinite(maximum)?exp(scores[j]-maximum):0;denominator+=prob[j];}
        lse[q*p->num_heads+h]=denominator?(float)(maximum+log(denominator)):-FLT_MAX;
        for(unsigned d=0;d<p->head_dim;d++){
            double sum=0;for(unsigned j=0;j<p->seq_len;j++)if(prob[j])sum+=prob[j]/denominator*replay_keep(p,batch,h,q-start,j)*v[(start+j)*hidden+h*p->head_dim+d];
            y[q*hidden+h*p->head_dim+d]=(float)sum;
        }
        double row_delta=0;for(unsigned d=0;d<p->head_dim;d++)row_delta+=(double)dout[q*hidden+h*p->head_dim+d]*y[q*hidden+h*p->head_dim+d];delta[q*p->num_heads+h]=(float)row_delta;
        for(unsigned j=0;j<p->seq_len;j++)if(prob[j]){
            unsigned key=start+j;double dp=0;for(unsigned d=0;d<p->head_dim;d++)dp+=(double)dout[q*hidden+h*p->head_dim+d]*v[key*hidden+h*p->head_dim+d];
            double probability=prob[j]/denominator,keep=replay_keep(p,batch,h,q-start,j),ds=probability*(keep*dp-row_delta)*p->scale;
            for(unsigned d=0;d<p->head_dim;d++){unsigned qi=q*hidden+h*p->head_dim+d,ki=key*hidden+h*p->head_dim+d;dq[qi]+=(float)(ds*k[ki]);dk[ki]+=(float)(ds*qkv[qi]);dv[ki]+=(float)(probability*keep*dout[qi]);}
        }
    }
}
int main(int argc,const char**argv){@autoreleasepool{
    bool bench=argc==2&&!strcmp(argv[1],"--bench");id<MTLDevice>d=MTLCreateSystemDefaultDevice();require_family(d!=nil,"Metal device");id<MTLCommandQueue>queue=[d newCommandQueue];
    for(unsigned mode=0;mode<2;mode++){
        MTLCompileOptions*options=[MTLCompileOptions new];if(mode)options.mathMode=MTLMathModeSafe;NSError*error=nil;
        id<MTLLibrary>a=[d newLibraryWithSource:baseline_termite_metal_shader_source() options:options error:&error];require_family(a!=nil,"original main library");
        id<MTLLibrary>b=[d newLibraryWithSource:termite_metal_shader_source() options:options error:&error];if(!b)fprintf(stderr,"%s\n",error.localizedDescription.UTF8String);require_family(b!=nil,"candidate main library");
#ifdef ANTFLY_METAL_FAMILY_BASELINE_HEADER
        id<MTLLibrary>am=[d newLibraryWithSource:baseline_termite_metal_lightonocr3_tensor_source() options:options error:&error];
#else
        id<MTLLibrary>am=[d newLibraryWithSource:termite_metal_tensor_program_source() options:options error:&error];
#endif
        id<MTLLibrary>bm=[d newLibraryWithSource:termite_metal_tensor_program_source() options:options error:&error];require_family(am&&bm,"MPP libraries");
        // Separate-product policy must retain the F32 multiplication boundary
        // even when the enclosing library uses fast math.
        NSString*strict_source=[termite_metal_shader_source() stringByAppendingString:@"\nkernel void antfly_test_strict_stencil(device const float*x[[buffer(0)]],device const float*w[[buffer(1)]],device float*y[[buffer(2)]],uint i[[thread_position_in_grid]]){if(i<2)y[i]=antfly_causal_depthwise<false,false,false,true>(x,w,x,i,0u,1u,2u,0u);}"];
        id<MTLLibrary>strict_library=[d newLibraryWithSource:strict_source options:options error:&error];require_family(strict_library!=nil,"strict stencil library");
        id<MTLComputePipelineState>strict_pipeline=[d newComputePipelineStateWithFunction:[strict_library newFunctionWithName:@"antfly_test_strict_stencil"] error:&error];require_family(strict_pipeline!=nil,"strict stencil pipeline");
        id<MTLBuffer>sx=floats(d,2,0),sw=floats(d,2,0),sy=floats(d,10,0);((float*)sx.contents)[0]=1;((float*)sx.contents)[1]=0x1.000002p0f;((float*)sw.contents)[0]=-0x1.000004p0f;((float*)sw.contents)[1]=0x1.000002p0f;
        id<MTLCommandBuffer>strict_cb=[queue commandBuffer];id<MTLComputeCommandEncoder>strict_encoder=[strict_cb computeCommandEncoder];[strict_encoder setComputePipelineState:strict_pipeline];[strict_encoder setBuffer:sx offset:0 atIndex:0];[strict_encoder setBuffer:sw offset:0 atIndex:1];[strict_encoder setBuffer:sy offset:0 atIndex:2];[strict_encoder dispatchThreadgroups:MTLSizeMake(1,1,1) threadsPerThreadgroup:MTLSizeMake(32,1,1)];[strict_encoder endEncoding];[strict_cb commit];[strict_cb waitUntilCompleted];require_family(strict_cb.status==MTLCommandBufferStatusCompleted,"strict stencil completion");
        require_family(((float*)sy.contents)[0]==0x1.000002p0f&&((float*)sy.contents)[1]==0.0f,"separate product rounding");
        printf("{\"kind\":\"rounding\",\"mode\":%u,\"pass\":true}\n",mode);
        for(unsigned shape=0;shape<4;shape++){
            unsigned rows=(unsigned[]){1,3,8,17}[shape],in=(unsigned[]){128,1024,2560,1024}[shape],out=(unsigned[]){64,1024,3584,256}[shape];
            id<MTLBuffer>x=floats(d,rows*in,1),gw=family_buffer(d,(size_t)in*out*2),uw=family_buffer(d,gw.length),gb=floats(d,out,.1f),ub=floats(d,out,.2f),y=floats(d,rows*out+8,0);
            for(size_t i=0;i<gw.length/2;i++){float f=sample_value(i)*.02f,g=sample_value(i+11)*.03f;uint32_t bits;memcpy(&bits,&f,4);((uint16_t*)gw.contents)[i]=bits>>16;memcpy(&bits,&g,4);((uint16_t*)uw.contents)[i]=bits>>16;}
            uint32_t params[4]={rows,in,out,0};
            for(unsigned kind=0;kind<3;kind++)for(unsigned variant=0;variant<2;variant++){
                if(kind&&rows>8)continue;unsigned planes=kind==1?2:1,S=kind==1?4:variant?16:8,N=kind==0?(variant?32:16):kind==1?(variant?32:16):16;
                NSString*name=[NSString stringWithFormat:@"%s%u_%u",kind==0?"antfly_mpp_split_affine_rows":kind==1?"antfly_mpp_split_gate":"antfly_mpp_split_affine",S,N];
                NSArray*buffers=planes==1?@[x,gw,gb,y]:@[x,gw,uw,gb,ub,y];unsigned output=planes==1?3:5,pi=planes==1?4:6;
                float*expected=calloc((size_t)rows*out,4);require_family(expected!=NULL,"reference allocation");
                for(unsigned r=0;r<rows;r++)for(unsigned c=0;c<out;c++){double g=0,u=0;for(unsigned k=0;k<in;k++){g+=(double)((float*)x.contents)[r*in+k]*bf16_value(((uint16_t*)gw.contents)[(size_t)c*in+k]);if(planes==2)u+=(double)((float*)x.contents)[r*in+k]*bf16_value(((uint16_t*)uw.contents)[(size_t)c*in+k]);}g+=((float*)gb.contents)[c];if(planes==2){u+=((float*)ub.contents)[c];g=g/(1+exp(-g))*u;}expected[r*out+c]=(float)g;}
                qualify_family(d,queue,am,bm,name,buffers,params,sizeof params,pi,MTLSizeMake(out/N,kind==0?(rows+7)/8:1,1),S*32,[NSIndexSet indexSetWithIndex:output],bench,mode,shape,planes*S*8*N*4,@{@(output):reference_data(expected,(size_t)rows*out)});free(expected);
            }
            // Segment masks include empty queries, three disjoint ranges,
            // local windows, tile tails, and replayable probability dropout.
            unsigned seq=(unsigned[]){7,17,33,65}[shape],batch=2,heads=2,dim=shape==0?16:shape==1?32:64,tokens=seq*batch,hidden=heads*dim,n=tokens*hidden,stats=tokens*heads;
            antfly_segment_training_attention_params sp={.tokens=tokens,.seq_len=seq,.num_heads=heads,.head_dim=dim,.window=shape==1?3:UINT32_MAX,.scale=1.0f/sqrtf(dim),.dropout_probability=shape==3?.25f:0,.dropout_seed=123456789};
            id<MTLBuffer>qkv=floats(d,3*n,.4f),ctrl=family_buffer(d,tokens*7*4),dout=floats(d,n,.3f),replay=floats(d,n+8,0),lse=floats(d,stats+8,0),delta=floats(d,stats+8,0),dq=floats(d,n+8,0),dk=floats(d,n+8,0),dv=floats(d,n+8,0),forward=floats(d,n+8,0),forward_lse=floats(d,stats+8,0);
            int*control=ctrl.contents;for(unsigned t=0;t<tokens;t++){unsigned start=t/seq*seq;control[tokens*6+t]=t%seq;for(unsigned j=0;j<6;j++)control[t*6+j]=0;if(t%7){control[t*6]=start;control[t*6+1]=start+seq/3;control[t*6+2]=start+seq/3+1;control[t*6+3]=start+2*seq/3;control[t*6+4]=start+2*seq/3+1;control[t*6+5]=start+seq;}}
            float*ry=calloc(n,4),*rl=calloc(stats,4),*rd=calloc(stats,4),*rq=calloc(n,4),*rk=calloc(n,4),*rv=calloc(n,4);require_family(ry&&rl&&rd&&rq&&rk&&rv,"attention references");
            segment_references(&sp,qkv.contents,ctrl.contents,dout.contents,ry,rl,rd,rq,rk,rv);memcpy(replay.contents,ry,n*4);memcpy(lse.contents,rl,stats*4);memcpy(delta.contents,rd,stats*4);
            for(unsigned flash=0;flash<2;flash++){
                if(!flash&&sp.dropout_probability)continue;NSString*prefix=flash?@"antfly_segment_training_attention_flash_":@"antfly_segment_training_attention_";
                MTLSize groups=flash?MTLSizeMake(batch*((seq+31)/32),heads,1):MTLSizeMake(tokens*heads,1,1);
                qualify_family(d,queue,a,b,[prefix stringByAppendingString:@"forward"],@[qkv,ctrl,forward,forward_lse],&sp,sizeof sp,4,groups,128,[NSIndexSet indexSetWithIndexesInRange:NSMakeRange(2,2)],bench,mode,shape,flash?0:1536,@{@2:reference_data(ry,n),@3:reference_data(rl,stats)});
                qualify_family(d,queue,a,b,[prefix stringByAppendingString:@"dq"],@[qkv,ctrl,dout,replay,lse,delta,dq],&sp,sizeof sp,7,groups,128,[NSIndexSet indexSetWithIndexesInRange:NSMakeRange(5,2)],bench,mode,shape,flash?0:1536,@{@5:reference_data(rd,stats),@6:reference_data(rq,n)});
                qualify_family(d,queue,a,b,[prefix stringByAppendingString:@"dkdv"],@[qkv,ctrl,dout,lse,delta,dk,dv],&sp,sizeof sp,7,groups,128,[NSIndexSet indexSetWithIndexesInRange:NSMakeRange(5,2)],bench,mode,shape,flash?0:2048,@{@5:reference_data(rk,n),@6:reference_data(rv,n)});
            }
            free(ry);free(rl);free(rd);free(rq);free(rk);free(rv);
            unsigned cr=(unsigned[]){1,7,31,129}[shape],cd=(unsigned[]){1,7,64,128}[shape],ck=(unsigned[]){1,3,4,7}[shape];
            termite_metal_gemma4_audio_rows_params cp={.rows=cr,.dim=cd,.extra=ck,.reserved=shape>1};
            id<MTLBuffer>cx=floats(d,cr*cd,.6f),cw=floats(d,ck*cd,.2f),cy=floats(d,cr*cd+8,0),starts=floats(d,cr,0);
            for(unsigned t=0;t<cr;t++)((float*)starts.contents)[t]=(t/11)*11;
            float*expected=calloc(cr*cd,4);require_family(expected!=NULL,"stencil reference");
            for(unsigned t=0;t<cr;t++)for(unsigned c=0;c<cd;c++){double sum=0;unsigned start=cp.reserved?(unsigned)((float*)starts.contents)[t]:0;
                for(unsigned k=0;k<ck;k++){int source=(int)t+(int)k+1-(int)ck;if(source<(int)start)continue;sum+=(double)((float*)cx.contents)[source*cd+c]*((float*)cw.contents)[k*cd+c];}expected[t*cd+c]=(float)sum;}
            qualify_family(d,queue,a,b,@"antfly_causal_depthwise_segmented_f32",@[cx,cw,cy,starts],&cp,sizeof cp,4,MTLSizeMake((cr*cd+63)/64,1,1),64,[NSIndexSet indexSetWithIndex:2],bench,mode,shape,0,@{@2:reference_data(expected,cr*cd)});free(expected);

        }
    }
}}
