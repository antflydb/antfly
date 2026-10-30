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

#define main antfly_existing_family_main
#include "check_metal_model_families.m"
#undef main
static NSData* q8_reference(NSArray<id<MTLBuffer>>*buf,const termite_metal_sdpa_f32_params*p){
    unsigned s=p->seq_len,h=p->num_heads,n=p->batch*s*h*64;
    NSMutableData*r=[NSMutableData dataWithLength:n*4];float*out=r.mutableBytes;
    float*q=buf[0].contents,*k=buf[1].contents,*v=buf[2].contents,*mask=buf[4].contents;
    for(unsigned b=0;b<p->batch;b++)for(unsigned head=0;head<h;head++)for(unsigned i=0;i<s;i++){
        double scores[256],peak=-INFINITY,total=0;unsigned qb=p->layout==1?(b*s+i)*h*64+head*64:((b*h+head)*s+i)*64;
        for(unsigned j=0;j<s;j++){
            unsigned kb=p->layout==1?(b*s+j)*h*64+head*64:((b*h+head)*s+j)*64;
            double dot=0;for(unsigned d=0;d<64;d++)dot+=(double)q[qb+d]*k[kb+d];
            scores[j]=p->has_mask&&mask[b*s+j]==0?-INFINITY:dot*.125;peak=fmax(peak,scores[j]);
        }
        for(unsigned j=0;j<s;j++){scores[j]=scores[j]==-INFINITY?0:exp(scores[j]-peak);total+=scores[j];}
        for(unsigned d=0;d<64;d++){
            double sum=0;for(unsigned j=0;j<s;j++){unsigned vb=p->layout==1?(b*s+j)*h*64+head*64:((b*h+head)*s+j)*64;sum+=scores[j]*v[vb+d];}
            out[qb+d]=total>0?sum/total:0;
        }
    }return r;
}
static NSData* pool_reference(NSArray<id<MTLBuffer>>*buf,const termite_metal_nomic_pool_params*p){
    NSMutableData*r=[NSMutableData dataWithLength:p->batch*p->hidden_size*4];float*out=r.mutableBytes,*x=buf[0].contents;uint32_t*m=buf[1].contents;
    for(unsigned b=0;b<p->batch;b++){
        double count=0,norm=0;for(unsigned t=0;t<p->seq_len;t++)count+=m[b*p->seq_len+t]!=0;
        for(unsigned d=0;d<p->hidden_size;d++){double sum=0;for(unsigned t=0;t<p->seq_len;t++)if(m[b*p->seq_len+t])sum+=x[(b*p->seq_len+t)*p->hidden_size+d];out[b*p->hidden_size+d]=count?sum/count:0;norm+=(double)out[b*p->hidden_size+d]*out[b*p->hidden_size+d];}
        if(p->normalize&&norm>0)for(unsigned d=0;d<p->hidden_size;d++)out[b*p->hidden_size+d]/=sqrt(norm);
    }return r;
}
int main(int argc,const char**argv){@autoreleasepool{
    require_family(argc==2||argc==3,"frozen source [--bench]");bool bench=argc==3&&!strcmp(argv[2],"--bench");NSError*error=nil;
    NSString*original=[NSString stringWithContentsOfFile:[NSString stringWithUTF8String:argv[1]] encoding:NSUTF8StringEncoding error:&error];require_family(original!=nil,"frozen source");
    id<MTLDevice>d=MTLCreateSystemDefaultDevice();require_family(d!=nil,"device");id<MTLCommandQueue>queue=[d newCommandQueue];
    for(unsigned mode=0;mode<2;mode++){
        MTLCompileOptions*options=[MTLCompileOptions new];if(mode)options.mathMode=MTLMathModeSafe;
        id<MTLLibrary>a=[d newLibraryWithSource:original options:options error:&error];require_family(a!=nil,"original library");
        id<MTLLibrary>b=[d newLibraryWithSource:termite_metal_shader_source() options:options error:&error];if(!b)fprintf(stderr,"%s\n",error.description.UTF8String);require_family(b!=nil,"candidate library");
        for(unsigned shape=0;shape<9;shape++){
            bool bert=shape<2;unsigned seq=bert?256:(unsigned[]){8,16,32,40,72,120,128}[shape-2],batch=2,heads=2;
            unsigned n=batch*seq*heads*64;NSArray*buffers=@[floats(d,n,.3f),floats(d,n,.25f),floats(d,n,.7f),floats(d,1,0),floats(d,batch*seq,0),floats(d,n+8,123)];
            float*mask=((id<MTLBuffer>)buffers[4]).contents;for(unsigned i=0;i<batch*seq;i++)mask[i]=i<seq&&i%5!=0?1:0;
            termite_metal_sdpa_f32_params p={.batch=batch,.seq_len=seq,.num_heads=heads,.head_dim=64,.layout=bert?shape:1,.has_mask=bert?0:shape%2};
            NSString*name=bert?@"termite_sdpa_f32_bert_prefill_s256_hd64_q8":@"termite_sdpa_f32_nomic_q8";
            id<MTLComputePipelineState>pa=[d newComputePipelineStateWithFunction:[a newFunctionWithName:name] error:&error],pb=[d newComputePipelineStateWithFunction:[b newFunctionWithName:name] error:&error];
            require_family(pa&&pb&&pa.staticThreadgroupMemoryLength==pb.staticThreadgroupMemoryLength&&pa.maxTotalThreadsPerThreadgroup==pb.maxTotalThreadsPerThreadgroup,"Q8 native resources");
            qualify_family(d,queue,a,b,name,buffers,&p,sizeof p,6,MTLSizeMake(batch*heads*seq/8,1,1),256,[NSIndexSet indexSetWithIndex:5],bench&&shape==1,mode,shape,0,@{@5:q8_reference(buffers,&p)});
        }
        for(unsigned shape=0;shape<8;shape++){
            unsigned seq=(unsigned[]){1,7,32,65}[shape%4],width=(unsigned[]){1,63,768,1025}[shape%4],batch=3;
            NSArray*buffers=@[floats(d,batch*seq*width,.7f),family_buffer(d,batch*seq*4),floats(d,batch*width+8,123)];uint32_t*mask=((id<MTLBuffer>)buffers[1]).contents;
            for(unsigned i=0;i<batch*seq;i++)mask[i]=i/seq==1?0:i%3?7:0;
            termite_metal_nomic_pool_params p={.batch=batch,.seq_len=seq,.hidden_size=width,.normalize=shape/4};
            qualify_family(d,queue,a,b,@"termite_nomic_pool_normalize",buffers,&p,sizeof p,3,MTLSizeMake(batch,1,1),256,[NSIndexSet indexSetWithIndex:2],bench&&shape==6,mode,shape,1024,@{@2:pool_reference(buffers,&p)});
        }
        for(unsigned shape=0;shape<3;shape++){
            unsigned rows=(unsigned[]){1,7,65}[shape],stride=73,width=shape?63:1;NSArray*buffers=@[floats(d,rows*stride,.4f),family_buffer(d,rows*8),floats(d,width+8,123)];int64_t*mask=((id<MTLBuffer>)buffers[1]).contents;unsigned count=0;
            for(unsigned r=0;r<rows;r++){mask[r]=r%3!=1;count+=mask[r]!=0;}
            uint32_t p[]={rows,stride,width,count};NSMutableData*ref=[NSMutableData dataWithLength:width*4];float*out=ref.mutableBytes,*x=((id<MTLBuffer>)buffers[0]).contents;
            for(unsigned c=0;c<width;c++){double sum=0;for(unsigned r=0;r<rows;r++)if(mask[r])sum+=(double)x[r*stride+c]/count;out[c]=sum;}
            qualify_family(d,queue,a,b,@"termite_masked_mean_rows",buffers,p,sizeof p,3,MTLSizeMake((width+255)/256,1,1),256,[NSIndexSet indexSetWithIndex:2],false,mode,shape,0,@{@2:ref});
        }
    }return 0;
}}
