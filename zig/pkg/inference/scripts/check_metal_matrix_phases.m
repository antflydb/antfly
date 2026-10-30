// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Frozen-program identity, native resources and independent matrix references.
#define main antfly_existing_model_family_main
#include "check_metal_model_families.m"
#undef main

static id<MTLBuffer> matrix_weights(id<MTLDevice>d,unsigned in,unsigned out,bool q8,unsigned salt){
    unsigned bytes=q8?34:18,blocks=in/32;id<MTLBuffer>w=family_buffer(d,(size_t)out*blocks*bytes);unsigned char*dst=w.contents;
    for(unsigned r=0;r<out;r++)for(unsigned b=0;b<blocks;b++){
        unsigned char*p=dst+(r*blocks+b)*bytes;uint16_t scale=0x2800;memcpy(p,&scale,2); // exactly 1/32
        for(unsigned i=0;i<32;i++){
            int value=(int)((r*7+(b*32+i)*3+salt)%(q8?31:15))-(q8?15:7);
            if(q8)p[2+i]=(unsigned char)(int8_t)value;
            else if(i<16)p[2+i]=(unsigned char)(value+8);
            else p[2+i-16]|=(unsigned char)((value+8)<<4);
        }
    }return w;
}
static double matrix_dot(id<MTLBuffer>x,unsigned row,unsigned col,unsigned in,bool q8,unsigned salt){
    const float*v=x.contents;double sum=0;
    for(unsigned i=0;i<in;i++){int weight=(int)((col*7+i*3+salt)%(q8?31:15))-(q8?15:7);sum+=(double)v[row*in+i]*weight/32;}
    return sum;
}
static NSData* grouped_reference(NSArray<id<MTLBuffer>>*buffers,const termite_metal_sdpa_segments_f32_params*p){
    unsigned d=512,heads=p->num_heads,kv=p->reserved0;float scale;memcpy(&scale,&p->reserved1,4);
    const float*q=buffers[0].contents,*k=buffers[1].contents,*v=buffers[2].contents;const uint32_t*ranges=buffers[3].contents;const int32_t*qp=buffers[4].contents,*kp=buffers[5].contents;
    NSMutableData*data=[NSMutableData dataWithLength:(size_t)p->queries*heads*d*4];float*out=data.mutableBytes;double*weights=calloc(p->keys,sizeof(double));require_family(weights!=NULL,"reference allocation");
    for(unsigned r=0;r<p->queries;r++)for(unsigned h=0;h<heads;h++){
        unsigned kh=h/(heads/kv);double peak=-INFINITY,total=0;
        for(unsigned c=0;c<p->keys;c++){
            const uint32_t*x=ranges+6*r;uint32_t distance=qp[r]>=kp[c]?(uint32_t)qp[r]-(uint32_t)kp[c]:(uint32_t)kp[c]-(uint32_t)qp[r];
            bool visible=((c>=x[0]&&c<x[1])||(c>=x[2]&&c<x[3])||(c>=x[4]&&c<x[5]))&&(p->window==UINT32_MAX||distance<=p->window);
            double score=-INFINITY;if(visible){score=0;for(unsigned i=0;i<d;i++)score+=(double)q[(r*heads+h)*d+i]*k[(c*kv+kh)*d+i];score*=scale;}
            weights[c]=score;peak=fmax(peak,score);
        }
        for(unsigned c=0;c<p->keys;c++){weights[c]=weights[c]==-INFINITY?0:exp(weights[c]-peak);total+=weights[c];}
        if(total)for(unsigned i=0;i<d;i++){double sum=0;for(unsigned c=0;c<p->keys;c++)sum+=weights[c]*v[(c*kv+kh)*d+i];out[(r*heads+h)*d+i]=sum/total;}
    }free(weights);return data;
}
static void matrix_resources(id<MTLDevice>d,id<MTLLibrary>a,id<MTLLibrary>b,NSString*name,unsigned mode){
    NSError*error=nil;id<MTLComputePipelineState>x=[d newComputePipelineStateWithFunction:[a newFunctionWithName:name] error:&error],y=[d newComputePipelineStateWithFunction:[b newFunctionWithName:name] error:&error];
    require_family(x&&y,"pipeline");require_family(x.staticThreadgroupMemoryLength==y.staticThreadgroupMemoryLength&&x.maxTotalThreadsPerThreadgroup==y.maxTotalThreadsPerThreadgroup&&x.threadExecutionWidth==y.threadExecutionWidth,"native resources");
    printf("{\"kind\":\"resources\",\"case\":\"%s\",\"mode\":%u,\"pass\":true}\n",name.UTF8String,mode);
}
int main(int argc,const char**argv){@autoreleasepool{
    require_family(argc==2||argc==3,"frozen source [--bench]");bool bench=argc==3&&!strcmp(argv[2],"--bench");NSError*error=nil;
    NSString*original=[NSString stringWithContentsOfFile:[NSString stringWithUTF8String:argv[1]] encoding:NSUTF8StringEncoding error:&error];require_family(original&&[original isEqualToString:termite_metal_shader_source()],"complete shader identity");
    id<MTLDevice>d=MTLCreateSystemDefaultDevice();require_family(d!=nil,"device");id<MTLCommandQueue>queue=[d newCommandQueue];
    for(unsigned mode=0;mode<2;mode++){
        MTLCompileOptions*options=[MTLCompileOptions new];if(mode)options.mathMode=MTLMathModeSafe;
        id<MTLLibrary>a=[d newLibraryWithSource:original options:options error:&error],b=[d newLibraryWithSource:termite_metal_shader_source() options:options error:&error];require_family(a&&b,"libraries");
        NSArray<NSString*>*names=@[@"termite_q4_0_linear_mm_sg",@"termite_q4_0_linear_mm_sg_aligned",@"termite_q4_0_linear_mm_sg_aligned_tail",@"termite_q4_0_linear_mm_sg_unrolled",@"termite_q8_0_linear_mm_sg",@"termite_q8_0_linear_mm_sg_v2",@"termite_q8_0_linear_mm_sg_v2_tail",@"termite_q8_0_linear_mm_sg_m64",@"termite_q8_0_linear_mm_sg_m64_tail"];
        for(unsigned family=0;family<names.count;family++){
            matrix_resources(d,a,b,names[family],mode);
            for(unsigned shape=0;shape<2;shape++){
                bool q8=family>=4,m64=family>=7;unsigned in=shape?128:96,cols=family==3&&shape?65:64,rows=(family==1||family==5)?32:m64?(family==7?64:65):shape?33:7;
                id<MTLBuffer>x=floats(d,rows*in,0),w=matrix_weights(d,in,cols,q8,5),out=floats(d,(size_t)rows*cols+8,123);
                for(unsigned i=0;i<rows*in;i++)((float*)x.contents)[i]=((int)(i%19)-9)/32.0f;
                NSMutableData*ref=[NSMutableData dataWithLength:(size_t)rows*cols*4];float*expected=ref.mutableBytes;
                for(unsigned r=0;r<rows;r++)for(unsigned c=0;c<cols;c++)expected[r*cols+c]=matrix_dot(x,r,c,in,q8,5);
                termite_metal_linear_params p={.rows=rows,.in_dim=in,.out_dim=cols,.row_blocks=in/32};
                qualify_family(d,queue,a,b,names[family],@[x,w,out],&p,sizeof p,3,MTLSizeMake((cols+63)/64,(rows+(m64?63:31))/(m64?64:32),1),m64?256:128,[NSIndexSet indexSetWithIndex:2],bench&&family==7&&shape==1,mode,shape,family==8?16384:8192,@{@2:ref});
            }
        }
        for(unsigned shape=0;shape<2;shape++){
            unsigned rows=shape?33:7,cols=32,in=128;id<MTLBuffer>x=floats(d,rows*in,0),w=matrix_weights(d,in,cols,false,3),up=matrix_weights(d,in,cols,false,11),out=floats(d,rows*cols+8,123),ap=family_buffer(d,sizeof(termite_metal_apply_activation_params));
            for(unsigned i=0;i<rows*in;i++)((float*)x.contents)[i]=((int)(i%19)-9)/32.0f;
            termite_metal_apply_activation_params activation={.activation_kind=2,.rows=rows,.dim=cols};memcpy(ap.contents,&activation,sizeof activation);
            NSMutableData*ref=[NSMutableData dataWithLength:rows*cols*4];float*expected=ref.mutableBytes;
            for(unsigned r=0;r<rows;r++)for(unsigned c=0;c<cols;c++){double gate=matrix_dot(x,r,c,in,false,3),u=matrix_dot(x,r,c,in,false,11);expected[r*cols+c]=gate/(1+exp(-gate))*u;}
            termite_metal_linear_params p={.rows=rows,.in_dim=in,.out_dim=cols,.row_blocks=in/32};NSString*name=@"termite_q4_0_pair_activation_mm_m32_n32_tail";matrix_resources(d,a,b,name,mode);
            qualify_family(d,queue,a,b,name,@[x,w,up,out,family_buffer(d,24),ap],&p,sizeof p,4,MTLSizeMake(1,(rows+31)/32,1),128,[NSIndexSet indexSetWithIndex:3],false,mode,shape,8192,@{@3:ref});
        }
        for(unsigned family=0;family<2;family++)for(unsigned shape=0;shape<3;shape++){
            unsigned queries=family?((unsigned[]){1,17,33})[shape]:16+16*shape,keys=family?((unsigned[]){7,33,65})[shape]:64+64*shape,heads=2,kv=shape%2?1:2;
            id<MTLBuffer>q=floats(d,queries*heads*512,.2f),k=floats(d,keys*kv*512,.15f),v=floats(d,keys*kv*512,.5f),ranges=family_buffer(d,queries*24),qp=family_buffer(d,queries*4),kp=family_buffer(d,keys*4),out=floats(d,queries*heads*512+8,123);
            for(unsigned r=0;r<queries;r++){uint32_t*x=(uint32_t*)ranges.contents+r*6;x[0]=0;x[1]=keys/3;x[2]=keys/3+1;x[3]=keys;x[4]=x[5]=0;if(r%11==5)memset(x,0,24);((int32_t*)qp.contents)[r]=r;}
            for(unsigned c=0;c<keys;c++)((int32_t*)kp.contents)[c]=c;
            float scale=1/sqrtf(512);termite_metal_sdpa_segments_f32_params p={.queries=queries,.keys=keys,.num_heads=heads,.head_dim=512,.window=shape==2?17:UINT32_MAX,.reserved0=kv};memcpy(&p.reserved1,&scale,4);
            NSArray*buffers=@[q,k,v,ranges,qp,kp,out];NSString*name=family?@"termite_sdpa_f32_grouped_matrix32":@"termite_sdpa_f32_grouped_matrix64";matrix_resources(d,a,b,name,mode);
            qualify_family(d,queue,a,b,name,buffers,&p,sizeof p,7,MTLSizeMake((queries+15)/16,heads,1),256,[NSIndexSet indexSetWithIndex:6],bench&&family==1&&shape==1,mode,shape,0,@{@6:grouped_reference(buffers,&p)});
        }
    }return 0;
}}
