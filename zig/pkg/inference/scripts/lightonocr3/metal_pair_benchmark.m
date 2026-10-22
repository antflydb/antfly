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

// Benchmark-only bridge. Calls production dispatchers; never linked into Antfly.
#include "../../src/backends/metal_kernels.m"
#ifndef OCR_PAIR_RUNTIME_SHA
#define OCR_PAIR_RUNTIME_SHA "unknown"
#endif
#ifndef OCR_PAIR_BRIDGE_SHA
#define OCR_PAIR_BRIDGE_SHA "unknown"
#endif
static termite_metal_decode_runtime *pair_runtime;
@interface OCRPairCase : NSObject {
  @public
    size_t kind, rows, in_dim, out_dim;
    void *weight_storage;
    id<MTLBuffer> input, output, gates, state;
}
@end
@implementation OCRPairCase
- (void)dealloc {
    if (weight_storage) {
        if (pair_runtime) {
            (void)termite_metal_decode_runtime_clear_linear_slot(pair_runtime, 0);
            if (kind == 3)
                (void)termite_metal_decode_runtime_clear_linear_slot(pair_runtime, 1);
        }
        free(weight_storage);
    }
}
@end
const char *ocr_pair_runtime_sha(void) { return OCR_PAIR_RUNTIME_SHA; }
const char *ocr_pair_bridge_sha(void) { return OCR_PAIR_BRIDGE_SHA; }
void *ocr_pair_create(size_t kind, size_t rows, size_t in_dim, size_t out_dim, const uint16_t *weights,
                      const float *values, const float *norm, const float *gates) {
    @autoreleasepool {
        if (!pair_runtime) {
            pair_runtime = termite_metal_decode_runtime_create();
            if (!pair_runtime)
                return NULL;
            termite_metal_decode_runtime_set_lightonocr3_projections(pair_runtime, 1);
            termite_metal_decode_runtime_set_qwen35_ocr_vector_loads(pair_runtime, 1);
        }
        termite_metal_decode_runtime *r = pair_runtime;
        if (rows == 0 || rows > 2048 || in_dim == 0 || in_dim > 32768 || out_dim == 0 || out_dim > 248320 ||
            kind > 3)
            return NULL;
        OCRPairCase *c = [OCRPairCase new];
        c->kind = kind;
        c->rows = rows;
        c->in_dim = in_dim;
        c->out_dim = out_dim;
        c->input = [r->device newBufferWithBytes:values
                                          length:rows * in_dim * 4
                                         options:MTLResourceStorageModeShared];
        size_t output_count = rows * (kind == 1 ? in_dim : kind == 2 ? out_dim * 128 : out_dim);
        c->output = [r->device newBufferWithLength:output_count * 4 options:MTLResourceStorageModeShared];
        if (!c->input || !c->output)
            return NULL;
        if (kind == 0 || kind == 3) {
            if (!weights)
                return NULL;
            const size_t matrix_bytes = in_dim * out_dim * 2;
            const size_t total_bytes = matrix_bytes * (kind == 3 ? 2 : 1);
            const size_t page_bytes = (size_t)getpagesize();
            if (matrix_bytes % page_bytes != 0 || posix_memalign(&c->weight_storage, page_bytes, total_bytes))
                return NULL;
            memcpy(c->weight_storage, weights, total_bytes);
            weights = c->weight_storage;
            float *bias = calloc(out_dim, 4);
            if (!bias)
                return NULL;
            int rc = termite_metal_decode_runtime_prepare_linear_bf16_no_copy(
                r, 0, (const uint8_t *)weights, in_dim * out_dim * 2, bias, in_dim, out_dim);
            if (!rc && kind == 3)
                rc = termite_metal_decode_runtime_prepare_linear_bf16_no_copy(
                    r, 1, (const uint8_t *)(weights + in_dim * out_dim), in_dim * out_dim * 2, bias, in_dim,
                    out_dim);
            free(bias);
            if (rc)
                return NULL;
            if (kind == 3) {
                c->gates = [r->device newBufferWithLength:rows * out_dim * 4
                                                  options:MTLResourceStorageModePrivate];
                c->state = [r->device newBufferWithLength:rows * out_dim * 4
                                                  options:MTLResourceStorageModePrivate];
                if (!c->gates || !c->state)
                    return NULL;
            }
        } else if (kind == 1) {
            if (!norm || termite_metal_decode_runtime_prepare_rms_norm(r, 0, norm, in_dim))
                return NULL;
        } else {
            if (!gates || rows < 128 || (out_dim != 16 && out_dim != 32) || in_dim != (32 + out_dim) * 128)
                return NULL;
            c->gates = [r->device newBufferWithBytes:gates
                                              length:rows * out_dim * 2 * 4
                                             options:MTLResourceStorageModeShared];
            c->state = [r->device newBufferWithLength:out_dim * 128 * 128 * 4
                                              options:MTLResourceStorageModeShared];
            if (!c->gates || !c->state || !r->qwen35_recurrent_columns4_pipeline)
                return NULL;
        }
        return (__bridge_retained void *)c;
    }
}
static NSString *ocr_pair_variant_source(void) {
    return @""
            "#include <metal_stdlib>\n"
            "using namespace metal;\n"
            "struct P{uint rows,in_dim,out_dim,pad;};\n"
            "template<uint SN>inline void body(device const float*x,device const ushort*w,device const "
            "float*bias,device float*y,constant P&p,uint lane,uint group){\n"
            " uint col=group*(32/SN)+lane/SN,k=lane%SN;float4 acc[4];for(uint b=0;b<4;b++)acc[b]=0;\n"
            " for(uint i=k*4;i<p.in_dim;i+=SN*4){float4 weights=as_type<float4>(uint4(*((device const "
            "ushort4*)(w+col*p.in_dim+i)))<<16);\n"
            " #pragma unroll\n"
            " for(uint b=0;b<4;b++)acc[b]+=*((device const float4*)(x+b*p.in_dim+i))*weights;\n"
            " }\n"
            " #pragma unroll\n"
            " for(uint b=0;b<4;b++){float sum=dot(acc[b],float4(1));\n"
            " #pragma unroll\n"
            " for(uint step=SN/2;step>0;step/=2)sum+=simd_shuffle_xor(sum,step);\n"
            " if(k==0)y[b*p.out_dim+col]=sum+bias[col];}\n"
            "}\n"
            "kernel void segment_4(device const float*x[[buffer(0)]],device const "
            "ushort*w[[buffer(1)]],device const float*bias[[buffer(2)]],device float*y[[buffer(3)]],constant "
            "P&p[[buffer(4)]],uint lane[[thread_index_in_threadgroup]],uint "
            "group[[threadgroup_position_in_grid]]){body<4>(x,w,bias,y,p,lane,group);}\n"
            "kernel void segment_8(device const float*x[[buffer(0)]],device const "
            "ushort*w[[buffer(1)]],device const float*bias[[buffer(2)]],device float*y[[buffer(3)]],constant "
            "P&p[[buffer(4)]],uint lane[[thread_index_in_threadgroup]],uint "
            "group[[threadgroup_position_in_grid]]){body<8>(x,w,bias,y,p,lane,group);}\n"
            "kernel void segment_16(device const float*x[[buffer(0)]],device const "
            "ushort*w[[buffer(1)]],device const float*bias[[buffer(2)]],device float*y[[buffer(3)]],constant "
            "P&p[[buffer(4)]],uint lane[[thread_index_in_threadgroup]],uint "
            "group[[threadgroup_position_in_grid]]){body<16>(x,w,bias,y,p,lane,group);}\n"
            "kernel void segment_32(device const float*x[[buffer(0)]],device const "
            "ushort*w[[buffer(1)]],device const float*bias[[buffer(2)]],device float*y[[buffer(3)]],constant "
            "P&p[[buffer(4)]],uint lane[[thread_index_in_threadgroup]],uint "
            "group[[threadgroup_position_in_grid]]){body<32>(x,w,bias,y,p,lane,group);}\n"
            "\n"
            "struct PN{uint rows,hidden_size;float eps;};\n"
            "kernel void pair_norm(device const float*x[[buffer(0)]],device const "
            "float*w[[buffer(1)]],device float*y[[buffer(2)]],constant PN&p[[buffer(3)]],uint "
            "group[[threadgroup_position_in_grid]],uint sg[[simdgroup_index_in_threadgroup]],uint "
            "lane[[thread_index_in_simdgroup]]){\n"
            " uint row=group*4+sg;if(row>=p.rows)return;uint base=row*(p.hidden_size/4);float sum=0;\n"
            " for(uint i=lane;i<p.hidden_size/4;i+=32){float4 v=((device const "
            "float4*)x)[base+i];sum+=dot(v,v);}\n"
            " float scale=rsqrt(simd_sum(sum)/float(p.hidden_size)+p.eps);\n"
            " for(uint i=lane;i<p.hidden_size/4;i+=32)((device float4*)y)[base+i]=((device const "
            "float4*)x)[base+i]*(scale*((device const float4*)w)[i]);\n"
            "}\n"
            "kernel void pair_flush(device uint*x[[buffer(0)]],constant uint&count[[buffer(1)]],uint "
            "i[[thread_position_in_grid]]){if(i<count)x[i]^=0x9e3779b9u;}\n"
            "\n"
            "template<uint H>inline void cached_norm_body(device const float4*x,device const float4*w,device "
            "float4*y,constant PN&p,threadgroup float*partial,uint tid,uint sg,uint lane,uint row){\n"
            " if(row>=p.rows)return;constexpr uint count=(H/4+127)/128;float4 stored[count];float sum=0;\n"
            " #pragma unroll\n"
            " for(uint i=0;i<count;i++){uint "
            "col=tid+i*128;stored[i]=col<H/4?x[row*(H/4)+col]:float4(0);sum+=dot(stored[i],stored[i]);}\n"
            " sum=simd_sum(sum);if(lane==0)partial[sg]=sum;threadgroup_barrier(mem_flags::mem_threadgroup);\n"
            " float scale=rsqrt(simd_sum(lane<4?partial[lane]:0)/float(H)+p.eps);\n"
            " #pragma unroll\n"
            " for(uint i=0;i<count;i++){uint "
            "col=tid+i*128;if(col<H/4)y[row*(H/4)+col]=stored[i]*(scale*w[col]);}\n"
            "}\n"
            "#define NORM(H) kernel void pair_cached_##H(device const float4*x[[buffer(0)]],device const "
            "float4*w[[buffer(1)]],device float4*y[[buffer(2)]],constant PN&p[[buffer(3)]],threadgroup "
            "float*partial[[threadgroup(0)]],uint tid[[thread_index_in_threadgroup]],uint "
            "sg[[simdgroup_index_in_threadgroup]],uint lane[[thread_index_in_simdgroup]],uint "
            "row[[threadgroup_position_in_grid]]){cached_norm_body<H>(x,w,y,p,partial,tid,sg,lane,row);}\n"
            "NORM(1024) NORM(2560)\n"
            "\n"
            "template<uint B,uint SG>inline void pair_exact_body(device const float*x,device const "
            "ushort*w,device const float*bias,device float*y,constant P&p,uint lane,uint sg,uint group){\n"
            " uint row=group*SG+sg;if(row>=p.out_dim)return;float sums[B];for(uint b=0;b<B;b++)sums[b]=0;\n"
            " for(uint start=lane*4;start<1024;start+=1024){\n"
            "  float4 loaded[8];\n"
            "  #pragma unroll\n"
            "  for(uint j=0;j<8;j++)loaded[j]=true?as_type<float4>(uint4(*((device const "
            "ushort4*)(w+row*1024+start+j*128)))<<16):float4(0);\n"
            "  #pragma unroll\n"
            "  for(uint b=0;b<B;b++){float partial=0;\n"
            "   #pragma unroll\n"
            "   for(uint j=0;j<8;j++)partial+=dot(loaded[j],*((device const "
            "float4*)(x+b*1024+start+j*128)));\n"
            "   sums[b]+=partial;\n"
            "  }\n"
            " }\n"
            " #pragma unroll\n"
            " for(uint b=0;b<B;b++){float "
            "total=simd_sum(sums[b]);if(lane==0)y[b*p.out_dim+row]=total+bias[row];}\n"
            "}\n"
            "\n"
            "kernel void pair_exact(device const float*x[[buffer(0)]],device const "
            "ushort*w[[buffer(1)]],device const float*bias[[buffer(2)]],device float*y[[buffer(3)]],constant "
            "P&p[[buffer(4)]],uint lane[[thread_index_in_simdgroup]],uint "
            "sg[[simdgroup_index_in_threadgroup]],uint "
            "group[[threadgroup_position_in_grid]]){pair_exact_body<4,4>(x,w,bias,y,p,lane,sg,group);}\n"
            "\n"
            "kernel void pair_packed(device const float*x[[buffer(0)]],device const "
            "ushort*w[[buffer(1)]],device const float*bias[[buffer(2)]],device float*y[[buffer(3)]],constant "
            "P&p[[buffer(4)]],uint lane[[thread_index_in_simdgroup]],uint "
            "sg[[simdgroup_index_in_threadgroup]],uint group[[threadgroup_position_in_grid]]) {\n"
            " uint first=(group*4+sg)*2;float accum[4][2];\n"
            " #pragma unroll\n"
            " for(uint b=0;b<4;b++) {\n"
            "  #pragma unroll\n"
            "  for(uint c=0;c<2;c++)accum[b][c]=0;\n"
            " }\n"
            " for(uint i=lane*4;i<p.in_dim;i+=128){float4 weights[2];\n"
            "  #pragma unroll\n"
            "  for(uint c=0;c<2;c++)weights[c]=as_type<float4>(uint4(*((device const "
            "ushort4*)(w+(first+c)*p.in_dim+i)))<<16);\n"
            "  #pragma unroll\n"
            "  for(uint b=0;b<4;b++){float4 v=*((device const float4*)(x+b*p.in_dim+i));\n"
            "   #pragma unroll\n"
            "   for(uint c=0;c<2;c++)accum[b][c]+=dot(v,weights[c]);\n"
            "  }\n"
            " }\n"
            " #pragma unroll\n"
            " for(uint b=0;b<4;b++) {\n"
            "  #pragma unroll\n"
            "  for(uint c=0;c<2;c++){float "
            "value=simd_sum(accum[b][c]);if(lane==0)y[b*p.out_dim+first+c]=value+bias[first+c];}\n"
            " }\n"
            "}\n"
            "[[max_total_threads_per_threadgroup(128)]] kernel void pair_stream_bfloat(device const "
            "float4*x[[buffer(0)]],device const "
            "bfloat4*w[[buffer(1)]],device const float*bias[[buffer(2)]],device "
            "float*y[[buffer(3)]],constant P&p[[buffer(4)]],uint lane[[thread_index_in_simdgroup]],uint "
            "sg[[simdgroup_index_in_threadgroup]],uint group[[threadgroup_position_in_grid]]){\n"
            " uint col=group*4+sg;if(col>=p.out_dim)return;float4 coeff[8];\n"
            " #pragma unroll\n"
            " for(uint j=0;j<8;j++)coeff[j]=float4(w[col*256+lane+j*32]);\n"
            " #pragma unroll\n"
            " for(uint row=0;row<4;row++){float sum=0;\n"
            " #pragma unroll\n"
            " for(uint j=0;j<8;j++)sum+=dot(coeff[j],x[row*256+lane+j*32]);\n"
            " sum=simd_sum(sum);if(lane==0)y[row*p.out_dim+col]=sum+bias[col];}\n"
            "}\n";
}
static NSString *ocr_pair_split_source(void) {
    return @"#include <metal_stdlib>\n#include "
           @"<MetalPerformancePrimitives/MetalPerformancePrimitives.h>\nusing namespace metal;using "
           @"namespace mpp::tensor_ops;\nstruct P{uint rows,in_dim,out_dim,pad;};\ntemplate<uint N> inline "
           @"void body(device float*x,device bfloat*gw,device bfloat*uw,device float*gb,device "
           @"float*ub,device float*y,constant P&p,uint2 group){\n uint chunk=p.in_dim/p.pad;\n tensor<device "
           @"float,dextents<int,2>,tensor_inline>a(x+group.y*chunk,dextents<int,2>(chunk,p.rows),array<int,2>"
           @"{1,int(p.in_dim)});\n tensor<device "
           @"bfloat,dextents<int,2>,tensor_inline>gmat(gw+group.y*chunk,dextents<int,2>(chunk,p.out_dim),"
           @"array<int,2>{1,int(p.in_dim)}),umat(uw+group.y*chunk,dextents<int,2>(chunk,p.out_dim),array<int,"
           @"2>{1,int(p.in_dim)});\n tensor<device "
           @"float,dextents<int,2>,tensor_inline>dest(y+group.y*p.rows*p.out_dim,dextents<int,2>(p.out_dim,p."
           @"rows),array<int,2>{1,int(p.out_dim)});\n tensor<device "
           @"float,dextents<int,2>,tensor_inline>destU(y+(p.pad+group.y)*p.rows*p.out_dim,dextents<int,2>(p."
           @"out_dim,p.rows),array<int,2>{1,int(p.out_dim)});\n auto left=a.slice(0,0);auto "
           @"rightG=gmat.slice(0,int(group.x)*N);auto rightU=umat.slice(0,int(group.x)*N);auto "
           @"out=dest.slice(int(group.x)*N,0);auto outU=destU.slice(int(group.x)*N,0);\n constexpr auto "
           @"desc=matmul2d_descriptor(8,N,dynamic_length_v<int>,false,true,false);matmul2d<desc,execution_"
           @"simdgroups<1>>op;\n auto g=op.template "
           @"get_destination_cooperative_tensor<decltype(left),decltype(rightG),float>();auto u=op.template "
           @"get_destination_cooperative_tensor<decltype(left),decltype(rightU),float>();\n "
           @"op.run(left,rightG,g);op.run(left,rightU,u);\n g.store(out);u.store(outU);\n}\n#define K(N) "
           @"kernel void fused##N(device float*x[[buffer(0)]],device bfloat*gw[[buffer(1)]],device "
           @"bfloat*uw[[buffer(2)]],device float*gb[[buffer(3)]],device float*ub[[buffer(4)]],device "
           @"float*y[[buffer(5)]],constant P&p[[buffer(6)]],uint2 "
           @"group[[threadgroup_position_in_grid]]){body<N>(x,gw,uw,gb,ub,y,p,group);}\nK(16) K(32) "
           @"K(64)\nkernel void actmul(device const float*g[[buffer(0)]],device const "
           @"float*u[[buffer(1)]],device float*y[[buffer(2)]],constant uint&n[[buffer(3)]],uint "
           @"i[[thread_position_in_grid]]){if(i<n)y[i]=(g[i]/(1.0f+exp(-g[i])))*u[i];}\n\nkernel void "
           @"fusedreduce(device const float*s[[buffer(0)]],device const float*gb[[buffer(1)]],device const "
           @"float*ub[[buffer(2)]],device float*y[[buffer(3)]],constant P&p[[buffer(4)]],uint "
           @"i[[thread_position_in_grid]]){uint count=p.rows*p.out_dim;if(i>=count)return;float "
           @"g=0,u=0;for(uint "
           @"j=0;j<p.pad;j++){g+=s[j*count+i];u+=s[(p.pad+j)*count+i];}g+=gb[i%p.out_dim];u+=ub[i%p.out_dim];"
           @"y[i]=(g/(1.0f+exp(-g)))*u;}\n";
}
static NSString *ocr_pair_stream_gate_source(void) {
    return @"#include <metal_stdlib>\nusing namespace metal;\nstruct P { uint rows, in_dim, out_dim, pad; "
           @"};\nkernel void gate4(device const float4*x[[buffer(0)]],device const "
           @"ushort4*gw[[buffer(1)]],device const ushort4*uw[[buffer(2)]],device const "
           @"float*gb[[buffer(3)]],device const float*ub[[buffer(4)]],device float*y[[buffer(5)]],constant "
           @"P&p[[buffer(6)]],uint col[[threadgroup_position_in_grid]],uint "
           @"lane[[thread_index_in_simdgroup]]) {\n    float4 g[4],u[4];\n    for(uint "
           @"r=0;r<4;r++){g[r]=0;u[r]=0;}\n    uint stride=p.in_dim/4;\n    for(uint "
           @"k=lane;k<stride;k+=32){\n        float4 wg=as_type<float4>(uint4(gw[col*stride+k])<<16);\n      "
           @"  float4 wu=as_type<float4>(uint4(uw[col*stride+k])<<16);\n        #pragma unroll\n        "
           @"for(uint r=0;r<4;r++) {\n            float4 "
           @"a=x[r*stride+k];g[r]=fma(a,wg,g[r]);u[r]=fma(a,wu,u[r]);\n        }\n    }\n    #pragma "
           @"unroll\n    for(uint r=0;r<4;r++) {\n        float gs=simd_sum(dot(g[r],float4(1)))+gb[col];\n  "
           @"      float us=simd_sum(dot(u[r],float4(1)))+ub[col];\n        "
           @"if(lane==0)y[r*p.out_dim+col]=(gs/(1+exp(-gs)))*us;\n    }\n}\ntemplate<uint R> inline void "
           @"pair_partial_vec_body(device const float*x,device const ushort*w,device const float*bias,device "
           @"float*y,constant P&p,uint col,uint lane){\n    float4 acc[R];\n    #pragma unroll\n    for(uint "
           @"r=0;r<R;r++)acc[r]=0;\n    for(uint k=lane*4;k<p.in_dim;k+=128){\n        float4 "
           @"weight=as_type<float4>(uint4(*((device const ushort4*)(w+col*p.in_dim+k)))<<16);\n        "
           @"#pragma unroll\n        for(uint r=0;r<R;r++)acc[r]=fma(*((device const "
           @"float4*)(x+r*p.in_dim+k)),weight,acc[r]);\n    }\n    #pragma unroll\n    for(uint "
           @"r=0;r<R;r++){\n        float sum=simd_sum(dot(acc[r],float4(1)));\n        "
           @"if(lane==0)y[r*p.out_dim+col]=sum+bias[col];\n    }\n}\nkernel void pair_vec2(device const "
           @"float*x[[buffer(0)]],device const ushort*w[[buffer(1)]],device const "
           @"float*bias[[buffer(2)]],device float*y[[buffer(3)]],constant P&p[[buffer(4)]],uint "
           @"col[[threadgroup_position_in_grid]],uint "
           @"lane[[thread_index_in_simdgroup]]){pair_partial_vec_body<2>(x,w,bias,y,p,col,lane);}\nkernel "
           @"void pair_vec3(device const float*x[[buffer(0)]],device const ushort*w[[buffer(1)]],device "
           @"const float*bias[[buffer(2)]],device float*y[[buffer(3)]],constant P&p[[buffer(4)]],uint "
           @"col[[threadgroup_position_in_grid]],uint "
           @"lane[[thread_index_in_simdgroup]]){pair_partial_vec_body<3>(x,w,bias,y,p,col,lane);}\n";
}
static id<MTLLibrary> pair_stream_gate_library;
static id<MTLComputePipelineState> pair_stream_gate, pair_vec2, pair_vec3;
static size_t pair_variant;
static id<MTLLibrary> pair_variant_library, pair_split_library, pair_gate_library;
static id<MTLComputePipelineState> pair_gate8, pair_gate64;
static id<MTLComputePipelineState> pair_cached1024, pair_cached2560, pair_exact, pair_stream_bfloat,
    pair_packed;
static id<MTLComputePipelineState> pair_norm, pair_segment8, pair_segment16, pair_gate_split,
    pair_gate_reduce;
void ocr_pair_set_variant(size_t variant) { pair_variant = variant; }
size_t ocr_pair_candidate_applies(void *handle) {
    OCRPairCase *c = (__bridge OCRPairCase *)handle;
    if (!c)
        return 0;
    switch (pair_variant) {
    case 1:
        return c->kind == 0 && ((c->rows >= 2 && c->rows <= 3 && c->out_dim <= 9216) ||
                                (c->rows == 4 && c->in_dim == 1024 && c->out_dim == 6144));
    case 2:
        return c->kind == 0 && c->rows == 4 && c->in_dim == 3584 && c->out_dim == 1024;
    case 3:
        return c->kind == 1;
    case 4:
    case 5:
        return c->kind == 0 && c->rows == 4 && c->in_dim == 1024 && c->out_dim == 3584;
    case 6:
        return c->kind == 0 && c->rows == 1 && c->in_dim == 3584 && c->out_dim == 1024;
    case 28:
        return c->kind == 3 && ((c->in_dim == 1024 && c->rows >= 2 && c->rows <= 8) ||
                                (c->in_dim == 2560 && c->rows >= 2 && c->rows <= 3));
    case 7:
        return c->kind == 3 && ((c->in_dim == 1024 && c->rows >= 2 && c->rows <= 8) ||
                                (c->in_dim == 2560 && c->rows >= 2 && c->rows <= 7));
    case 29:
        return c->kind == 0 && c->rows >= 2 && c->rows <= 7 && c->out_dim <= 9216;
    case 8:
        return c->kind == 0 && ((c->rows == 4 && c->out_dim <= 9216) ||
                                (c->rows >= 5 && c->rows <= 7 && c->in_dim == 1024 && c->out_dim == 6144));
    case 26:
    case 27:
        return c->kind == 0 && c->rows == (pair_variant == 26 ? 2 : 3) && c->out_dim <= 9216;
    case 24:
    case 25:
        return c->kind == 0 && c->rows >= 4 && c->rows <= 8 && c->in_dim == 3584 && c->out_dim == 1024;
    case 9:
    case 10:
    case 11:
        return c->kind == 1;
    case 12:
        return c->kind == 1;
    case 13:
        return c->kind == 0 && c->rows == 4 && c->in_dim == 1024 &&
               (c->out_dim == 3584 || c->out_dim == 6144);
    case 14:
        return c->kind == 0 && c->rows == 4;
    case 23:
        return c->kind == 3 && c->rows >= 4 && c->rows <= 8 && c->in_dim == 1024 && c->out_dim == 3584;
    case 22:
        return c->kind == 3 && c->rows == 4 && c->in_dim == 1024 && c->out_dim == 3584;
    case 20:
    case 21:
        return c->kind == 0 && ((c->rows == 4 && c->in_dim == 3584 && c->out_dim == 1024) ||
                                (c->rows == 8 && c->out_dim == 248320));
    case 17:
    case 18:
    case 19:
        return c->kind == 3;
    case 16:
        return c->kind == 0 && c->rows == 4 && c->out_dim % 8 == 0;
    case 15:
        return c->kind == 0 && c->rows == 4 && c->in_dim == 1024 &&
               (c->out_dim == 3584 || c->out_dim == 6144);
    default:
        return 0;
    }
}
static int ocr_pair_encode_candidate(OCRPairCase *c) {
    termite_metal_decode_runtime *r = pair_runtime;
    if ((pair_variant == 22 || pair_variant == 26 || pair_variant == 27) && !pair_stream_gate_library) {
        MTLCompileOptions *options = [MTLCompileOptions new];
        options.mathMode = MTLMathModeFast;
        options.languageVersion = MTLLanguageVersion4_0;
        NSError *error = nil;
        pair_stream_gate_library = [r->device newLibraryWithSource:ocr_pair_stream_gate_source()
                                                           options:options
                                                             error:&error];
        if (!pair_stream_gate_library) {
            fprintf(stderr, "stream gate: %s\n", error.localizedDescription.UTF8String);
            return -1;
        }
        pair_stream_gate = termite_metal_make_pipeline(r->device, pair_stream_gate_library, @"gate4");
        pair_vec2 = termite_metal_make_pipeline(r->device, pair_stream_gate_library, @"pair_vec2");
        pair_vec3 = termite_metal_make_pipeline(r->device, pair_stream_gate_library, @"pair_vec3");
    }
    if (pair_variant == 3 || pair_variant == 4 || pair_variant == 5 || pair_variant == 12 ||
        pair_variant == 13 || pair_variant == 15 || pair_variant == 16) {
        if (!pair_variant_library) {
            MTLCompileOptions *opt = [MTLCompileOptions new];
            opt.mathMode = MTLMathModeFast;
            opt.languageVersion = MTLLanguageVersion4_0;
            NSError *error = nil;
            pair_variant_library = [r->device newLibraryWithSource:ocr_pair_variant_source()
                                                           options:opt
                                                             error:&error];
            if (!pair_variant_library) {
                fprintf(stderr, "candidate: %s\n", error.localizedDescription.UTF8String);
                return -1;
            }
            pair_norm = termite_metal_make_pipeline(r->device, pair_variant_library, @"pair_norm");
            pair_segment8 = termite_metal_make_pipeline(r->device, pair_variant_library, @"segment_8");
            pair_segment16 = termite_metal_make_pipeline(r->device, pair_variant_library, @"segment_16");
        }
    }
    if ((pair_variant == 17 || pair_variant == 19) && !pair_gate_library) {
        MTLCompileOptions *options = [MTLCompileOptions new];
        options.languageVersion = MTLLanguageVersion4_0;
        options.mathMode = MTLMathModeSafe;
        NSString *extra =
            @"\n#define G(N) kernel void pair_gate##N(device float*x[[buffer(0)]],device "
            @"bfloat*g[[buffer(1)]],device bfloat*u[[buffer(2)]],device float*gb[[buffer(3)]],device "
            @"float*ub[[buffer(4)]],device float*y[[buffer(5)]],constant Params&p[[buffer(6)]],uint2 "
            @"group[[threadgroup_position_in_grid]]){termite_lightonocr3_gate_up_small_body<N>(x,g,u,gb,ub,y,"
            @"p,group); }\nG(8) G(64)\n";
        extra = [extra stringByReplacingOccurrencesOfString:@"G(8) G(64)"
                                                 withString:pair_variant == 17 ? @"G(8)" : @"G(64)"];
        NSError *error = nil;
        pair_gate_library = [r->device
            newLibraryWithSource:[termite_metal_lightonocr3_tensor_source() stringByAppendingString:extra]
                         options:options
                           error:&error];
        if (!pair_gate_library) {
            fprintf(stderr, "gate candidate: %s\n", error.localizedDescription.UTF8String);
            return -1;
        }
        if (pair_variant == 17)
            pair_gate8 = termite_metal_make_pipeline(r->device, pair_gate_library, @"pair_gate8");
        else
            pair_gate64 = termite_metal_make_pipeline(r->device, pair_gate_library, @"pair_gate64");
    }
    if (pair_variant == 12) {
        if (!pair_cached1024)
            pair_cached1024 =
                termite_metal_make_pipeline(r->device, pair_variant_library, @"pair_cached_1024");
        if (!pair_cached2560)
            pair_cached2560 =
                termite_metal_make_pipeline(r->device, pair_variant_library, @"pair_cached_2560");
    }
    if (pair_variant == 16 && !pair_packed)
        pair_packed = termite_metal_make_pipeline(r->device, pair_variant_library, @"pair_packed");
    if (pair_variant == 13 && !pair_exact)
        pair_exact = termite_metal_make_pipeline(r->device, pair_variant_library, @"pair_exact");
    if (pair_variant == 15 && !pair_stream_bfloat)
        pair_stream_bfloat =
            termite_metal_make_pipeline(r->device, pair_variant_library, @"pair_stream_bfloat");
    if ((pair_variant == 7 || pair_variant == 28) && !pair_split_library) {
        MTLCompileOptions *opt = [MTLCompileOptions new];
        opt.languageVersion = MTLLanguageVersion4_0;
        opt.mathMode = MTLMathModeSafe;
        NSError *error = nil;
        pair_split_library = [r->device newLibraryWithSource:ocr_pair_split_source()
                                                     options:opt
                                                       error:&error];
        if (!pair_split_library) {
            fprintf(stderr, "candidate: %s\n", error.localizedDescription.UTF8String);
            return -1;
        }
        pair_gate_split = termite_metal_make_pipeline(r->device, pair_split_library, @"fused16");
        pair_gate_reduce = termite_metal_make_pipeline(r->device, pair_split_library, @"fusedreduce");
    }
    if ((pair_variant == 15 && !pair_stream_bfloat) ||
        (pair_variant == 12 && (!pair_cached1024 || !pair_cached2560)) ||
        (pair_variant == 13 && !pair_exact) ||
        ((pair_variant == 7 || pair_variant == 28) && (!pair_gate_split || !pair_gate_reduce)))
        return -1;
    // Experimental launches also fail before opening an encoder if a PSO is unavailable.
    id<MTLComputePipelineState> selected = pair_variant == 26   ? pair_vec2
                                           : pair_variant == 27 ? pair_vec3
                                           : pair_variant == 23 ? r->lightonocr3_gate_up_small16_pipeline
                                           : pair_variant == 22 ? pair_stream_gate
                                           : pair_variant == 20 ? r->lightonocr3_mpp_four_row_pipeline
                                           : pair_variant == 21 ? r->lightonocr3_mpp_small_tile_pipeline
                                           : pair_variant == 17 ? pair_gate8
                                           : pair_variant == 18 ? r->lightonocr3_gate_up_small32_pipeline
                                           : pair_variant == 19 ? pair_gate64
                                           : pair_variant == 16 ? pair_packed
                                           : pair_variant == 1  ? r->lightonocr3_mpp_four_row_pipeline
                                           : (pair_variant == 2 || pair_variant == 8 || pair_variant == 29 ||
                                              pair_variant == 24 || pair_variant == 25)
                                               ? r->lightonocr3_down_split32_pipeline
                                           : pair_variant == 3 ? pair_norm
                                           : pair_variant == 4 ? pair_segment8
                                           : pair_variant == 5 ? pair_segment16
                                           : pair_variant == 6 ? r->lightonocr3_gemv_pipeline
                                           : (pair_variant == 7 || pair_variant == 28) ? pair_gate_split
                                           : pair_variant == 12
                                               ? (c->in_dim == 1024 ? pair_cached1024 : pair_cached2560)
                                           : pair_variant == 13 ? pair_exact
                                           : pair_variant == 14 ? r->lightonocr3_batch4_pipeline
                                           : pair_variant == 15 ? pair_stream_bfloat
                                                                : r->rms_norm_rows_simd4_reduce_pipeline;
    const size_t required_threads = pair_variant == 11 ? 256
                                    : (pair_variant == 3 || pair_variant == 10 || pair_variant == 12 ||
                                       pair_variant == 13 || pair_variant == 15 || pair_variant == 16)
                                        ? 128
                                    : pair_variant == 9 ? 64
                                                        : 32;
    if (!selected || selected.threadExecutionWidth != 32 ||
        selected.maxTotalThreadsPerThreadgroup < required_threads ||
        ((pair_variant == 2 || pair_variant == 8 || pair_variant == 29 || pair_variant == 24 ||
          pair_variant == 25) &&
         !r->lightonocr3_down_split_reduce_pipeline))
        return -1;
    id<MTLBuffer> scratch = ((pair_variant == 2 || pair_variant == 8 || pair_variant == 29 ||
                              pair_variant == 24 || pair_variant == 25) ||
                             (pair_variant == 7 || pair_variant == 28))
                                ? termite_metal_decode_gqa_split_scratch_for_encoding(r)
                                : nil;
    size_t splits = pair_variant == 28                                               ? 8
                    : pair_variant == 24                                             ? 16
                    : pair_variant == 25                                             ? 32
                    : (pair_variant == 2 || pair_variant == 8 || pair_variant == 29) ? 8
                                                                                     : 4;
    if (((pair_variant == 2 || pair_variant == 8 || pair_variant == 29 || pair_variant == 24 ||
          pair_variant == 25) ||
         (pair_variant == 7 || pair_variant == 28)) &&
        (!scratch || scratch.length < splits * c->rows * c->out_dim * 4 *
                                          ((pair_variant == 7 || pair_variant == 28) ? 2 : 1)))
        return -1;
    id<MTLComputeCommandEncoder> e = termite_metal_tracked_compute_command_encoder_for(
        r->active_frame_cb, TERMITE_METAL_COMPUTE_SOURCE_OTHER);
    if (!e)
        return -1;
    termite_metal_linear_params p = {(uint32_t)c->rows, (uint32_t)c->in_dim, (uint32_t)c->out_dim,
                                     (uint32_t)splits};
    if (pair_variant == 26 || pair_variant == 27) {
        [e setComputePipelineState:selected];
        [e setBuffer:c->input offset:0 atIndex:0];
        [e setBuffer:r->linear_weight_buffers[0] offset:0 atIndex:1];
        [e setBuffer:r->linear_bias_buffers[0] offset:0 atIndex:2];
        [e setBuffer:c->output offset:0 atIndex:3];
        [e setBytes:&p length:sizeof(p) atIndex:4];
        [e dispatchThreadgroups:MTLSizeMake(c->out_dim, 1, 1) threadsPerThreadgroup:MTLSizeMake(32, 1, 1)];
    } else if (pair_variant == 20 || pair_variant == 21) {
        [e setComputePipelineState:selected];
        [e setBuffer:c->input offset:0 atIndex:0];
        [e setBuffer:r->linear_weight_buffers[0] offset:0 atIndex:1];
        [e setBuffer:r->linear_bias_buffers[0] offset:0 atIndex:2];
        [e setBuffer:c->output offset:0 atIndex:3];
        [e setBytes:&p length:sizeof(p) atIndex:4];
        [e dispatchThreadgroups:MTLSizeMake(c->out_dim / (pair_variant == 20 ? 16 : 32), 1, 1)
            threadsPerThreadgroup:MTLSizeMake(32, 1, 1)];
    } else if ((pair_variant >= 17 && pair_variant <= 19) || pair_variant == 22 || pair_variant == 23) {
        [e setComputePipelineState:selected];
        [e setBuffer:c->input offset:0 atIndex:0];
        [e setBuffer:r->linear_weight_buffers[0] offset:0 atIndex:1];
        [e setBuffer:r->linear_weight_buffers[1] offset:0 atIndex:2];
        [e setBuffer:r->linear_bias_buffers[0] offset:0 atIndex:3];
        [e setBuffer:r->linear_bias_buffers[1] offset:0 atIndex:4];
        [e setBuffer:c->output offset:0 atIndex:5];
        [e setBytes:&p length:sizeof(p) atIndex:6];
        size_t columns = pair_variant == 23   ? 16
                         : pair_variant == 22 ? 1
                         : pair_variant == 17 ? 8
                         : pair_variant == 18 ? 32
                                              : 64;
        [e dispatchThreadgroups:MTLSizeMake(c->out_dim / columns, 1, 1)
            threadsPerThreadgroup:MTLSizeMake(32, 1, 1)];
    } else if (pair_variant == 16) {
        [e setComputePipelineState:pair_packed];
        [e setBuffer:c->input offset:0 atIndex:0];
        [e setBuffer:r->linear_weight_buffers[0] offset:0 atIndex:1];
        [e setBuffer:r->linear_bias_buffers[0] offset:0 atIndex:2];
        [e setBuffer:c->output offset:0 atIndex:3];
        [e setBytes:&p length:sizeof(p) atIndex:4];
        [e dispatchThreadgroups:MTLSizeMake(c->out_dim / 8, 1, 1)
            threadsPerThreadgroup:MTLSizeMake(128, 1, 1)];
    } else if (pair_variant == 3 || pair_variant == 9 || pair_variant == 10 || pair_variant == 11 ||
               pair_variant == 12) {
        termite_metal_apply_row_norm_params np = {(uint32_t)c->rows, (uint32_t)c->in_dim, 1e-6f};
        [e setComputePipelineState:pair_variant == 12
                                       ? (c->in_dim == 1024 ? pair_cached1024 : pair_cached2560)
                                   : pair_variant == 3 ? pair_norm
                                                       : r->rms_norm_rows_simd4_reduce_pipeline];
        [e setBuffer:c->input offset:0 atIndex:0];
        [e setBuffer:r->rms_norm_weight_buffers[0] offset:0 atIndex:1];
        [e setBuffer:c->output offset:0 atIndex:2];
        [e setBytes:&np length:sizeof(np) atIndex:3];
        if (pair_variant == 12) {
            [e setThreadgroupMemoryLength:16 atIndex:0];
            [e dispatchThreadgroups:MTLSizeMake(c->rows, 1, 1) threadsPerThreadgroup:MTLSizeMake(128, 1, 1)];
        } else if (pair_variant == 3)
            [e dispatchThreadgroups:MTLSizeMake((c->rows + 3) / 4, 1, 1)
                threadsPerThreadgroup:MTLSizeMake(128, 1, 1)];
        else {
            [e setThreadgroupMemoryLength:128 atIndex:0];
            [e dispatchThreadgroups:MTLSizeMake(c->rows, 1, 1)
                threadsPerThreadgroup:MTLSizeMake(pair_variant == 9    ? 64
                                                  : pair_variant == 10 ? 128
                                                                       : 256,
                                                  1, 1)];
        }
    } else if (pair_variant == 13 || pair_variant == 15 || pair_variant == 14) {
        [e setComputePipelineState:pair_variant == 15   ? pair_stream_bfloat
                                   : pair_variant == 14 ? r->lightonocr3_batch4_pipeline
                                                        : pair_exact];
        [e setBuffer:c->input offset:0 atIndex:0];
        [e setBuffer:r->linear_weight_buffers[0] offset:0 atIndex:1];
        [e setBuffer:r->linear_bias_buffers[0] offset:0 atIndex:2];
        [e setBuffer:c->output offset:0 atIndex:3];
        [e setBytes:&p length:sizeof(p) atIndex:4];
        [e dispatchThreadgroups:MTLSizeMake(c->out_dim / (pair_variant == 14 ? 1 : 4), 1, 1)
            threadsPerThreadgroup:MTLSizeMake(pair_variant == 14 ? 32 : 128, 1, 1)];
    } else if ((pair_variant == 7 || pair_variant == 28)) {
        [e setComputePipelineState:pair_gate_split];
        [e setBuffer:c->input offset:0 atIndex:0];
        [e setBuffer:r->linear_weight_buffers[0] offset:0 atIndex:1];
        [e setBuffer:r->linear_weight_buffers[1] offset:0 atIndex:2];
        [e setBuffer:r->linear_bias_buffers[0] offset:0 atIndex:3];
        [e setBuffer:r->linear_bias_buffers[1] offset:0 atIndex:4];
        [e setBuffer:scratch offset:0 atIndex:5];
        [e setBytes:&p length:sizeof(p) atIndex:6];
        [e dispatchThreadgroups:MTLSizeMake(c->out_dim / 16, splits, 1)
            threadsPerThreadgroup:MTLSizeMake(32, 1, 1)];
        [e memoryBarrierWithScope:MTLBarrierScopeBuffers];
        [e setComputePipelineState:pair_gate_reduce];
        [e setBuffer:scratch offset:0 atIndex:0];
        [e setBuffer:r->linear_bias_buffers[0] offset:0 atIndex:1];
        [e setBuffer:r->linear_bias_buffers[1] offset:0 atIndex:2];
        [e setBuffer:c->output offset:0 atIndex:3];
        [e setBytes:&p length:sizeof(p) atIndex:4];
        [e dispatchThreads:MTLSizeMake(c->rows * c->out_dim, 1, 1)
            threadsPerThreadgroup:MTLSizeMake(128, 1, 1)];
    } else {
        id<MTLComputePipelineState> pipeline =
            pair_variant == 1 ? r->lightonocr3_mpp_four_row_pipeline
            : (pair_variant == 2 || pair_variant == 8 || pair_variant == 29 || pair_variant == 24 ||
               pair_variant == 25)
                ? r->lightonocr3_down_split32_pipeline
            : pair_variant == 4 ? pair_segment8
            : pair_variant == 5 ? pair_segment16
                                : r->lightonocr3_gemv_pipeline;
        [e setComputePipelineState:pipeline];
        [e setBuffer:c->input offset:0 atIndex:0];
        [e setBuffer:r->linear_weight_buffers[0] offset:0 atIndex:1];
        [e setBuffer:(pair_variant == 2 || pair_variant == 8 || pair_variant == 29 || pair_variant == 24 ||
                      pair_variant == 25)
                         ? scratch
                         : r->linear_bias_buffers[0]
              offset:0
             atIndex:2];
        [e setBuffer:c->output offset:0 atIndex:3];
        if (pair_variant == 6) {
            termite_metal_apply_linear_params sp = {(uint32_t)c->in_dim, (uint32_t)c->out_dim};
            [e setBytes:&sp length:sizeof(sp) atIndex:4];
        } else if ((pair_variant == 2 || pair_variant == 8 || pair_variant == 29 || pair_variant == 24 ||
                    pair_variant == 25))
            [e setBytes:&p length:sizeof(p) atIndex:3];
        else
            [e setBytes:&p length:sizeof(p) atIndex:4];
        size_t columns = pair_variant == 1 ? 16
                         : (pair_variant == 2 || pair_variant == 8 || pair_variant == 29 ||
                            pair_variant == 24 || pair_variant == 25)
                             ? 32
                         : pair_variant == 4 ? 4
                         : pair_variant == 5 ? 2
                                             : 1;
        [e dispatchThreadgroups:MTLSizeMake(c->out_dim / columns,
                                            (pair_variant == 2 || pair_variant == 8 || pair_variant == 29 ||
                                             pair_variant == 24 || pair_variant == 25)
                                                ? splits
                                                : 1,
                                            1)
            threadsPerThreadgroup:MTLSizeMake(32, 1, 1)];
        if ((pair_variant == 2 || pair_variant == 8 || pair_variant == 29 || pair_variant == 24 ||
             pair_variant == 25)) {
            [e memoryBarrierWithScope:MTLBarrierScopeBuffers];
            [e setComputePipelineState:r->lightonocr3_down_split_reduce_pipeline];
            [e setBuffer:scratch offset:0 atIndex:0];
            [e setBuffer:r->linear_bias_buffers[0] offset:0 atIndex:1];
            [e setBuffer:c->output offset:0 atIndex:2];
            [e setBytes:&p length:sizeof(p) atIndex:3];
            [e dispatchThreads:MTLSizeMake(c->rows * c->out_dim, 1, 1)
                threadsPerThreadgroup:MTLSizeMake(128, 1, 1)];
        }
    }
    [e endEncoding];
    return 0;
}

static id<MTLBuffer> pair_flush_buffer;
static id<MTLComputePipelineState> pair_flush_pipeline;
int ocr_pair_flush(size_t mib) {
    if (!pair_runtime || mib == 0 || mib > 512)
        return -1;
    @autoreleasepool {
        if (!pair_variant_library) {
            MTLCompileOptions *opt = [MTLCompileOptions new];
            opt.mathMode = MTLMathModeFast;
            opt.languageVersion = MTLLanguageVersion4_0;
            NSError *error = nil;
            pair_variant_library = [pair_runtime->device newLibraryWithSource:ocr_pair_variant_source()
                                                                      options:opt
                                                                        error:&error];
            if (!pair_variant_library) {
                fprintf(stderr, "cache flush: %s\n", error.localizedDescription.UTF8String);
                return -1;
            }
            pair_norm = termite_metal_make_pipeline(pair_runtime->device, pair_variant_library, @"pair_norm");
            pair_segment8 =
                termite_metal_make_pipeline(pair_runtime->device, pair_variant_library, @"segment_8");
            pair_segment16 =
                termite_metal_make_pipeline(pair_runtime->device, pair_variant_library, @"segment_16");
        }
        if (!pair_flush_pipeline)
            pair_flush_pipeline =
                termite_metal_make_pipeline(pair_runtime->device, pair_variant_library, @"pair_flush");
        size_t bytes = mib * 1024 * 1024;
        if (!pair_flush_buffer || pair_flush_buffer.length != bytes)
            pair_flush_buffer = [pair_runtime->device newBufferWithLength:bytes
                                                                  options:MTLResourceStorageModePrivate];
        if (!pair_flush_buffer || !pair_flush_pipeline)
            return -1;
        id<MTLCommandBuffer> cb = [pair_runtime->queue commandBuffer];
        id<MTLComputeCommandEncoder> e = [cb computeCommandEncoder];
        if (!e)
            return -1;
        uint32_t count = (uint32_t)(bytes / 4);
        [e setComputePipelineState:pair_flush_pipeline];
        [e setBuffer:pair_flush_buffer offset:0 atIndex:0];
        [e setBytes:&count length:4 atIndex:1];
        [e dispatchThreads:MTLSizeMake(count, 1, 1) threadsPerThreadgroup:MTLSizeMake(256, 1, 1)];
        [e endEncoding];
        [cb commit];
        [cb waitUntilCompleted];
        return cb.status == MTLCommandBufferStatusCompleted ? 0 : -1;
    }
}

uint64_t ocr_pair_run(void *handle) {
    @autoreleasepool {
        OCRPairCase *c = (__bridge OCRPairCase *)handle;
        termite_metal_decode_runtime *r = pair_runtime;
        if (!c || !r || termite_metal_decode_runtime_begin_frame(r))
            return 0;
        int rc = 0;
        if (ocr_pair_candidate_applies(handle))
            rc = ocr_pair_encode_candidate(c);
        else if (c->kind == 0)
            rc = c->rows == 1
                     ? termite_metal_decode_runtime_apply_linear_device(r, 0, (__bridge void *)c->input, 0,
                                                                        c->in_dim, c->out_dim,
                                                                        (__bridge void *)c->output, 0)
                     : termite_metal_decode_runtime_apply_linear_multi_row_device(
                           r, 0, (__bridge void *)c->input, 0, c->rows, c->in_dim, c->out_dim,
                           (__bridge void *)c->output, 0);
        else if (c->kind == 1)
            rc = termite_metal_decode_runtime_apply_rms_norm_rows_device(
                r, 0, (__bridge void *)c->input, 0, c->rows, c->in_dim, 1e-6f, (__bridge void *)c->output, 0);
        else if (c->kind == 3) {
            if (termite_metal_decode_runtime_should_apply_bf16_gate_up(r, 0, 1, 0, c->rows, c->in_dim,
                                                                       c->out_dim))
                rc = termite_metal_decode_runtime_apply_bf16_gate_up_device(
                    r, 0, 1, (__bridge void *)c->input, 0, c->rows, c->in_dim, c->out_dim,
                    (__bridge void *)c->output, 0);
            else {
                rc = termite_metal_decode_runtime_apply_linear_multi_row_device(
                    r, 0, (__bridge void *)c->input, 0, c->rows, c->in_dim, c->out_dim,
                    (__bridge void *)c->gates, 0);
                if (!rc)
                    rc = termite_metal_decode_runtime_apply_linear_multi_row_device(
                        r, 1, (__bridge void *)c->input, 0, c->rows, c->in_dim, c->out_dim,
                        (__bridge void *)c->state, 0);
                if (!rc)
                    rc = termite_metal_decode_runtime_apply_activation_multiply_device(
                        r, 2, (__bridge void *)c->gates, 0, (__bridge void *)c->state, 0, c->rows, c->out_dim,
                        (__bridge void *)c->output, 0);
            }
        } else {
            // Same production kernel and launch used by the qualified prefill
            // selector; compare the recurrent stage independently of its core.
            uint32_t p[6] = {(uint32_t)c->rows, 16, (uint32_t)c->out_dim, 128, 4, 0};
            id<MTLComputeCommandEncoder> e = termite_metal_tracked_compute_command_encoder_for(
                r->active_frame_cb, TERMITE_METAL_COMPUTE_SOURCE_OTHER);
            if (!e)
                rc = -1;
            else {
                [e setComputePipelineState:r->qwen35_recurrent_columns4_pipeline];
                [e setBuffer:c->state offset:0 atIndex:8];
                [e setBuffer:c->input offset:0 atIndex:9];
                [e setBuffer:c->gates offset:0 atIndex:10];
                [e setBuffer:c->output offset:0 atIndex:11];
                [e setBytes:p length:24 atIndex:15];
                [e dispatchThreads:MTLSizeMake(c->out_dim * 128 * 8, 1, 1)
                    threadsPerThreadgroup:MTLSizeMake(32, 1, 1)];
                [e endEncoding];
            }
        }
        if (rc) {
            (void)termite_metal_decode_runtime_cancel_frame(r);
            return 0;
        }
        if (termite_metal_decode_runtime_submit_frame(r) || termite_metal_decode_runtime_wait_frame(r))
            return 0;
        return r->last_frame_gpu_nanos;
    }
}
const float *ocr_pair_output(void *handle) {
    OCRPairCase *c = (__bridge OCRPairCase *)handle;
    return c ? c->output.contents : NULL;
}
void ocr_pair_release(void *handle) {
    if (handle) {
        id c = CFBridgingRelease(handle);
        (void)c;
    }
}
void ocr_pair_shutdown(void) {
    if (pair_runtime) {
        termite_metal_decode_runtime_destroy(pair_runtime);
        pair_runtime = NULL;
    }
}
