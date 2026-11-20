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

// Historical ABI exports remain consumed by the Metal runtime pipelines.
// native-program:entry termite_glide_core_conv
           "kernel void termite_glide_core_conv(device const float *mixed [[buffer(0)]], device const float *weight [[buffer(1)]], device const uint *pos [[buffer(2)]], device float *conv [[buffer(3)]], constant termite_glide_core_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {\n"
           "    uint channels=(2u*p.key_heads+p.value_heads)*128u,quads=channels/4u;\n"
           "    if(gid>=p.tokens*quads)return;\n"
           "    uint t=gid/quads,c=(gid%quads)*4u,position=pos[t];\n"
           "    float4 w0=*((device const float4*)(weight+c*4u)),w1=*((device const float4*)(weight+(c+1u)*4u)),w2=*((device const float4*)(weight+(c+2u)*4u)),w3=*((device const float4*)(weight+(c+3u)*4u));\n"
           "    float4 sum=0;\n"
           "    #pragma unroll\n"
           "    for(uint k=0;k<4u;k++){float4 x=0;if(position+k>=3u)x=*((device const float4*)(mixed+(t+k-3u)*channels+c));sum+=x*float4(w0[k],w1[k],w2[k],w3[k]);}\n"
           "    *((device float4*)(conv+t*channels+c))=sum/(1.0f+exp(-sum));\n"
           "}\n"
// native-program:entry termite_glide_core_prepare
           "kernel void termite_glide_core_prepare(device const float *beta [[buffer(0)]], device const float *a [[buffer(1)]], device const float *alog [[buffer(2)]], device const float *dt [[buffer(3)]], device float *conv [[buffer(4)]], device float *gates [[buffer(5)]], constant termite_glide_core_params &p [[buffer(6)]], uint gid [[thread_position_in_grid]], ushort lane [[thread_index_in_simdgroup]]) {\n"
           "    uint item=gid/32u;\n"
           "    if(item>=p.tokens*p.value_heads)return;\n"
           "    uint t=item/p.value_heads,h=item%p.value_heads,channels=(2u*p.key_heads+p.value_heads)*128u;\n"
           "    if(h<p.key_heads){\n"
           "        uint qb=t*channels+h*128u,kb=qb+p.key_heads*128u;\n"
           "        float qs=0,ks=0;\n"
           "        for(uint d=uint(lane);d<128u;d+=32u){float q=conv[qb+d],k=conv[kb+d];qs+=q*q;ks+=k*k;}\n"
           "        float qn=1.0f/sqrt(simd_sum(qs)+1e-6f)/sqrt(128.0f),kn=1.0f/sqrt(simd_sum(ks)+1e-6f);\n"
           "        for(uint d=uint(lane);d<128u;d+=32u){conv[qb+d]*=qn;conv[kb+d]*=kn;}\n"
           "    }\n"
           "    if(lane==0){\n"
           "        float x=a[item]+dt[h];\n"
           "        float sp=x>20.0f?x:(x< -20.0f?exp(x):log(1.0f+exp(x)));\n"
           "        gates[item*2u]=exp(-exp(alog[h])*sp);\n"
           "        gates[item*2u+1u]=1.0f/(1.0f+exp(-beta[item]));\n"
           "    }\n"
           "}\n"
// native-program:entry termite_glide_core_recurrent
           "kernel void termite_glide_core_recurrent(device const float *conv [[buffer(0)]], device const float *gates [[buffer(1)]], device float *core [[buffer(2)]], device const uint *spans [[buffer(3)]], constant termite_glide_core_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]], ushort lane [[thread_index_in_simdgroup]]) {\n"
           "    uint column=gid/32u,per_row=p.value_heads*128u,r=column/per_row;\n"
           "    if(r>=p.rows)return;\n"
           "    uint hc=column%per_row,h=hc/128u,col=hc%128u,kh=h/(p.value_heads/p.key_heads);\n"
           "    uint channels=(2u*p.key_heads+p.value_heads)*128u,start=spans[r*2u],end=start+spans[r*2u+1u];\n"
           "    // A SIMD group owns one value column of one row: four state values per lane.\n"
           "    float4 values=float4(0.0f);\n"
           "    for(uint t=start;t<end;t++){\n"
           "        uint qb=t*channels+kh*128u+uint(lane),kb=qb+p.key_heads*128u;\n"
           "        float4 q=float4(conv[qb],conv[qb+32u],conv[qb+64u],conv[qb+96u]);\n"
           "        float4 k=float4(conv[kb],conv[kb+32u],conv[kb+64u],conv[kb+96u]);\n"
           "        float decay=gates[(t*p.value_heads+h)*2u],beta=gates[(t*p.value_heads+h)*2u+1u];\n"
           "        values*=decay;\n"
           "        float memory=simd_sum(dot(values,k));\n"
           "        float delta=(conv[t*channels+2u*p.key_heads*128u+h*128u+col]-memory)*beta;\n"
           "        values+=k*delta;\n"
           "        float out=simd_sum(dot(values,q));\n"
           "        if(lane==0)core[(t*p.value_heads+h)*128u+col]=out;\n"
           "    }\n"
           "}\n"
// native-program:entry termite_glide_core_norm
           "kernel void termite_glide_core_norm(device const float *core [[buffer(0)]], device const float *z [[buffer(1)]], device const float *weight [[buffer(2)]], device float *output [[buffer(3)]], constant termite_glide_core_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]], ushort lane [[thread_index_in_simdgroup]]) {\n"
           "    uint item=gid/32u;\n"
           "    if(item>=p.tokens*p.value_heads)return;\n"
           "    uint base=item*128u;\n"
           "    float sum=0;\n"
           "    for(uint d=uint(lane);d<128u;d+=32u){float v=core[base+d];sum+=v*v;}\n"
           "    float scale=1.0f/sqrt(simd_sum(sum)/128.0f+1e-6f);\n"
           "    for(uint d=uint(lane);d<128u;d+=32u){float gate=z[base+d];output[base+d]=core[base+d]*scale*weight[d]*(gate/(1.0f+exp(-gate)));}\n"
           "}\n"
