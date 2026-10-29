// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

// Shared MSL owner for the decode and batched recurrent libraries.
#define ANTFLY_MSL_GDN_SOURCE \
    "// Fixed scalar/SIMD schedules preserve division-by-sqrt and epsilon placement.\n" \
    "struct antfly_gdn_params { uint seq, key_heads, value_heads, dim, kernel_size, initialized; };\n" \
    "inline float2 antfly_gdn_gates(float a, float dt, float alog, float beta) {\n" \
    "    float x=a+dt;\n" \
    "    float sp=x>20.0f?x:(x< -20.0f?exp(x):log(1.0f+exp(x)));\n" \
    "    return float2(exp(-exp(alog)*sp),1.0f/(1.0f+exp(-beta)));\n" \
    "}\n" \
    "inline void antfly_gdn_decode_conv(device const float *mixed,device const float *weight,\n" \
    "    device const float *history,device float *next,thread const uint *indices,\n" \
    "    thread float *values,uint h,uint ratio,uint initialized) {\n" \
    "    for(uint j=0;j<3;j++) {\n" \
    "        uint c=indices[j]; float sum=0;\n" \
    "        for(uint t=0;t<4;t++) {\n" \
    "            float x=t==3?mixed[c]:(initialized?history[c*4+t+1]:0.0f);\n" \
    "            sum+=x*weight[c*4+t];\n" \
    "            if(j==2 || h%ratio==0) next[c*4+t]=x;\n" \
    "        }\n" \
    "        values[j]=sum/(1.0f+exp(-sum));\n" \
    "    }\n" \
    "}\n" \
    "template<uint Step,bool Batched> __attribute__((always_inline)) inline void antfly_gdn_prepare(device const float *beta,device const float *a,\n" \
    "    device const float *alog,device const float *dt,device float *conv,device float *gates,\n" \
    "    constant antfly_gdn_params &p,uint row,uint lane) {\n" \
    "    uint h,t;if constexpr(Batched){h=row%p.value_heads;t=row/p.value_heads;}else{h=row;t=0;}\n" \
    "    if(h<p.key_heads) {\n" \
    "        uint qb=t*(2*p.key_heads+p.value_heads)*p.dim+h*p.dim,kb=qb+p.key_heads*p.dim;\n" \
    "        float qs=0,ks=0;\n" \
    "        for(uint d=lane;d<p.dim;d+=Step){float q=conv[qb+d],k=conv[kb+d];qs+=q*q;ks+=k*k;}\n" \
    "        if constexpr(Step==32){qs=simd_sum(qs);ks=simd_sum(ks);}\n" \
    "        float qn=1.0f/sqrt(qs+1e-6f)/sqrt(float(p.dim)),kn=1.0f/sqrt(ks+1e-6f);\n" \
    "        for(uint d=lane;d<p.dim;d+=Step){conv[qb+d]*=qn;conv[kb+d]*=kn;}\n" \
    "    }\n" \
    "    if(lane==0){float2 g=antfly_gdn_gates(a[row],dt[h],alog[h],beta[row]);gates[row*2]=g.x;gates[row*2+1]=g.y;}\n" \
    "}\n" \
    "template<uint Step> inline void antfly_gdn_norm(device const float *core,device const float *z,\n" \
    "    device const float *weight,device float *output,uint dim,uint row,uint lane) {\n" \
    "    uint base=row*dim; float sum=0;\n" \
    "    for(uint d=lane;d<dim;d+=Step){float v=core[base+d];sum+=v*v;}\n" \
    "    if constexpr(Step==32)sum=simd_sum(sum);\n" \
    "    float scale=1.0f/sqrt(sum/float(dim)+1e-6f);\n" \
    "    for(uint d=lane;d<dim;d+=Step){float gate=z[base+d];output[base+d]=core[base+d]*scale*weight[d]*(gate/(1.0f+exp(-gate)));}\n" \
    "}\n" \
    "// Staged chooses shared V storage for 512-thread column tiles; decode keeps\n" \
    "// its V in registers. Both retain four ordered SIMD partials and the same barriers.\n" \
    "template<bool Staged> __attribute__((always_inline)) inline float antfly_gdn_fused_prepare(\n" \
    "    device const float *mixed,device const float *weight,device const float *beta,device const float *a,\n" \
    "    device const float *alog,device const float *dt,device const float *history,device float *next,\n" \
    "    device float *conv,device float *gates,constant antfly_gdn_params &p,uint h,uint d,uint lane,uint simd,\n" \
    "    threadgroup float *q,threadgroup float *k,threadgroup float *v,\n" \
    "    threadgroup float *reduction,threadgroup float *scales) {\n" \
    "    uint ratio=p.value_heads/p.key_heads,kh=h/ratio;\n" \
    "    uint indices[3]={kh*128+d,(p.key_heads+kh)*128+d,2*p.key_heads*128+h*128+d};\n" \
    "    float values[3];\n" \
    "    if(d<128){\n" \
    "        antfly_gdn_decode_conv(mixed,weight,history,next,indices,values,h,ratio,p.initialized);\n" \
    "        float qs=simd_sum(values[0]*values[0]),ks=simd_sum(values[1]*values[1]);\n" \
    "        if(lane==0){reduction[simd]=qs;reduction[4+simd]=ks;}\n" \
    "        if constexpr(Staged){q[d]=values[0];k[d]=values[1];v[d]=values[2];}\n" \
    "    }\n" \
    "    threadgroup_barrier(mem_flags::mem_threadgroup);\n" \
    "    if(d==0){\n" \
    "        float qs=0,ks=0;for(uint s=0;s<4;s++){qs+=reduction[s];ks+=reduction[4+s];}\n" \
    "        scales[0]=1.0f/sqrt(qs+1e-6f)/sqrt(128.0f);scales[1]=1.0f/sqrt(ks+1e-6f);\n" \
    "        float2 g=antfly_gdn_gates(a[h],dt[h],alog[h],beta[h]);scales[2]=g.x;scales[3]=g.y;\n" \
    "        gates[h*2]=scales[2];gates[h*2+1]=scales[3];\n" \
    "    }\n" \
    "    threadgroup_barrier(mem_flags::mem_threadgroup);\n" \
    "    if(d<128){\n" \
    "        if constexpr(Staged){q[d]*=scales[0];k[d]*=scales[1];}\n" \
    "        else {q[d]=values[0]*scales[0];k[d]=values[1]*scales[1];}\n" \
    "        if(h%ratio==0){conv[indices[0]]=q[d];conv[indices[1]]=k[d];}\n" \
    "        if constexpr(Staged)conv[indices[2]]=v[d];else conv[indices[2]]=values[2];\n" \
    "    }\n" \
    "    threadgroup_barrier(mem_flags::mem_threadgroup);\n" \
    "    if constexpr(Staged)return 0.0f;else return values[2];\n" \
    "}\n"

#define ANTFLY_MSL_GDN_PREPARE(Name, Params, Step, Batched, Limit, LaneDecl, LaneExpr) \
    "kernel void " #Name "(device const float *beta [[buffer(2)]], device const float *a [[buffer(3)]], device const float *alog [[buffer(4)]], device const float *dt [[buffer(5)]], device float *conv [[buffer(9)]], device float *gates [[buffer(10)]], constant " #Params " &p [[buffer(15)]], uint gid [[thread_position_in_grid]]" LaneDecl ") {\n" \
    "    uint row=gid/" #Step "; if(row>=" #Limit ")return;\n" \
    "    antfly_gdn_prepare<" #Step "," #Batched ">(beta,a,alog,dt,conv,gates,p,row," LaneExpr ");\n" \
    "}\n"

#define ANTFLY_MSL_GDN_NORM(Name, Params, Step, Limit, LaneDecl, LaneExpr) \
    "kernel void " #Name "(device const float *core [[buffer(11)]], device const float *z [[buffer(12)]], device const float *weight [[buffer(13)]], device float *output [[buffer(14)]], constant " #Params " &p [[buffer(15)]], uint gid [[thread_position_in_grid]]" LaneDecl ") {\n" \
    "    uint row=gid/" #Step "; if(row>=" #Limit ")return;\n" \
    "    antfly_gdn_norm<" #Step ">(core,z,weight,output,p.dim,row," LaneExpr ");\n" \
    "}\n"
