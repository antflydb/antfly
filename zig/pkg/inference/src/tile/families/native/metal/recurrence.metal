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

// native-program:entry termite_lightonocr3_recurrent_parallel
// native-program:indent 8
kernel void termite_lightonocr3_recurrent_parallel(constant CoreRow *rows [[buffer(0)]],
                                                   uint2 group [[threadgroup_position_in_grid]],
                                                   uint d [[thread_index_in_threadgroup]],
                                                   ushort lane [[thread_index_in_simdgroup]],
                                                   ushort simd [[simdgroup_index_in_threadgroup]]) {
    constant CoreRow &r = rows[group.y];
    constant CoreParams &p = r.params;
    uint h = group.x;
    device const float *mixed = r.buffers[0];
    device const float *weight = r.buffers[1];
    device const float *beta = r.buffers[2];
    device const float *a = r.buffers[3];
    device const float *alog = r.buffers[4];
    device const float *dt = r.buffers[5];
    device const float *history = r.buffers[6];
    device float *next = r.buffers[7];
    device float *state = r.buffers[8];
    device float *conv = r.buffers[9];
    device float *gates = r.buffers[10];
    device float *core = r.buffers[11];
    device const float *z = r.buffers[12];
    device const float *norm = r.buffers[13];
    device float *output = r.buffers[14];

    threadgroup float q[128], k[128], values[128], reduction[64], scales[4];
    const uint TM = 8, SG = 128 / TM;
    antfly_gdn_fused_prepare<true>(mixed,weight,beta,a,alog,dt,history,next,conv,gates,p,h,d,uint(lane),uint(simd),q,k,values,reduction,scales);
    uint col = simd * TM;
    float4 memories[TM / 4], previous[4][TM / 4], outputs[TM / 4];
#pragma unroll
    for (uint t = 0; t < TM / 4; t++)
        memories[t] = 0;
#pragma unroll
    for (uint i = 0; i < 4; i++) {
        uint key = lane * 4 + i;
#pragma unroll
        for (uint t = 0; t < TM / 4; t++) {
            float4 v = p.initialized
                           ? *((device const float4 *)(state + h * 128 * 128 + key * 128 + col + t * 4))
                           : float4(0);
            previous[i][t] = v * scales[2];
            memories[t] += previous[i][t] * k[key];
        }
    }
#pragma unroll
    for (uint t = 0; t < TM / 4; t++) {
        float4 memory = float4(simd_sum(memories[t].x), simd_sum(memories[t].y), simd_sum(memories[t].z),
                               simd_sum(memories[t].w));
        float4 delta = (float4(values[col + t * 4], values[col + t * 4 + 1], values[col + t * 4 + 2],
                               values[col + t * 4 + 3]) -
                        memory) *
                       scales[3];
        outputs[t] = 0;
#pragma unroll
        for (uint i = 0; i < 4; i++) {
            uint key = lane * 4 + i;
            float4 v = previous[i][t] + k[key] * delta;
            *((device float4 *)(state + h * 128 * 128 + key * 128 + col + t * 4)) = v;
            outputs[t] += v * q[key];
        }
        outputs[t] = float4(simd_sum(outputs[t].x), simd_sum(outputs[t].y), simd_sum(outputs[t].z),
                            simd_sum(outputs[t].w));
    }
    if (lane == 0) {
        float sum = 0;
#pragma unroll
        for (uint t = 0; t < TM / 4; t++) {
            float4 v = outputs[t];
            sum += dot(v, v);
            *((device float4 *)(core + h * 128 + col + t * 4)) = v;
        }
        reduction[simd] = sum;
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    if (d == 0) {
        float sum = 0;
        for (uint s = 0; s < SG; s++)
            sum += reduction[s];
        scales[0] = 1.0f / sqrt(sum / 128.0f + 1e-6f);
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    if (lane == 0) {
#pragma unroll
        for (uint t = 0; t < TM / 4; t++) {
            float4 gate = *((device const float4 *)(z + h * 128 + col + t * 4)),
                   n = *((device const float4 *)(norm + col + t * 4));
            *((device float4 *)(output + h * 128 + col + t * 4)) =
                outputs[t] * scales[0] * n * (gate / (1.0f + exp(-gate)));
        }
    }
}
// native-program:entry termite_qwen35_fused_decode
// native-program:indent 11
kernel void termite_qwen35_fused_decode(
    device const float *mixed [[buffer(0)]], device const float *weight [[buffer(1)]],
    device const float *beta [[buffer(2)]], device const float *a [[buffer(3)]],
    device const float *alog [[buffer(4)]], device const float *dt [[buffer(5)]],
    device const float *history [[buffer(6)]], device float *next [[buffer(7)]],
    device float *state [[buffer(8)]], device float *conv [[buffer(9)]],
    device float *gates [[buffer(10)]], device float *core [[buffer(11)]],
    device const float *z [[buffer(12)]], device const float *norm [[buffer(13)]],
    device float *output [[buffer(14)]], constant termite_qwen35_params &p [[buffer(15)]],
    uint h [[threadgroup_position_in_grid]], uint d [[thread_index_in_threadgroup]],
    ushort lane [[thread_index_in_simdgroup]], ushort simd [[simdgroup_index_in_threadgroup]]) {
    threadgroup float q[128], k[128], reduction[12], scales[4];
    float value_input=antfly_gdn_fused_prepare<false>(mixed,weight,beta,a,alog,dt,history,next,conv,gates,p,h,d,uint(lane),uint(simd),q,k,q,reduction,scales);
    uint base=h*128*128+d;
    float memory=0;
    for(uint row=0;row<128;row++) {
        float value=p.initialized?state[base+row*128]:0.0f;
        memory+=(value*scales[2])*k[row];
    }
    float delta=(value_input-memory)*scales[3],value=0;
    for(uint row=0;row<128;row++) {
        float previous=p.initialized?state[base+row*128]:0.0f;
        float updated=previous*scales[2]+k[row]*delta;
        state[base+row*128]=updated;value+=updated*q[row];
    }
    core[h*128+d]=value;
    float sum=simd_sum(value*value);
    if(lane==0)reduction[8+simd]=sum;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    if(d==0) {
        float sum=0;for(uint s=0;s<4;s++)sum+=reduction[8+s];
        scales[0]=1.0f/sqrt(sum/128.0f+1e-6f);
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    float gate=z[h*128+d];
    output[h*128+d]=value*scales[0]*norm[d]*(gate/(1.0f+exp(-gate)));
}
// native-program:entry antfly_gdn_causal_conv_vec4_f32
// native-program:indent 11
kernel void antfly_gdn_causal_conv_vec4_f32(device const float*mixed[[buffer(0)]],device const float*weight[[buffer(1)]],device const float*history[[buffer(6)]],device float*conv[[buffer(9)]],constant termite_qwen35_params&p[[buffer(15)]],uint gid[[thread_position_in_grid]]){
 uint channels=(2*p.key_heads+p.value_heads)*p.dim;
 if(gid>=p.seq*(channels/4))return;uint t=gid/(channels/4),c=gid%(channels/4)*4;
 float4 w0=*((device const float4*)(weight+c*4)),w1=*((device const float4*)(weight+(c+1)*4)),w2=*((device const float4*)(weight+(c+2)*4)),w3=*((device const float4*)(weight+(c+3)*4));
 float4 sum=0;
 #pragma unroll
 for(uint k=0;k<4;k++){int src=int(t)+int(k)-3;float4 x=0;if(src>=0)x=*((device const float4*)(mixed+uint(src)*channels+c));else if(p.initialized)x=float4(history[c*4+uint(4+src)],history[(c+1)*4+uint(4+src)],history[(c+2)*4+uint(4+src)],history[(c+3)*4+uint(4+src)]);sum+=x*float4(w0[k],w1[k],w2[k],w3[k]);}
 *((device float4*)(conv+t*channels+c))=sum/(1.0f+exp(-sum));
}
// native-program:entry antfly_gdn_causal_conv_f32
// native-program:indent 11
kernel void antfly_gdn_causal_conv_f32(device const float *mixed [[buffer(0)]], device const float *weight [[buffer(1)]], device const float *history [[buffer(6)]], device float *conv [[buffer(9)]], constant termite_qwen35_params &p [[buffer(15)]], uint gid [[thread_position_in_grid]]) {
    uint channels=(2*p.key_heads+p.value_heads)*p.dim;if(gid>=p.seq*channels)return;
    conv[gid]=antfly_causal_depthwise<true,true,true,false>(mixed,weight,history,gid/channels,gid%channels,channels,p.kernel_size,p.initialized);
}
// native-program:entry termite_qwen35_recurrent_decode
// native-program:indent 11
kernel void termite_qwen35_recurrent_decode(device float *state [[buffer(8)]], device const float *conv [[buffer(9)]], device const float *gates [[buffer(10)]], device float *output [[buffer(11)]], constant termite_qwen35_params &p [[buffer(15)]], uint gid [[thread_position_in_grid]]) {
    if(gid>=p.value_heads*p.dim)return;
    uint h=gid/p.dim,col=gid%p.dim,kh=h/(p.value_heads/p.key_heads);
    uint channels=(2*p.key_heads+p.value_heads)*p.dim;
    uint base=h*p.dim*p.dim+col;
    if(!p.initialized)for(uint k=0;k<p.dim;k++)state[base+k*p.dim]=0.0f;
    // Each lane owns one value column for the entire token stream. Adjacent
    // lanes access adjacent state elements; columns and heads are independent.
    for(uint t=0;t<p.seq;t++) {
        uint qb=t*channels+kh*p.dim,kb=qb+p.key_heads*p.dim;
        float decay=gates[(t*p.value_heads+h)*2],beta=gates[(t*p.value_heads+h)*2+1];
        float memory=0;
        for(uint k=0;k<p.dim;k++)memory+=(state[base+k*p.dim]*decay)*conv[kb+k];
        float value=conv[t*channels+2*p.key_heads*p.dim+h*p.dim+col];
        float delta=(value-memory)*beta,out=0;
        for(uint k=0;k<p.dim;k++) {
            float decayed=state[base+k*p.dim]*decay;
            float updated=decayed+conv[kb+k]*delta;
            state[base+k*p.dim]=updated;
            out+=updated*conv[qb+k];
        }
        output[(t*p.value_heads+h)*p.dim+col]=out;
    }
}
// native-program:entry termite_qwen35_recurrent_columns4
// native-program:indent 11
kernel void termite_qwen35_recurrent_columns4(device float*s[[buffer(8)]],device const float*x[[buffer(9)]],device const float*g[[buffer(10)]],device float*y[[buffer(11)]],constant termite_qwen35_params&p[[buffer(15)]],uint gid[[thread_position_in_grid]],uint lane[[thread_index_in_simdgroup]]){termite_qwen35_recurrent_columns_body<4>(s,x,g,y,p,gid,lane);}
// native-program:entry termite_qwen35_recurrent
// native-program:indent 11
kernel void termite_qwen35_recurrent(device float *state [[buffer(8)]], device const float *conv [[buffer(9)]], device const float *gates [[buffer(10)]], device float *output [[buffer(11)]], constant termite_qwen35_params &p [[buffer(15)]], uint gid [[thread_position_in_grid]], ushort lane [[thread_index_in_simdgroup]]) {
    uint column=gid/32;
    if(column>=p.value_heads*128)return;
    uint h=column/128,col=column%128,kh=h/(p.value_heads/p.key_heads);
    uint channels=(2*p.key_heads+p.value_heads)*128;
    uint base=h*128*128+uint(lane)*128+col;
    // A SIMD group owns one value column: four recurrent values per lane.
    // Only the initial/final state touches device memory across the stream.
    float4 values=p.initialized?float4(state[base],state[base+32*128],state[base+64*128],state[base+96*128]):float4(0.0f);
    for(uint t=0;t<p.seq;t++) {
        uint qb=t*channels+kh*128+uint(lane),kb=qb+p.key_heads*128;
        float4 q=float4(conv[qb],conv[qb+32],conv[qb+64],conv[qb+96]);
        float4 k=float4(conv[kb],conv[kb+32],conv[kb+64],conv[kb+96]);
        float decay=gates[(t*p.value_heads+h)*2],beta=gates[(t*p.value_heads+h)*2+1];
        values*=decay;
        float memory=simd_sum(dot(values,k));
        float value=conv[t*channels+2*p.key_heads*128+h*128+col];
        float delta=(value-memory)*beta;
        values+=k*delta;
        float out=simd_sum(dot(values,q));
        if(lane==0)output[(t*p.value_heads+h)*128+col]=out;
    }
    state[base]=values.x;state[base+32*128]=values.y;state[base+64*128]=values.z;state[base+96*128]=values.w;
}
