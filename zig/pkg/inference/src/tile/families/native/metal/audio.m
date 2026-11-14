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

// native-program:entry antfly_audio_pad
           "kernel void antfly_audio_pad(device const float *x [[buffer(0)]], device float *y [[buffer(1)]], constant antfly_audio_pad_params &p [[buffer(2)]], uint i [[thread_position_in_grid]]) {\n"
           "uint n=p.batch*p.channels*p.output_rows*p.width; if(i>=n)return; uint f=i%p.width,t=(i/p.width)%p.output_rows,c=(i/p.width/p.output_rows)%p.channels,b=i/p.width/p.output_rows/p.channels; y[i]=t<p.valid_rows[b] ? x[((b*p.channels+c)*p.rows+t)*p.width+f] : 0.0f; }\n"
// native-program:entry antfly_audio_frontend
"kernel void antfly_audio_frontend(device const float *input [[buffer(0)]], device const float *aux [[buffer(1)]], device float *out [[buffer(2)]], constant antfly_audio_frontend_params &p [[buffer(3)]], uint i [[thread_position_in_grid]]) {\n"
"uint total=p.rows*p.width*(p.mode==4 ? p.hop : 1)*p.batch;\n"
"if (i>=total) return;\n"
"if (p.mode==0) out[i]=input[(i/p.width)*p.hop+i%p.width]*aux[i%p.width];\n"
"else if (p.mode==1) { float re=input[2*i], im=input[2*i+1]; out[i]=sqrt(re*re+im*im); }\n"
"else if (p.mode==2) out[i]=log(input[i]+p.floor)*aux[i/p.width];\n"
"else if(p.mode==3) {\n"
"if(i>=p.rows*p.batch) return;\n"
"uint b=i/p.rows,pos=i%p.rows,base=b*p.rows*p.width;\n"
"antfly_channel_norm_relu(input,aux,out,base+pos,p.rows,p.width,p.floor);\n"
"} else { uint per=p.rows*p.width*p.hop,b=i/per,j=i%per;out[i]=input[b*per+((j%p.hop)*p.rows+j/(p.width*p.hop))*p.width+(j/p.hop)%p.width]; }\n"
"}\n"
// native-program:entry antfly_audio_fft512
"kernel void antfly_audio_fft512(device const float *input [[buffer(0)]],device const float *tw [[buffer(1)]],device float *out [[buffer(2)]],constant uint &frames [[buffer(3)]],uint tid [[thread_index_in_threadgroup]],uint3 group [[threadgroup_position_in_grid]]) {\n"
"threadgroup float2 values[512]; uint frame=group.x;\n"
"for(uint i=tid;i<512;i+=256) { uint r=0; for(uint k=0;k<9;k++) r=(r<<1)|((i>>k)&1);values[r]=float2(i<320 ? input[frame*320+i] : 0.0f,0.0f); }\n"
"threadgroup_barrier(mem_flags::mem_threadgroup);\n"
"for(uint len=2;len<=512;len*=2) { uint span=len/2,j=tid%span,a=(tid/span)*len+j,b=a+span,w=j*(512/len);float2 x=values[a],y=values[b];float2 v=float2(y.x*tw[2*w]-y.y*tw[2*w+1],y.x*tw[2*w+1]+y.y*tw[2*w]);values[a]=x+v;values[b]=x-v;threadgroup_barrier(mem_flags::mem_threadgroup); }\n"
"for(uint i=tid;i<257;i+=256) { float2 x=values[i];out[frame*257+i]=sqrt(x.x*x.x+x.y*x.y); }\n"
"}\n"
// native-program:entry termite_gemma4_audio_channel_norm_relu
           "kernel void termite_gemma4_audio_channel_norm_relu(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_gemma4_audio_channel_norm_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {\n"
           "    if (gid >= p.positions) return;\n"
"    antfly_channel_norm_relu(input,weight,output,gid,p.positions,p.channels,p.eps);\n"
           "}\n"
// native-program:entry antfly_audio_logmel
"kernel void antfly_audio_logmel(device const float *input [[buffer(0)]],device const float *plan [[buffer(1)]],device const float *mask [[buffer(2)]],device float *out [[buffer(3)]],constant uint2 &p [[buffer(4)]],uint tid [[thread_index_in_threadgroup]],uint3 group [[threadgroup_position_in_grid]]) {\n"
"uint frame=group.x,hop=p.x;float floor=as_type<float>(p.y);threadgroup float2 values[512];threadgroup float mag[257];threadgroup atomic_uint bad_flag;if(tid==0) atomic_store_explicit(&bad_flag,0u,memory_order_relaxed);threadgroup_barrier(mem_flags::mem_threadgroup);\n"
"for(uint i=tid;i<512;i+=256) { uint r=0;for(uint k=0;k<9;k++) r=(r<<1)|((i>>k)&1); values[r]=float2(i<320 ? input[frame*hop+i]*plan[512+i] : 0.f,0.f); }\n"
"threadgroup_barrier(mem_flags::mem_threadgroup);\n"
"for(uint len=2;len<=512;len*=2) { uint span=len/2,j=tid%span,a=(tid/span)*len+j,b=a+span,w=j*(512/len);float2 x=values[a],y=values[b];float2 v=float2(SUB(MUL(y.x,plan[2*w]),MUL(y.y,plan[2*w+1])),ADD(MUL(y.x,plan[2*w+1]),MUL(y.y,plan[2*w])));values[a]=float2(ADD(x.x,v.x),ADD(x.y,v.y));values[b]=float2(SUB(x.x,v.x),SUB(x.y,v.y));threadgroup_barrier(mem_flags::mem_threadgroup); }\n"
"bool bad=false;\n"
"for(uint i=tid;i<257;i+=256) { float2 x=values[i];mag[i]=sqrt(ADD(MUL(x.x,x.x),MUL(x.y,x.y)));bad|=!isfinite(mag[i]); }\n"
"if(bad) atomic_fetch_or_explicit(&bad_flag,1u,memory_order_relaxed);threadgroup_barrier(mem_flags::mem_threadgroup);bool nonfinite=atomic_load_explicit(&bad_flag,memory_order_relaxed)!=0;\n"
"if(tid<128) { uint first=(uint)plan[832+tid*3],end=(uint)plan[833+tid*3],off=(uint)plan[834+tid*3];float sum=0.f;for(uint k=nonfinite ? 0 : first;k<(nonfinite ? 257 : end);k++) { float weight=k>=first && k<end ? plan[off+k-first] : 0.f; sum=ADD(sum,MUL(mag[k],weight)); }out[frame*128+tid]=log(sum+floor)*mask[frame]; }\n"
" }\n"
// native-program:entry antfly_audio_conv_tiled
"kernel void antfly_audio_conv_tiled(device const float *input [[buffer(0)]],device const float *weight [[buffer(1)]],device const float *bias [[buffer(2)]],device float *out [[buffer(3)]],constant termite_metal_conv2d_f32_params &p [[buffer(4)]],uint3 tid [[thread_position_in_threadgroup]],uint3 group [[threadgroup_position_in_grid]]) {\n"
"threadgroup float a[16][16],b[16][16];uint x=tid.x,y=tid.y,ch=group.y*16+y,pos=group.x*16+x,batch=group.z;float acc=(p.has_bias!=0 && ch<p.out_channels) ? bias[ch] : 0.0f;\n"
"for(uint base=0;base<p.in_channels*9;base+=16) { uint ak=base+x,bk=base+y;\n"
"a[y][x]=(ch<p.out_channels && ak<p.in_channels*9) ? weight[ch*p.in_channels*9+ak] : 0.0f;\n"
"int iy=int((pos/p.out_w)*2+bk/3%3)-1,ix=int((pos%p.out_w)*2+bk%3)-1;\n"
"b[y][x]=(pos<p.out_h*p.out_w && bk<p.in_channels*9 && iy>=0 && iy<int(p.height) && ix>=0 && ix<int(p.width)) ? input[((batch*p.in_channels+bk/9)*p.height+uint(iy))*p.width+uint(ix)] : 0.0f;\n"
"threadgroup_barrier(mem_flags::mem_threadgroup);\n"
"for(uint k=0;k<min(16u,p.in_channels*9-base);k++) acc+=a[y][k]*b[k][x];\n"
"threadgroup_barrier(mem_flags::mem_threadgroup); }\n"
"if(ch<p.out_channels && pos<p.out_h*p.out_w) out[((batch*p.out_channels+ch)*p.out_h*p.out_w)+pos]=acc;\n"
"}\n"
// native-program:entry antfly_audio_conv_matrix
"kernel void antfly_audio_conv_matrix(device const float *input [[buffer(0)]],device const float *weight [[buffer(1)]],device const float *bias [[buffer(2)]],device float *out [[buffer(3)]],constant termite_metal_conv2d_f32_params &p [[buffer(4)]],uint tid [[thread_index_in_threadgroup]],ushort sg [[simdgroup_index_in_threadgroup]],uint3 group [[threadgroup_position_in_grid]]) {\n"
"threadgroup float a[512],b[512],c[256];uint ch0=group.y*16,pos0=group.x*16,batch=group.z;\n"
"simdgroup_float8x8 ma,mb,acc=make_filled_simdgroup_matrix<float,8>(0.0f);\n"
"for(uint base=0;base<p.in_channels*9;base+=32) {\n"
"for(uint i=tid;i<512;i+=128) {uint row=i/32,col=i%32,k=base+col; a[i]=(ch0+row<p.out_channels && k<p.in_channels*9) ? weight[(ch0+row)*p.in_channels*9+k] : 0.0f;\n"
"uint bk=base+i/16,pos=pos0+i%16;int iy=int((pos/p.out_w)*2+bk/3%3)-1,ix=int((pos%p.out_w)*2+bk%3)-1;\n"
"b[i]=(pos<p.out_h*p.out_w && bk<p.in_channels*9 && iy>=0 && iy<int(p.height) && ix>=0 && ix<int(p.width)) ? input[((batch*p.in_channels+bk/9)*p.height+uint(iy))*p.width+uint(ix)] : 0.0f;}\n"
"threadgroup_barrier(mem_flags::mem_threadgroup);\n"
"for(uint k=0;k<32;k+=8) {simdgroup_load(ma,a+(uint(sg)/2)*8*32+k,32);simdgroup_load(mb,b+k*16+(uint(sg)%2)*8,16);simdgroup_multiply_accumulate(acc,ma,mb,acc);}\n"
"threadgroup_barrier(mem_flags::mem_threadgroup);}\n"
"simdgroup_store(acc,c+(uint(sg)/2)*8*16+(uint(sg)%2)*8,16);threadgroup_barrier(mem_flags::mem_threadgroup);\n"
"for(uint i=tid;i<256;i+=128) {uint ch=ch0+i/16,pos=pos0+i%16;if(ch<p.out_channels && pos<p.out_h*p.out_w) out[((batch*p.out_channels+ch)*p.out_h*p.out_w)+pos]=c[i]+(p.has_bias!=0 ? bias[ch] : 0.0f);}\n"
"}\n"
