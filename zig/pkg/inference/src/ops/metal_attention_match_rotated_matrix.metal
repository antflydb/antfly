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

#include <metal_stdlib>
using namespace metal;

struct Params {
    uint op, n, dim, groups, position, stride, mode, index;
    float epsilon, scale, theta, reserved;
};
struct Metadata { float bias; uint position_lo, position_hi, coverage_lo, coverage_hi; };
struct StorageHeader {
    uint encoding, first_row, rows, span_count, compressed_rows;
    uint key_payload_offset, key_row_bytes, value_row_bytes;
    float key_scale, value_scale;
    uint reserved0, reserved1;
};
struct StorageSpan { uint first, rows, offset; };

// Nonnegative means a packed row; negative means -(exact row + 1).
inline int storage_index(device const uchar *encoded, StorageHeader h, uint row) {
    if (h.encoding == 1u) {
        if (row >= h.first_row && row - h.first_row < h.rows)
            return int(row - h.first_row);
        return -int(row < h.first_row ? row : row - h.rows) - 1;
    }
    if (h.encoding == 2u) {
        device const StorageSpan *spans = reinterpret_cast<device const StorageSpan *>(encoded + sizeof(StorageHeader));
        uint low = 0u, high = h.span_count;
        while (low < high) {
            const uint mid = low + (high - low) / 2u;
            if (spans[mid].first <= row) low = mid + 1u; else high = mid;
        }
        if (low != 0u) {
            const StorageSpan span = spans[low - 1u];
            if (row - span.first < span.rows) return int(span.offset + row - span.first);
            return -int(row - (span.offset + span.rows)) - 1;
        }
    }
    return -int(row) - 1;
}

constant float rotated_centroids[16] = {
    -2.73258957099535f, -2.06901722653160f, -1.61804638602210f, -1.25623119734739f,
    -0.94234045648714f, -0.65675911853260f, -0.38804829949038f, -0.12839502985118f,
     0.12839502985118f,  0.38804829949038f,  0.65675911853260f,  0.94234045648714f,
     1.25623119734739f,  1.61804638602210f,  2.06901722653160f,  2.73258957099535f
};
inline float rotated_sign(uint round, uint coordinate) {
    ulong x = (round == 0u ? 0x8f3f73b5cf1c9adeul : 0x24d8a6511b73c409ul)
        + ulong(coordinate) * 0x9e3779b97f4a7c15ul;
    x = (x ^ (x >> 30u)) * 0xbf58476d1ce4e5b9ul;
    x = (x ^ (x >> 27u)) * 0x94d049bb133111ebul;
    return ((x ^ (x >> 31u)) & 1ul) == 0ul ? 1.0f : -1.0f;
}

// SIMD lanes own coordinates lane + 32*i. All butterflies preserve v1's
// f32 operation order, including pair - self in the upper half. Rotate once
// per query and retain the original query for protected/exact rows.
inline void rotate_query(device const float *query, uint dim, uint lane,
                         thread float *rotated, device atomic_uint *status) {
    const uint count = dim / 32u;
    for (uint i = 0; i < count; ++i) rotated[i] = query[lane + i * 32u];
    const float normalization = 1.0f / sqrt(float(dim));
    for (uint round = 0; round < 2u; ++round) {
        for (uint i = 0; i < count; ++i) rotated[i] *= rotated_sign(round, lane + i * 32u);
        for (uint width = 1u; width < 32u; width *= 2u) {
            for (uint i = 0; i < count; ++i) {
                const float own = rotated[i];
                const float pair = simd_shuffle_xor(own, ushort(width));
                rotated[i] = (lane & width) != 0u ? pair - own : own + pair;
            }
        }
        for (uint width = 1u; width < count; width *= 2u) {
            for (uint i = 0; i < count; ++i) if ((i & width) == 0u) {
                const float left = rotated[i], right = rotated[i + width];
                rotated[i] = left + right;
                rotated[i + width] = left - right;
            }
        }
        for (uint i = 0; i < count; ++i) {
            rotated[i] *= normalization;
            if (!isfinite(rotated[i])) atomic_store_explicit(status, 1u, memory_order_relaxed);
        }
    }
}
inline float packed_scale(device const uchar *payload, device atomic_uint *status, bool allow_zero) {
    const float scale = *reinterpret_cast<device const float *>(payload);
    if (!isfinite(scale) || scale < 0.0f || (!allow_zero && scale == 0.0f)) {
        atomic_store_explicit(status, 1u, memory_order_relaxed);
        return 0.0f;
    }
    return scale;
}
inline float rotated_key(device const uchar *encoded, StorageHeader h, uint row, uint d) {
    const uint offset = h.key_payload_offset + row * h.key_row_bytes;
    const uint code = (encoded[offset + 4u + d / 2u] >> ((d & 1u) * 4u)) & 15u;
    return rotated_centroids[code];
}
inline float rotated_value(device const uchar *encoded, StorageHeader h, uint row, uint d,
                           device atomic_uint *status) {
    const uint offset = row * h.value_row_bytes;
    const char code = as_type<char>(encoded[offset + 4u + d]);
    if (code == -128) atomic_store_explicit(status, 1u, memory_order_relaxed);
    return float(code) * packed_scale(encoded + offset, status, false);
}

constant uint split_rows = 256u;

inline float fp8_decode(uchar bits, float scale, device atomic_uint *status) {
    // E4M3FN reserves both signed exponent/mantissa 0x7f encodings. They must
    // fail the dispatch rather than alias finite +/-480 values.
    if ((bits & 0x7fu) == 0x7fu) {
        atomic_store_explicit(status, 1u, memory_order_relaxed);
        return as_type<float>(0x7fc00000u);
    }
    const uint exponent = (bits >> 3) & 15u;
    const uint mantissa = bits & 7u;
    const float magnitude = exponent == 0u
        ? float(mantissa) * (1.0f / 512.0f)
        : as_type<float>(((exponent + 120u) << 23) | (mantissa << 20));
    const float decoded = (bits & 128u) != 0u ? -magnitude * scale : magnitude * scale;
    return float(half(decoded));
}


#ifndef MATRIX_PARTITION
#define MATRIX_PARTITION 64u
#endif
#ifndef MATRIX_DIM
#define MATRIX_DIM 512u
#endif
// Produce original and rotated FP32 query rows once. The original input may
// have only four-byte alignment; neither queries nor centroids are narrowed.
kernel void metal_attention_match_rotate_queries(
 device const float *input [[buffer(0)]], device float *output [[buffer(7)]],
 device atomic_uint *status [[buffer(6)]], constant Params &p [[buffer(8)]],
 uint3 gp [[threadgroup_position_in_grid]], uint lane [[thread_index_in_threadgroup]]) {
 if (gp.x>=p.groups || lane>=32u) return;
 float rotated[16];
 rotate_query(input+gp.x*p.dim,p.dim,lane,rotated,status);
 for(uint i=0u;i<p.dim/32u;i++) {
  const uint d=lane+32u*i;
  const float value=input[gp.x*p.dim+d];
  if(!isfinite(value)) atomic_store_explicit(status,1u,memory_order_relaxed);
  output[gp.x*p.dim+d]=value;
  output[p.groups*p.dim+gp.x*p.dim+d]=rotated[i];
 }
}
kernel void metal_attention_match_matrix_stage(
 device const float*queries [[buffer(0)]],device const half*keys [[buffer(1)]],device const half*values [[buffer(2)]],
 device const uchar*encoded_keys [[buffer(3)]],device const uchar*encoded_values [[buffer(4)]],
 device const Metadata*metadata [[buffer(5)]],device atomic_uint*status [[buffer(6)]],device float*partials [[buffer(7)]],
 constant Params&p [[buffer(8)]],uint3 gp [[threadgroup_position_in_grid]],uint tid [[thread_index_in_threadgroup]],
 uint lane [[thread_index_in_simdgroup]],uint sg [[simdgroup_index_in_threadgroup]]) {
 const uint splits=p.n,split=gp.x,actual_gqa=p.stride>>16,actual_first=p.stride&0xffffu,dim=MATRIX_DIM;
 // E4B has eight real query heads and two KV heads. Both KV tiles can load
 // those eight queries directly, with disjoint scratch records. The reducer
 // selects the four rows belonging to each KV owner. No padded query buffer.
 const bool padded=actual_gqa==4u;
 const uint gqa=8u,first=padded?0u:actual_first,output_first=padded?actual_first*2u:actual_first;
 if(split>=splits)return;
 if((actual_gqa!=8u&&!padded)||(padded&&(p.groups!=8u||(actual_first!=0u&&actual_first!=4u)))||p.dim!=dim){if(tid==0u)atomic_store_explicit(status,1u,memory_order_relaxed);return;}
 const uint partition=MATRIX_PARTITION,begin=split*partition;
 if(begin>=p.index){
  for(uint i=tid;i<gqa*dim;i+=256u)partials[((output_first+i/dim)*splits+split)*(dim+2u)+i%dim]=0.0f;
  if(tid<gqa){const uint rec=((output_first+tid)*splits+split)*(dim+2u);partials[rec+dim]=0.0f;partials[rec+dim+1u]=-INFINITY;}
  return;
 }
 const uint count=min(p.index-begin,partition),ring=as_type<uint>(p.theta),capacity=as_type<uint>(p.reserved);
 const StorageHeader storage=*reinterpret_cast<device const StorageHeader*>(encoded_keys);
 if(storage.encoding!=2u||storage.key_row_bytes!=4u+dim/2u||storage.value_row_bytes!=4u+dim){
  if(tid==0u)atomic_store_explicit(status,1u,memory_order_relaxed);
  return;
 }
 threadgroup int storage_rows[MATRIX_PARTITION];
 threadgroup float key_scales[MATRIX_PARTITION];
 threadgroup uint basis_mask;
 threadgroup float tile[4096];
 threadgroup float scores[8u*MATRIX_PARTITION],biases[MATRIX_PARTITION];
 threadgroup uint row_indices[MATRIX_PARTITION],visible[MATRIX_PARTITION];
 threadgroup float maxima[8],denominators[8];
 for(uint r=tid;r<partition;r+=256u){
  uint physical=0u;bool valid=false;float bias=0.0f;
  if(r<count){physical=ring+begin+r;if(physical>=capacity)physical-=capacity;const Metadata m=metadata[physical];
   const ulong pos=ulong(m.position_lo)|(ulong(m.position_hi)<<32),coverage=ulong(m.coverage_lo)|(ulong(m.coverage_hi)<<32);
   valid=pos<=ulong(p.position)&&coverage!=0ul&&coverage<=ulong(p.position)+1ul&&(p.mode==0u||ulong(p.position)-pos<ulong(p.mode));bias=m.bias;
  }
  row_indices[r]=physical;visible[r]=valid?1u:0u;biases[r]=bias;
  const int row=valid?storage_index(encoded_keys,storage,physical):-1;
  storage_rows[r]=row;
  key_scales[r]=(valid&&row>=0)?packed_scale(encoded_keys+storage.key_payload_offset+uint(row)*storage.key_row_bytes,status,true):1.0f;
 }
 threadgroup_barrier(mem_flags::mem_threadgroup);
 for(uint i=tid;i<gqa*partition;i+=256u)scores[i]=0.0f;
 if(tid==0u){uint mask=0u;for(uint r=0u;r<partition;r++)if(visible[r]!=0u)mask|=storage_rows[r]>=0?2u:1u;basis_mask=mask;}
 threadgroup_barrier(mem_flags::mem_threadgroup);
 for(uint rb=0u;rb<partition;rb+=64u)for(uint basis=0u;basis<2u;basis++){
  if((basis_mask&(1u<<basis))==0u)continue;
  simdgroup_float8x8 dot=make_filled_simdgroup_matrix<float,8>(0.0f);
  for(uint dc=0u;dc<dim;dc+=64u){
   for(uint i=tid;i<4096u;i+=256u){const uint r=rb+i/64u,c=i%64u;
    float k=0.0f;
    if(r<partition&&visible[r]!=0u){const int row=storage_rows[r];
     if(basis==1u&&row>=0)k=rotated_key(encoded_keys,storage,uint(row),dc+c);
     if(basis==0u&&row<0)k=float(keys[uint(-row-1)*dim+dc+c]);
    }
    if(!isfinite(k)){atomic_store_explicit(status,1u,memory_order_relaxed);k=0.0f;}tile[i]=k;
   }
   threadgroup_barrier(mem_flags::mem_threadgroup);
   for(uint dc8=0u;dc8<64u;dc8+=8u){simdgroup_float8x8 q,k;
    simdgroup_load(q,queries+basis*p.groups*dim+first*dim+dc+dc8,dim);
    simdgroup_load(k,tile+sg*8u*64u+dc8,64u,0,true);simdgroup_multiply_accumulate(dot,q,k,dot);
   }
   threadgroup_barrier(mem_flags::mem_threadgroup);
  }
  // Disjoint exact/rotated rows let this pass overwrite only its own rows.
  if(rb+sg*8u<partition){
   simdgroup_store(dot,tile+sg*64u,8u);
   simdgroup_barrier(mem_flags::mem_threadgroup);
   for(uint i=lane;i<64u;i+=32u){const uint r=rb+sg*8u+i%8u,h=i/8u;
    if(r<partition&&visible[r]!=0u&&((storage_rows[r]>=0)==(basis==1u)))scores[h*partition+r]=tile[sg*64u+i];
   }
  }
  threadgroup_barrier(mem_flags::mem_threadgroup);
 }
 for(uint i=tid;i<gqa*partition;i+=256u){const uint r=i%partition;float score=(scores[i]*key_scales[r])*p.scale;
  if(visible[r]!=0u){if(!isfinite(score)){atomic_store_explicit(status,1u,memory_order_relaxed);score=0.0f;}if(p.epsilon>0.0f)score=p.epsilon*tanh(score/p.epsilon);score+=biases[r];if(!isfinite(score)){atomic_store_explicit(status,1u,memory_order_relaxed);score=0.0f;}}
  else score=-INFINITY;scores[i]=score;
 }
 threadgroup_barrier(mem_flags::mem_threadgroup);
 float local_max=-INFINITY;
 for(uint r=lane;r<partition;r+=32u)local_max=max(local_max,scores[sg*partition+r]);
 const float maximum=simd_max(local_max);float local_den=0.0f;
 for(uint r=lane;r<partition;r+=32u){const float weight=isfinite(maximum)?exp(scores[sg*partition+r]-maximum):0.0f;scores[sg*partition+r]=weight;local_den+=weight;}
 const float denominator=simd_sum(local_den);if(lane==0u){maxima[sg]=maximum;denominators[sg]=denominator;}
 threadgroup_barrier(mem_flags::mem_threadgroup);
 const uint dims_per_group=dim/8u;simdgroup_float8x8 output[MATRIX_DIM/64u];for(uint i=0;i<MATRIX_DIM/64u;i++)output[i]=make_filled_simdgroup_matrix<float,8>(0.0f);
 for(uint rb=0u;rb<partition;rb+=8u){
  for(uint i=tid;i<8u*dim;i+=256u){const uint r=rb+i/dim,d=i%dim;
   float value=0.0f;
   if(visible[r]!=0u){const int row=storage_rows[r];
    value=row>=0?rotated_value(encoded_values,storage,uint(row),d,status):float(values[uint(-row-1)*dim+d]);
   }
   if(!isfinite(value)){atomic_store_explicit(status,1u,memory_order_relaxed);value=0.0f;}tile[i]=value;
  }
  threadgroup_barrier(mem_flags::mem_threadgroup);simdgroup_float8x8 weights;simdgroup_load(weights,scores+rb,partition);
  for(uint i=0;i<dims_per_group/8u;i++){simdgroup_float8x8 v;simdgroup_load(v,tile+sg*dims_per_group+i*8u,dim);simdgroup_multiply_accumulate(output[i],weights,v,output[i]);}
  threadgroup_barrier(mem_flags::mem_threadgroup);
 }
 for(uint i=0;i<dims_per_group/8u;i++)simdgroup_store(output[i],partials+(output_first*splits+split)*(dim+2u)+sg*dims_per_group+i*8u,splits*(dim+2u));
 if(tid<gqa){const uint rec=((output_first+tid)*splits+split)*(dim+2u);partials[rec+dim]=denominators[tid];partials[rec+dim+1u]=maxima[tid];}
}
