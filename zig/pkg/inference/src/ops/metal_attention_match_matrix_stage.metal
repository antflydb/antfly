// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Resident half or packed FP8 K/V; queries, weights, dot products, partials,
// and accumulators remain float. No query conversion or expanded resident cache.
// One 256-thread group handles all eight query heads for one row partition.
#include <metal_stdlib>
using namespace metal;
struct Params {uint op,n,dim,groups,position,stride,mode,index;float epsilon,scale,theta,reserved;};
struct Metadata {float bias;uint position_lo,position_hi,coverage_lo,coverage_hi;};
#ifndef MATRIX_ENCODING
#define MATRIX_ENCODING 0u
#endif
#if MATRIX_ENCODING == 1u
struct StorageHeader {
    uint encoding, first_row, rows, span_count, compressed_rows;
    uint key_payload_offset, key_row_bytes, value_row_bytes;
    float key_scale, value_scale;
    uint reserved0, reserved1;
};
// FP8 retains a contiguous packed middle and exact half prefix/suffix rows.
// Only the current threadgroup tile is decoded; resident buffers stay packed.
inline int fp8_storage_index(StorageHeader storage, uint row) {
    if (row >= storage.first_row && row - storage.first_row < storage.rows)
        return int(row - storage.first_row);
    return -int(row < storage.first_row ? row : row - storage.rows) - 1;
}
inline float fp8_decode(uchar bits, float scale, device atomic_uint *status) {
    if ((bits & 0x7fu) == 0x7fu) {
        atomic_store_explicit(status, 1u, memory_order_relaxed);
        return as_type<float>(0x7fc00000u);
    }
    const uint exponent = (bits >> 3u) & 15u, mantissa = bits & 7u;
    const float magnitude = exponent == 0u
        ? float(mantissa) * (1.0f / 512.0f)
        : as_type<float>(((exponent + 120u) << 23u) | (mantissa << 20u));
    const float decoded = (bits & 128u) != 0u ? -magnitude * scale : magnitude * scale;
    // Match the existing resident-FP8 attention contract exactly.
    return float(half(decoded));
}
#endif
#ifndef MATRIX_PARTITION
#define MATRIX_PARTITION 64u
#endif
#ifndef MATRIX_DIM
#define MATRIX_DIM 512u
#endif
// Short local windows map one independent query head to each SIMD group.
// Each eight-lane subgroup computes one row score; only SIMD-local barriers
// are needed before the float softmax and value pass. No query is narrowed.
#if MATRIX_DIM == 256u && MATRIX_PARTITION == 16u
#define QUERY_LANES 8u
kernel void metal_attention_match_matrix_stage(
 device const float*queries [[buffer(0)]],device const half*keys [[buffer(1)]],device const half*values [[buffer(2)]],
 device const uchar*unused_k [[buffer(3)]],device const uchar*unused_v [[buffer(4)]],device const Metadata*metadata [[buffer(5)]],
 device atomic_uint*status [[buffer(6)]],device float*partials [[buffer(7)]],constant Params&p [[buffer(8)]],
 uint3 gp [[threadgroup_position_in_grid]],uint tid [[thread_index_in_threadgroup]],uint lane [[thread_index_in_simdgroup]],uint sg [[simdgroup_index_in_threadgroup]]){
 const uint dim=MATRIX_DIM,gqa=p.stride>>16,first=p.stride&0xffffu,head=first+sg,split=gp.x,begin=split*MATRIX_PARTITION;
 if(sg>=gqa||split>=p.n)return;
 if(p.dim!=dim||gqa>8u){if(lane==0u)atomic_store_explicit(status,1u,memory_order_relaxed);return;}
 const uint record=(head*p.n+split)*(dim+2u),ring=as_type<uint>(p.theta),capacity=as_type<uint>(p.reserved);
 threadgroup float scores[256];threadgroup uint rows[256];
 float2 acc[MATRIX_DIM/64u];for(uint i=0u;i<dim/64u;i++)acc[i]=float2(0.0f);
 const uint qlane=lane%QUERY_LANES,row_group=lane/QUERY_LANES,rows_per_iteration=32u/QUERY_LANES;
 float4 q[MATRIX_DIM/(4u*QUERY_LANES)];
 for(uint i=0u;i<dim/(4u*QUERY_LANES);i++){const uint base=head*dim+qlane*4u+i*QUERY_LANES*4u;q[i]=float4(queries[base],queries[base+1u],queries[base+2u],queries[base+3u]);}
 const uint count=begin<p.index?min(uint(MATRIX_PARTITION),p.index-begin):0u;
 for(uint base=0u;base<count;base+=rows_per_iteration){
  const uint r=base+row_group;uint physical=ring+begin+r;if(physical>=capacity)physical-=capacity;float sum=0.0f;
  if(r<count)for(uint i=0u;i<dim/(4u*QUERY_LANES);i++){const float4 k=float4(reinterpret_cast<device const half4*>(keys)[physical*(dim/4u)+qlane+i*QUERY_LANES]);sum+=dot(q[i],k);}
  for(uint shift=1u;shift<QUERY_LANES;shift*=2u)sum+=simd_shuffle_xor(sum,ushort(shift));
  if(qlane==0u&&r<count)scores[sg*32u+r]=sum;
 }
 simdgroup_barrier(mem_flags::mem_threadgroup);
 float score=-INFINITY;bool visible=false;uint physical=0u;
 if(lane<count){physical=ring+begin+lane;if(physical>=capacity)physical-=capacity;const Metadata m=metadata[physical];const ulong pos=ulong(m.position_lo)|(ulong(m.position_hi)<<32),coverage=ulong(m.coverage_lo)|(ulong(m.coverage_hi)<<32);
  visible=pos<=ulong(p.position)&&coverage!=0ul&&coverage<=ulong(p.position)+1ul&&(p.mode==0u||ulong(p.position)-pos<ulong(p.mode));
  if(visible){score=scores[sg*32u+lane]*p.scale;if(!isfinite(score)){atomic_store_explicit(status,1u,memory_order_relaxed);score=0.0f;}if(p.epsilon>0.0f)score=p.epsilon*tanh(score/p.epsilon);score+=m.bias;if(!isfinite(score)){atomic_store_explicit(status,1u,memory_order_relaxed);score=0.0f;}}
 }
 const float maximum=simd_max(score),weight=visible&&isfinite(maximum)?exp(score-maximum):0.0f,denominator=simd_sum(weight);
 scores[sg*32u+lane]=weight;rows[sg*32u+lane]=visible?physical+1u:0u;
 simdgroup_barrier(mem_flags::mem_threadgroup);
 for(uint r=0u;r<count;r++){const uint encoded=rows[sg*32u+r];if(encoded==0u)continue;const uint physical_row=encoded-1u;const float w=scores[sg*32u+r];
  for(uint i=0u;i<dim/64u;i++){float2 v=float2(reinterpret_cast<device const half2*>(values)[physical_row*(dim/2u)+lane+i*32u]);if(!isfinite(v.x)){atomic_store_explicit(status,1u,memory_order_relaxed);v.x=0.0f;}if(!isfinite(v.y)){atomic_store_explicit(status,1u,memory_order_relaxed);v.y=0.0f;}acc[i]+=w*v;}
 }
 for(uint i=0u;i<dim/64u;i++){partials[record+lane*2u+i*64u]=acc[i].x;partials[record+lane*2u+i*64u+1u]=acc[i].y;}
 if(lane==0u){partials[record+dim]=denominator;partials[record+dim+1u]=maximum;}
}

#else
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
#if MATRIX_ENCODING == 1u
 const StorageHeader storage=*reinterpret_cast<device const StorageHeader*>(encoded_keys);
 if(storage.encoding!=1u||storage.first_row>p.index||storage.rows>p.index-storage.first_row){
  if(tid==0u)atomic_store_explicit(status,1u,memory_order_relaxed);
  return;
 }
 threadgroup int storage_rows[MATRIX_PARTITION];
#endif
 threadgroup half tile[4096];
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
#if MATRIX_ENCODING == 1u
  storage_rows[r]=valid?fp8_storage_index(storage,physical):-1;
#endif
 }
 threadgroup_barrier(mem_flags::mem_threadgroup);
 for(uint rb=0u;rb<partition;rb+=64u){
  simdgroup_float8x8 dot=make_filled_simdgroup_matrix<float,8>(0.0f);
  for(uint dc=0u;dc<dim;dc+=64u){
   for(uint i=tid;i<4096u;i+=256u){const uint r=rb+i/64u,c=i%64u;
#if MATRIX_ENCODING == 1u
    float k=0.0f;
    if(r<partition&&visible[r]!=0u){const int row=storage_rows[r];k=row>=0?fp8_decode(encoded_keys[storage.key_payload_offset+uint(row)*dim+dc+c],storage.key_scale,status):float(keys[uint(-row-1)*dim+dc+c]);}
#else
    float k=(r<partition&&visible[r]!=0u)?float(keys[row_indices[r]*dim+dc+c]):0.0f;
#endif
    if(!isfinite(k)){atomic_store_explicit(status,1u,memory_order_relaxed);k=0.0f;}tile[i]=half(k);
   }
   threadgroup_barrier(mem_flags::mem_threadgroup);
   for(uint dc8=0u;dc8<64u;dc8+=8u){simdgroup_float8x8 q;simdgroup_half8x8 k;
    simdgroup_load(q,queries+first*dim+dc+dc8,dim);simdgroup_load(k,tile+sg*8u*64u+dc8,64u,0,true);simdgroup_multiply_accumulate(dot,q,k,dot);
   }
   threadgroup_barrier(mem_flags::mem_threadgroup);
  }
  if(rb+sg*8u<partition)simdgroup_store(dot,scores+rb+sg*8u,partition);
 }
 threadgroup_barrier(mem_flags::mem_threadgroup);
 for(uint i=tid;i<gqa*partition;i+=256u){const uint r=i%partition;float score=scores[i]*p.scale;
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
#if MATRIX_ENCODING == 1u
   float value=0.0f;
   if(visible[r]!=0u){const int row=storage_rows[r];value=row>=0?fp8_decode(encoded_values[uint(row)*dim+d],storage.value_scale,status):float(values[uint(-row-1)*dim+d]);}
#else
   float value=visible[r]!=0u?float(values[row_indices[r]*dim+d]):0.0f;
#endif
   if(!isfinite(value)){atomic_store_explicit(status,1u,memory_order_relaxed);value=0.0f;}tile[i]=half(value);
  }
  threadgroup_barrier(mem_flags::mem_threadgroup);simdgroup_float8x8 weights;simdgroup_load(weights,scores+rb,partition);
  for(uint i=0;i<dims_per_group/8u;i++){simdgroup_half8x8 v;simdgroup_load(v,tile+sg*dims_per_group+i*8u,dim);simdgroup_multiply_accumulate(output[i],weights,v,output[i]);}
  threadgroup_barrier(mem_flags::mem_threadgroup);
 }
 for(uint i=0;i<dims_per_group/8u;i++)simdgroup_store(output[i],partials+(output_first*splits+split)*(dim+2u)+sg*dims_per_group+i*8u,splits*(dim+2u));
 if(tid<gqa){const uint rec=((output_first+tid)*splits+split)*(dim+2u);partials[rec+dim]=denominators[tid];partials[rec+dim+1u]=maxima[tid];}
}

#endif
