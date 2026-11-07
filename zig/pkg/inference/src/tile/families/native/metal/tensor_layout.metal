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

// native-program:entry termite_lightonocr3_copy16
// native-program:indent 8
kernel void termite_lightonocr3_copy16(device const uint4 *x [[buffer(0)]], device uint4 *y [[buffer(1)]], constant uint &n [[buffer(2)]], uint i [[thread_position_in_grid]]) { if (i < n) y[i] = x[i]; }
// native-program:entry vision_pack_half
// native-program:indent 8
kernel void vision_pack_half(device const float*q[[buffer(0)]],device const float*k[[buffer(1)]],device const float*v[[buffer(2)]],device half*out[[buffer(3)]],constant VisionParams&p[[buffer(4)]],uint i[[thread_position_in_grid]]){
 uint count=p.batch*p.num_heads*p.reserved1*64;if(i>=count)return;uint d=i%64,s=(i/64)%p.reserved1,h=(i/(64*p.reserved1))%p.num_heads,b=i/(64*p.reserved1*p.num_heads);uint offset=((b*p.seq_len+s)*p.num_heads+h)*64+d;
 out[i]=s<p.seq_len?half(q[offset]*0.125f):half(0);out[count+i]=s<p.seq_len?half(k[offset]):half(0);out[2*count+i]=s<p.seq_len?half(v[offset]):half(0);
}
// native-program:entry termite_lightonocr3_pack_bf16
// native-program:indent 11
kernel void termite_lightonocr3_pack_bf16(device const float *input [[buffer(0)]], device ushort *output [[buffer(1)]], constant uint &count [[buffer(2)]], uint i [[thread_position_in_grid]]) {
 if(i>=count)return;uint bits=as_type<uint>(input[i]);
 output[i]=(bits&0x7fffffffu)>0x7f800000u?ushort((bits>>16)|0x40u):ushort((bits+0x7fffu+((bits>>16)&1u))>>16);
}
// native-program:entry termite_broadcast_last_dim_f32_2d
// native-program:indent 11
kernel void termite_broadcast_last_dim_f32_2d(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_broadcast_last_dim_params &p [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
    const uint total = p.rows * p.out_dim;
    if (gid >= total || p.in_dim == 0u || p.out_dim == 0u) return;
    const uint row = gid / p.out_dim;
    const uint col = gid - row * p.out_dim;
    const uint in_col = p.in_dim == 1u ? 0u : col;
    output[gid] = input[row * p.in_dim + in_col];
}
// native-program:entry termite_broadcast_f32
// native-program:indent 11
kernel void termite_broadcast_f32(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_broadcast_f32_params &p [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
    if (gid >= p.total || p.rank == 0u || p.rank > 8u) return;
    uint flat_in = termite_affine_input_offset(gid, p.rank, p.out_strides, p.input_strides_for_out);
    output[gid] = input[flat_in];
}
// native-program:entry termite_reduce_axis_f32
// native-program:indent 11
kernel void termite_reduce_axis_f32(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_reduce_axis_f32_params &p [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
    if (gid >= p.out_total || p.rank == 0u || p.rank > 8u || p.reduce_dim == 0u) return;
    uint base = termite_affine_input_offset(gid, p.rank, p.out_strides, p.input_strides_for_out);
    float acc = p.kind == 1u ? -INFINITY : 0.0f;
    for (uint i = 0u; i < p.reduce_dim; ++i) {float x=input[base+i*p.reduce_stride];acc=p.kind==1u?((isnan(acc)||isnan(x))?NAN:max(acc,x)):acc+x;}
    output[gid] = p.kind == 2u ? acc / float(p.reduce_dim) : acc;
}
// native-program:entry termite_spatial_pool_prepare
// native-program:indent 11
kernel void termite_spatial_pool_prepare(device const float *input [[buffer(0)]], device const float *bias [[buffer(1)]], device const float *scale [[buffer(2)]], device float *output [[buffer(3)]], constant termite_spatial_pool_params &p [[buffer(4)]], uint i [[thread_position_in_grid]]) {
#pragma clang fp contract(off)
    if (i >= p.total) return;
    uint channel=i%p.dim, row=i/p.dim, px=p.gx/p.merge, py=p.gy/p.merge;
    uint batch=row/(px*py), y=(row/px)%py, x=row%px;
    uint base=(batch*p.gx*p.gy+y*p.merge*p.gx+x*p.merge)*p.dim+channel;
    float sum=input[base];
    for (uint dy=0; dy<p.merge; ++dy) for (uint dx=0; dx<p.merge; ++dx) if (dy || dx) sum=sum+input[base+(dy*p.gx+dx)*p.dim];
    float value=sum*p.factor;
    if (p.flags) { value=value-bias[channel]; value=value*scale[channel]; }
    output[i]=value;
}
// native-program:entry termite_apply_where_select_1x
// native-program:indent 11
kernel void termite_apply_where_select_1x(device const float *cond [[buffer(0)]], device const float *on_true [[buffer(1)]], device const float *on_false [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_apply_add_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {
    uint total = p.rows * p.dim; if (gid >= total) return;
    float c = cond[(p.flags & 1u) != 0u ? 0u : gid]; float t = on_true[(p.flags & 2u) != 0u ? 0u : gid]; float f = on_false[(p.flags & 4u) != 0u ? 0u : gid];
    output[gid] = c != 0.0f ? t : f;
}
// native-program:entry termite_slice_last_dim_f32_2d
// native-program:indent 11
kernel void termite_slice_last_dim_f32_2d(device const float *src [[buffer(0)]], device float *dst [[buffer(1)]], constant termite_metal_slice_last_dim_f32_2d_params &p [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
    uint out_cols = p.out_cols; if (out_cols == 0u) return; uint row = gid / out_cols; uint col = gid - row * out_cols; if (row >= p.rows) return;
    dst[row * out_cols + col] = src[row * p.cols + p.start + col];
}
// native-program:entry termite_copy_bytes
// native-program:indent 11
kernel void termite_copy_bytes(device const uchar *src [[buffer(0)]], device uchar *dst [[buffer(1)]], constant uint &count [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
    if (gid < count) dst[gid] = src[gid];
}
// native-program:entry termite_gather_axis0_f32_2d
// native-program:indent 11
kernel void termite_gather_axis0_f32_2d(device const float *input [[buffer(0)]], device const float *indices [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_gather_axis0_f32_2d_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    termite_gather_axis0_impl<false>(input, input, indices, output, p, gid);
}
// native-program:entry termite_gather_axis0_add_bias_f32_2d
// native-program:indent 11
kernel void termite_gather_axis0_add_bias_f32_2d(device const float *input [[buffer(0)]], device const float *bias [[buffer(1)]], device const float *indices [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_gather_axis0_f32_2d_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {
    termite_gather_axis0_impl<true>(input, bias, indices, output, p, gid);
}
// native-program:entry termite_concat_lastdim_f32_2d
// native-program:indent 11
kernel void termite_concat_lastdim_f32_2d(device const float *a [[buffer(0)]], device const float *b [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_concat_lastdim_f32_2d_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    uint out_dim = p.dim_a + p.dim_b; uint total = p.rows * out_dim; if (gid >= total || out_dim == 0u) return;
    uint row = gid / out_dim; uint col = gid - row * out_dim;
    output[gid] = col < p.dim_a ? a[row * p.dim_a + col] : b[row * p.dim_b + (col - p.dim_a)];
}
// native-program:entry termite_argmax_axis_f32
// native-program:indent 11
kernel void termite_argmax_axis_f32(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_argmax_axis_f32_params &p [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
    uint out_total = p.outer * p.inner; if (gid >= out_total || p.axis_dim == 0u || p.inner == 0u) return;
    uint outer_idx = gid / p.inner; uint inner_idx = gid - outer_idx * p.inner;
    uint base = outer_idx * p.axis_dim * p.inner + inner_idx;
    float best = input[base]; uint best_idx = 0u;
    for (uint i = 1u; i < p.axis_dim; ++i) { float candidate = input[base + i * p.inner]; if (candidate > best) { best = candidate; best_idx = i; } }
    output[gid] = float(best_idx);
}
// native-program:entry termite_cumulative_sum_i32
// native-program:indent 11
kernel void termite_cumulative_sum_i32(device const uint *input [[buffer(0)]], device uint *output [[buffer(1)]], constant termite_scan_params &p [[buffer(2)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {
    threadgroup uint totals[256];
    termite_cumulative_sum_impl<uint>(input, output, p, tid, row, totals);
}
// native-program:entry termite_cumulative_sum_i16
// native-program:indent 11
kernel void termite_cumulative_sum_i16(device const ushort *input [[buffer(0)]], device ushort *output [[buffer(1)]], constant termite_scan_params &p [[buffer(2)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {
    threadgroup ushort totals[256];
    termite_cumulative_sum_impl<ushort>(input, output, p, tid, row, totals);
}
// native-program:entry termite_gather_typed
// native-program:indent 11
kernel void termite_gather_typed(device const uchar *input [[buffer(0)]], device const uchar *indices [[buffer(1)]], device uchar *output [[buffer(2)]], constant uint *p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    if (gid >= p[0]) return;
    uint inner = p[1], count = p[2], extent = p[3], bytes = p[4];
    uint index_pos = (gid / inner) % count;
    long index = termite_read_integer(indices, p[5], index_pos);
    if (index < 0) index += long(extent);
    ulong source = (ulong(gid / (inner * count)) * extent + ulong(index)) * inner + gid % inner;
    for (uint j = 0; j < bytes; ++j) output[ulong(gid) * bytes + j] = input[source * bytes + j];
}
// native-program:entry termite_slice_typed
// native-program:indent 11
kernel void termite_slice_typed(device const uchar *src [[buffer(0)]], device const uchar *unused [[buffer(1)]], device uchar *out [[buffer(2)]], constant long *p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
  if (ulong(gid) >= ulong(p[0])) return; long index = p[3]; ulong rem = gid;
  if (p[2] == 1) index += long(gid) * p[12]; else for (uint a = 0; a < uint(p[2]); ++a) { ulong coord = rem / ulong(p[4+a]); rem %= ulong(p[4+a]); index += long(coord) * p[12+a]; }
  switch (p[1]) { case 8: ((device ulong *)out)[gid] = ((device const ulong *)src)[index]; break; case 4: ((device uint *)out)[gid] = ((device const uint *)src)[index]; break; case 2: ((device ushort *)out)[gid] = ((device const ushort *)src)[index]; break; default: out[gid] = src[index]; }
}
// native-program:entry termite_select_typed
// native-program:indent 11
kernel void termite_select_typed(device const uchar *c [[buffer(0)]], device const uchar *t [[buffer(1)]], device const uchar *f [[buffer(2)]], device uchar *out [[buffer(3)]], constant uint *p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {
  if (gid >= p[0]) return; uint rem = gid; uint3 ix = uint3(0);
  if (p[1] == 1) ix = gid * uint3(p[13], p[21], p[29]); else for (uint a = 0; a < p[1]; ++a) { uint coord = rem / p[5+a]; rem %= p[5+a]; ix += coord * uint3(p[13+a], p[21+a], p[29+a]); }
  device const uchar *src; uint index;
  if (p[2]) { src = c; index = ix.x; } else { bool take = p[3] == 0 ? ((device const float *)c)[ix.x] != 0.0f : termite_read_integer(c, p[3], ix.x) != 0; src = take ? t : f; index = take ? ix.y : ix.z; }
  switch (p[4]) { case 8: ((device ulong *)out)[gid] = ((device const ulong *)src)[index]; break; case 4: ((device uint *)out)[gid] = ((device const uint *)src)[index]; break; case 2: ((device ushort *)out)[gid] = ((device const ushort *)src)[index]; break; default: out[gid] = src[index]; }
}
// native-program:entry termite_cast_typed
// native-program:indent 11
kernel void termite_cast_typed(device const uchar *input [[buffer(0)]], device uchar *output [[buffer(1)]], constant uint *p [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
    if (gid >= p[0]) return;
    long integer = 0;
    switch (p[1]) {
        case 1: integer = ((device const int *)input)[gid]; break;
        case 2: integer = ((device const long *)input)[gid]; break;
        case 3: integer = ((device const char *)input)[gid]; break;
        case 4: integer = ((device const short *)input)[gid]; break;
        case 5: case 6: integer = input[gid]; break;
    }
    float floating = p[1] == 0u ? ((device const float *)input)[gid] : (p[1] == 7u ? float(((device const half *)input)[gid]) : float(integer));
    if (p[2] == 0u) { ((device float *)output)[gid] = floating; return; }
    if (p[2] == 7u) { ((device half *)output)[gid] = half(floating); return; }
    if (p[2] == 6u) { output[gid] = p[1] == 0u ? uchar(floating != 0.0f) : uchar(integer != 0); return; }
    if (p[1] == 0u) integer = long(floating);
    switch (p[2]) {
        case 1: ((device uint *)output)[gid] = uint(integer); break;
        case 2: ((device ulong *)output)[gid] = ulong(integer); break;
        case 3: case 5: output[gid] = uchar(integer); break;
        case 4: ((device ushort *)output)[gid] = ushort(integer); break;
    }
}
// native-program:entry antfly_compose_rows
// native-program:indent 11
kernel void antfly_compose_rows(device const float *wrappers [[buffer(0)]], device const float *media [[buffer(1)]], device const int *map [[buffer(2)]], device float *output [[buffer(3)]], constant uint2 &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {
    if (gid >= p.x * p.y) return; uint row = gid / p.y, col = gid % p.y; int source = map[row];
    output[gid] = source < 0 ? media[uint(-source - 1) * p.y + col] : wrappers[uint(source) * p.y + col];
}
// native-program:entry termite_transpose_f32
// native-program:indent 11
kernel void termite_transpose_f32(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_transpose_f32_params &p [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
    if (gid >= p.total || p.rank == 0u || p.rank > 8u) return;
    uint rem = gid; uint flat_in = 0u;
    for (uint axis = 0u; axis < p.rank; ++axis) { uint stride = p.out_strides[axis]; uint coord = stride == 0u ? 0u : rem / stride; if (stride != 0u) rem -= coord * stride; uint in_axis = p.perm[axis]; if (in_axis >= p.rank) return; flat_in += coord * p.in_strides[in_axis]; }
    output[gid] = input[flat_in];
}
// native-program:entry termite_transpose_last2_tiled_f32
// native-program:indent 11
kernel void termite_transpose_last2_tiled_f32(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_transpose_last2_params &p [[buffer(2)]], uint3 tid [[thread_position_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {
    threadgroup float tile[32][33];
    const uint R = p.rows; const uint C = p.cols;
    const uint base = tg.z * R * C;
    const uint x = tg.x * 32u + tid.x;
    const uint y = tg.y * 32u + tid.y;
    if (x < C && y < R) tile[tid.y][tid.x] = input[base + y * C + x];
    threadgroup_barrier(mem_flags::mem_threadgroup);
    const uint ox = tg.y * 32u + tid.x;
    const uint oy = tg.x * 32u + tid.y;
    if (ox < R && oy < C) output[base + oy * R + ox] = tile[tid.x][tid.y];
}
// native-program:entry termite_dot_general_rank1_reduce_f32
// native-program:indent 11
kernel void termite_dot_general_rank1_reduce_f32(device const float *lhs [[buffer(0)]], device const float *rhs [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_dot_general_2d_f32_params &p [[buffer(3)]], threadgroup float *partials [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {
    if (row >= p.m || p.n != 1u || p.k == 0u) return; float acc = 0.0f;
    for (uint kk = uint(sgitg) * 32u + uint(lane); kk < p.k; kk += 256u) acc += lhs[row * p.k + kk] * rhs[kk];
    if (sgitg == 0u) partials[lane] = 0.0f; acc = simd_sum(acc); threadgroup_barrier(mem_flags::mem_threadgroup);
    if (lane == 0u) partials[sgitg] = acc; threadgroup_barrier(mem_flags::mem_threadgroup);
    float total_acc = simd_sum(partials[lane]); if (lane == 0u && sgitg == 0u) output[row] = total_acc;
}
// native-program:entry termite_scatter_add_axis0_f32
// native-program:indent 11
kernel void termite_scatter_add_axis0_f32(device const float *values [[buffer(0)]], device const uchar *indices [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_scatter_add_axis0_f32_params &p [[buffer(3)]], threadgroup uint *matches [[threadgroup(0)]], uint lid [[thread_index_in_threadgroup]], uint2 tpg [[threads_per_threadgroup]], uint2 group [[threadgroup_position_in_grid]]) {
    uint out_row = group.x; uint col = group.y * tpg.x + lid; if (out_row >= p.out_rows || p.dim == 0u) return; float acc = 0.0f;
    for (uint tile = 0u; tile < p.value_rows; tile += tpg.x) {
        if (lid == 0u) { uint count = 0u; uint end = min(tile + tpg.x, p.value_rows); for (uint row = tile; row < end; ++row) { uint target; if (p.reserved0 == 2u) { uint2 wide = ((device const uint2 *)indices)[row]; if (wide.y == 0u) target = wide.x; else if (wide.y == 0xffffffffu) target = wide.x + p.out_rows; else continue; } else if (p.reserved0 == 1u) { int index = ((device const int *)indices)[row]; target = index < 0 ? uint(index) + p.out_rows : uint(index); } else { target = uint(rint(((device const float *)indices)[row])); } if (target == out_row) matches[1u + count++] = row; } matches[0] = count; }
        threadgroup_barrier(mem_flags::mem_threadgroup);
        if (col < p.dim) { uint count = matches[0]; for (uint match = 0u; match < count; ++match) acc += values[matches[1u + match] * p.dim + col]; }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    if (col < p.dim) output[out_row * p.dim + col] = acc;
}
// native-program:entry termite_packed_gelu
// native-program:indent 11
kernel void termite_packed_gelu(device const float *x [[buffer(0)]], device float *y [[buffer(1)]], constant uint2 &p [[buffer(2)]], uint i [[thread_position_in_grid]]) { if (i >= p.x * p.y) return; uint row = i / p.y, col = i % p.y; y[i] = termite_gated_activation_product(x[row * p.y * 2u + col], x[row * p.y * 2u + p.y + col], 0u); }
