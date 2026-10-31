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

// native-program:entry termite_mapped_page_touch
// native-program:indent 11
kernel void termite_mapped_page_touch(device const uchar *input [[buffer(0)]], device uint *output [[buffer(1)]], constant termite_metal_mapped_page_touch_params &p [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
    if (gid >= p.page_count) return; output[gid] = uint(input[ulong(gid) * ulong(p.page_size)]);
}
// native-program:entry termite_mapped_selected_expert_page_touch
// native-program:indent 11
kernel void termite_mapped_selected_expert_page_touch(device const uchar *input [[buffer(0)]], device const uint *expert_ids [[buffer(1)]], device uint *output [[buffer(2)]], constant termite_metal_mapped_selected_expert_page_touch_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    uint total = p.route_count * p.max_pages_per_route; if (gid >= total || p.page_size == 0u) return; uint route = gid / p.max_pages_per_route; uint local_page = gid - route * p.max_pages_per_route; uint expert = expert_ids[route]; if (expert >= p.expert_count) { output[gid] = 0u; return; }
    ulong first_row = ulong(expert) * ulong(p.source_out_dim) + ulong(p.row_offset); ulong span_start = ulong(p.weight_view_offset) + first_row * ulong(p.row_stride); ulong span_bytes = ulong(p.logical_out_dim) * ulong(p.row_stride); ulong first_page = (span_start / ulong(p.page_size)) * ulong(p.page_size); ulong end_page = ((span_start + span_bytes + ulong(p.page_size) - 1ul) / ulong(p.page_size)) * ulong(p.page_size); ulong page_count = (end_page - first_page) / ulong(p.page_size); if (ulong(local_page) >= page_count) { output[gid] = 0u; return; }
    output[gid] = uint(input[first_page + ulong(local_page) * ulong(p.page_size)]);
}
// native-program:entry termite_paged_f32_v_seed
// native-program:indent 11
kernel void termite_paged_f32_v_seed(device const float *src_v [[buffer(0)]], device float *dst_v [[buffer(1)]], device const uint *block_table [[buffer(2)]], constant termite_metal_paged_f32_kv_seed_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    uint values = p.v_row_stride; if (values == 0u) return; uint row = gid / values; uint col = gid - row * values; if (row >= p.suffix_tokens) return;
    uint logical = p.total_tokens - p.suffix_tokens + row; uint logical_block = logical / p.page_size; uint token_in_block = logical - logical_block * p.page_size; uint physical = block_table[logical_block] + token_in_block;
    dst_v[physical * p.v_row_stride + col] = src_v[row * p.v_row_stride + col];
}
// native-program:entry termite_encode_int8_per_head
// native-program:indent 11
kernel void termite_encode_int8_per_head(device const float *in [[buffer(0)]], device uchar *out [[buffer(1)]], constant termite_metal_encode_key_params &p [[buffer(2)]], uint2 gid [[thread_position_in_grid]], uint2 tid [[thread_position_in_threadgroup]], uint2 tg_size [[threads_per_threadgroup]]) {
    uint head_dim = p.head_dim; uint num_kv_heads = p.num_kv_heads; uint total_heads = p.rows * num_kv_heads; if (gid.y >= total_heads) return;
    uint row = gid.y / num_kv_heads; uint head = gid.y - row * num_kv_heads; uint d = gid.x; uint row_values = num_kv_heads * head_dim; uint in_base = row * row_values + head * head_dim; uint out_base = row * p.key_row_bytes + head * (head_dim + 4u);
    float val = (d < head_dim) ? in[in_base + d] : 0.0f; float abs_val = (d < head_dim) ? fabs(val) : 0.0f;
    threadgroup float tg_max[256]; tg_max[tid.x] = abs_val; threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint stride = tg_size.x >> 1u; stride > 0u; stride >>= 1u) { if (tid.x < stride) { tg_max[tid.x] = max(tg_max[tid.x], tg_max[tid.x + stride]); } threadgroup_barrier(mem_flags::mem_threadgroup); }
    float max_abs = tg_max[0]; float scale = max_abs == 0.0f ? 1.0f : max_abs / 127.0f; float inv_scale = 1.0f / scale;
    if (tid.x == 0u) { uint scale_bits = as_type<uint>(scale); out[out_base + 0u] = uchar(scale_bits & 0xffu); out[out_base + 1u] = uchar((scale_bits >> 8u) & 0xffu); out[out_base + 2u] = uchar((scale_bits >> 16u) & 0xffu); out[out_base + 3u] = uchar((scale_bits >> 24u) & 0xffu); }
    if (d < head_dim) { int q = int(clamp(round(val * inv_scale), -127.0f, 127.0f)); out[out_base + 4u + d] = uchar(q); }
}
