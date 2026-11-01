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

// native-program:entry termite_copy_f32
extern "C" __global__ void termite_copy_f32(float* dst, const float* src, unsigned int n) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) dst[i] = src[i];
}
// native-program:entry termite_copy_u8
extern "C" __global__ void termite_copy_u8(unsigned char* dst, const unsigned char* src, unsigned int n) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) dst[i] = src[i];
}
// native-program:entry termite_f32_to_bf16
extern "C" __global__ void termite_f32_to_bf16(
    unsigned short* dst,
    const float* input,
    unsigned int count
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    dst[idx] = termite_f32_to_bf16(input[idx]);
}
// native-program:entry termite_f32_to_f16
extern "C" __global__ void termite_f32_to_f16(
    unsigned short* dst,
    const float* input,
    unsigned int count
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    __half value = __float2half_rn(input[idx]);
    dst[idx] = __half_as_ushort(value);
}
// native-program:entry termite_f32_to_i32
extern "C" __global__ void termite_f32_to_i32(
    int* dst,
    const float* input,
    unsigned int count
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    dst[idx] = __float2int_rn(input[idx]);
}
// native-program:entry termite_elementwise_broadcast_f32
extern "C" __global__ void termite_elementwise_broadcast_f32(
    float* dst,
    const float* a,
    const float* b,
    unsigned int count,
    unsigned int a_rank,
    unsigned int b_rank,
    unsigned int output_rank,
    unsigned int op,
    unsigned int a0, unsigned int a1, unsigned int a2, unsigned int a3,
    unsigned int a4, unsigned int a5, unsigned int a6, unsigned int a7,
    unsigned int b0, unsigned int b1, unsigned int b2, unsigned int b3,
    unsigned int b4, unsigned int b5, unsigned int b6, unsigned int b7,
    unsigned int o0, unsigned int o1, unsigned int o2, unsigned int o3,
    unsigned int o4, unsigned int o5, unsigned int o6, unsigned int o7
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    unsigned int a_dims[8] = { a0, a1, a2, a3, a4, a5, a6, a7 };
    unsigned int b_dims[8] = { b0, b1, b2, b3, b4, b5, b6, b7 };
    unsigned int out_dims[8] = { o0, o1, o2, o3, o4, o5, o6, o7 };
    unsigned int coords[8] = { 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u };
    unsigned int remaining = idx;
    for (int d = (int)output_rank - 1; d >= 0; --d) {
        coords[d] = remaining % out_dims[d];
        remaining /= out_dims[d];
    }
    unsigned int a_idx = 0u;
    for (unsigned int d = 0; d < a_rank; ++d) {
        unsigned int out_d = output_rank - a_rank + d;
        unsigned int coord = a_dims[d] == 1u ? 0u : coords[out_d];
        a_idx = a_idx * a_dims[d] + coord;
    }
    unsigned int b_idx = 0u;
    for (unsigned int d = 0; d < b_rank; ++d) {
        unsigned int out_d = output_rank - b_rank + d;
        unsigned int coord = b_dims[d] == 1u ? 0u : coords[out_d];
        b_idx = b_idx * b_dims[d] + coord;
    }
    float x = a[a_idx];
    float y = b[b_idx];
    if (op == 0u) dst[idx] = x + y;
    else if (op == 1u) dst[idx] = x * y;
    else if (op == 8u) dst[idx] = x - y;
    else if (op == 9u) dst[idx] = x / y;
    else dst[idx] = x < y ? 1.0f : 0.0f;
}
// native-program:entry termite_primitive_where_f32
extern "C" __global__ void termite_primitive_where_f32(
    float* output,
    const float* cond,
    const float* on_true,
    const float* on_false,
    unsigned int count
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < count) output[idx] = cond[idx] != 0.0f ? on_true[idx] : on_false[idx];
}
// native-program:entry termite_activation_multiply_slice_last_dim_f32
extern "C" __global__ void termite_activation_multiply_slice_last_dim_f32(
    float* dst,
    const float* gate,
    const float* source,
    unsigned int rows,
    unsigned int source_cols,
    unsigned int start,
    unsigned int out_cols,
    unsigned int activation
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * out_cols;
    if (i >= count) return;
    unsigned int row = i / out_cols;
    unsigned int col = i - row * out_cols;
    dst[i] = termite_decoder_activation_f32(gate[i], activation) * source[row * source_cols + start + col];
}
// native-program:entry termite_take_rows_f32
extern "C" __global__ void termite_take_rows_f32(
    float* dst,
    const float* input,
    const unsigned int* row_ids,
    unsigned int source_rows,
    unsigned int rows,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * dim;
    if (idx >= count) return;
    unsigned int row = idx / dim;
    unsigned int col = idx - row * dim;
    unsigned int src_row = row_ids[row];
    dst[idx] = src_row < source_rows ? input[src_row * dim + col] : 0.0f;
}
// native-program:entry termite_slice_last_dim_f32
extern "C" __global__ void termite_slice_last_dim_f32(
    float* dst,
    const float* input,
    unsigned int rows,
    unsigned int cols,
    unsigned int start,
    unsigned int out_cols
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * out_cols;
    if (idx >= count) return;
    unsigned int row = idx / out_cols;
    unsigned int out_col = idx - row * out_cols;
    dst[idx] = input[row * cols + start + out_col];
}
// native-program:entry termite_repeat_first_row_f32
extern "C" __global__ void termite_repeat_first_row_f32(
    float* dst,
    const float* src,
    unsigned int rows,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * dim;
    if (idx >= count) return;
    unsigned int col = idx % dim;
    dst[idx] = src[col];
}
// native-program:entry termite_concat_lastdim_f32
extern "C" __global__ void termite_concat_lastdim_f32(
    float* dst,
    const float* a,
    const float* b,
    unsigned int total,
    unsigned int dim_a,
    unsigned int dim_b
) {
    unsigned int out_dim = dim_a + dim_b;
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = total * out_dim;
    if (idx >= count) return;
    unsigned int row = idx / out_dim;
    unsigned int col = idx - row * out_dim;
    if (col < dim_a) {
        dst[idx] = a[row * dim_a + col];
    } else {
        dst[idx] = b[row * dim_b + (col - dim_a)];
    }
}
// native-program:entry termite_token_to_nchw_f32
extern "C" __global__ void termite_token_to_nchw_f32(ANTFLY_CUDA_TOKEN_TO_NCHW_F32_PARAMS) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = batch * height * width * channels;
    if (idx >= total) return;
    unsigned int c = idx % channels;
    unsigned int tmp = idx / channels;
    unsigned int x = tmp % width;
    tmp /= width;
    unsigned int y = tmp % height;
    unsigned int b = tmp / height;
    unsigned int dst_idx = ((b * channels + c) * height + y) * width + x;
    dst[dst_idx] = src[idx];
}
// native-program:entry termite_nchw_to_token_f32
extern "C" __global__ void termite_nchw_to_token_f32(ANTFLY_CUDA_TOKEN_TO_NCHW_F32_PARAMS) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = batch * height * width * channels;
    if (idx >= total) return;
    unsigned int c = idx % channels;
    unsigned int tmp = idx / channels;
    unsigned int x = tmp % width;
    tmp /= width;
    unsigned int y = tmp % height;
    unsigned int b = tmp / height;
    unsigned int src_idx = ((b * channels + c) * height + y) * width + x;
    dst[idx] = src[src_idx];
}
// native-program:entry termite_pack_windows_f32
extern "C" __global__ void termite_pack_windows_f32(ANTFLY_CUDA_PACK_WINDOWS_F32_PARAMS) {
    unsigned int windows_h = padded_h / window_size;
    unsigned int windows_w = padded_w / window_size;
    unsigned int window_area = window_size * window_size;
    unsigned int window_count = batch * windows_h * windows_w;
    unsigned int total = window_count * window_area * dim;
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= total) return;

    unsigned int feature = idx % dim;
    unsigned int token = (idx / dim) % window_area;
    unsigned int window_idx = idx / (dim * window_area);
    unsigned int dx = token % window_size;
    unsigned int dy = token / window_size;
    unsigned int ww = window_idx % windows_w;
    unsigned int tmp = window_idx / windows_w;
    unsigned int wh = tmp % windows_h;
    unsigned int b = tmp / windows_h;
    unsigned int src_y = wh * window_size + dy;
    unsigned int src_x = ww * window_size + dx;
    if (src_y >= height || src_x >= width) {
        dst[idx] = 0.0f;
        return;
    }
    unsigned int src_idx = (b * height * width + src_y * width + src_x) * dim + feature;
    dst[idx] = src[src_idx];
}
// native-program:entry termite_unpad_windows_f32
extern "C" __global__ void termite_unpad_windows_f32(ANTFLY_CUDA_PACK_WINDOWS_F32_PARAMS) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = batch * height * width * dim;
    if (idx >= total) return;

    unsigned int feature = idx % dim;
    unsigned int tmp = idx / dim;
    unsigned int x = tmp % width;
    tmp /= width;
    unsigned int y = tmp % height;
    unsigned int b = tmp / height;
    unsigned int windows_w = padded_w / window_size;
    unsigned int wh = y / window_size;
    unsigned int ww = x / window_size;
    unsigned int dy = y % window_size;
    unsigned int dx = x % window_size;
    unsigned int window_idx = (b * (padded_h / window_size) + wh) * windows_w + ww;
    unsigned int token = dy * window_size + dx;
    unsigned int src_idx = (window_idx * window_size * window_size + token) * dim + feature;
    dst[idx] = src[src_idx];
}
// native-program:entry termite_pack_q8_0_tensor_core
extern "C" __global__ void termite_pack_q8_0_tensor_core(
    unsigned char* dst,
    const unsigned char* src,
    unsigned int block_count
) {
    size_t scale_bytes = (size_t)block_count * 2u;
    size_t quant_bytes = (size_t)block_count * 32u;
    size_t total_bytes = scale_bytes + quant_bytes;
    size_t idx = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    size_t stride = (size_t)gridDim.x * blockDim.x;
    for (; idx < total_bytes; idx += stride) {
        if (idx < scale_bytes) {
            size_t block = idx >> 1u;
            size_t lane = idx & 1u;
            dst[idx] = src[block * 34u + lane];
        } else {
            size_t quant_idx = idx - scale_bytes;
            size_t block = quant_idx >> 5u;
            size_t lane = quant_idx & 31u;
            dst[idx] = src[block * 34u + 2u + lane];
        }
    }
}
// native-program:entry termite_primitive_reduce_f32
extern "C" __global__ void termite_primitive_reduce_f32(
    float* output,
    const float* input,
    unsigned int input_count,
    unsigned int output_count,
    unsigned int dim0,
    unsigned int dim1,
    unsigned int dim2,
    unsigned int dim3,
    unsigned int dim4,
    unsigned int dim5,
    unsigned int dim6,
    unsigned int dim7,
    unsigned int rank,
    unsigned int reduce_mask,
    unsigned int mode,
    unsigned int reduce_count
) {
    unsigned int out_idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (out_idx >= output_count) return;
    unsigned int dims[8] = {dim0, dim1, dim2, dim3, dim4, dim5, dim6, dim7};
    float acc = mode == 1u ? -3.402823466e+38f : 0.0f;
    // A contiguous set of reduced axes is a logical [outer, reduce, inner]
    // tensor. Decode the output coordinate once, rather than doing rank-many
    // integer divisions for every input value. Preserve the serial FP32 sum
    // order so this indexing optimization does not change training numerics.
    unsigned int first = rank, last = 0u;
    for (unsigned int d = 0; d < rank; ++d) {
        if ((reduce_mask & (1u << d)) != 0u) {
            first = min(first, d);
            last = d;
        }
    }
    bool contiguous = first < rank;
    for (unsigned int d = first; d <= last && d < rank; ++d)
        contiguous = contiguous && ((reduce_mask & (1u << d)) != 0u);
    if (contiguous) {
        unsigned int inner = 1u;
        for (unsigned int d = last + 1u; d < rank; ++d) inner *= dims[d];
        unsigned int base = (out_idx / inner) * reduce_count * inner + out_idx % inner;
        for (unsigned int r = 0; r < reduce_count; ++r) {
            float value = input[base + r * inner];
            acc = mode == 1u ? fmaxf(acc, value) : acc + value;
        }
        if (mode == 2u && reduce_count != 0u) acc /= (float)reduce_count;
        output[out_idx] = acc;
        return;
    }
    for (unsigned int reduced_idx = 0; reduced_idx < reduce_count; ++reduced_idx) {
        unsigned int output_remaining = out_idx;
        unsigned int reduced_remaining = reduced_idx;
        unsigned int input_idx = 0u;
        unsigned int input_stride = 1u;
        for (int d = (int)rank - 1; d >= 0; --d) {
            unsigned int coord;
            if ((reduce_mask & (1u << d)) != 0u) {
                coord = reduced_remaining % dims[d];
                reduced_remaining /= dims[d];
            } else {
                coord = output_remaining % dims[d];
                output_remaining /= dims[d];
            }
            input_idx += coord * input_stride;
            input_stride *= dims[d];
        }
        if (input_idx >= input_count) continue;
        float value = input[input_idx];
        acc = mode == 1u ? fmaxf(acc, value) : acc + value;
    }
    if (mode == 2u && reduce_count != 0u) acc /= (float)reduce_count;
    output[out_idx] = acc;
}
// native-program:entry termite_primitive_broadcast_f32
extern "C" __global__ void termite_primitive_broadcast_f32(
    float* output,
    const float* input,
    unsigned int output_count,
    unsigned int in0, unsigned int in1, unsigned int in2, unsigned int in3,
    unsigned int in4, unsigned int in5, unsigned int in6, unsigned int in7,
    unsigned int out0, unsigned int out1, unsigned int out2, unsigned int out3,
    unsigned int out4, unsigned int out5, unsigned int out6, unsigned int out7,
    unsigned int axis0, unsigned int axis1, unsigned int axis2, unsigned int axis3,
    unsigned int axis4, unsigned int axis5, unsigned int axis6, unsigned int axis7,
    unsigned int input_rank,
    unsigned int output_rank
) {
    unsigned int out_idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (out_idx >= output_count) return;
    unsigned int input_dims[8] = {in0, in1, in2, in3, in4, in5, in6, in7};
    unsigned int output_dims[8] = {out0, out1, out2, out3, out4, out5, out6, out7};
    unsigned int axes[8] = {axis0, axis1, axis2, axis3, axis4, axis5, axis6, axis7};
    unsigned int input_flat = 0u;
    for (unsigned int in_d = 0; in_d < input_rank; ++in_d) {
        unsigned int axis = axes[in_d];
        unsigned int stride = 1u;
        for (unsigned int d = axis + 1u; d < output_rank; ++d) stride *= output_dims[d];
        unsigned int coord = (out_idx / stride) % output_dims[axis];
        if (input_dims[in_d] == 1u) coord = 0u;
        else coord %= input_dims[in_d];
        input_flat = input_flat * input_dims[in_d] + coord;
    }
    output[out_idx] = input[input_flat];
}
// native-program:entry termite_primitive_gather_f32
extern "C" __global__ void termite_primitive_gather_f32(ANTFLY_CUDA_PRIMITIVE_GATHER_PARAMS) {
    termite_primitive_gather_impl<0u>(output, input, indices, output_count, index_count, axis_extent, suffix_size);
}
// native-program:entry termite_primitive_gather_bf16_f32
extern "C" __global__ void termite_primitive_gather_bf16_f32(ANTFLY_CUDA_PRIMITIVE_GATHER_BF16_PARAMS) {
    termite_primitive_gather_impl<1u>(output, input, indices, output_count, index_count, axis_extent, suffix_size);
}
// native-program:entry termite_primitive_gather_f32_i32
extern "C" __global__ void termite_primitive_gather_f32_i32(ANTFLY_CUDA_PRIMITIVE_GATHER_I32_PARAMS) {
    termite_primitive_gather_integer_impl<0u, float, int>(output, input, indices, output_count, index_count, axis_extent, suffix_size);
}
// native-program:entry termite_primitive_gather_f32_i64
extern "C" __global__ void termite_primitive_gather_f32_i64(ANTFLY_CUDA_PRIMITIVE_GATHER_I64_PARAMS) {
    termite_primitive_gather_integer_impl<0u, float, long long>(output, input, indices, output_count, index_count, axis_extent, suffix_size);
}
// native-program:entry termite_primitive_gather_bf16_f32_i32
extern "C" __global__ void termite_primitive_gather_bf16_f32_i32(ANTFLY_CUDA_PRIMITIVE_GATHER_BF16_I32_PARAMS) {
    termite_primitive_gather_integer_impl<1u, unsigned short, int>(output, input, indices, output_count, index_count, axis_extent, suffix_size);
}
// native-program:entry termite_primitive_gather_bf16_f32_i64
extern "C" __global__ void termite_primitive_gather_bf16_f32_i64(ANTFLY_CUDA_PRIMITIVE_GATHER_BF16_I64_PARAMS) {
    termite_primitive_gather_integer_impl<1u, unsigned short, long long>(output, input, indices, output_count, index_count, axis_extent, suffix_size);
}
// native-program:entry termite_primitive_scatter_add_axis0_f32
extern "C" __global__ void termite_primitive_scatter_add_axis0_f32(ANTFLY_CUDA_PRIMITIVE_SCATTER_PARAMS) {
#include "../kernels/primitive_scatter.cuh"
}
// native-program:entry termite_primitive_scatter_add_axis0_f32_i32
extern "C" __global__ void termite_primitive_scatter_add_axis0_f32_i32(ANTFLY_CUDA_PRIMITIVE_SCATTER_I32_PARAMS) {
    termite_primitive_scatter_add_axis0_integer_impl<int>(output, input, indices, rows, cols, output_rows);
}
// native-program:entry termite_primitive_scatter_add_axis0_f32_i64
extern "C" __global__ void termite_primitive_scatter_add_axis0_f32_i64(ANTFLY_CUDA_PRIMITIVE_SCATTER_I64_PARAMS) {
    termite_primitive_scatter_add_axis0_integer_impl<long long>(output, input, indices, rows, cols, output_rows);
}
// native-program:entry termite_primitive_transpose_f32
extern "C" __global__ void termite_primitive_transpose_f32(
    float* output,
    const float* input,
    unsigned int count,
    unsigned int rank,
    unsigned int d0, unsigned int d1, unsigned int d2, unsigned int d3,
    unsigned int d4, unsigned int d5, unsigned int d6, unsigned int d7,
    unsigned int p0, unsigned int p1, unsigned int p2, unsigned int p3,
    unsigned int p4, unsigned int p5, unsigned int p6, unsigned int p7
) {
    unsigned int out_idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (out_idx >= count) return;
    unsigned int dims[8] = { d0, d1, d2, d3, d4, d5, d6, d7 };
    unsigned int perm[8] = { p0, p1, p2, p3, p4, p5, p6, p7 };
    unsigned int input_strides[8];
    unsigned int stride = 1u;
    for (int d = (int)rank - 1; d >= 0; --d) {
        input_strides[d] = stride;
        stride *= dims[d];
    }
    unsigned int remaining = out_idx;
    unsigned int input_idx = 0u;
    for (int out_d = (int)rank - 1; out_d >= 0; --out_d) {
        unsigned int input_axis = perm[out_d];
        unsigned int coord = remaining % dims[input_axis];
        remaining /= dims[input_axis];
        input_idx += coord * input_strides[input_axis];
    }
    output[out_idx] = input[input_idx];
}
// native-program:entry termite_primitive_transpose_2d_f32
extern "C" __global__ void termite_primitive_transpose_2d_f32(
    float* output,
    const float* input,
    unsigned int rows,
    unsigned int cols,
    unsigned int tile_cols
) {
    __shared__ float tile[32][33];
    unsigned int tid = threadIdx.x;
    unsigned int tx = tid & 31u;
    unsigned int ty = tid >> 5u;
    unsigned int tile_x = blockIdx.x % tile_cols;
    unsigned int tile_y = blockIdx.x / tile_cols;
    unsigned int input_x = tile_x * 32u + tx;
    unsigned int input_y = tile_y * 32u + ty;

    #pragma unroll
    for (unsigned int offset = 0u; offset < 32u; offset += 8u) {
        if (input_x < cols && input_y + offset < rows) {
            tile[ty + offset][tx] = input[(input_y + offset) * cols + input_x];
        }
    }
    __syncthreads();

    unsigned int output_x = tile_y * 32u + tx;
    unsigned int output_y = tile_x * 32u + ty;
    #pragma unroll
    for (unsigned int offset = 0u; offset < 32u; offset += 8u) {
        if (output_x < rows && output_y + offset < cols) {
            output[(output_y + offset) * rows + output_x] = tile[tx][ty + offset];
        }
    }
}
// native-program:entry termite_primitive_concat_f32
extern "C" __global__ void termite_primitive_concat_f32(
    float* output,
    const float* a,
    const float* b,
    unsigned int output_count,
    unsigned int a_axis,
    unsigned int b_axis,
    unsigned int inner
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= output_count) return;
    unsigned int inner_coord = idx % inner;
    unsigned int outer_axis = idx / inner;
    unsigned int output_axis = a_axis + b_axis;
    unsigned int axis_coord = outer_axis % output_axis;
    unsigned int outer = outer_axis / output_axis;
    if (axis_coord < a_axis) {
        output[idx] = a[(outer * a_axis + axis_coord) * inner + inner_coord];
    } else {
        unsigned int b_coord = axis_coord - a_axis;
        output[idx] = b[(outer * b_axis + b_coord) * inner + inner_coord];
    }
}
// native-program:entry termite_primitive_slice_f32
extern "C" __global__ void termite_primitive_slice_f32(
    float* output,
    const float* input,
    unsigned int output_count,
    unsigned int rank,
    unsigned int i0, unsigned int i1, unsigned int i2, unsigned int i3,
    unsigned int i4, unsigned int i5, unsigned int i6, unsigned int i7,
    unsigned int o0, unsigned int o1, unsigned int o2, unsigned int o3,
    unsigned int o4, unsigned int o5, unsigned int o6, unsigned int o7,
    unsigned int s0, unsigned int s1, unsigned int s2, unsigned int s3,
    unsigned int s4, unsigned int s5, unsigned int s6, unsigned int s7,
    unsigned int t0, unsigned int t1, unsigned int t2, unsigned int t3,
    unsigned int t4, unsigned int t5, unsigned int t6, unsigned int t7
) {
    unsigned int out_idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (out_idx >= output_count) return;
    unsigned int input_dims[8] = { i0, i1, i2, i3, i4, i5, i6, i7 };
    unsigned int output_dims[8] = { o0, o1, o2, o3, o4, o5, o6, o7 };
    unsigned int starts[8] = { s0, s1, s2, s3, s4, s5, s6, s7 };
    unsigned int strides[8] = { t0, t1, t2, t3, t4, t5, t6, t7 };
    unsigned int coords[8] = { 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u };
    unsigned int remaining = out_idx;
    for (int d = (int)rank - 1; d >= 0; --d) {
        coords[d] = remaining % output_dims[d];
        remaining /= output_dims[d];
    }
    unsigned int input_idx = 0u;
    for (unsigned int d = 0; d < rank; ++d) {
        unsigned int input_coord = starts[d] + coords[d] * strides[d];
        input_idx = input_idx * input_dims[d] + input_coord;
    }
    output[out_idx] = input[input_idx];
}
// native-program:entry termite_deberta_pack_heads_f16
extern "C" __global__ void termite_deberta_pack_heads_f16(ANTFLY_CUDA_DEBERTA_PACK_HEADS_F16_PARAMS) {
    const unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    const unsigned int hidden = num_heads * head_dim;
    const unsigned int token_count = batch * seq_len * hidden;
    const unsigned int rel_len = seq_len * 2u - 1u;
    const unsigned int rel_count = batch * num_heads * rel_len * head_dim;
    if (idx < token_count) {
        const unsigned int d = idx % head_dim;
        const unsigned int tmp = idx / head_dim;
        const unsigned int h = tmp % num_heads;
        const unsigned int t = (tmp / num_heads) % seq_len;
        const unsigned int b = tmp / (num_heads * seq_len);
        const unsigned int packed = ((b * num_heads + h) * seq_len + t) * head_dim + d;
        q_out[packed] = __half_as_ushort(__float2half_rn(q[idx]));
        k_out[packed] = __half_as_ushort(__float2half_rn(k[idx]));
        // P*V uses V as the right/weight operand of the row-major GEMM, so
        // store it as [head_dim, seq_len] rather than the Q/K [seq_len,
        // head_dim] layout above. This transpose is folded into packing.
        const unsigned int v_packed = ((b * num_heads + h) * head_dim + d) * seq_len + t;
        v_out[v_packed] = __half_as_ushort(__float2half_rn(v[idx]));
    }
    if (idx < rel_count) {
        const unsigned int d = idx % head_dim;
        const unsigned int tmp = idx / head_dim;
        const unsigned int rel = tmp % rel_len;
        const unsigned int matrix = tmp / rel_len;
        const unsigned int h = matrix % num_heads;
        const unsigned int src = (rel * hidden) + h * head_dim + d;
        q_r_out[idx] = __half_as_ushort(__float2half_rn(q_r[src]));
        k_r_out[idx] = __half_as_ushort(__float2half_rn(k_r[src]));
    }
}
// native-program:entry termite_deberta_unpack_heads_f32
extern "C" __global__ void termite_deberta_unpack_heads_f32(
    float* dst,
    const float* packed,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim
) {
    const unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    const unsigned int hidden = num_heads * head_dim;
    const unsigned int count = batch * seq_len * hidden;
    if (idx >= count) return;
    const unsigned int d = idx % head_dim;
    const unsigned int tmp = idx / head_dim;
    const unsigned int h = tmp % num_heads;
    const unsigned int t = (tmp / num_heads) % seq_len;
    const unsigned int b = tmp / (num_heads * seq_len);
    const unsigned int src = ((b * num_heads + h) * seq_len + t) * head_dim + d;
    dst[idx] = packed[src];
}
// native-program:entry termite_split_last_dim3_f32
extern "C" __global__ void termite_split_last_dim3_f32(
    float* first,
    float* second,
    float* third,
    const float* input,
    unsigned int rows,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * dim;
    if (idx >= total) return;
    unsigned int row = idx / dim;
    unsigned int col = idx - row * dim;
    unsigned int src = row * dim * 3u + col;
{{interleaved_three_stores}}}
