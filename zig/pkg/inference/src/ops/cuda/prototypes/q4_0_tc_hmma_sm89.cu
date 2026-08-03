// Copyright 2026 Antfly, Inc.
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

// Standalone-only Q4_0 W4A16 tensor-core (tc_hmma) correctness prototype.
// This translation unit is never included by production codegen or runtime
// dispatch. It carries a self-contained copy of the promoted-candidate
// termite_linear_q4_0_f32_tc_hmma kernel (MODE 0, no epilogue) plus a host
// main that:
//   1. generates random GGUF Q4_0 weight blocks (18 bytes: f16 LE scale +
//      16 quant bytes, low nibble = element i, high nibble = element i+16),
//   2. packs them into the q4_0_hmma device layout (scales region of
//      block_count * 2 bytes followed by quants region of block_count * 16
//      bytes, block_index = col * row_blocks + block),
//   3. runs the kernel on Gemma4-E2B-shaped problems and compares against a
//      double-precision CPU reference GEMM within an absolute tolerance that
//      accounts for the f16 activation/weight rounding of the tc path.
//
// Build (CPU-side compile only; executing the binary initializes CUDA and
// must happen on the GPU box):
//   /usr/local/cuda-13.2/bin/nvcc -arch=sm_89 -std=c++17 \
//     src/ops/cuda/prototypes/q4_0_tc_hmma_sm89.cu -o /tmp/q4_0_tc_hmma_proto
// Run (GPU box): /tmp/q4_0_tc_hmma_proto   (exits nonzero on mismatch)

#include <cuda_fp16.h>
#include <cuda_runtime.h>
#include <mma.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

using namespace nvcuda;

static constexpr unsigned int PROTO_QTC_M = 64u;
static constexpr unsigned int PROTO_QTC_N = 32u;
static constexpr unsigned int PROTO_QTC_K = 16u;
static constexpr unsigned int PROTO_QTC_THREADS = 256u;

__device__ __forceinline__ half proto_half_from_le(const unsigned char* src) {
    unsigned short bits = (unsigned short)src[0] | ((unsigned short)src[1] << 8);
    return __ushort_as_half(bits);
}

// Mirror of termite_q4_0_tc_value_at in
// src/ops/cuda/artifacts/inference_cuda_kernels.cu; layout contract lives
// there.
__device__ __forceinline__ half proto_q4_0_tc_value_at(
    const unsigned char* packed,
    unsigned int out_dim,
    unsigned int col,
    unsigned int row_blocks,
    unsigned int k_abs
) {
    unsigned int block = k_abs / 32u;
    unsigned int lane = k_abs & 31u;
    unsigned int block_index = col * row_blocks + block;
    unsigned int block_count = out_dim * row_blocks;
    half d = proto_half_from_le(packed + block_index * 2u);
    unsigned char packed_q = packed[block_count * 2u + block_index * 16u + (lane & 15u)];
    int q = lane < 16u ? (int)(packed_q & 0x0fu) : (int)(packed_q >> 4u);
    return __float2half_rn((float)(q - 8) * __half2float(d));
}

extern "C" __global__ void proto_linear_q4_0_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int tid = threadIdx.x;
    unsigned int warp = tid >> 5;
    if (warp >= 8u) return;
    unsigned int warp_m = warp & 3u;
    unsigned int warp_n = warp >> 2;
    unsigned int row_base = blockIdx.y * PROTO_QTC_M;
    unsigned int col_base = blockIdx.x * PROTO_QTC_N;
    unsigned int row_blocks = in_dim / 32u;

    __shared__ half a_tile[PROTO_QTC_M * PROTO_QTC_K];
    __shared__ half b_tile[PROTO_QTC_K * PROTO_QTC_N];
    __shared__ float c_tile[PROTO_QTC_M * PROTO_QTC_N];

    wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
    wmma::fill_fragment(acc, 0.0f);

    for (unsigned int k_base = 0; k_base < in_dim; k_base += PROTO_QTC_K) {
        for (unsigned int i = tid; i < PROTO_QTC_M * PROTO_QTC_K; i += PROTO_QTC_THREADS) {
            unsigned int local_row = i / PROTO_QTC_K;
            unsigned int local_k = i - local_row * PROTO_QTC_K;
            unsigned int row = row_base + local_row;
            unsigned int k_abs = k_base + local_k;
            float x = (row < rows && k_abs < in_dim) ? input[row * in_dim + k_abs] : 0.0f;
            a_tile[i] = __float2half_rn(x);
        }
        for (unsigned int i = tid; i < PROTO_QTC_K * PROTO_QTC_N; i += PROTO_QTC_THREADS) {
            unsigned int local_k = i / PROTO_QTC_N;
            unsigned int local_col = i - local_k * PROTO_QTC_N;
            unsigned int col = col_base + local_col;
            unsigned int k_abs = k_base + local_k;
            half w = __float2half_rn(0.0f);
            if (col < out_dim && k_abs < in_dim) {
                w = proto_q4_0_tc_value_at(packed_weight, out_dim, col, row_blocks, k_abs);
            }
            b_tile[i] = w;
        }
        __syncthreads();

        wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> a_frag;
        wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> b_frag;
        wmma::load_matrix_sync(a_frag, a_tile + warp_m * 16u * PROTO_QTC_K, PROTO_QTC_K);
        wmma::load_matrix_sync(b_frag, b_tile + warp_n * 16u, PROTO_QTC_N);
        wmma::mma_sync(acc, a_frag, b_frag, acc);
        __syncthreads();
    }

    wmma::store_matrix_sync(c_tile + warp_m * 16u * PROTO_QTC_N + warp_n * 16u, acc, PROTO_QTC_N, wmma::mem_row_major);
    __syncthreads();

    for (unsigned int i = tid; i < PROTO_QTC_M * PROTO_QTC_N; i += PROTO_QTC_THREADS) {
        unsigned int local_row = i / PROTO_QTC_N;
        unsigned int local_col = i - local_row * PROTO_QTC_N;
        unsigned int row = row_base + local_row;
        unsigned int col = col_base + local_col;
        if (row >= rows || col >= out_dim) continue;
        dst[row * out_dim + col] = c_tile[i];
    }
}

static unsigned short proto_f32_to_f16_bits(float value) {
    __half h = __float2half_rn(value);
    unsigned short bits;
    memcpy(&bits, &h, sizeof(bits));
    return bits;
}

static float proto_f16_bits_to_f32(unsigned short bits) {
    __half h;
    memcpy(&h, &bits, sizeof(bits));
    return __half2float(h);
}

static void proto_check(cudaError_t status, const char* what) {
    if (status != cudaSuccess) {
        fprintf(stderr, "CUDA error at %s: %s\n", what, cudaGetErrorString(status));
        exit(2);
    }
}

// Fills one raw GGUF Q4_0 block: f16 LE scale + 16 packed nibble bytes.
static void proto_fill_q4_0_block(unsigned char* block, unsigned int seed) {
    float d = 0.004f + 0.00003f * (float)(seed % 29u);
    unsigned short d_bits = proto_f32_to_f16_bits(d);
    block[0] = (unsigned char)(d_bits & 0xffu);
    block[1] = (unsigned char)(d_bits >> 8);
    for (unsigned int j = 0; j < 16u; ++j) {
        unsigned int low = (seed * 7u + j * 3u) % 16u;
        unsigned int high = (seed * 5u + j * 11u + 4u) % 16u;
        block[2u + j] = (unsigned char)(low | (high << 4u));
    }
}

static float proto_q4_0_raw_value(const unsigned char* block, unsigned int lane) {
    unsigned short d_bits = (unsigned short)block[0] | ((unsigned short)block[1] << 8);
    float d = proto_f16_bits_to_f32(d_bits);
    unsigned char packed = block[2u + (lane & 15u)];
    int q = lane < 16u ? (int)(packed & 0x0fu) : (int)(packed >> 4u);
    return (float)(q - 8) * d;
}

static int proto_run_shape(const char* label, unsigned int rows, unsigned int in_dim, unsigned int out_dim) {
    if (in_dim % 256u != 0u) {
        fprintf(stderr, "%s: in_dim %% 256 != 0\n", label);
        return 1;
    }
    const unsigned int row_blocks = in_dim / 32u;
    const size_t block_count = (size_t)out_dim * row_blocks;
    const size_t raw_bytes = block_count * 18u;
    const size_t packed_bytes = block_count * 18u;
    const size_t scales_bytes = block_count * 2u;
    const size_t input_count = (size_t)rows * in_dim;
    const size_t output_count = (size_t)rows * out_dim;

    unsigned char* raw = (unsigned char*)malloc(raw_bytes);
    unsigned char* packed = (unsigned char*)malloc(packed_bytes);
    float* input = (float*)malloc(input_count * sizeof(float));
    float* reference = (float*)malloc(output_count * sizeof(float));
    float* result = (float*)malloc(output_count * sizeof(float));
    if (raw == NULL || packed == NULL || input == NULL || reference == NULL || result == NULL) {
        fprintf(stderr, "%s: host allocation failed\n", label);
        return 1;
    }

    for (size_t block = 0; block < block_count; ++block) {
        proto_fill_q4_0_block(raw + block * 18u, (unsigned int)(block % 9973u));
    }
    // Host-side pack loop matching the q4_0_hmma layout contract.
    for (size_t block = 0; block < block_count; ++block) {
        memcpy(packed + block * 2u, raw + block * 18u, 2u);
        memcpy(packed + scales_bytes + block * 16u, raw + block * 18u + 2u, 16u);
    }
    for (size_t i = 0; i < input_count; ++i) {
        input[i] = ((float)((i % 97u)) - 48.0f) * 0.005f;
    }

    // Double-precision reference over the exact dequantized weights. The tc
    // kernel rounds activations and dequantized weights to f16 before the
    // f32-accumulating MMA, so allow an absolute budget scaled to that
    // rounding (empirically well below 0.05 at these magnitudes).
    for (unsigned int row = 0; row < rows; ++row) {
        for (unsigned int col = 0; col < out_dim; ++col) {
            double acc = 0.0;
            for (unsigned int block = 0; block < row_blocks; ++block) {
                const unsigned char* bp = raw + ((size_t)col * row_blocks + block) * 18u;
                for (unsigned int lane = 0; lane < 32u; ++lane) {
                    acc += (double)input[(size_t)row * in_dim + block * 32u + lane] * (double)proto_q4_0_raw_value(bp, lane);
                }
            }
            reference[(size_t)row * out_dim + col] = (float)acc;
        }
    }

    unsigned char* d_packed = NULL;
    float* d_input = NULL;
    float* d_output = NULL;
    proto_check(cudaMalloc(&d_packed, packed_bytes), "cudaMalloc packed");
    proto_check(cudaMalloc(&d_input, input_count * sizeof(float)), "cudaMalloc input");
    proto_check(cudaMalloc(&d_output, output_count * sizeof(float)), "cudaMalloc output");
    proto_check(cudaMemcpy(d_packed, packed, packed_bytes, cudaMemcpyHostToDevice), "upload packed");
    proto_check(cudaMemcpy(d_input, input, input_count * sizeof(float), cudaMemcpyHostToDevice), "upload input");
    proto_check(cudaMemset(d_output, 0xff, output_count * sizeof(float)), "poison output");

    dim3 grid((out_dim + PROTO_QTC_N - 1u) / PROTO_QTC_N, (rows + PROTO_QTC_M - 1u) / PROTO_QTC_M, 1u);
    proto_linear_q4_0_f32_tc_hmma<<<grid, PROTO_QTC_THREADS>>>(d_output, d_input, d_packed, rows, in_dim, out_dim);
    proto_check(cudaGetLastError(), "kernel launch");
    proto_check(cudaDeviceSynchronize(), "kernel sync");
    proto_check(cudaMemcpy(result, d_output, output_count * sizeof(float), cudaMemcpyDeviceToHost), "download output");

    const float tolerance_abs = 0.05f;
    float max_abs_diff = 0.0f;
    size_t worst_index = 0;
    for (size_t i = 0; i < output_count; ++i) {
        if (!isfinite(result[i])) {
            fprintf(stderr, "%s: non-finite output at %zu\n", label, i);
            return 1;
        }
        float diff = fabsf(result[i] - reference[i]);
        if (diff > max_abs_diff) {
            max_abs_diff = diff;
            worst_index = i;
        }
    }
    int failed = max_abs_diff > tolerance_abs;
    printf(
        "%s rows=%u in=%u out=%u max_abs_diff=%.6f tolerance=%.6f worst=[%zu] got=%.6f want=%.6f %s\n",
        label, rows, in_dim, out_dim, max_abs_diff, tolerance_abs,
        worst_index, result[worst_index], reference[worst_index],
        failed ? "FAIL" : "ok"
    );

    proto_check(cudaFree(d_packed), "free packed");
    proto_check(cudaFree(d_input), "free input");
    proto_check(cudaFree(d_output), "free output");
    free(raw);
    free(packed);
    free(input);
    free(reference);
    free(result);
    return failed;
}

int main(void) {
    int failures = 0;
    // Boundary coverage: exact-tile shape, ragged rows, and ragged columns.
    failures += proto_run_shape("E2B Q proj tile-exact", 64u, 1536u, 2048u);
    failures += proto_run_shape("E2B FFN down ragged-rows", 33u, 12288u, 256u);
    failures += proto_run_shape("E2B gate/up ragged-cols", 96u, 1536u, 280u);
    if (failures != 0) {
        fprintf(stderr, "q4_0 tc_hmma prototype FAILED (%d shape(s))\n", failures);
        return 1;
    }
    printf("q4_0 tc_hmma prototype passed\n");
    return 0;
}
