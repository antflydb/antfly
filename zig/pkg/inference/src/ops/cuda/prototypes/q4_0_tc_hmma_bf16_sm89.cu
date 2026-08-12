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
//
// Self-contained correctness prototype for the BF16 tensor-core Q4_0 linear
// kernel that mirrors termite_linear_q4_0_f32_tc_hmma_bf16 in
// src/ops/cuda/artifacts/inference_cuda_kernels.cu.
//
// It launches BOTH the bf16-tile and the f16-tile variant of the exact
// production tile (MODE 0, FMT Q4_0) over the same q4_0_hmma packed weights and
// compares each against a DOUBLE-precision reference matmul computed over the
// identical dequantized weights. It prints max_abs_diff AND max_rel_diff for
// BOTH tiles so the bf16-vs-f16 accuracy gap is visible per shape.
//
// The final "large-activation overflow" case injects activations above f16's
// 65504 range: the f16 tile overflows to +/-inf (huge error) while the bf16
// tile -- which shares f32's exponent range -- stays accurate. This is the
// numeric reason the bf16 mirror fixes the Gemma f16 divergence at token 29.
//
// Build (CPU-side only; DO NOT run here -- another process owns the GPU):
//   nvcc -arch=sm_89 -std=c++17 q4_0_tc_hmma_bf16_sm89.cu -o /tmp/.../proto
// Optional host parallelism for the f64 reference:
//   nvcc -arch=sm_89 -std=c++17 -Xcompiler -fopenmp ... -o proto
//
// Exit code: nonzero if the bf16 tile exceeds the (generous) tolerance on ANY
// shape. The f16 tile is reported for comparison only and is EXPECTED to fail
// the overflow case.

#include <cuda_bf16.h>
#include <cuda_fp16.h>
#include <cuda_runtime.h>
#include <mma.h>

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <vector>

// ---------------------------------------------------------------------------
// Host-side IEEE half <-> float (exact f16->f32; round-to-nearest-even f32->f16)
// so the host packer and the f64 reference read the same f16 block scale `d`
// the device kernels read via termite_half_from_le. Self-contained: no reliance
// on device fp16 intrinsics being callable on the host.
// ---------------------------------------------------------------------------
static float half_bits_to_float(uint16_t h) {
    uint32_t sign = (uint32_t)(h & 0x8000u) << 16;
    uint32_t exp = (h >> 10) & 0x1Fu;
    uint32_t mant = h & 0x3FFu;
    uint32_t f;
    if (exp == 0u) {
        if (mant == 0u) {
            f = sign;
        } else {
            int e = -1;
            do {
                mant <<= 1;
                e++;
            } while ((mant & 0x400u) == 0u);
            mant &= 0x3FFu;
            uint32_t fe = (uint32_t)(127 - 15 - e);
            f = sign | (fe << 23) | (mant << 13);
        }
    } else if (exp == 0x1Fu) {
        f = sign | 0x7F800000u | (mant << 13);
    } else {
        uint32_t fe = exp - 15u + 127u;
        f = sign | (fe << 23) | (mant << 13);
    }
    float out;
    std::memcpy(&out, &f, sizeof(out));
    return out;
}

static uint16_t float_to_half_bits(float value) {
    uint32_t x;
    std::memcpy(&x, &value, sizeof(x));
    uint32_t sign = (x >> 16) & 0x8000u;
    uint32_t biased = (x >> 23) & 0xFFu;
    uint32_t mant = x & 0x7FFFFFu;
    if (biased == 0xFFu) {  // inf / nan
        return (uint16_t)(sign | 0x7C00u | (mant ? 0x200u : 0u));
    }
    int32_t exp = (int32_t)biased - 127 + 15;
    if (exp >= 0x1F) {  // overflow -> inf
        return (uint16_t)(sign | 0x7C00u);
    }
    if (exp <= 0) {
        if (exp < -10) return (uint16_t)sign;  // underflow -> 0
        mant |= 0x800000u;
        uint32_t shift = (uint32_t)(14 - exp);
        uint32_t hm = mant >> shift;
        uint32_t rem = mant & ((1u << shift) - 1u);
        uint32_t halfway = 1u << (shift - 1);
        if (rem > halfway || (rem == halfway && (hm & 1u))) hm++;
        return (uint16_t)(sign | hm);
    }
    uint16_t h = (uint16_t)(sign | ((uint32_t)exp << 10) | (mant >> 13));
    uint32_t rem = mant & 0x1FFFu;
    if (rem > 0x1000u || (rem == 0x1000u && (h & 1u))) h++;
    return h;
}

// ---------------------------------------------------------------------------
// Device tile -- an exact excerpt of the production tile for MODE 0, FMT Q4_0.
// termite_qtc_tile_ops<WmmaElem>::from_float and termite_q4_0_tc_value_at_f32
// match the checked-in kernel; proto_q4_0_tc_hmma_f16 is numerically identical
// to termite_linear_q4_0_f32_tc_hmma and proto_q4_0_tc_hmma_bf16 to
// termite_linear_q4_0_f32_tc_hmma_bf16.
// ---------------------------------------------------------------------------
static constexpr unsigned int TERMITE_QTC_M = 64u;
static constexpr unsigned int TERMITE_QTC_N = 32u;
static constexpr unsigned int TERMITE_QTC_K = 16u;
static constexpr unsigned int TERMITE_QTC_THREADS = 256u;

__device__ __forceinline__ half termite_half_from_le(const unsigned char* src) {
    unsigned short bits = (unsigned short)src[0] | ((unsigned short)src[1] << 8);
    return __ushort_as_half(bits);
}

__device__ __forceinline__ float termite_q4_0_tc_value_at_f32(
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
    half d = termite_half_from_le(packed + block_index * 2u);
    unsigned char packed_q = packed[block_count * 2u + block_index * 16u + (lane & 15u)];
    int q = lane < 16u ? (int)(packed_q & 0x0fu) : (int)(packed_q >> 4u);
    return (float)(q - 8) * __half2float(d);
}

template <typename WmmaElem>
struct termite_qtc_tile_ops;

template <>
struct termite_qtc_tile_ops<half> {
    static __device__ __forceinline__ half from_float(float x) {
        return __float2half_rn(x);
    }
};

template <>
struct termite_qtc_tile_ops<__nv_bfloat16> {
    static __device__ __forceinline__ __nv_bfloat16 from_float(float x) {
        return __float2bfloat16_rn(x);
    }
};

template <typename WmmaElem>
__device__ void proto_q4_0_tile(
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
    unsigned int row_base = blockIdx.y * TERMITE_QTC_M;
    unsigned int col_base = blockIdx.x * TERMITE_QTC_N;
    unsigned int row_blocks = in_dim / 32u;

    __shared__ WmmaElem a_tile[TERMITE_QTC_M * TERMITE_QTC_K];
    __shared__ WmmaElem b_tile[TERMITE_QTC_K * TERMITE_QTC_N];
    __shared__ float c_tile[TERMITE_QTC_M * TERMITE_QTC_N];

    nvcuda::wmma::fragment<nvcuda::wmma::accumulator, 16, 16, 16, float> acc;
    nvcuda::wmma::fill_fragment(acc, 0.0f);

    for (unsigned int k_base = 0; k_base < in_dim; k_base += TERMITE_QTC_K) {
        for (unsigned int i = tid; i < TERMITE_QTC_M * TERMITE_QTC_K; i += TERMITE_QTC_THREADS) {
            unsigned int local_row = i / TERMITE_QTC_K;
            unsigned int local_k = i - local_row * TERMITE_QTC_K;
            unsigned int row = row_base + local_row;
            unsigned int k_abs = k_base + local_k;
            float x = (row < rows && k_abs < in_dim) ? input[row * in_dim + k_abs] : 0.0f;
            a_tile[i] = termite_qtc_tile_ops<WmmaElem>::from_float(x);
        }
        for (unsigned int i = tid; i < TERMITE_QTC_K * TERMITE_QTC_N; i += TERMITE_QTC_THREADS) {
            unsigned int local_k = i / TERMITE_QTC_N;
            unsigned int local_col = i - local_k * TERMITE_QTC_N;
            unsigned int col = col_base + local_col;
            unsigned int k_abs = k_base + local_k;
            WmmaElem w = termite_qtc_tile_ops<WmmaElem>::from_float(0.0f);
            if (col < out_dim && k_abs < in_dim) {
                w = termite_qtc_tile_ops<WmmaElem>::from_float(
                    termite_q4_0_tc_value_at_f32(packed_weight, out_dim, col, row_blocks, k_abs));
            }
            b_tile[i] = w;
        }
        __syncthreads();

        nvcuda::wmma::fragment<nvcuda::wmma::matrix_a, 16, 16, 16, WmmaElem, nvcuda::wmma::row_major> a_frag;
        nvcuda::wmma::fragment<nvcuda::wmma::matrix_b, 16, 16, 16, WmmaElem, nvcuda::wmma::row_major> b_frag;
        nvcuda::wmma::load_matrix_sync(a_frag, a_tile + warp_m * 16u * TERMITE_QTC_K, TERMITE_QTC_K);
        nvcuda::wmma::load_matrix_sync(b_frag, b_tile + warp_n * 16u, TERMITE_QTC_N);
        nvcuda::wmma::mma_sync(acc, a_frag, b_frag, acc);
        __syncthreads();
    }

    nvcuda::wmma::store_matrix_sync(c_tile + warp_m * 16u * TERMITE_QTC_N + warp_n * 16u, acc, TERMITE_QTC_N, nvcuda::wmma::mem_row_major);
    __syncthreads();

    for (unsigned int i = tid; i < TERMITE_QTC_M * TERMITE_QTC_N; i += TERMITE_QTC_THREADS) {
        unsigned int local_row = i / TERMITE_QTC_N;
        unsigned int local_col = i - local_row * TERMITE_QTC_N;
        unsigned int row = row_base + local_row;
        unsigned int col = col_base + local_col;
        if (row >= rows || col >= out_dim) continue;
        dst[row * out_dim + col] = c_tile[i];
    }
}

extern "C" __global__ void proto_q4_0_tc_hmma_f16(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    proto_q4_0_tile<half>(dst, input, packed_weight, rows, in_dim, out_dim);
}

extern "C" __global__ void proto_q4_0_tc_hmma_bf16(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    proto_q4_0_tile<__nv_bfloat16>(dst, input, packed_weight, rows, in_dim, out_dim);
}

// ---------------------------------------------------------------------------
// Host: Q4_0 quantize + q4_0_hmma pack, f64 reference, launch + compare.
// ---------------------------------------------------------------------------
#define CUDA_CHECK(expr)                                                          \
    do {                                                                          \
        cudaError_t err__ = (expr);                                              \
        if (err__ != cudaSuccess) {                                              \
            std::fprintf(stderr, "CUDA error %s at %s:%d: %s\n", #expr,          \
                         __FILE__, __LINE__, cudaGetErrorString(err__));         \
            std::exit(2);                                                        \
        }                                                                        \
    } while (0)

static constexpr int Q4_0_BLOCK = 32;

static size_t packed_size_bytes(int in_dim, int out_dim) {
    size_t row_blocks = (size_t)in_dim / Q4_0_BLOCK;
    size_t block_count = (size_t)out_dim * row_blocks;
    return block_count * 2u + block_count * 16u;  // scales region | quants region
}

// Quantize row-major weight[out_dim][in_dim] to the q4_0_hmma packed layout:
//   scales region: block_index*2   -> f16 LE block scale d
//   quants region: block_count*2 + block_index*16 -> 16 bytes (low/high nibbles)
//   block_index = col*row_blocks + block, value = (nibble - 8) * d
static void quantize_and_pack(const float* weight, int out_dim, int in_dim, uint8_t* packed) {
    int row_blocks = in_dim / Q4_0_BLOCK;
    size_t block_count = (size_t)out_dim * row_blocks;
    uint8_t* quants = packed + block_count * 2u;
    for (int col = 0; col < out_dim; ++col) {
        for (int block = 0; block < row_blocks; ++block) {
            const float* src = weight + (size_t)col * in_dim + (size_t)block * Q4_0_BLOCK;
            float amax = 0.0f;
            for (int t = 0; t < Q4_0_BLOCK; ++t) amax = std::fmax(amax, std::fabs(src[t]));
            float d = amax / 7.0f;
            uint16_t d_h = float_to_half_bits(d);
            float d_used = half_bits_to_float(d_h);
            float inv = d_used != 0.0f ? 1.0f / d_used : 0.0f;
            size_t block_index = (size_t)col * row_blocks + block;
            packed[block_index * 2u + 0u] = (uint8_t)(d_h & 0xFFu);
            packed[block_index * 2u + 1u] = (uint8_t)(d_h >> 8);
            uint8_t nib[Q4_0_BLOCK];
            for (int t = 0; t < Q4_0_BLOCK; ++t) {
                int q = (int)std::lround(src[t] * inv) + 8;
                if (q < 0) q = 0;
                if (q > 15) q = 15;
                nib[t] = (uint8_t)q;
            }
            uint8_t* qout = quants + block_index * 16u;
            for (int t = 0; t < 16; ++t) {
                qout[t] = (uint8_t)((nib[t] & 0x0Fu) | (nib[t + 16] << 4));
            }
        }
    }
}

// f64 dequant of a single weight element, reading the identical packed bytes and
// the identical f16 scale the kernels use.
static double packed_value_f64(const uint8_t* packed, int out_dim, int in_dim, int col, int k) {
    int row_blocks = in_dim / Q4_0_BLOCK;
    int block = k / Q4_0_BLOCK;
    int lane = k % Q4_0_BLOCK;
    size_t block_index = (size_t)col * row_blocks + block;
    size_t block_count = (size_t)out_dim * row_blocks;
    uint16_t d_h = (uint16_t)packed[block_index * 2u] | ((uint16_t)packed[block_index * 2u + 1u] << 8);
    double d = (double)half_bits_to_float(d_h);
    uint8_t byte = packed[block_count * 2u + block_index * 16u + (lane & 15)];
    int q = lane < 16 ? (byte & 0x0F) : (byte >> 4);
    return (double)(q - 8) * d;
}

static void reference_matmul(
    const uint8_t* packed,
    const std::vector<float>& input,
    int rows,
    int in_dim,
    int out_dim,
    std::vector<double>& out
) {
#pragma omp parallel for schedule(static)
    for (int col = 0; col < out_dim; ++col) {
        std::vector<double> wcol((size_t)in_dim);
        for (int k = 0; k < in_dim; ++k) wcol[k] = packed_value_f64(packed, out_dim, in_dim, col, k);
        for (int row = 0; row < rows; ++row) {
            const float* a = &input[(size_t)row * in_dim];
            double acc = 0.0;
            for (int k = 0; k < in_dim; ++k) acc += (double)a[k] * wcol[k];
            out[(size_t)row * out_dim + col] = acc;
        }
    }
}

struct DiffResult {
    double max_abs;
    double max_rel;
    bool nonfinite;
};

static DiffResult diff_vs_ref(const std::vector<float>& got, const std::vector<double>& ref) {
    DiffResult r{0.0, 0.0, false};
    for (size_t i = 0; i < ref.size(); ++i) {
        double g = (double)got[i];
        if (!std::isfinite(g)) {
            r.nonfinite = true;
            r.max_abs = std::numeric_limits<double>::infinity();
            r.max_rel = std::numeric_limits<double>::infinity();
            continue;
        }
        double ad = std::fabs(g - ref[i]);
        double rd = ad / std::fmax(std::fabs(ref[i]), 1e-6);
        if (ad > r.max_abs) r.max_abs = ad;
        if (rd > r.max_rel) r.max_rel = rd;
    }
    return r;
}

static void launch_tile(
    const void* kernel,
    float* d_dst,
    const float* d_input,
    const unsigned char* d_packed,
    int rows,
    int in_dim,
    int out_dim
) {
    unsigned int r = (unsigned int)rows;
    unsigned int i = (unsigned int)in_dim;
    unsigned int o = (unsigned int)out_dim;
    void* args[] = {&d_dst, &d_input, &d_packed, &r, &i, &o};
    dim3 grid((out_dim + (int)TERMITE_QTC_N - 1) / (int)TERMITE_QTC_N,
              (rows + (int)TERMITE_QTC_M - 1) / (int)TERMITE_QTC_M,
              1);
    CUDA_CHECK(cudaLaunchKernel(kernel, grid, dim3(TERMITE_QTC_THREADS, 1, 1), args, 0, nullptr));
    CUDA_CHECK(cudaGetLastError());
    CUDA_CHECK(cudaDeviceSynchronize());
}

// Generous absolute + relative budget: bf16 keeps 7 mantissa bits, so its
// per-element rounding is ~2^-8 and accumulation stays well inside this.
static constexpr double kAbsTol = 0.1;
static constexpr double kRelTol = 0.05;

static int run_shape(const char* label, int rows, int in_dim, int out_dim, bool inject_overflow) {
    std::vector<float> input((size_t)rows * in_dim);
    for (size_t i = 0; i < input.size(); ++i) {
        int lane = (int)(i % 97);
        input[i] = (float)(lane - 48) * 0.005f;  // ~[-0.24, 0.245]
    }
    if (inject_overflow) {
        // Values above f16's 65504 max: f16 rounds these to +/-inf, bf16 keeps them.
        for (int row = 0; row < rows; ++row) {
            input[(size_t)row * in_dim + ((size_t)row % in_dim)] = 1.0e5f;
            input[(size_t)row * in_dim + ((size_t)(row * 7 + 3) % in_dim)] = -7.0e4f;
        }
    }

    std::vector<float> weight((size_t)out_dim * in_dim);
    for (size_t i = 0; i < weight.size(); ++i) {
        int lane = (int)(i % 29);
        weight[i] = (float)(lane - 14) * 0.01f;  // ~[-0.14, 0.15]
    }
    std::vector<uint8_t> packed(packed_size_bytes(in_dim, out_dim));
    quantize_and_pack(weight.data(), out_dim, in_dim, packed.data());

    std::vector<double> ref((size_t)rows * out_dim);
    reference_matmul(packed.data(), input, rows, in_dim, out_dim, ref);

    float* d_input = nullptr;
    unsigned char* d_packed = nullptr;
    float* d_out = nullptr;
    CUDA_CHECK(cudaMalloc(&d_input, input.size() * sizeof(float)));
    CUDA_CHECK(cudaMalloc(&d_packed, packed.size()));
    CUDA_CHECK(cudaMalloc(&d_out, (size_t)rows * out_dim * sizeof(float)));
    CUDA_CHECK(cudaMemcpy(d_input, input.data(), input.size() * sizeof(float), cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(d_packed, packed.data(), packed.size(), cudaMemcpyHostToDevice));

    std::vector<float> got_f16((size_t)rows * out_dim);
    std::vector<float> got_bf16((size_t)rows * out_dim);

    CUDA_CHECK(cudaMemset(d_out, 0x7f, (size_t)rows * out_dim * sizeof(float)));  // large sentinel: a no-op kernel yields a huge diff
    launch_tile((const void*)proto_q4_0_tc_hmma_f16, d_out, d_input, d_packed, rows, in_dim, out_dim);
    CUDA_CHECK(cudaMemcpy(got_f16.data(), d_out, got_f16.size() * sizeof(float), cudaMemcpyDeviceToHost));

    CUDA_CHECK(cudaMemset(d_out, 0x7f, (size_t)rows * out_dim * sizeof(float)));
    launch_tile((const void*)proto_q4_0_tc_hmma_bf16, d_out, d_input, d_packed, rows, in_dim, out_dim);
    CUDA_CHECK(cudaMemcpy(got_bf16.data(), d_out, got_bf16.size() * sizeof(float), cudaMemcpyDeviceToHost));

    CUDA_CHECK(cudaFree(d_input));
    CUDA_CHECK(cudaFree(d_packed));
    CUDA_CHECK(cudaFree(d_out));

    DiffResult df = diff_vs_ref(got_f16, ref);
    DiffResult db = diff_vs_ref(got_bf16, ref);
    double max_ref = 0.0;
    for (double v : ref) max_ref = std::fmax(max_ref, std::fabs(v));
    double tol = kAbsTol + kRelTol * max_ref;
    bool bf_pass = db.max_abs <= tol;

    std::printf(
        "shape=%-18s rows=%4d in=%6d out=%6d | bf16 abs=%.6g rel=%.6g | f16 abs=%.6g rel=%.6g | max|ref|=%.6g tol=%.4g -> bf16 %s%s\n",
        label, rows, in_dim, out_dim,
        db.max_abs, db.max_rel, df.max_abs, df.max_rel,
        max_ref, tol,
        bf_pass ? "PASS" : "FAIL",
        (inject_overflow && df.nonfinite) ? "  [f16 OVERFLOWED to inf/nan -- bf16 stayed finite]" : "");

    return bf_pass ? 0 : 1;
}

int main() {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        std::fprintf(stderr, "no CUDA device available\n");
        return 2;
    }
    cudaDeviceProp prop{};
    CUDA_CHECK(cudaGetDeviceProperties(&prop, 0));
    std::printf("device=%s cc=%d.%d\n", prop.name, prop.major, prop.minor);
    std::printf("comparing bf16-tile and f16-tile Q4_0 tensor-core linear vs f64 reference\n\n");

    int failures = 0;
    // E2B QAT prefill projection shapes at rows in {64, 512}.
    const int rows_list[] = {64, 512};
    struct {
        const char* label;
        int in_dim;
        int out_dim;
    } shapes[] = {
        {"E2B Q proj", 1536, 2048},
        {"E2B FFN gate/up", 1536, 6144},
        {"E2B FFN down", 12288, 1536},
    };
    for (int r : rows_list) {
        for (auto& s : shapes) {
            failures += run_shape(s.label, r, s.in_dim, s.out_dim, false);
        }
    }
    // Ragged case: rows and out_dim are not tile-aligned (M tile=64, N tile=32).
    failures += run_shape("ragged", 100, 1536, 2050, false);

    // Money result: activations above f16 range. f16 overflows, bf16 is accurate.
    std::printf("\n-- large-activation overflow case (activations > f16 max 65504) --\n");
    failures += run_shape("overflow(1e5)", 64, 1536, 2048, true);

    std::printf("\n%s (%d shape(s) failed the bf16 tolerance)\n",
                failures == 0 ? "ALL BF16 SHAPES PASS" : "BF16 FAILURE", failures);
    return failures == 0 ? 0 : 1;
}
