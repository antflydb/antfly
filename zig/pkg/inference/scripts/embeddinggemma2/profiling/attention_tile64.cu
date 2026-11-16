// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Benchmark candidate, not a production dispatch. Reuses the checked-in kernel
// as the numerical and timing control. Requires SM89 and sequence % 64 == 0.
// The control header expects the CUDA BF16 and WMMA declarations first.
// clang-format off
#include <cuda_bf16.h>
#include <mma.h>
#include <math.h>
#include "../../../src/ops/cuda/kernels/embedding_gemma2.cuh"
// clang-format on
template <unsigned D, unsigned QT>
__device__ __forceinline__ void
attention64_impl(__nv_bfloat16 *out, const __nv_bfloat16 *q,
                 const __nv_bfloat16 *k, const __nv_bfloat16 *v,
                 const long long *mask, unsigned batch, unsigned seq,
                 unsigned qh, unsigned kh, unsigned radius) {
  using namespace nvcuda;
  constexpr unsigned KT = 64, KS = KT + 4, PS = KT + 8, DS = D + 8, WARPS = 16,
                     THREADS = 512, F = D / 16 / WARPS, QM = QT / 16,
                     KEYT = KT / 16;
  constexpr unsigned POOL =
      (KT * DS * 2 > QT * KS * 4 ? KT * DS * 2 : QT * KS * 4);
  unsigned tid = threadIdx.x, warp = tid / 32, lane = tid % 32,
           head = blockIdx.x, qt = blockIdx.y * QT, b = blockIdx.z,
           kvhead = head * kh / qh;
  if (blockDim.x != THREADS || b >= batch || head >= qh || qt + QT > seq)
    return;
  extern __shared__ unsigned char memory64[];
  auto kvs = reinterpret_cast<__nv_bfloat16 *>(memory64);
  auto scores = reinterpret_cast<float *>(memory64);
  auto probs = reinterpret_cast<__nv_bfloat16 *>(memory64 + POOL);
  auto maxima = reinterpret_cast<float *>(probs + QT * PS);
  auto sums = maxima + QT;
  auto alpha = sums + QT;
  auto tilemax = alpha + QT;
  auto tilesum = tilemax + QT * 2;
  if (tid < QT) {
    maxima[tid] = -INFINITY;
    sums[tid] = 0;
  }
  __syncthreads();
  float acc[QM][F][2][4] = {};
  unsigned first = radius ? (qt > radius ? qt - radius : 0) : 0,
           last = radius ? min(seq, qt + QT + radius) : seq;
  first = first / KT * KT;
  last = (last + KT - 1) / KT * KT;
  for (unsigned kt = first; kt < last; kt += KT) {
    for (unsigned i = tid; i < KT * D; i += THREADS) {
      unsigned key = kt + i / D, d = i % D;
      bool valid = key < seq && (!mask || mask[(size_t)b * seq + key]);
      kvs[(i / D) * DS + d] =
          valid ? k[(((size_t)b * seq + key) * kh + kvhead) * D + d]
                : __float2bfloat16_rn(0);
    }
    __syncthreads();
    wmma::fragment<wmma::accumulator, 16, 16, 16, float> sf;
    wmma::fill_fragment(sf, 0.f);
    unsigned qs = warp / KEYT * 16, ks = warp % KEYT * 16;
    if (warp < QM * KEYT) {
#pragma unroll
      for (unsigned d = 0; d < D; d += 16) {
        wmma::fragment<wmma::matrix_a, 16, 16, 16, __nv_bfloat16,
                       wmma::row_major>
            af;
        wmma::fragment<wmma::matrix_b, 16, 16, 16, __nv_bfloat16,
                       wmma::col_major>
            bf;
        wmma::load_matrix_sync(
            af, q + (((size_t)b * seq + qt + qs) * qh + head) * D + d, qh * D);
        wmma::load_matrix_sync(bf, kvs + ks * DS + d, DS);
        wmma::mma_sync(sf, af, bf, sf);
      }
    }
    __syncthreads();
    if (warp < QM * KEYT)
      wmma::store_matrix_sync(scores + qs * KS + ks, sf, KS,
                              wmma::mem_row_major);
    __syncthreads();
    float ss[QT * KT / THREADS];
#pragma unroll
    for (unsigned j = 0; j < QT * KT / THREADS; ++j) {
      unsigned i = tid + j * THREADS, row = i / KT, col = i % KT,
               query = qt + row, key = kt + col;
      float x = scores[row * KS + col];
      bool valid =
          key < seq &&
          (!mask ||
           (mask[(size_t)b * seq + query] && mask[(size_t)b * seq + key])) &&
          (!radius || (key <= query + radius && query <= key + radius));
      x = valid ? x : -INFINITY;
      ss[j] = x;
      float mx = x;
#pragma unroll
      for (unsigned n = 16; n; n /= 2)
        mx = fmaxf(mx, __shfl_xor_sync(0xffffffffu, mx, n));
      if (lane == 0)
        tilemax[row * 2 + col / 32] = mx;
    }
    __syncthreads();
    if (tid < QT) {
      float old = maxima[tid],
            next = fmaxf(old, fmaxf(tilemax[tid * 2], tilemax[tid * 2 + 1]));
      alpha[tid] = isfinite(old) ? expf(old - next) : 0;
      maxima[tid] = next;
      sums[tid] *= alpha[tid];
    }
    __syncthreads();
#pragma unroll
    for (unsigned j = 0; j < QT * KT / THREADS; ++j) {
      unsigned i = tid + j * THREADS, row = i / KT, col = i % KT;
      float p = isfinite(ss[j]) ? expf(ss[j] - maxima[row]) : 0;
      probs[row * PS + col] = __float2bfloat16_rn(p);
      float sum = p;
#pragma unroll
      for (unsigned n = 16; n; n /= 2)
        sum += __shfl_xor_sync(0xffffffffu, sum, n);
      if (lane == 0)
        tilesum[row * 2 + col / 32] = sum;
    }
    __syncthreads();
    if (tid < QT)
      sums[tid] += tilesum[tid * 2] + tilesum[tid * 2 + 1];
    for (unsigned i = tid; i < KT * D; i += THREADS) {
      unsigned key = kt + i / D, d = i % D;
      bool valid = key < seq && (!mask || mask[(size_t)b * seq + key]);
      kvs[(i / D) * DS + d] =
          valid ? v[(((size_t)b * seq + key) * kh + kvhead) * D + d]
                : __float2bfloat16_rn(0);
    }
    __syncthreads();
#pragma unroll
    for (unsigned qs = 0; qs < QM; ++qs) {
#pragma unroll
      for (unsigned f = 0; f < F; ++f) {
#pragma unroll
        for (unsigned h = 0; h < 2; ++h) {
#pragma unroll
          for (unsigned i = 0; i < 4; ++i) {
            unsigned row = qs * 16 + lane / 4 + (i >= 2 ? 8 : 0);
            acc[qs][f][h][i] *= alpha[row];
          }
        }
#pragma unroll
        for (unsigned ks = 0; ks < KEYT; ++ks) {
          unsigned pf[4], vf[4], ot = warp + f * WARPS;
          unsigned pp = __cvta_generic_to_shared(probs + qs * 16 * PS +
                                                 lane % 16 * PS +
                                                 lane / 16 * 8 + ks * 16),
                   vp = __cvta_generic_to_shared(kvs +
                                                 (ks * 16 + lane % 16) * DS +
                                                 ot * 16 + lane / 16 * 8);
          asm volatile(
              "ldmatrix.sync.aligned.m8n8.x4.shared.b16 {%0,%1,%2,%3}, [%4];"
              : "=r"(pf[0]), "=r"(pf[1]), "=r"(pf[2]), "=r"(pf[3])
              : "r"(pp));
          asm volatile("ldmatrix.sync.aligned.m8n8.x4.trans.shared.b16 "
                       "{%0,%1,%2,%3}, [%4];"
                       : "=r"(vf[0]), "=r"(vf[1]), "=r"(vf[2]), "=r"(vf[3])
                       : "r"(vp));
#pragma unroll
          for (unsigned h = 0; h < 2; ++h) {
            auto t = acc[qs][f][h];
            asm volatile("mma.sync.aligned.m16n8k16.row.col.f32.bf16.bf16.f32 "
                         "{%0,%1,%2,%3}, {%4,%5,%6,%7}, {%8,%9}, {%0,%1,%2,%3};"
                         : "+f"(t[0]), "+f"(t[1]), "+f"(t[2]), "+f"(t[3])
                         : "r"(pf[0]), "r"(pf[1]), "r"(pf[2]), "r"(pf[3]),
                           "r"(vf[h * 2]), "r"(vf[h * 2 + 1]));
          }
        }
      }
    }
    __syncthreads();
  }
#pragma unroll
  for (unsigned qs = 0; qs < QM; ++qs) {
#pragma unroll
    for (unsigned f = 0; f < F; ++f) {
#pragma unroll
      for (unsigned h = 0; h < 2; ++h) {
#pragma unroll
        for (unsigned i = 0; i < 4; ++i) {
          unsigned row = qs * 16 + lane / 4 + (i >= 2 ? 8 : 0),
                   col = h * 8 + (lane % 4) * 2 + (i & 1),
                   ot = warp + f * WARPS;
          out[(((size_t)b * seq + qt + row) * qh + head) * D + ot * 16 + col] =
              __float2bfloat16_rn(sums[row] > 0 ? acc[qs][f][h][i] / sums[row]
                                                : 0);
        }
      }
    }
  }
}
#define W(D, T, M)                                                             \
  extern "C" __global__ __launch_bounds__(512, M) void attention64_##D(        \
      __nv_bfloat16 *o, const __nv_bfloat16 *q, const __nv_bfloat16 *k,        \
      const __nv_bfloat16 *v, const long long *mask, unsigned b, unsigned s,   \
      unsigned qh, unsigned kh, unsigned dim, unsigned r) {                    \
    attention64_impl<D, T>(o, q, k, v, mask, b, s, qh, kh, r);                 \
  }
W(256, 32, 2)
W(512, 64, 1)
