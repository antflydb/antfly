// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Research-only dense causal GQA prefill attention for the Memorilla research
// mode (`memorilla_qualification.researchFastPrefill`), compiled with NVRTC on
// first use. F32 in, F32 out, head_dim 128, no mask/bias/sliding window.
//
// One block = 32 query rows of one (batch, head); 256 threads. Q (pre-scaled)
// stays in shared memory; 32-key tiles of K, then V, stream through one shared
// buffer; online softmax per row (running max, running sum, rescale), so each
// score is computed once. Layouts match termite_gqa_attention_f32:
// q/dst [batch, q_seq, heads, 128], k/v [batch, kv_seq, kv_heads, 128].

#define MEMORILLA_D 128
#define MEMORILLA_BQ 32
#define MEMORILLA_BK 32
#define MEMORILLA_THREADS 256
#define MEMORILLA_NEG_INF __int_as_float(0xff800000)

extern "C" __global__ void __launch_bounds__(MEMORILLA_THREADS) memorilla_gqa_prefill_tiled_f32(
    float* __restrict__ dst,
    const float* __restrict__ q,
    const float* __restrict__ k,
    const float* __restrict__ v,
    unsigned int batch,
    unsigned int q_seq_len,
    unsigned int kv_seq_len,
    unsigned int num_heads,
    unsigned int num_kv_heads,
    unsigned int query_position_offset,
    unsigned int kv_position_offset)
{
    __shared__ float qs[MEMORILLA_BQ][MEMORILLA_D];
    __shared__ float kvs[MEMORILLA_BK][MEMORILLA_D + 1];
    __shared__ float ss[MEMORILLA_BQ][MEMORILLA_BK + 1];
    __shared__ float row_m[MEMORILLA_BQ];
    __shared__ float row_l[MEMORILLA_BQ];
    __shared__ float row_scale[MEMORILLA_BQ];

    const unsigned int tid = threadIdx.x;
    const unsigned int q0 = blockIdx.x * MEMORILLA_BQ;
    const unsigned int head = blockIdx.y;
    const unsigned int b = blockIdx.z;
    if (b >= batch || head >= num_heads || q0 >= q_seq_len || num_kv_heads == 0u || (num_heads % num_kv_heads) != 0u) return;
    const unsigned int kv_head = head / (num_heads / num_kv_heads);
    const size_t q_hidden = (size_t)num_heads * MEMORILLA_D;
    const size_t kv_hidden = (size_t)num_kv_heads * MEMORILLA_D;
    const float scale = rsqrtf((float)MEMORILLA_D);

    for (unsigned int i = tid; i < MEMORILLA_BQ * MEMORILLA_D; i += MEMORILLA_THREADS) {
        const unsigned int r = i / MEMORILLA_D, c = i % MEMORILLA_D;
        const unsigned int qi = q0 + r;
        qs[r][c] = qi < q_seq_len ? q[((size_t)b * q_seq_len + qi) * q_hidden + (size_t)head * MEMORILLA_D + c] * scale : 0.0f;
    }
    if (tid < MEMORILLA_BQ) {
        row_m[tid] = MEMORILLA_NEG_INF;
        row_l[tid] = 0.0f;
        row_scale[tid] = 1.0f;
    }
    const unsigned int orow = tid >> 3;
    const unsigned int ocol = (tid & 7u) * 16u;
    float acc[16];
#pragma unroll
    for (int j = 0; j < 16; ++j) acc[j] = 0.0f;
    __syncthreads();

    // Keys visible to the tile's last query (absolute positions, causal).
    const unsigned int q_last = min(q0 + MEMORILLA_BQ, q_seq_len) - 1u;
    const long long last_visible = (long long)query_position_offset + q_last - (long long)kv_position_offset;
    if (last_visible < 0) return;
    const unsigned int kv_end = (unsigned int)min((long long)kv_seq_len, last_visible + 1);

    for (unsigned int k0 = 0; k0 < kv_end; k0 += MEMORILLA_BK) {
        for (unsigned int i = tid; i < MEMORILLA_BK * MEMORILLA_D; i += MEMORILLA_THREADS) {
            const unsigned int r = i / MEMORILLA_D, c = i % MEMORILLA_D;
            const unsigned int ki = k0 + r;
            kvs[r][c] = ki < kv_seq_len ? k[((size_t)b * kv_seq_len + ki) * kv_hidden + (size_t)kv_head * MEMORILLA_D + c] : 0.0f;
        }
        __syncthreads();
#pragma unroll
        for (int j = 0; j < 4; ++j) {
            const unsigned int e = tid + MEMORILLA_THREADS * j;
            const unsigned int r = e >> 5, kc = e & 31u;
            float dot = 0.0f;
#pragma unroll 8
            for (int c = 0; c < MEMORILLA_D; ++c) dot = fmaf(qs[r][c], kvs[kc][c], dot);
            const unsigned long long qpos = (unsigned long long)query_position_offset + q0 + r;
            const unsigned long long kpos = (unsigned long long)kv_position_offset + k0 + kc;
            const bool valid = (q0 + r) < q_seq_len && (k0 + kc) < kv_seq_len && kpos <= qpos;
            ss[r][kc] = valid ? dot : MEMORILLA_NEG_INF;
        }
        __syncthreads();
        {
            const unsigned int warp = tid >> 5, lane = tid & 31u;
#pragma unroll
            for (int rr = 0; rr < 4; ++rr) {
                const unsigned int r = warp * 4u + rr;
                const float s = ss[r][lane];
                float mx = s;
#pragma unroll
                for (int off = 16; off > 0; off >>= 1) mx = fmaxf(mx, __shfl_xor_sync(0xffffffffu, mx, off));
                const float m_old = row_m[r];
                const float m_new = fmaxf(m_old, mx);
                const float p = (s == MEMORILLA_NEG_INF) ? 0.0f : expf(s - m_new);
                float sum = p;
#pragma unroll
                for (int off = 16; off > 0; off >>= 1) sum += __shfl_xor_sync(0xffffffffu, sum, off);
                ss[r][lane] = p;
                __syncwarp();
                if (lane == 0) {
                    const float alpha = (m_old == MEMORILLA_NEG_INF) ? 0.0f : expf(m_old - m_new);
                    row_scale[r] = alpha;
                    row_l[r] = row_l[r] * alpha + sum;
                    row_m[r] = m_new;
                }
            }
        }
        __syncthreads();
        for (unsigned int i = tid; i < MEMORILLA_BK * MEMORILLA_D; i += MEMORILLA_THREADS) {
            const unsigned int r = i / MEMORILLA_D, c = i % MEMORILLA_D;
            const unsigned int ki = k0 + r;
            kvs[r][c] = ki < kv_seq_len ? v[((size_t)b * kv_seq_len + ki) * kv_hidden + (size_t)kv_head * MEMORILLA_D + c] : 0.0f;
        }
        __syncthreads();
        const float alpha = row_scale[orow];
#pragma unroll
        for (int j = 0; j < 16; ++j) acc[j] *= alpha;
        for (int kc = 0; kc < MEMORILLA_BK; ++kc) {
            const float p = ss[orow][kc];
#pragma unroll
            for (int j = 0; j < 16; ++j) acc[j] = fmaf(p, kvs[kc][ocol + j], acc[j]);
        }
        __syncthreads();
    }

    const unsigned int qi = q0 + orow;
    if (qi < q_seq_len) {
        const float l = row_l[orow];
        const float inv = l > 0.0f ? 1.0f / l : 0.0f;
        float* out = dst + ((size_t)b * q_seq_len + qi) * q_hidden + (size_t)head * MEMORILLA_D + ocol;
#pragma unroll
        for (int j = 0; j < 16; ++j) out[j] = acc[j] * inv;
    }
}

// ---------------------------------------------------------------------------
// Research-only BF16 tensor-core variant (same layouts, same causal contract
// over absolute positions): FlashAttention-2 style forward on
// mma.sync.m16n8k16 (BF16 operands, F32 accumulation). Q/K/V are read as F32
// and rounded to BF16 (round-to-nearest-even) while loading, P is rounded to
// BF16 for the PV product, softmax statistics and O stay F32, output F32.
// One block = 64 query rows of one (batch, head), 4 warps x 16 rows; 64-key
// tiles of K and V in shared memory (row stride 136 BF16: conflict-free
// fragment loads). Rows with no visible key are written as zeros.

#include <cuda_bf16.h>

#define MFA_D 128
#define MFA_BM 64
#define MFA_BN 64
#define MFA_THREADS 128
#define MFA_STRIDE (MFA_D + 8)

__device__ __forceinline__ unsigned int mfa_pack(float lo, float hi)
{
    __nv_bfloat162 v = __floats2bfloat162_rn(lo, hi);
    return *reinterpret_cast<unsigned int*>(&v);
}

__device__ __forceinline__ void mfa_mma(float (&d)[4], const unsigned int (&a)[4], unsigned int b0, unsigned int b1)
{
    asm volatile(
        "mma.sync.aligned.m16n8k16.row.col.f32.bf16.bf16.f32 {%0,%1,%2,%3}, {%4,%5,%6,%7}, {%8,%9}, {%0,%1,%2,%3};\n"
        : "+f"(d[0]), "+f"(d[1]), "+f"(d[2]), "+f"(d[3])
        : "r"(a[0]), "r"(a[1]), "r"(a[2]), "r"(a[3]), "r"(b0), "r"(b1));
}

extern "C" __global__ void __launch_bounds__(MFA_THREADS) memorilla_gqa_prefill_flash_bf16(
    float* __restrict__ dst,
    const float* __restrict__ q,
    const float* __restrict__ k,
    const float* __restrict__ v,
    unsigned int batch,
    unsigned int q_seq_len,
    unsigned int kv_seq_len,
    unsigned int num_heads,
    unsigned int num_kv_heads,
    unsigned int query_position_offset,
    unsigned int kv_position_offset)
{
    __shared__ __align__(16) __nv_bfloat16 ks[MFA_BN * MFA_STRIDE];
    __shared__ __align__(16) __nv_bfloat16 vs[MFA_BN * MFA_STRIDE];

    const unsigned int tid = threadIdx.x;
    const unsigned int warp = tid >> 5, lane = tid & 31u;
    const unsigned int g = lane >> 2, t = lane & 3u;
    const unsigned int q0 = blockIdx.x * MFA_BM;
    const unsigned int head = blockIdx.y;
    const unsigned int b = blockIdx.z;
    if (b >= batch || head >= num_heads || q0 >= q_seq_len || num_kv_heads == 0u || (num_heads % num_kv_heads) != 0u) return;
    const unsigned int kv_head = head / (num_heads / num_kv_heads);
    const size_t q_hidden = (size_t)num_heads * MFA_D;
    const size_t kv_hidden = (size_t)num_kv_heads * MFA_D;
    const float scale_log2 = rsqrtf((float)MFA_D) * 1.4426950408889634f;

    // This thread's two query rows (g and g + 8 of the warp's 16).
    const unsigned int r0 = q0 + warp * 16u + g, r1 = r0 + 8u;
    const long long qpos0 = (long long)query_position_offset + r0;
    const long long qpos1 = (long long)query_position_offset + r1;

    // Q A-fragments for the 8 k-steps of head_dim, straight from global.
    unsigned int qa[8][4];
    {
        const float* q_row0 = q + ((size_t)b * q_seq_len + r0) * q_hidden + (size_t)head * MFA_D;
        const float* q_row1 = q + ((size_t)b * q_seq_len + r1) * q_hidden + (size_t)head * MFA_D;
#pragma unroll
        for (int kk = 0; kk < 8; ++kk) {
            const unsigned int c = kk * 16u + t * 2u;
            float2 a0 = r0 < q_seq_len ? *reinterpret_cast<const float2*>(q_row0 + c) : make_float2(0.f, 0.f);
            float2 a1 = r1 < q_seq_len ? *reinterpret_cast<const float2*>(q_row1 + c) : make_float2(0.f, 0.f);
            float2 a2 = r0 < q_seq_len ? *reinterpret_cast<const float2*>(q_row0 + c + 8u) : make_float2(0.f, 0.f);
            float2 a3 = r1 < q_seq_len ? *reinterpret_cast<const float2*>(q_row1 + c + 8u) : make_float2(0.f, 0.f);
            qa[kk][0] = mfa_pack(a0.x, a0.y);
            qa[kk][1] = mfa_pack(a1.x, a1.y);
            qa[kk][2] = mfa_pack(a2.x, a2.y);
            qa[kk][3] = mfa_pack(a3.x, a3.y);
        }
    }

    float o[16][4];
#pragma unroll
    for (int n = 0; n < 16; ++n) o[n][0] = o[n][1] = o[n][2] = o[n][3] = 0.f;
    float m0 = MEMORILLA_NEG_INF, m1 = MEMORILLA_NEG_INF, l0 = 0.f, l1 = 0.f;

    const unsigned int q_last = min(q0 + MFA_BM, q_seq_len) - 1u;
    const long long last_visible = (long long)query_position_offset + q_last - (long long)kv_position_offset;
    const unsigned int kv_end = last_visible < 0 ? 0u : (unsigned int)min((long long)kv_seq_len, last_visible + 1);

    for (unsigned int k0 = 0; k0 < kv_end; k0 += MFA_BN) {
        // K and V tiles: F32 global -> BF16 shared, float4 per thread step.
        for (unsigned int i = tid; i < MFA_BN * (MFA_D / 4); i += MFA_THREADS) {
            const unsigned int r = i >> 5, c = (i & 31u) * 4u;
            const unsigned int ki = k0 + r;
            float4 kv4 = make_float4(0.f, 0.f, 0.f, 0.f), vv4 = make_float4(0.f, 0.f, 0.f, 0.f);
            if (ki < kv_seq_len) {
                const size_t off = ((size_t)b * kv_seq_len + ki) * kv_hidden + (size_t)kv_head * MFA_D + c;
                kv4 = *reinterpret_cast<const float4*>(k + off);
                vv4 = *reinterpret_cast<const float4*>(v + off);
            }
            *reinterpret_cast<uint2*>(&ks[r * MFA_STRIDE + c]) = make_uint2(mfa_pack(kv4.x, kv4.y), mfa_pack(kv4.z, kv4.w));
            *reinterpret_cast<uint2*>(&vs[r * MFA_STRIDE + c]) = make_uint2(mfa_pack(vv4.x, vv4.y), mfa_pack(vv4.z, vv4.w));
        }
        __syncthreads();

        // S = Q K^T for this warp's 16 rows x 64 keys (8 n-tiles of 8 keys).
        float s[8][4];
#pragma unroll
        for (int n = 0; n < 8; ++n) {
            s[n][0] = s[n][1] = s[n][2] = s[n][3] = 0.f;
            const __nv_bfloat16* krow = &ks[(n * 8u + g) * MFA_STRIDE + t * 2u];
#pragma unroll
            for (int kk = 0; kk < 8; ++kk) {
                const unsigned int b0 = *reinterpret_cast<const unsigned int*>(krow + kk * 16);
                const unsigned int b1 = *reinterpret_cast<const unsigned int*>(krow + kk * 16 + 8);
                mfa_mma(s[n], qa[kk], b0, b1);
            }
        }

        // Scale, causal/bounds mask, online softmax (rows g and g + 8).
        float mx0 = MEMORILLA_NEG_INF, mx1 = MEMORILLA_NEG_INF;
#pragma unroll
        for (int n = 0; n < 8; ++n) {
#pragma unroll
            for (int j = 0; j < 2; ++j) {
                const unsigned int ki = k0 + n * 8u + t * 2u + j;
                const long long kpos = (long long)kv_position_offset + ki;
                const bool in = ki < kv_seq_len;
                s[n][j] = (in && kpos <= qpos0) ? s[n][j] * scale_log2 : MEMORILLA_NEG_INF;
                s[n][2 + j] = (in && kpos <= qpos1) ? s[n][2 + j] * scale_log2 : MEMORILLA_NEG_INF;
                mx0 = fmaxf(mx0, s[n][j]);
                mx1 = fmaxf(mx1, s[n][2 + j]);
            }
        }
        mx0 = fmaxf(mx0, __shfl_xor_sync(0xffffffffu, mx0, 1));
        mx0 = fmaxf(mx0, __shfl_xor_sync(0xffffffffu, mx0, 2));
        mx1 = fmaxf(mx1, __shfl_xor_sync(0xffffffffu, mx1, 1));
        mx1 = fmaxf(mx1, __shfl_xor_sync(0xffffffffu, mx1, 2));
        const float mn0 = fmaxf(m0, mx0), mn1 = fmaxf(m1, mx1);
        const float alpha0 = (m0 == MEMORILLA_NEG_INF) ? 0.f : exp2f(m0 - mn0);
        const float alpha1 = (m1 == MEMORILLA_NEG_INF) ? 0.f : exp2f(m1 - mn1);
        float sum0 = 0.f, sum1 = 0.f;
#pragma unroll
        for (int n = 0; n < 8; ++n) {
#pragma unroll
            for (int j = 0; j < 2; ++j) {
                s[n][j] = (s[n][j] == MEMORILLA_NEG_INF) ? 0.f : exp2f(s[n][j] - mn0);
                s[n][2 + j] = (s[n][2 + j] == MEMORILLA_NEG_INF) ? 0.f : exp2f(s[n][2 + j] - mn1);
                sum0 += s[n][j];
                sum1 += s[n][2 + j];
            }
        }
        l0 = l0 * alpha0 + sum0;
        l1 = l1 * alpha1 + sum1;
        m0 = mn0;
        m1 = mn1;
#pragma unroll
        for (int n = 0; n < 16; ++n) {
            o[n][0] *= alpha0;
            o[n][1] *= alpha0;
            o[n][2] *= alpha1;
            o[n][3] *= alpha1;
        }

        // O += P V: P re-packed as A fragments (4 k-steps of 16 keys), V
        // B-fragments by ldmatrix.trans from row-major [key][dim].
#pragma unroll
        for (int j = 0; j < 4; ++j) {
            const unsigned int pa[4] = {
                mfa_pack(s[2 * j][0], s[2 * j][1]),
                mfa_pack(s[2 * j][2], s[2 * j][3]),
                mfa_pack(s[2 * j + 1][0], s[2 * j + 1][1]),
                mfa_pack(s[2 * j + 1][2], s[2 * j + 1][3]),
            };
            const unsigned int mat = lane >> 3, mrow = lane & 7u;
            const unsigned int key = j * 16u + (mat & 1u) * 8u + mrow;
#pragma unroll
            for (int p = 0; p < 8; ++p) {
                const unsigned int dim = p * 16u + (mat >> 1) * 8u;
                const unsigned int addr = (unsigned int)__cvta_generic_to_shared(&vs[key * MFA_STRIDE + dim]);
                unsigned int v0, v1, v2, v3;
                asm volatile("ldmatrix.sync.aligned.m8n8.x4.trans.shared.b16 {%0,%1,%2,%3}, [%4];\n"
                             : "=r"(v0), "=r"(v1), "=r"(v2), "=r"(v3)
                             : "r"(addr));
                mfa_mma(o[2 * p], pa, v0, v1);
                mfa_mma(o[2 * p + 1], pa, v2, v3);
            }
        }
        __syncthreads();
    }

    l0 += __shfl_xor_sync(0xffffffffu, l0, 1);
    l0 += __shfl_xor_sync(0xffffffffu, l0, 2);
    l1 += __shfl_xor_sync(0xffffffffu, l1, 1);
    l1 += __shfl_xor_sync(0xffffffffu, l1, 2);
    const float inv0 = l0 > 0.f ? 1.f / l0 : 0.f;
    const float inv1 = l1 > 0.f ? 1.f / l1 : 0.f;
    if (r0 < q_seq_len) {
        float* out = dst + ((size_t)b * q_seq_len + r0) * q_hidden + (size_t)head * MFA_D + t * 2u;
#pragma unroll
        for (int n = 0; n < 16; ++n) *reinterpret_cast<float2*>(out + n * 8) = make_float2(o[n][0] * inv0, o[n][1] * inv0);
    }
    if (r1 < q_seq_len) {
        float* out = dst + ((size_t)b * q_seq_len + r1) * q_hidden + (size_t)head * MFA_D + t * 2u;
#pragma unroll
        for (int n = 0; n < 16; ++n) *reinterpret_cast<float2*>(out + n * 8) = make_float2(o[n][2] * inv1, o[n][3] * inv1);
    }
}

// Rank-r adapter epilogue (research cubin): out = base + scale * down @ up^T,
// F32, base/out [rows, out_dim], down [rows, rank], up [out_dim, rank],
// rank <= 64. One block = 8 rows x 256 output columns; the 8 down rows sit in
// shared memory, each thread reads its up row once for all 8 rows.
extern "C" __global__ void __launch_bounds__(256) memorilla_lora_up_add_f32(
    float* __restrict__ out,
    const float* __restrict__ base,
    const float* __restrict__ down,
    const float* __restrict__ up,
    float scale,
    unsigned int rows,
    unsigned int out_dim,
    unsigned int rank)
{
    __shared__ float sd[8][64];
    if (rank == 0u || rank > 64u) return;
    const unsigned int r0 = blockIdx.x * 8u;
    const unsigned int j = blockIdx.y * 256u + threadIdx.x;
    for (unsigned int i = threadIdx.x; i < 8u * rank; i += 256u) {
        const unsigned int rr = i / rank, k = i % rank;
        sd[rr][k] = (r0 + rr < rows) ? down[(size_t)(r0 + rr) * rank + k] : 0.f;
    }
    __syncthreads();
    if (j >= out_dim) return;
    float acc[8];
#pragma unroll
    for (int rr = 0; rr < 8; ++rr) acc[rr] = 0.f;
    const float* uj = up + (size_t)j * rank;
    for (unsigned int k = 0; k < rank; ++k) {
        const float w = uj[k];
#pragma unroll
        for (int rr = 0; rr < 8; ++rr) acc[rr] = fmaf(sd[rr][k], w, acc[rr]);
    }
#pragma unroll
    for (int rr = 0; rr < 8; ++rr) {
        const unsigned int r = r0 + rr;
        if (r < rows) {
            const size_t o = (size_t)r * out_dim + j;
            out[o] = base[o] + scale * acc[rr];
        }
    }
}
