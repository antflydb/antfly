// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Intentionally included inside both native M64 entrypoints; no include guard.
// The caller supplies constexpr bool Compact. Keeping the shared declarations
// directly in each kernel avoids SM100+ register/address-code regressions from
// NVCC's inlined device-function storage lowering. Arithmetic, barriers and
// exact array extents have one owner; only relative scatter is a static policy.
    const unsigned int batch=p.dims[0], seq=p.dims[1], heads=p.dims[2], dim=p.dims[3], relative_rows=p.dims[4];
    const int* mask=reinterpret_cast<const int*>(mask_values);
    const int* buckets=reinterpret_cast<const int*>(bucket_values);
    constexpr unsigned int M = 64u, N = 64u, D = 64u, P = 72u, S = 68u, C = 20u, T = 256u;
    if (!seq || seq > 16384u || dim != D || blockDim.x != T) return;
    const unsigned int tid = threadIdx.x, lane = tid & 31u, warp = tid >> 5u;
    const unsigned int tiles = (seq + M - 1u) / M;
    const unsigned int start = (blockIdx.x % tiles) * M;
    const unsigned int matrix = blockIdx.x / tiles, head = matrix % heads, b = matrix / heads;
    if (b >= batch) return;
    const unsigned int hidden = heads * D, head_off = head * D;
    __shared__ __align__(16) half kv[N * P], qr[16u * P], kr[16u * P];
    __shared__ __align__(16) float shared_scores[M * P / 2u + 32u * S];
    __shared__ __align__(16) float relative_scores[2u * M * C];
    half* qt = reinterpret_cast<half*>(shared_scores);
    half* probabilities = reinterpret_cast<half*>(shared_scores);
    float* scratch = shared_scores + M * P / 2u;
    __shared__ float maximum[M], denominator[M], alpha[M];
    __shared__ unsigned int invalid[M];
    wmma::fragment<wmma::accumulator, 16, 16, 16, float> output[2];
    wmma::fragment<wmma::matrix_a,16,16,16,half,wmma::row_major> rows[4];
    #pragma unroll
    for (unsigned int g = 0u; g < 2u; ++g) wmma::fill_fragment(output[g], 0.0f);
    for (unsigned int i = tid; i < M * D; i += T) {
        const unsigned int row = i / D, d = i % D;
        qt[row * P + d] = __float2half_rn(start + row < seq ? q[(b * seq + start + row) * hidden + head_off + d] : 0.0f);
    }
    if (tid < M) { maximum[tid] = -INFINITY; denominator[tid] = 0.0f; invalid[tid] = 0u; }
    __syncthreads();
    if (warp < 4u) {
        #pragma unroll
        for (unsigned int d=0u;d<4u;++d) wmma::load_matrix_sync(rows[d], qt+warp*16u*P+d*16u, P);
    }
    __syncthreads();
    for (unsigned int key_start = 0u; key_start < seq; key_start += N) {
        for (unsigned int i = tid; i < N * D; i += T) {
            const unsigned int row = i / D, d = i % D, key = key_start + row;
            const bool valid = key < seq && (!mask || mask[b * seq + key] != 0ll);
            const unsigned int offset = (b * seq + key) * hidden + head_off + d;
            kv[row * P + d] = __float2half_rn(valid ? k[offset] : 0.0f);
        }
        __syncthreads();
        if (warp < 4u) {
            #pragma unroll
            for (unsigned int kc=0u;kc<4u;++kc) {
                ANTFLY_CUDA_WMMA_PRELOADED_DOT16_F32(dot, rows, kv+kc*16u*P, 4u, P,
                    shared_scores+warp*16u*S+kc*16u, S, keys);
            }
        } else {
            #pragma unroll
            for (unsigned int d=0u;d<4u;++d) wmma::load_matrix_sync(rows[d],kv+(warp-4u)*16u*P+d*16u,P);
        }
        __syncthreads();
        if constexpr (Compact) {
            __shared__ unsigned int warp_counts[8], run_start[128], run_count;
            __shared__ int run_bucket[128];
            // Both relative products are computed in compact 16-column tiles.
            // Scatter only their exact diagonal gather into QK before reusing the
            // tile. No token-by-relative or quadratic global buffer is needed.
            const int first_offset=(int)start+(int)seq-1-(int)key_start-(int)(N-1u);
            const int here=first_offset+(int)tid;
            const int bucket_value=here>=0 && here<(int)(2u*seq-1u)?buckets[here]:-1;
            const int previous=here>0 && here<=(int)(2u*seq-1u)?buckets[here-1]:-1;
            const bool begins=tid<M+N-1u && (tid==0 || bucket_value!=previous);
            unsigned int prefix=begins?1u:0u;
            #pragma unroll
            for (unsigned int d=1;d<32;d*=2) { unsigned int earlier=__shfl_up_sync(0xffffffffu,prefix,d); if(lane>=d)prefix+=earlier; }
            if (lane==31u)warp_counts[warp]=prefix;
            __syncthreads();
            for (unsigned int w=0;w<warp;++w)prefix+=warp_counts[w];
            if (begins) { run_start[prefix-1u]=tid;run_bucket[prefix-1u]=bucket_value; }
            if (tid==M+N-2u)run_count=prefix;
            __syncthreads();
            const bool uniform=run_count==1u;
            const unsigned int relative_count=run_count;
            for (unsigned int rs=0u;rs<relative_count;rs+=16u) {
                for (unsigned int i=tid;i<16u*D;i+=T) {
                    const unsigned int r=i/D,d=i%D;
                    const int bucket=rs+r<run_count?run_bucket[rs+r]:-1;
                    const bool valid=bucket>=0 && (unsigned int)bucket<relative_rows;
                    qr[r*P+d]=__float2half_rn(valid?q_r[(unsigned int)bucket*hidden+head_off+d]:0.0f);
                    kr[r*P+d]=__float2half_rn(valid?k_r[(unsigned int)bucket*hidden+head_off+d]:0.0f);
                }
                __syncthreads();
                ANTFLY_CUDA_WMMA_PRELOADED_DOT16_F32(relative, rows, (warp<4u?kr:qr), 4u, P,
                    relative_scores+warp*16u*C, C, rf);
                __syncthreads();
                if (uniform) {
                    for (unsigned int i=tid;i<M*N;i+=T) {
                        const unsigned int row=i/N,col=i%N;
                        shared_scores[row*S+col]=(shared_scores[row*S+col]+relative_scores[row*C])+relative_scores[M*C+col*C];
                    }
                } else for (unsigned int i=tid;i<M*16u;i+=T) {
                    const unsigned int row=i/16u,r=rs+i%16u;
                    if (r<run_count) {
                        const int lo=max(0,(int)row+(int)N-(int)(r+1u<run_count?run_start[r+1u]:M+N-1u));
                        const int hi=min((int)N,(int)row+(int)N-(int)run_start[r]);
                        for (int col=lo;col<hi;++col)
                            shared_scores[row*S+col]=(shared_scores[row*S+col]+relative_scores[row*C+i%16u])+relative_scores[M*C+(unsigned int)col*C+i%16u];
                    }
                }
                __syncthreads();
            }
        } else {
            // Both relative products are computed in compact 16-column tiles.
            // Scatter only their exact diagonal gather into QK before reusing the
            // tile. No token-by-relative or quadratic global buffer is needed.
            for (unsigned int rs=0u;rs<M+N-1u;rs+=16u) {
                for (unsigned int i=tid;i<16u*D;i+=T) {
                    const unsigned int r=i/D,d=i%D;
                    const int offset=(int)start+(int)seq-1-(int)key_start-(int)(N-1u)+(int)rs+(int)r;
                    const int bucket=offset>=0 && offset<(int)(2u*seq-1u)?buckets[offset]:-1;
                    const bool valid=bucket>=0 && (unsigned int)bucket<relative_rows;
                    qr[r*P+d]=__float2half_rn(valid?q_r[(unsigned int)bucket*hidden+head_off+d]:0.0f);
                    kr[r*P+d]=__float2half_rn(valid?k_r[(unsigned int)bucket*hidden+head_off+d]:0.0f);
                }
                __syncthreads();
                ANTFLY_CUDA_WMMA_PRELOADED_DOT16_F32(relative, rows, (warp<4u?kr:qr), 4u, P,
                    relative_scores+warp*16u*C, C, rf);
                __syncthreads();
                for (unsigned int i=tid;i<M*16u;i+=T) {
                    const unsigned int row=i/16u,r=rs+i%16u;
                    const int col=(int)row+(int)N-1-(int)r;
                    if (col>=0 && col<(int)N)
                        shared_scores[row*S+col]=(shared_scores[row*S+col]+relative_scores[row*C+i%16u])+relative_scores[M*C+(unsigned int)col*C+i%16u];
                }
                __syncthreads();
            }
        }
        float staged_probabilities[8][2];
        #pragma unroll
        for (unsigned int g = 0u; g < 8u; ++g) {
            const unsigned int row = warp * 8u + g;
            float values[2], next_max = -INFINITY;
            #pragma unroll
            for (unsigned int j = 0u; j < 2u; ++j) {
                const unsigned int n = lane + j * 32u, key = key_start + n;
                const bool valid = start + row < seq && key < seq && (!mask || mask[b * seq + key] != 0ll);
                float score=-INFINITY;
                if (valid) {
                    const int bucket=buckets[start+row+seq-1u-key];
                    if (bucket<0 || (unsigned int)bucket>=relative_rows) atomicOr(&invalid[row],1u);
                    else {
                        score=shared_scores[row*S+n]*rsqrtf(192.0f);
                        if (!isfinite(score)) atomicOr(&invalid[row],1u);
                    }
                }
                values[j] = score;
                next_max = fmaxf(next_max, values[j]);
            }
            ANTFLY_CUDA_ATTENTION_ONLINE_ROW64(expf, staged_probabilities[g][j] = p;)
        }
        ANTFLY_CUDA_ATTENTION_PUBLISH_HALF_ROWS64()
        for (unsigned int i=tid;i<N*D;i+=T) {
            const unsigned int row=i/D,d=i%D,key=key_start+row;
            const bool valid=key<seq && (!mask||mask[b*seq+key]!=0);
            kv[row*P+d]=__float2half_rn(valid?v[(b*seq+key)*hidden+head_off+d]:0.0f);
        }
        __syncthreads();
        ANTFLY_CUDA_ATTENTION_PV_ROWS64(kv)
    }
    #pragma unroll
    for (unsigned int g = 0u; g < 2u; ++g) {
        wmma::store_matrix_sync(scratch + (warp / 4u) * 16u * S + (warp % 4u) * 16u, output[g], S, wmma::mem_row_major);
        __syncthreads();
        for (unsigned int i = tid; i < 32u * D; i += T) {
            const unsigned int row = g * 32u + i / D, d = i % D;
            if (start + row < seq)
                dst[(b * seq + start + row) * hidden + head_off + d] = (mask && mask[b*seq+start+row]==0) ? 0.0f : (invalid[row]==0u && denominator[row] > 0.0f ? scratch[(i / D) * S + i % D] / denominator[row] : NAN);
        }
        __syncthreads();
    }
