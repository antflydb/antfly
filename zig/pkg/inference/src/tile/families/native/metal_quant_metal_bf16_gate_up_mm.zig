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

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

pub const source_0 =
    program.join(&.{
        \\           TERMITE_MSL_ABI_BF16_GATE_UP_MATRIX(termite_apply_bf16_gate_up_mm_m64_n32_vec4) "\n"
        \\
        ,
        program.metal(
            \\    threadgroup float4 storage[1024];
            \\    threadgroup float *shared=reinterpret_cast<threadgroup float *>(storage);
            \\    threadgroup float *a=shared, *g=shared+2048, *u=g+1024;
            \\    uint row0=tg.y*64u, col0=tg.x*32u;
            \\    uint sr=(uint(sg)/2u)*16u, sc=(uint(sg)%2u)*16u;
            \\    simdgroup_float8x8 ag[4],au[4];
            \\    TERMITE_FOR_UNROLL(uint i=0;i<4u;i++){ag[i]=make_filled_simdgroup_matrix<float,8>(0.0f);au[i]=make_filled_simdgroup_matrix<float,8>(0.0f);}
            \\    for(uint k0=0;k0<p.in_dim;k0+=32u){
            \\        // Dispatch admits aligned bases. All tails retain the measured
            \\        // scalar staging. Float4 shared storage guarantees alignment.
            \\        bool full=(p.in_dim&3u)==0u&&row0+64u<=p.rows&&col0+32u<=p.out_dim&&k0+32u<=p.in_dim;
            \\        if(full){
            \\            termite_bf16_matrix_stage_input_vec4(x, a, p, row0, k0, tid);
            \\            for(uint i=uint(tid);i<256u;i+=256u){
            \\                uint c=i/8u,k=(i%8u)*4u;
            \\                ushort4 gb=*reinterpret_cast<device const ushort4 *>(wg+(col0+c)*p.in_dim+k0+k);
            \\                ushort4 ub=*reinterpret_cast<device const ushort4 *>(wu+(col0+c)*p.in_dim+k0+k);
            \\                float4 gv=as_type<float4>(uint4(gb)<<16u),uv=as_type<float4>(uint4(ub)<<16u);
            \\                uint j=(k/8u*4u+c/8u)*64u+k%8u*8u+c%8u;
            \\                TERMITE_FOR_UNROLL(uint q=0u;q<4u;q++){g[j+q*8u]=gv[q];u[j+q*8u]=uv[q];}
            \\            }
            \\        }else{
            \\        termite_bf16_matrix_stage_input_scalar(x, a, p, row0, k0, tid);
            \\        for(uint i=uint(tid);i<1024u;i+=256u){
            \\            uint c=i/32u,k=i%32u,j=(k/8u*4u+c/8u)*64u+k%8u*8u+c%8u;
            \\            bool valid=col0+c<p.out_dim&&k0+k<p.in_dim;
            \\            g[j]=valid?termite_bf16_to_f32(wg[(col0+c)*p.in_dim+k0+k]):0.0f;
            \\            u[j]=valid?termite_bf16_to_f32(wu[(col0+c)*p.in_dim+k0+k]):0.0f;
            \\        }
            \\        }
            \\
        ),

        \\           TERMITE_MSL_BF16_GATE_UP_MMA
        \\
        ,
        program.metal(
            \\    }
            \\
        ),

        \\           TERMITE_MSL_BF16_GATE_UP_PUBLISH
        \\
        ,
        program.metal(
            \\}
            \\
        ),

        \\           TERMITE_MSL_ABI_BF16_GATE_UP_MATRIX(termite_apply_bf16_gate_up_mm_m64_n32) "\n"
        \\
        ,
        program.metal(
            \\    // Share exact F32 activations across two BF16 projections; accumulate in F32.
            \\    // Eight simdgroups cover a 64x32 tile. Reuse the 16 KiB staging area for
            \\    // both results before the precise SiLU/multiply epilogue. No global temporaries.
            \\    threadgroup float shared[4096];
            \\    threadgroup float *a=shared, *g=shared+2048, *u=g+1024;
            \\    uint row0=tg.y*64u, col0=tg.x*32u;
            \\    uint sr=(uint(sg)/2u)*16u, sc=(uint(sg)%2u)*16u;
            \\    simdgroup_float8x8 ag[4],au[4];
            \\    TERMITE_FOR_UNROLL(uint i=0;i<4u;i++){ag[i]=make_filled_simdgroup_matrix<float,8>(0.0f);au[i]=make_filled_simdgroup_matrix<float,8>(0.0f);}
            \\    for(uint k0=0;k0<p.in_dim;k0+=32u){
            \\        termite_bf16_matrix_stage_input_scalar(x, a, p, row0, k0, tid);
            \\        for(uint i=uint(tid);i<1024u;i+=256u){
            \\            uint c=i/32u,k=i%32u,j=(k/8u*4u+c/8u)*64u+k%8u*8u+c%8u;
            \\            bool valid=col0+c<p.out_dim&&k0+k<p.in_dim;
            \\            g[j]=valid?termite_bf16_to_f32(wg[(col0+c)*p.in_dim+k0+k]):0.0f;
            \\            u[j]=valid?termite_bf16_to_f32(wu[(col0+c)*p.in_dim+k0+k]):0.0f;
            \\        }
            \\
        ),

        \\           TERMITE_MSL_BF16_GATE_UP_MMA
        \\
        ,
        program.metal(
            \\    }
            \\
        ),

        \\           TERMITE_MSL_BF16_GATE_UP_PUBLISH
        \\
        ,
        program.metal(
            \\}
            \\
        ),
    });
