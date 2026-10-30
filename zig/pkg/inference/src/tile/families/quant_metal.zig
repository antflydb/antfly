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

//! Generated kernel regions of the `quant_metal` families.

const Region = @import("../region.zig").Region;
const msl = @import("../quant_metal_msl.zig");
const wrap = @import("../quant_metal_wrap.zig");
const gemv = @import("../quant_metal_gemv.zig");
const q4_0 = @import("../quant_metal_q4_0.zig");
const dense = @import("../quant_metal_dense.zig");
const q4_k = @import("../quant_metal_q4_k.zig");
const q8_0 = @import("../quant_metal_q8_0.zig");
const mm_sg = @import("../quant_metal_mm_sg.zig");
const io = @import("../quant_metal_io.zig");

pub const regions = [_]Region{
    msl.metal("q4_0_shared", wrap.q4_0Shared()),
    msl.metal("q4_0_mmv_pair", wrap.q4_0PairActivation() ++ .{
        msl.comment(&.{ "A4B shared-FFN candidate: NQ8 gives the 88-block K axis exact", "eight-way striping instead of the NQ16 6/5-iteration tail." }),
        q4_0.nq8[0],
        msl.comment(&.{ "Dev candidate: Q4_0 blocks are transposed inside each output-row", "tile, [tile][block][row][18], so the four accumulator rows fetch", "one compact 72-byte span instead of four distant cache lines." }),
    } ++ q4_0.pair_activation_mmv16 ++ .{q4_0.rms_scale_pair} ++ q4_0.activation_rhs_mmv16),
    msl.metal("q4_0_mmv_linear", &[_]msl.Item{ q4_0.nq8[1], q4_0.linear_packed_t4_nsg4, q4_0.sumsq[0] } ++ q4_0.linear_mmv16_io[0..3].* ++ .{ q4_0.sumsq[1], q4_0.linear_mmv16_io[3], gemv.scalar("q4_1"), gemv.reduce("q4_1"), gemv.scalar("q5_0"), gemv.reduce("q5_0"), gemv.scalar("q5_1"), gemv.reduce("q5_1") }),
    msl.metal("q8_0_linear_r_ext", wrap.q8_0_linear_r_ext),
    msl.metal("q4_0_linear_r_ext", wrap.q4_0_linear_r_ext),
    msl.metal("q4_0_linear_specialized", &.{ wrap.q4_0Specialized(2, 4), wrap.q4_0Specialized(3, 2) }),
    msl.metal("q4_0_linear_r_ext4", wrap.q4_0_linear_r_ext4),
    msl.metal("q4_0_pair_activation_mm_m32_n64", &wrap.q4_0_pair_activation_mm_m32_n64),
    msl.metal("q4_0_pair_activation_mm_m32_n32", &wrap.q4_0_pair_activation_mm_m32_n32),
    msl.metal("q8_0_linear_mm_sg_v2", &wrap.q8_0_linear_mm_sg_v2),
    msl.metal("q8_0_linear_mm_sg_m64", &wrap.q8_0_linear_mm_sg_m64),
    msl.metal("q8_0_linear_mm_sg_m64_f16", &wrap.q8_0_linear_mm_sg_m64_f16),
    msl.metal("q8_0_kv_pair_mm_sg", &wrap.q8_0_kv_pair_mm_sg),
    msl.metal("q8_0_pair_activation_mm_sg_v2", &(wrap.q8_0_pair_activation_mm_sg_v2 ++ io.q8_0_linear_mm_kernels ++ io.q8_0_pair_activation_multiply_mm_kernels)),
    msl.metal("q8_0_pair_linear_r_ext", wrap.q8_0_pair_linear_r_ext),
    msl.metal("q8_0_pair_activation_multiply_r_ext", wrap.q8_0_pair_activation_multiply_r_ext),
    msl.metal("q8_0_pair_activation", &[_]msl.Item{q8_0.pair_reduce4} ++ q8_0.pair_ext1r),
    msl.metal("q8_0_activation_rhs_rows", wrap.q4_0_pair_activation_multiply_r_ext ++ .{ q8_0.rhs_ext1r, q8_0.rhs_reduce4 } ++ @import("../quant_metal_rows.zig").items() ++ .{ gemv.scalar("q8_k"), gemv.reduce("q8_k") }),
    msl.metal("q6_k_nbodd", wrap.q6_kNbOdd()),
    msl.metal("gemv_q1_0", &.{ gemv.scalar("q1_0"), gemv.scalar("i8_s"), gemv.scalar("q4_0") }),
    msl.metal("gemv_q8_0", &.{gemv.scalar("q8_0")}),
    msl.metal("gemv_q8_1", &[_]msl.Item{ gemv.scalar("q8_1"), gemv.reduce("q8_1") } ++ gemv.q8_0_mmv),
    msl.metal("gemv_q2_k", &.{ gemv.scalar("q2_k"), gemv.scalar("q3_k"), gemv.scalar("q4_k"), q4_k.linear }),
    msl.metal("q4_k_pair", &[_]msl.Item{q4_k.pair_linear} ++ q4_k.pair_activation ++ .{gemv.scalar("q5_k")}),
    msl.metal("gemv_q6_k", &.{gemv.scalar("q6_k")}),
    msl.metal("gemv_iq4_nl", &.{ gemv.scalar("iq4_nl"), gemv.reduce("iq4_nl"), gemv.scalar("iq4_xs"), gemv.reduce("iq4_xs"), gemv.scalar("mxfp4"), gemv.reduce("mxfp4") }),
    msl.metal("gemv_nvfp4", &.{gemv.scalar("nvfp4")}),
    msl.metal("gemv_iq2_xs", &.{gemv.scalar("iq2_xs")}),
    msl.metal("dense_f32_1x", &.{ dense.single(dense.f32_), dense.singleReduce(dense.f32_) }),
    msl.metal("dense_f16_1x", &.{ dense.single(dense.f16_), dense.singleReduce(dense.f16_) }),
    msl.metal("dense_bf16_1x", &.{ dense.single(dense.bf16_), dense.singleReduce(dense.bf16_) }),
    msl.metal("dense_f32_multi_row", &[_]msl.Item{ dense.multiRow(dense.f32_), dense.multiRowReduce(dense.f32_) } ++ io.multi_row_tiled_kernels),
    msl.metal("dense_f16_multi_row", &.{ io.multi_row_tiled32_m16_f32, dense.multiRow(dense.f16_), dense.multiRowReduce(dense.f16_) }),
    msl.metal("dense_bf16_multi_row", &.{ io.multi_row_tiled32_m16_bf16, dense.multiRow(dense.bf16_), dense.multiRowReduce(dense.bf16_) }),
    msl.metal("q4_0_mm_sg", &mm_sg.q4_0_kernels),
    msl.metal("q8_0_mm_sg", &.{mm_sg.q8_0_kernel}),
    msl.metal("florence_q4_k_mm_matrix", &.{io.florence_q4_k_mm_matrix_m64_n64}),
    msl.metal("florence_q4_k_mm_matrix_in_f16", &.{io.florence_q4_k_mm_matrix_m64_n64_in_f16}),
};
