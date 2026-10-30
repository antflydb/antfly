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

pub const regions = [_]Region{
    msl.raw("rows", @import("../quant_metal_rows.zig").region()),
    msl.metal("q4_0_shared", wrap.q4_0Shared()),
    msl.metal("q4_0_pair_activation_nr_nsg", wrap.q4_0PairActivation()),
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
    msl.metal("q8_0_pair_activation_mm_sg_v2", &wrap.q8_0_pair_activation_mm_sg_v2),
    msl.metal("q8_0_pair_linear_r_ext", wrap.q8_0_pair_linear_r_ext),
    msl.metal("q8_0_pair_activation_multiply_r_ext", wrap.q8_0_pair_activation_multiply_r_ext),
    msl.metal("q4_0_pair_activation_multiply_r_ext", wrap.q4_0_pair_activation_multiply_r_ext),
    msl.metal("q6_k_nbodd", wrap.q6_kNbOdd()),
};
