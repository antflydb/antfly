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

pub const source_0 =
    \\           TERMITE_MSL_ABI_VISION_SDPA(termite_sdpa_f32_vision_hd64_flash_q32) "\n"
    \\           "    threadgroup half sq[2048]; threadgroup half skv[2048]; threadgroup float ss[1024]; threadgroup half sp[1024]; threadgroup float sM[32]; threadgroup float sS[32]; threadgroup float sdiag[256]; threadgroup float so[2048];\n"
    \\           "    termite_vision_hd64_flash_q32<half>(q, k, v, bias, mask, output, p, tid, lane, sgitg, tg, sq, skv, ss, sp, sM, sS, sdiag, so);\n"
    \\           "}\n"
    \\
;

pub const source_1 =
    \\           TERMITE_MSL_ABI_VISION_SDPA(termite_sdpa_f32_vision_hd64_flash_q32_full) "\n"
    \\           "    threadgroup float sq[2048]; threadgroup float skv[2048]; threadgroup float ss[1024]; threadgroup float sp[1024]; threadgroup float sM[32]; threadgroup float sS[32]; threadgroup float sdiag[256]; \n"
    \\           "    termite_vision_hd64_flash_q32<float>(q, k, v, bias, mask, output, p, tid, lane, sgitg, tg, sq, skv, ss, sp, sM, sS, sdiag, sq);\n"
    \\           "}\n"
    \\
;
