// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

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
