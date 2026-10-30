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

//! Generated kernel regions of the `attention` families.

const r = @import("../region.zig");
const split = @import("../attention_split.zig");
const cu = @import("../attention_cuda_pairs.zig");
const mtl = @import("../attention_metal_pairs.zig");

pub const regions = [_]r.Region{
    r.metal("attention.kv_seed", mtl.kv_seed.kernel("termite_paged_f32_kv_seed", false) ++
        mtl.kv_seed.kernel("termite_paged_f16_kv_seed", true)),
    r.metal("attention.qkv_split", mtl.qkv_split.kernel("termite_dense_qkv_split_f32_2d", false) ++
        mtl.qkv_split.kernel("termite_dense_qkv_split_bias_f32_2d", true)),
    r.metal("attention.vision_flash_q32", mtl.vision_flash_q32.kernel("termite_sdpa_f32_vision_hd64_flash_q32", false) ++
        mtl.vision_flash_q32.kernel("termite_sdpa_f32_vision_hd64_flash_q32_full", true)),
    r.metal("attention.florence_window", mtl.florence_window.kernel("termite_sdpa_f32_florence_window_hd32", false) ++
        mtl.florence_window.kernel("termite_sdpa_f32_florence_window_parallel", true)),
    r.metal("attention.dense_causal_sg", mtl.dense_causal_sg.kernel("termite_attention_f32_dense_causal_sg", false) ++
        mtl.dense_causal_sg.kernel("termite_attention_f32_dense_causal_sg_q16", true)),
    r.metal("attention.gqa_split", split.split_region),
    r.metal("attention.gqa_batch", split.batch_region),
    r.cuda("attention.block", cu.block.kernel("termite_attention_f32_block", false)),
    r.cuda("attention.bert_prefill", cu.bert_prefill.kernel("termite_attention_f32_bert_prefill_s256_hd64_q8", false) ++ "\n" ++
        cu.bert_prefill.kernel("termite_attention_f32_bert_prefill_s256_hd64_q16", true)),
    r.cuda("attention.gqa_fast", cu.gqa_fast.kernel("termite_gqa_attention_decode_scalars_fast_f32", true) ++ "\n" ++
        cu.gqa_fast.kernel("termite_gqa_attention_prefill_fast_f32", false)),
    r.cuda("attention.gqa_decode", cu.gqa_decode.kernel("termite_gqa_attention_decode_f32", false) ++ "\n" ++
        cu.gqa_decode.kernel("termite_gqa_attention_decode_scalars_f32", true)),
    r.cuda("attention.qkv_tiled", cu.qkv_tiled.kernel("termite_linear_f32_qkv_nobias_tiled", false) ++ "\n" ++
        cu.qkv_tiled.kernel("termite_linear_bf16_weight_f32_qkv_nobias_tiled", true)),
    r.cuda("attention.block_laya", cu.block.kernel("termite_laya_local_attention_f32", true)),
    r.cuda("attention.warp", cu.warp.kernel("termite_laya_attention_warp_f32", true) ++ "\n" ++
        cu.warp.kernel("termite_gliner_encoder_attention_f32", false)),
    r.cuda("attention.gliner_boundary_tc", cu.gliner_boundary_tc.kernel("termite_gliner_boundary_attention_tc_f16_m64", false) ++ "\n" ++
        cu.gliner_boundary_tc.kernel("termite_gliner_boundary_attention_tc_f16_compact", true)),
};
