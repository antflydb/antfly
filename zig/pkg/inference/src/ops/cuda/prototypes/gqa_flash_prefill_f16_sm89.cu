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

// Standalone qualification wrapper. The renderer-owned template below is the
// single implementation source used by this prototype and the default-off
// production bundle. Prototype-only symbols remain unreachable from runtime.

#define ANTFLY_FLASH_NAMESPACE antfly_flash_prefill_hd256_prototype
#define ANTFLY_FLASH_KERNEL antfly_gqa_attention_prefill_flash_f16_sm89_hd256_swa512_f32_prototype
#define ANTFLY_FLASH_REFERENCE_KERNEL antfly_gqa_attention_prefill_reference_f16_hd256_swa512_f32_prototype
#define ANTFLY_FLASH_HEAD_DIM 256
#define ANTFLY_FLASH_SLIDING_WINDOW 512
#include "../../../graph/templates/cuda_gqa_flash_prefill_f16_sm89.cuh"
#undef ANTFLY_FLASH_SLIDING_WINDOW
#undef ANTFLY_FLASH_HEAD_DIM
#undef ANTFLY_FLASH_REFERENCE_KERNEL
#undef ANTFLY_FLASH_KERNEL
#undef ANTFLY_FLASH_NAMESPACE

#define ANTFLY_FLASH_NAMESPACE antfly_flash_prefill_hd512_prototype
#define ANTFLY_FLASH_KERNEL antfly_gqa_attention_prefill_flash_f16_sm89_hd512_global_f32_prototype
#define ANTFLY_FLASH_REFERENCE_KERNEL antfly_gqa_attention_prefill_reference_f16_hd512_global_f32_prototype
#define ANTFLY_FLASH_HEAD_DIM 512
#define ANTFLY_FLASH_SLIDING_WINDOW 0
#include "../../../graph/templates/cuda_gqa_flash_prefill_f16_sm89.cuh"
#undef ANTFLY_FLASH_SLIDING_WINDOW
#undef ANTFLY_FLASH_HEAD_DIM
#undef ANTFLY_FLASH_REFERENCE_KERNEL
#undef ANTFLY_FLASH_KERNEL
#undef ANTFLY_FLASH_NAMESPACE
