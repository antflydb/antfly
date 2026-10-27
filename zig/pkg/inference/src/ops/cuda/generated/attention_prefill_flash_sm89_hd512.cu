// Production generated Flash prefill from graph/quant_kernel_compiler.zig.
// plan_id=cuda/attention/prefill_flash/sm89/hd512/gqa8/page16/q512-or-q3/swa0/f32q-f16kv-hmma
// source_id=antfly_gqa_attention_prefill_flash_sm89_hd512_global_f32_v1
// kernel_id=antfly_gqa_attention_prefill_flash_sm89_hd512_global_f32_v1
// production_baseline=termite_gqa_attention_prefill_tiled_f16_warp_f32
// runtime_default_enabled=true
// exact_token_parity_qualified=true
// Default-on automatic SM89 selection; rollback: ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=off.
#define ANTFLY_FLASH_NAMESPACE antfly_flash_prefill_sm89_hd512_global_f32_v1
#define ANTFLY_FLASH_KERNEL antfly_gqa_attention_prefill_flash_sm89_hd512_global_f32_v1
#define ANTFLY_FLASH_KV_HEADS 1
#define ANTFLY_FLASH_HEAD_DIM 512
#define ANTFLY_FLASH_SLIDING_WINDOW 0
#include "../../../graph/templates/cuda_gqa_flash_prefill_f16_sm89.cuh"
#undef ANTFLY_FLASH_SLIDING_WINDOW
#undef ANTFLY_FLASH_KV_HEADS
#undef ANTFLY_FLASH_HEAD_DIM
#undef ANTFLY_FLASH_KERNEL
#undef ANTFLY_FLASH_NAMESPACE
