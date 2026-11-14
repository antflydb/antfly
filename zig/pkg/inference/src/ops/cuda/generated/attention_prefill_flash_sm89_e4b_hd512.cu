// Default-off generated Flash prefill from graph/quant_kernel_compiler.zig.
// plan_id=cuda/attention/prefill_flash/sm89/e4b/hd512/gqa8-kv2/page16/q2-2048-kv32k/swa0/f32q-f16kv-hmma
// source_id=antfly_gqa_attention_prefill_flash_sm89_e4b_hd512_global_f32_v1
// kernel_id=antfly_gqa_attention_prefill_flash_sm89_e4b_hd512_global_f32_v1
// production_baseline=termite_gqa_attention_prefill_turboquant_fast_f32
// runtime_default_enabled=false
// exact_token_parity_qualified=false
#define ANTFLY_FLASH_NAMESPACE antfly_flash_prefill_sm89_e4b_hd512_global_f32_v1
#define ANTFLY_FLASH_KERNEL antfly_gqa_attention_prefill_flash_sm89_e4b_hd512_global_f32_v1
#define ANTFLY_FLASH_KV_HEADS 2
#define ANTFLY_FLASH_HEAD_DIM 512
#define ANTFLY_FLASH_SLIDING_WINDOW 0
#include "../../../graph/templates/cuda_gqa_flash_prefill_f16_sm89.cuh"
#undef ANTFLY_FLASH_SLIDING_WINDOW
#undef ANTFLY_FLASH_KV_HEADS
#undef ANTFLY_FLASH_HEAD_DIM
#undef ANTFLY_FLASH_KERNEL
#undef ANTFLY_FLASH_NAMESPACE
