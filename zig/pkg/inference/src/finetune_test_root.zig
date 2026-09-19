// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

// Ordinary finetuning tests share one source and dependency ownership boundary.
// Inference-owned compatibility targets remain registered in build/finetune/tests.zig.
test {
    _ = @import("finetune/test/test_layoutlmv3_finetune.zig");
    _ = @import("finetune/test/test_colqwen2_finetune.zig");
    _ = @import("finetune/test/test_gliner2_data.zig");
    _ = @import("finetune/test/test_gliner2_e2e.zig");
    _ = @import("finetune/test/test_gliner2_backend_grad_parity.zig");
    _ = @import("finetune/test/test_gliner2_real_training.zig");
    _ = @import("finetune/test/test_gliner2_run_validation.zig");
    _ = @import("finetune/test/test_gliner2_recipe.zig");
    _ = @import("finetune/train/train_gliner2_autodiff.zig");
    _ = @import("finetune/tools/eval_gliner2_autodiff_adapter_dataset.zig");
    _ = @import("test_entity_cleanup_gliner_cache.zig");
    _ = @import("test_gliner2_cleanup_bundle.zig");
    _ = @import("finetune/test/test_reranker_data.zig");
    _ = @import("finetune/test/test_fused_chunker_data.zig");
    _ = @import("finetune/test/test_fused_chunker.zig");
    _ = @import("finetune/test/test_fused_chunker_loss.zig");
    _ = @import("finetune/test/test_infonce_cpu.zig");
    _ = @import("finetune/test/test_fused_chunker_splade.zig");
    _ = @import("finetune/test/test_fused_chunker_train.zig");
    _ = @import("finetune/test/test_fused_chunker_lora.zig");
    _ = @import("finetune/test/test_tokenizer_batch.zig");
}
