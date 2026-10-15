// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
test {
    _ = @import("finetune/embedding_gemma2_training.zig");
    _ = @import("finetune/embedding_gemma2_graph.zig");
    _ = @import("finetune/embedding_gemma2_adapter_state.zig");
    _ = @import("finetune/embedding_gemma2_cuda.zig");
    _ = @import("finetune/embedding_gemma2_cuda_tape.zig");
    _ = @import("finetune/embedding_gemma2_checkpoint.zig");
}
