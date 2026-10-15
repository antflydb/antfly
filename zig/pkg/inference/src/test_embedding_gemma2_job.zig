// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

test {
    _ = @import("finetune/embedding_gemma2_job.zig");
    _ = @import("finetune/embedding_gemma2_data.zig");
    _ = @import("finetune/embedding_gemma2_batch.zig");
    _ = @import("finetune/embedding_gemma2_loss.zig");
    _ = @import("finetune/embedding_gemma2_export.zig");
}
