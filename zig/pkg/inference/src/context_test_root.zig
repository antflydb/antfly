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

test {
    _ = @import("runtime/context_policy.zig");
    _ = @import("runtime/kv/payload.zig");
    _ = @import("runtime/kv/compaction_profile.zig");
    _ = @import("backends/compaction_coordinator.zig");
    _ = @import("backends/context_stored_all36_driver.zig");
    _ = @import("backends/context_stored_all36_coordinator_fixture.zig");
    _ = @import("backends/context_stored_installed_invocation.zig");
    _ = @import("context_stored_installed_caller.zig");
    _ = @import("context_model_test.zig");
    _ = @import("context_metal_checkpoint_qualify.zig");
    _ = @import("runtime/kv/metal_checkpoint_cycles.zig");
    _ = @import("runtime/kv/metal_checkpoint_soak.zig");
    _ = @import("cli/native_kv_upload.zig");
    _ = @import("runtime/kv/metal_checkpoint_quality.zig");
    _ = @import("context_native_checkpoint_quality.zig");
    _ = @import("context_query_campaign_capture.zig");
    _ = @import("context_fault_injection.zig");
    _ = @import("backends/context_teacher_forced.zig");
    _ = @import("context_numerical_capture_inputs.zig");
    _ = @import("context_native_head_qualify.zig");
    _ = @import("context_release_build_binding.zig");
    _ = @import("context_exact_test.zig");
    _ = @import("context_capture_coverage.zig");
    _ = @import("context_serving_test.zig");
    // Discover driver/message tests even when compile-time filters omit the
    // model-serving tests that otherwise instantiate these imports lazily.
    _ = @import("context_serving_driver.zig");
    _ = @import("context_serving_messages.zig");
    _ = @import("context_serving_protocol.zig");
    _ = @import("context_quality_materializer.zig");
    _ = @import("context_native_quality_materializer.zig");
    _ = @import("backends/context_kernels.zig");
    _ = @import("backends/context_flash.zig");
    _ = @import("backends/context_execution_pool.zig");
    _ = @import("backends/context_query_protocol.zig");
    _ = @import("backends/context_query_campaign_pool.zig");
    _ = @import("backends/context_study_runner.zig");
    _ = @import("backends/context_study_templates.zig");
    _ = @import("backends/context_compaction.zig");
    _ = @import("backends/context_attention_match.zig");
    _ = @import("backends/context_native_packed_parity_test.zig");
    _ = @import("backends/packed_context_storage.zig");
    _ = @import("backends/replay_context.zig");
    _ = @import("backends/model_replay_context.zig");
    _ = @import("backends/packed_context_session.zig");
    _ = @import("backends/context_fit_capture_test.zig");
    _ = @import("backends/metal_context_session.zig");
    _ = @import("backends/metal_kv_storage.zig");
    // These regressions must remain discoverable when focused filters omit
    // the higher-level tests that otherwise instantiate their imports.
    _ = @import("backends/metal_runtime.zig");
    _ = @import("backends/metal_generated_kernels.zig");
    _ = @import("backends/decoder_gated_runtime.zig");
    _ = @import("ops/metal_compute.zig");
    _ = @import("ops/metal_attention_match_fit.zig");
    _ = @import("ops/metal_attention_match_lapack_test.zig");
    _ = @import("ops/metal_attention_match_storage.zig");
    _ = @import("runtime/kv/context_view.zig");
    _ = @import("runtime/kv/attention_matching.zig");
    _ = @import("runtime/kv/attention_match.zig");
    _ = @import("runtime/kv/attention_match_engine.zig");
    _ = @import("runtime/kv/attention_match_checkpoint.zig");
    _ = @import("runtime/kv/attention_match_resume.zig");
    _ = @import("runtime/kv/attention_match_study.zig");
    _ = @import("runtime/kv/reference_replay.zig");
    _ = @import("native_generate.zig");
    _ = @import("pipelines/generation.zig");
    _ = @import("server/server.zig");
    _ = @import("server/native_kv_batch.zig");
    _ = @import("models/gpt.zig");
    _ = @import("runtime/kv/compaction.zig");
    _ = @import("runtime/kv/storage_runtime.zig");
}

test "context generation entrypoints compile" {
    // Keep admission tests from passing while executable-only wiring remains
    // uninstantiated by Zig's lazy analysis. No model is loaded by this test.
    const std = @import("std");
    std.mem.doNotOptimizeAway(&@import("native_generate.zig").main);
    std.mem.doNotOptimizeAway(&@import("server/server.zig").Node.generateContent);
    std.mem.doNotOptimizeAway(&@import("server/server.zig").Node.generateBatchContent);
    std.mem.doNotOptimizeAway(&@import("server/server.zig").Node.resumeNativeKvCheckpointBatch);
    std.mem.doNotOptimizeAway(&@import("pipelines/generation.zig").NativeGenerationPipeline.runCompactionStudy);
}
