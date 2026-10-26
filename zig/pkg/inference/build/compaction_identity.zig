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

//! Build graph for the context-compaction semantic source identity. Every
//! input is a file argument so Zig's cache and watch graph invalidate the
//! generated module when, and only when, a canonical source changes.
const std = @import("std");

pub fn create(b: *std.Build, root: std.Build.LazyPath) *std.Build.Module {
    const tool = b.addExecutable(.{
        .name = "compaction-source-identity",
        .root_module = b.createModule(.{
            .root_source_file = root.path(b, "tools/compaction_identity.zig"),
            .target = b.graph.host,
            .optimize = .safe,
        }),
    });
    const run = b.addRunArtifact(tool);

    for (canonical_sources) |source| {
        run.addArgs(&.{ "source", source.logical_name });
        run.addFileArg2(root.path(b, source.path), .{ .make_absolute = true });
    }
    run.addArgs(&.{ "profile", "runtime/kv/compaction_profile.zig" });
    run.addFileArg2(root.path(b, "src/runtime/kv/compaction_profile.zig"), .{ .make_absolute = true });
    run.addArg("--output");

    return b.createModule(.{
        .root_source_file = run.addOutputFileArg2("compaction_identity.zig", .{ .make_absolute = true }),
    });
}

const Source = struct {
    logical_name: []const u8,
    path: []const u8,
};

const canonical_sources = [_]Source{
    .{ .logical_name = "build/compaction_identity.zig", .path = "build/compaction_identity.zig" },
    .{ .logical_name = "tools/compaction_identity.zig", .path = "tools/compaction_identity.zig" },
    .{ .logical_name = "context_capture_coverage.zig", .path = "src/context_capture_coverage.zig" },
    .{ .logical_name = "context_query_campaign_capture.zig", .path = "src/context_query_campaign_capture.zig" },
    .{ .logical_name = "context_native_head_qualify.zig", .path = "src/context_native_head_qualify.zig" },
    .{ .logical_name = "context_fault_injection.zig", .path = "src/context_fault_injection.zig" },
    .{ .logical_name = "backends/context_teacher_forced.zig", .path = "src/backends/context_teacher_forced.zig" },
    .{ .logical_name = "backends/compaction_coordinator.zig", .path = "src/backends/compaction_coordinator.zig" },
    .{ .logical_name = "backends/context_stored_all36_driver.zig", .path = "src/backends/context_stored_all36_driver.zig" },
    .{ .logical_name = "backends/context_stored_all36_coordinator_fixture.zig", .path = "src/backends/context_stored_all36_coordinator_fixture.zig" },
    .{ .logical_name = "backends/context_stored_installed_invocation.zig", .path = "src/backends/context_stored_installed_invocation.zig" },
    .{ .logical_name = "context_stored_installed_caller.zig", .path = "src/context_stored_installed_caller.zig" },
    .{ .logical_name = "backends/context_attention.metal", .path = "src/backends/context_attention.metal" },
    .{ .logical_name = "backends/context_attention_match.metal", .path = "src/backends/context_attention_match.metal" },
    .{ .logical_name = "ops/metal_attention_match_fit.zig", .path = "src/ops/metal_attention_match_fit.zig" },
    .{ .logical_name = "ops/metal_attention_match_fit.metal", .path = "src/ops/metal_attention_match_fit.metal" },
    .{ .logical_name = "ops/metal_attention_match_storage.zig", .path = "src/ops/metal_attention_match_storage.zig" },
    .{ .logical_name = "ops/metal_attention_match_storage.metal", .path = "src/ops/metal_attention_match_storage.metal" },
    .{ .logical_name = "backends/context_attention_match.zig", .path = "src/backends/context_attention_match.zig" },
    .{ .logical_name = "backends/context_compaction.metal", .path = "src/backends/context_compaction.metal" },
    .{ .logical_name = "backends/context_compaction.zig", .path = "src/backends/context_compaction.zig" },
    .{ .logical_name = "backends/context_execution_pool.zig", .path = "src/backends/context_execution_pool.zig" },
    .{ .logical_name = "backends/context_flash.metal", .path = "src/backends/context_flash.metal" },
    .{ .logical_name = "backends/context_flash.zig", .path = "src/backends/context_flash.zig" },
    .{ .logical_name = "backends/context_kernels.zig", .path = "src/backends/context_kernels.zig" },
    .{ .logical_name = "backends/context_query_protocol.zig", .path = "src/backends/context_query_protocol.zig" },
    .{ .logical_name = "backends/context_query_campaign_pool.zig", .path = "src/backends/context_query_campaign_pool.zig" },
    .{ .logical_name = "backends/context_query_sampling.zig", .path = "src/backends/context_query_sampling.zig" },
    .{ .logical_name = "backends/context_replay.metal", .path = "src/backends/context_replay.metal" },
    .{ .logical_name = "backends/context_storage.metal", .path = "src/backends/context_storage.metal" },
    .{ .logical_name = "backends/context_study_runner.zig", .path = "src/backends/context_study_runner.zig" },
    .{ .logical_name = "backends/context_study_templates.zig", .path = "src/backends/context_study_templates.zig" },
    .{ .logical_name = "backends/decoder_gated_runtime.zig", .path = "src/backends/decoder_gated_runtime.zig" },
    .{ .logical_name = "backends/metal_context_session.zig", .path = "src/backends/metal_context_session.zig" },
    // Dispatch, frame ownership and the FFI are part of runtime authorization.
    // Hash the complete implementation so edits outside pipeline creation
    // cannot silently reuse a previously qualified profile.
    .{ .logical_name = "backends/metal_kernels.m", .path = "src/backends/metal_kernels.m" },
    .{ .logical_name = "backends/metal_kv_storage.zig", .path = "src/backends/metal_kv_storage.zig" },
    .{ .logical_name = "backends/metal_runtime.zig", .path = "src/backends/metal_runtime.zig" },
    .{ .logical_name = "backends/metal_tensor.zig", .path = "src/backends/metal_tensor.zig" },
    .{ .logical_name = "backends/model_replay_context.zig", .path = "src/backends/model_replay_context.zig" },
    .{ .logical_name = "backends/packed_context_session.zig", .path = "src/backends/packed_context_session.zig" },
    .{ .logical_name = "backends/packed_context_storage.zig", .path = "src/backends/packed_context_storage.zig" },
    .{ .logical_name = "backends/metal_generated_kernels.zig", .path = "src/backends/metal_generated_kernels.zig" },
    .{ .logical_name = "backends/replay_context.zig", .path = "src/backends/replay_context.zig" },
    .{ .logical_name = "architectures/gemma4_runtime.zig", .path = "src/architectures/gemma4_runtime.zig" },
    .{ .logical_name = "architectures/gpt.zig", .path = "src/architectures/gpt.zig" },
    .{ .logical_name = "architectures/session_factory.zig", .path = "src/architectures/session_factory.zig" },
    .{ .logical_name = "execution_control.zig", .path = "src/execution_control.zig" },
    .{ .logical_name = "graph/backend_contracts.zig", .path = "src/graph/backend_contracts.zig" },
    .{ .logical_name = "graph/metal_executor.zig", .path = "src/graph/metal_executor.zig" },
    .{ .logical_name = "models/gpt.zig", .path = "src/models/gpt.zig" },
    .{ .logical_name = "models/tensor_store.zig", .path = "src/models/tensor_store.zig" },
    .{ .logical_name = "native_generate.zig", .path = "src/native_generate.zig" },
    .{ .logical_name = "ops/metal_compute.zig", .path = "src/ops/metal_compute.zig" },
    .{ .logical_name = "ops/ops.zig", .path = "src/ops/ops.zig" },
    .{ .logical_name = "pipelines/generation.zig", .path = "src/pipelines/generation.zig" },
    .{ .logical_name = "runtime/context_policy.zig", .path = "src/runtime/context_policy.zig" },
    .{ .logical_name = "runtime/file_snapshot.zig", .path = "src/runtime/file_snapshot.zig" },
    .{ .logical_name = "runtime/kv/attention_matching.zig", .path = "src/runtime/kv/attention_matching.zig" },
    .{ .logical_name = "runtime/kv/attention_match.zig", .path = "src/runtime/kv/attention_match.zig" },
    .{ .logical_name = "runtime/kv/attention_match_study.zig", .path = "src/runtime/kv/attention_match_study.zig" },
    .{ .logical_name = "runtime/kv/compaction.zig", .path = "src/runtime/kv/compaction.zig" },
    .{ .logical_name = "runtime/kv/context_session.zig", .path = "src/runtime/kv/context_session.zig" },
    .{ .logical_name = "runtime/kv/context_view.zig", .path = "src/runtime/kv/context_view.zig" },
    .{ .logical_name = "runtime/kv/host_allocation_tracker.zig", .path = "src/runtime/kv/host_allocation_tracker.zig" },
    .{ .logical_name = "runtime/kv/linalg.zig", .path = "src/runtime/kv/linalg.zig" },
    .{ .logical_name = "runtime/kv/manager.zig", .path = "src/runtime/kv/manager.zig" },
    .{ .logical_name = "runtime/kv/payload.zig", .path = "src/runtime/kv/payload.zig" },
    .{ .logical_name = "runtime/kv/reference_replay.zig", .path = "src/runtime/kv/reference_replay.zig" },
    .{ .logical_name = "runtime/kv/storage_runtime.zig", .path = "src/runtime/kv/storage_runtime.zig" },
    .{ .logical_name = "runtime/scheduler/native_generate.zig", .path = "src/runtime/scheduler/native_generate.zig" },
    .{ .logical_name = "runtime/tier/memory.zig", .path = "src/runtime/tier/memory.zig" },
    .{ .logical_name = "server/model_manager.zig", .path = "src/server/model_manager.zig" },
    .{ .logical_name = "server/server.zig", .path = "src/server/server.zig" },
    .{ .logical_name = "lib/jinja/src/provenance.zig", .path = "../../lib/jinja/src/provenance.zig" },
    .{ .logical_name = "lib/jinja/src/eval.zig", .path = "../../lib/jinja/src/eval.zig" },
    .{ .logical_name = "lib/jinja/src/jinja.zig", .path = "../../lib/jinja/src/jinja.zig" },
    .{ .logical_name = "lib/tokenizer/src/hf_tokenizer.zig", .path = "../../lib/tokenizer/src/hf_tokenizer.zig" },
    .{ .logical_name = "lib/tokenizer/src/tokenizer.zig", .path = "../../lib/tokenizer/src/tokenizer.zig" },
};
