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

// Termite-specific graph IR bridge.
//
// Connects the reusable `ml` graph library to termite's ComputeBackend
// VTable. The tracing backend records ops into a Graph; the interpreter
// replays a Graph through a real backend.

const build_options = @import("build_options");
const supports_onnx_models = !build_options.enable_wasm;

pub const tracing_compute = @import("tracing_compute.zig");
pub const interpreter = @import("interpreter.zig");
pub const runtime = @import("runtime.zig");
pub const cache = @import("cache.zig");
pub const executor_stats = @import("executor_stats.zig");
pub const resident_ops = @import("resident_ops.zig");
pub const partition = @import("partition.zig");
pub const partition_export = @import("partition_export.zig");
pub const buffer_plan = @import("buffer_plan.zig");
pub const device_mesh = @import("device_mesh.zig");
pub const multi_executor = @import("multi_executor.zig");
pub const native_partition_executor = @import("native_partition_executor.zig");
pub const metal_partition_executor = @import("metal_partition_executor.zig");
pub const webgpu_partition_executor = @import("webgpu_partition_executor.zig");
pub const compiled_backend = @import("compiled_backend.zig");
pub const compiled_registry = @import("compiled_registry.zig");
pub const backend_contracts = @import("backend_contracts.zig");
pub const model_runtime = @import("model_runtime.zig");
pub const decode_state_runtime = @import("decode_state_runtime.zig");
pub const live_model_executor = @import("live_model_executor.zig");
pub const metal_command_planner = if (build_options.enable_metal) @import("metal_command_planner.zig") else struct {};
pub const metal_executor = if (build_options.enable_metal) @import("metal_executor.zig") else struct {
    const std = @import("std");
    const backends = @import("../backends/backends.zig");
    const gpt_mod = @import("../models/gpt.zig");
    const runtime_root = @import("../runtime/root.zig");
    const model_runtime_mod = @import("model_runtime.zig");

    pub const TimingStats = struct {
        prefill_calls: u64 = 0,
        prefill_prepare_nanos: u128 = 0,
        prefill_direct_last_logits_nanos: u128 = 0,
        prefill_direct_family_nanos: u128 = 0,
        prefill_direct_family_project_nanos: u128 = 0,
        prefill_direct_family_span_prep_nanos: u128 = 0,
        prefill_direct_family_quant_attn_nanos: u128 = 0,
        prefill_direct_family_block_apply_nanos: u128 = 0,
        prefill_fallback_logits_nanos: u128 = 0,
        decode_begin_step_nanos: u128 = 0,
        decode_sample_calls: u64 = 0,
        decode_sample_direct_nanos: u128 = 0,
        decode_sample_fallback_nanos: u128 = 0,
        decode_greedy_calls: u64 = 0,
        decode_greedy_direct_nanos: u128 = 0,
        decode_greedy_fallback_nanos: u128 = 0,
        ensure_prepared_calls: u64 = 0,
        ensure_prepared_nanos: u128 = 0,
        ensure_prepared_sync_nanos: u128 = 0,
        ensure_prepared_family_nanos: u128 = 0,
        ensure_prepared_greedy_nanos: u128 = 0,
        ensure_prepared_fast_hits: u64 = 0,
    };

    pub fn resetTimingStats() void {}
    pub fn getTimingStats() TimingStats {
        return .{};
    }
    pub fn supportsSession(_: backends.Session) bool {
        return false;
    }
    pub fn prewarmSharedDecoderRuntime(
        _: std.mem.Allocator,
        _: backends.Session,
        _: gpt_mod.Config,
    ) !bool {
        return false;
    }
    pub fn createModelExecutor(
        _: std.mem.Allocator,
        _: backends.Session,
        _: gpt_mod.Config,
        _: ?runtime_root.kv.pool.KvDType,
        _: ?*runtime_root.moe.shared.SharedExpertCache,
    ) !model_runtime_mod.ModelExecutor {
        return error.MetalNotEnabled;
    }
};
pub const execution = @import("execution.zig");
pub const onnx_kv_cache = @import("onnx_kv_cache.zig");
pub const onnx_artifact_executor = if (build_options.enable_onnx) @import("onnx_artifact_executor.zig") else struct {};
pub const compiled_onnx = if (build_options.enable_onnx) @import("compiled_onnx.zig") else struct {};
pub const compiled_pjrt = if (build_options.enable_pjrt) @import("compiled_pjrt.zig") else struct {};
pub const onnx_compiler = if (supports_onnx_models) @import("onnx_compiler.zig") else struct {};
pub const onnx_executor = if (build_options.enable_onnx) @import("onnx_executor.zig") else struct {};
pub const pjrt_compiler = if (build_options.enable_pjrt) @import("pjrt_compiler.zig") else struct {};
pub const pjrt_executor = if (build_options.enable_pjrt) @import("pjrt_executor.zig") else struct {};
pub const pjrt_artifact_executor = if (build_options.enable_pjrt) @import("pjrt_artifact_executor.zig") else struct {};
pub const pjrt_mesh = if (build_options.enable_pjrt) @import("pjrt_mesh.zig") else struct {};
pub const sharding = @import("sharding.zig");
pub const collective_ops = @import("collective_ops.zig");
pub const parallel_strategy = @import("parallel_strategy.zig");
pub const training = @import("training.zig");
pub const training_loop = @import("training_loop.zig");
pub const segmented_encoder = @import("segmented_encoder.zig");
pub const distributed_training = @import("distributed_training.zig");
pub const passes = @import("ml").graph.passes;

test {
    _ = tracing_compute;
    _ = interpreter;
    _ = runtime;
    _ = cache;
    _ = executor_stats;
    _ = resident_ops;
    _ = partition;
    _ = partition_export;
    _ = buffer_plan;
    _ = device_mesh;
    _ = multi_executor;
    _ = native_partition_executor;
    _ = metal_partition_executor;
    _ = webgpu_partition_executor;
    _ = compiled_backend;
    _ = compiled_registry;
    _ = backend_contracts;
    _ = model_runtime;
    _ = decode_state_runtime;
    _ = live_model_executor;
    if (build_options.enable_metal) {
        _ = metal_command_planner;
    }
    if (build_options.enable_metal) {
        _ = metal_executor;
    }
    _ = execution;
    _ = onnx_kv_cache;
    _ = sharding;
    _ = collective_ops;
    _ = parallel_strategy;
    _ = training;
    _ = training_loop;
    _ = segmented_encoder;
    _ = distributed_training;
    if (build_options.enable_onnx) {
        _ = onnx_artifact_executor;
        _ = compiled_onnx;
        _ = onnx_executor;
    }
    if (supports_onnx_models) {
        _ = onnx_compiler;
    }
    if (build_options.enable_pjrt) {
        _ = compiled_pjrt;
        _ = pjrt_compiler;
        _ = pjrt_executor;
        _ = pjrt_artifact_executor;
        _ = pjrt_mesh;
        _ = @import("pjrt_test.zig");
    }
}
