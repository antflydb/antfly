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

const std = @import("std");

pub const Modules = struct {
    metal: ?*std.Build.Module,
    cuda: ?*std.Build.Module,

    pub fn addImports(self: Modules, module: *std.Build.Module) void {
        if (self.metal) |identity| module.addImport("metal_jit_identity", identity);
        if (self.cuda) |identity| module.addImport("cuda_jit_identity", identity);
    }
};

/// Fingerprints belong to the enabled backend, not feature options. File
/// arguments participate in Zig's cache and watch graph. No source is read
/// while configuring the build, and disabled backends have no generator edge.
pub fn create(b: *std.Build, root: std.Build.LazyPath, metal: bool, cuda: bool) Modules {
    if (!metal and !cuda) return .{ .metal = null, .cuda = null };
    const tool = b.addExecutable(.{
        .name = "jit-source-identity",
        .root_module = b.createModule(.{
            .root_source_file = root.path(b, "tools/jit_identity.zig"),
            .target = b.graph.host,
            .optimize = .safe,
        }),
    });
    return .{
        .metal = if (metal) add(b, tool, root, .metal) else null,
        .cuda = if (cuda) add(b, tool, root, .cuda) else null,
    };
}

fn add(b: *std.Build, tool: *std.Build.Step.Compile, root: std.Build.LazyPath, backend: enum { metal, cuda }) *std.Build.Module {
    const run = b.addRunArtifact(tool);
    run.addArg("baseline");
    run.addFileArg2(root.path(b, switch (backend) {
        .metal => "src/backends/metal_kernels.m",
        .cuda => "src/ops/cuda/artifacts/inference_cuda_kernels.cu",
    }), .{ .make_absolute = true });
    run.addArg("qualification");
    const qualification: []const []const u8 = switch (backend) {
        .metal => &.{
            "src/backends/metal_runtime.zig",
            "src/graph/quant_kernel_metal_renderer.zig",
            "src/graph/quant_kernel_metal_attention_phases.zig",
            "src/graph/quant_kernel_metal_param_abi.zig",
            "src/graph/templates/metal_paged_prefill_compat.metal",
            "src/graph/templates/metal_projection_native.metal",
            "src/graph/templates/metal_paged_decode_native.metal",
            "src/backends/metal_compile_options.h",
            "src/backends/metal_pipeline_catalog.h",
            "src/graph/kernel_jit.zig",
            "src/graph/quant_kernel_compiler.zig",
            "src/graph/quant_matmul.zig",
            "src/gguf/quant_codec.zig",
            "src/gguf/tensor_types.zig",
        },
        .cuda => &.{
            "src/ops/cuda/kernels.zig",
            "src/graph/kernel_jit.zig",
            "src/graph/quant_kernel_compiler.zig",
            "src/graph/quant_kernel_cuda_renderer.zig",
            "src/graph/quant_matmul.zig",
            "src/gguf/quant_codec.zig",
            "src/gguf/tensor_types.zig",
        },
    };
    const tile_inputs = @import("../src/tile/source_inputs.zig").paths;
    const contracts = [_][]const u8{
        "src/ops/kernel_descriptor.zig",
        "src/graph/kernel_source_owner.zig",
        "src/ops/kernel_geometry.zig",
        "src/ops/kernel_math.zig",
        "src/ops/kernel_reduction.zig",
        "src/ops/gpu/depthwise_program.h",
        "src/ops/gpu/masked_pool_program.h",
        "src/graph/cuda_kernel_fold.zig",
        "src/tile/source_inputs.zig",
    };
    // Native includes and host ABI consumers affect qualification even when
    // the translation-unit text is unchanged. Declare the complete local
    // include closure so Zig's watch/cache graph sees those edits as well.
    const metal_dependencies = [_][]const u8{
        "src/backends/metal_param_abi.h",
        "src/backends/metal_gdn_source.h",
        "src/backends/metal_buffer_observation.h",
        "src/backends/metal_mtp_head.h",
        "src/backends/metal_mtp_head_impl.h",
        "src/ops/metal/generated/tile_whisper_logits_partials.msl.h",
    };
    const cuda_dependencies = [_][]const u8{
        "src/ops/cuda/embedded_module.zig",
        "src/ops/cuda/gated_delta.zig",
        "src/ops/cuda/paged_attention_bf16.zig",
        "src/ops/cuda/bf16_fusions.zig",
        "src/ops/cuda/vision_fusions.zig",
        "src/ops/cuda/encoder_decoder_fusions.zig",
        "src/ops/cuda/lighton_recurrent.ptx",
        "src/ops/cuda/lighton_attention.ptx",
        "src/ops/cuda/lighton_activation.ptx",
        "src/ops/cuda/lighton_vision.ptx",
        "src/ops/cuda/florence_fused.ptx",
        "src/ops/cuda/prototypes/gated_delta_recurrent.cu",
        "src/ops/cuda/prototypes/lighton_batched_attention.cu",
        "src/ops/cuda/prototypes/bf16_activation_programs.cu",
        "src/ops/cuda/prototypes/vision_projection_programs.cu",
        "src/ops/cuda/prototypes/vision_f16_programs.cu",
        "src/graph/templates/cuda_gqa_flash_prefill_f16_sm89.cuh",
        "src/graph/templates/cuda_gqa_decode_splitk_online_sm89.cuh",
        "src/graph/templates/cuda_gqa_split_decode_f32.cuh",
        "src/ops/cuda/launch_abi.zig",
        "src/ops/cuda/driver.zig",
        "src/graph/templates/cuda_q4_0_bf16_staged.cuh",
        "src/graph/templates/cuda_q4_q8_projection.cuh",
        "src/graph/templates/cuda_gqa_repeat_prefill_f16.cuh",
        "src/graph/templates/cuda_gqa_repeat_split_f16.cuh",
        "src/ops/cuda/kernels/kernel_abi.cuh",
        "src/ops/cuda/kernels/row_program.cuh",
        "src/ops/cuda/kernels/prefix_normalization_program.cuh",
        "src/ops/cuda/kernels/wmma_tile_program.cuh",
        "src/ops/cuda/kernels/rotary_program.cuh",
        "src/ops/cuda/kernels/encoder_multimodal_programs.cuh",
        "src/ops/cuda/kernels/rotary_frequency_training.cuh",
        "src/ops/cuda/kernels/encoder_fused_programs.cuh",
        "src/ops/cuda/kernels/ragged_boundary_programs.cuh",
        "src/ops/cuda/kernels/relative_attention_tensor_rows64.cuh",
        "src/ops/cuda/kernels/relative_attention_tensor_tiles.cuh",
        "src/ops/cuda/kernels/q8_projection_tile4.cuh",
        "src/ops/cuda/kernels/projection_rows_cols_init.cuh",
        "src/ops/cuda/kernels/projection_rows_cols_tree.cuh",
        "src/ops/cuda/kernels/quant_projection_epilogue.cuh",
        "src/ops/cuda/kernels/q4_k_tiled_dot.cuh",
        "src/ops/cuda/kernels/dense_qkv_tiled.cuh",
        "src/ops/cuda/kernels/quant_tile4_publish.cuh",
        "src/ops/cuda/kernels/quant_k_tile4_partials.cuh",
        "src/ops/cuda/kernels/scalar_argmax_fold.cuh",
        "src/ops/cuda/kernels/q8_1_zero_sum_store.cuh",
        "src/ops/cuda/kernels/q4_a4b_q8_acquire.cuh",
        "src/ops/cuda/kernels/tq_mma_visible_range.cuh",
        "src/ops/cuda/kernels/tq_mma_stage_kv.cuh",
        "src/ops/cuda/kernels/tq_mma_prefill.cuh",
        "src/ops/cuda/kernels/tq_scalar_online.cuh",
        "src/ops/cuda/kernels/vision_bf16_attention_m32n16.cuh",
        "src/ops/cuda/kernels/q6_e4b_tile8_partials.cuh",
        "src/ops/cuda/kernels/quant_argmax_publish.cuh",
        "src/ops/cuda/kernels/block_local_attention.cuh",
        "src/ops/cuda/kernels/primitive_gather.cuh",
        "src/ops/cuda/kernels/primitive_scatter.cuh",
        "src/ops/cuda/kernels/dense_fast_attention_online.cuh",
        "src/ops/cuda/kernels/training_validation.cuh",
        "src/ops/cuda/kernels/training_welford_layer_norm.cuh",
        "src/ops/cuda/kernels/training_row_softmax.cuh",
        "src/ops/cuda/kernels/relative_attention_training.cuh",
    };
    run.addArg(b.fmt("{d}", .{qualification.len + contracts.len + tile_inputs.len + if (backend == .cuda) cuda_dependencies.len else metal_dependencies.len}));
    for (qualification) |path| run.addFileArg2(root.path(b, path), .{ .make_absolute = true });
    for (contracts) |path| run.addFileArg2(root.path(b, path), .{ .make_absolute = true });
    for (if (backend == .cuda) &cuda_dependencies else &metal_dependencies) |path| run.addFileArg2(root.path(b, path), .{ .make_absolute = true });
    for (tile_inputs) |path| run.addFileArg2(root.path(b, path), .{ .make_absolute = true });
    if (backend == .cuda) {
        const dispatch_dependencies = [_][]const u8{
            "src/ops/cuda/cuda_compute.zig",
            "src/ops/cuda/launch_abi.zig",
            "src/ops/cuda/kernels/kernel_abi.cuh",
            "src/ops/kernel_descriptor.zig",
            "src/ops/kernel_geometry.zig",
            "src/graph/quant_kernel_compiler.zig",
            "src/graph/quant_matmul.zig",
            "src/gguf/tensor_types.zig",
        };
        run.addArgs(&.{ "dispatch", b.fmt("{d}", .{dispatch_dependencies.len}) });
        for (dispatch_dependencies) |path| run.addFileArg2(root.path(b, path), .{ .make_absolute = true });
    }
    run.addArg("--output");
    return b.createModule(.{ .root_source_file = run.addOutputFileArg2(b.fmt("{s}_identity.zig", .{@tagName(backend)}), .{ .make_absolute = true }) });
}
