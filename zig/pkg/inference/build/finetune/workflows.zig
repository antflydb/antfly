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

const common = @import("common.zig");

const workflow_imports = &.{ .termite_io_compat, .termite_c_file };

const workflow_commands = [_]common.CommandSpec{
    .{
        .name = "run-gliner2-boundary-task-head-smoke-workflow",
        .root_source_file = "src/finetune/train/run_gliner2_boundary_task_head_smoke_workflow.zig",
        .description = "Prepare GLiNER2 boundary caches, train/eval the boundary task head, materialize artifacts, and emit one smoke-workflow report",
        .imports = &.{ .build_options, .jinja, .ml, .inference_internal, .inference_hf_tokenizer },
        .native_link = .default,
    },
    .{
        .name = "run-gliner2-entity-cleanup-smoke-workflow",
        .root_source_file = "src/finetune/train/run_gliner2_entity_cleanup_smoke_workflow.zig",
        .description = "Prepare GLiNER2 cleanup caches, train/eval the cleanup head, materialize artifacts, and emit one smoke-workflow report",
        .imports = &.{ .build_options, .jinja, .ml, .inference_internal, .inference_hf_tokenizer },
        .native_link = .default,
    },
    .{
        .name = "gliner2-production-readiness",
        .root_source_file = "src/finetune/run_gliner2_production_readiness.zig",
        .description = "Run the GLiNER2 production-readiness gate: dataset checks, training, artifact validation, semantic eval, and optional materialization",
        .imports = &.{ .build_options, .ml, .inference_internal, .inference_hf_tokenizer, .protobuf, .inference_linalg },
        .native_link = .default,
    },
    .{
        .name = "run-gemma4-lora-pilot-workflow",
        .root_source_file = "src/finetune/train/run_gemma4_lora_pilot_workflow.zig",
        .description = "Run a larger single-device Gemma4 LoRA text or multimodal pilot workflow",
        .imports = workflow_imports,
        .link_libc = true,
    },
    .{
        .name = "run-gemma4-recursive-lora-smoke-workflow",
        .root_source_file = "src/finetune/train/run_gemma4_recursive_lora_smoke_workflow.zig",
        .description = "Run a bounded Gemma4 recursive LoRA distillation smoke workflow",
        .imports = workflow_imports,
        .link_libc = true,
    },
    .{
        .name = "run-gemma4-recursive-lora-sweep",
        .root_source_file = "src/finetune/train/run_gemma4_recursive_lora_sweep.zig",
        .description = "Run Gemma4 baseline-vs-recursive LoRA comparison sweep",
        .imports = workflow_imports,
        .link_libc = true,
    },
    .{
        .name = "run-layoutlmv3-lora-smoke-workflow",
        .root_source_file = "src/finetune/train/run_layoutlmv3_lora_smoke_workflow.zig",
        .description = "Bootstrap, train, inspect, and materialize a bounded LayoutLMv3 LoRA workflow",
        .imports = &.{ .build_options, .ml, .inference_internal },
        .native_link = .no_accel,
    },
};

pub fn register(ctx: common.Context) void {
    for (workflow_commands) |spec| common.addCommand(ctx, spec);
}
