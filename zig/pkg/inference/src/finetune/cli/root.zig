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

const std = @import("std");
const builtin = @import("builtin");

const recipe = @import("../recipe.zig");
const analyze_gemma4_recursive_lora_sweep = @import("../tools/analyze_gemma4_recursive_lora_sweep.zig");
const bootstrap_colqwen2_lora = @import("../tools/bootstrap_colqwen2_lora.zig");
const bootstrap_gemma4_lora = @import("../tools/bootstrap_gemma4_lora.zig");
const bootstrap_gliner2_lora = @import("../tools/bootstrap_gliner2_lora.zig");
const bootstrap_layoutlmv3_lora = @import("../tools/bootstrap_layoutlmv3_lora.zig");
const bootstrap_reranker_lora = @import("../tools/bootstrap_reranker_lora.zig");
const compose_lora_adapters = @import("../tools/compose_lora_adapters.zig");
const eval_fused_chunker = @import("../eval/eval_fused_chunker.zig");
const eval_gliner2_boundary_head = @import("../eval/eval_gliner2_top_layer_boundary_head.zig");
const eval_gliner2_boundary_task_head = @import("../eval/eval_gliner2_top_layer_boundary_task_head.zig");
const eval_reranker_checkpoint = @import("../eval/eval_reranker_checkpoint.zig");
const generate_gemma4_multimodal_pilot_dataset = @import("../tools/generate_gemma4_multimodal_pilot_dataset.zig");
const generate_gemma4_pilot_dataset = @import("../tools/generate_gemma4_pilot_dataset.zig");
const inspect_colqwen2_checkpoint = @import("../tools/inspect_colqwen2_checkpoint.zig");
const inspect_colqwen2_lora_bundle = @import("../tools/inspect_colqwen2_lora_bundle.zig");
const inspect_gemma4_lora_bundle = @import("../tools/inspect_gemma4_lora_bundle.zig");
const inspect_gliner2_checkpoint = @import("../tools/inspect_gliner2_checkpoint.zig");
const inspect_gliner2_dataset = @import("../tools/inspect_gliner2_dataset.zig");
const inspect_gliner2_lora_bundle = @import("../tools/inspect_gliner2_lora_bundle.zig");
const inspect_layoutlmv3_bundle = @import("../tools/inspect_layoutlmv3_bundle.zig");
const inspect_layoutlmv3_lora_bundle = @import("../tools/inspect_layoutlmv3_lora_bundle.zig");
const inspect_reranker_dataset = @import("../tools/inspect_reranker_dataset.zig");
const inspect_reranker_lora_bundle = @import("../tools/inspect_reranker_lora_bundle.zig");
const materialize_colqwen2_lora = @import("../tools/materialize_colqwen2_lora.zig");
const materialize_gemma4_lora = @import("../tools/materialize_gemma4_lora.zig");
const materialize_gemma4_recursive_base = @import("../tools/materialize_gemma4_recursive_base.zig");
const materialize_gemma4_teacher_targets = @import("../tools/materialize_gemma4_teacher_targets.zig");
const materialize_gliner2_lora = @import("../tools/materialize_gliner2_lora.zig");
const materialize_layoutlmv3_checkpoint = @import("../tools/materialize_layoutlmv3_checkpoint.zig");
const materialize_reranker_head = @import("../tools/materialize_reranker_head.zig");
const materialize_reranker_lora = @import("../tools/materialize_reranker_lora.zig");
const prepare_colqwen2_inputs = @import("../tools/prepare_colqwen2_inputs.zig");
const prepare_entity_cleanup_cache = @import("../tools/prepare_entity_cleanup_cache.zig");
const prepare_gemma4_lora_inputs = @import("../tools/prepare_gemma4_lora_inputs.zig");
const prepare_gemma4_multimodal_dataset = @import("../tools/prepare_gemma4_multimodal_dataset.zig");
const prepare_gemma4_text_dataset = @import("../tools/prepare_gemma4_text_dataset.zig");
const prepare_gliner2_entity_cleanup_cache = @import("../tools/prepare_gliner2_entity_cleanup_cache.zig");
const prepare_gliner2_top_layer_boundary_cache = @import("../tools/prepare_gliner2_top_layer_boundary_cache.zig");
const prepare_reranker_pooled_cache = @import("../tools/prepare_reranker_pooled_cache.zig");
const prepare_reranker_top_layer_cache = @import("../tools/prepare_reranker_top_layer_cache.zig");
const run_gemma4_lora_pilot_workflow = @import("../train/run_gemma4_lora_pilot_workflow.zig");
const run_gemma4_recursive_lora_smoke_workflow = @import("../train/run_gemma4_recursive_lora_smoke_workflow.zig");
const run_gemma4_recursive_lora_sweep = @import("../train/run_gemma4_recursive_lora_sweep.zig");
const run_gliner2_boundary_task_head_smoke_workflow = @import("../train/run_gliner2_boundary_task_head_smoke_workflow.zig");
const run_gliner2_entity_cleanup_smoke_workflow = @import("../train/run_gliner2_entity_cleanup_smoke_workflow.zig");
const run_layoutlmv3_lora_smoke_workflow = @import("../train/run_layoutlmv3_lora_smoke_workflow.zig");
const train_eval_colqwen2_lora_bundle = @import("../train/train_eval_colqwen2_lora_bundle.zig");
const train_eval_entity_cleanup_head = @import("../train/train_eval_entity_cleanup_head.zig");
const train_eval_gemma4_lora_bundle = @import("../train/train_eval_gemma4_lora_bundle.zig");
const train_eval_gliner2_lora_bundle = @import("../train/train_eval_gliner2_lora_bundle.zig");
const train_eval_gliner2_boundary_head = @import("../train/train_eval_gliner2_top_layer_boundary_head.zig");
const train_eval_gliner2_boundary_task_head = @import("../train/train_eval_gliner2_top_layer_boundary_task_head.zig");
const train_eval_layoutlmv3_lora_sequence = @import("../train/train_eval_layoutlmv3_lora_sequence.zig");
const train_eval_layoutlmv3_lora_token = @import("../train/train_eval_layoutlmv3_lora_token.zig");
const train_eval_reranker_head = @import("../train/train_eval_reranker_head.zig");
const train_eval_reranker_head_cached = @import("../train/train_eval_reranker_head_cached.zig");
const train_eval_reranker_head_top_layer_cached = @import("../train/train_eval_reranker_head_top_layer_cached.zig");
const train_eval_reranker_lora_surrogate = @import("../train/train_eval_reranker_lora_surrogate.zig");
const train_eval_reranker_lora_surrogate_cached = @import("../train/train_eval_reranker_lora_surrogate_cached.zig");
const train_eval_reranker_lora_top_layer_cached_surrogate = @import("../train/train_eval_reranker_lora_top_layer_cached_surrogate.zig");
const train_gliner2_autodiff = @import("../train/train_gliner2_autodiff.zig");
const train_layoutlmv3_lora_one_step = @import("../train/train_layoutlmv3_lora_one_step.zig");

const CommandMain = *const fn (std.process.Init) anyerror!void;

const Command = struct {
    domain: []const u8,
    action: []const u8,
    subject: []const u8,
    /// argv[0] forwarded to the existing command module while its parser is reused.
    adapter_argv0: []const u8,
    main_fn: CommandMain,
};

const commands = [_]Command{
    .{ .domain = "dataset", .action = "generate", .subject = "gemma4-pilot", .adapter_argv0 = "generate-gemma4-pilot-dataset", .main_fn = generate_gemma4_pilot_dataset.main },
    .{ .domain = "dataset", .action = "generate", .subject = "gemma4-multimodal-pilot", .adapter_argv0 = "generate-gemma4-multimodal-pilot-dataset", .main_fn = generate_gemma4_multimodal_pilot_dataset.main },
    .{ .domain = "dataset", .action = "inspect", .subject = "gliner2", .adapter_argv0 = "inspect-gliner2-dataset", .main_fn = inspect_gliner2_dataset.main },
    .{ .domain = "dataset", .action = "inspect", .subject = "reranker", .adapter_argv0 = "inspect-reranker-dataset", .main_fn = inspect_reranker_dataset.main },
    .{ .domain = "dataset", .action = "prepare", .subject = "gemma4-text", .adapter_argv0 = "prepare-gemma4-text-dataset", .main_fn = prepare_gemma4_text_dataset.main },
    .{ .domain = "dataset", .action = "prepare", .subject = "gemma4-multimodal", .adapter_argv0 = "prepare-gemma4-multimodal-dataset", .main_fn = prepare_gemma4_multimodal_dataset.main },
    .{ .domain = "dataset", .action = "prepare", .subject = "gemma4-lora", .adapter_argv0 = "prepare-gemma4-lora-inputs", .main_fn = prepare_gemma4_lora_inputs.main },
    .{ .domain = "dataset", .action = "prepare", .subject = "colqwen2", .adapter_argv0 = "prepare-colqwen2-inputs", .main_fn = prepare_colqwen2_inputs.main },
    .{ .domain = "dataset", .action = "prepare", .subject = "entity-cleanup-cache", .adapter_argv0 = "prepare-entity-cleanup-cache", .main_fn = prepare_entity_cleanup_cache.main },
    .{ .domain = "dataset", .action = "prepare", .subject = "gliner2-entity-cleanup-cache", .adapter_argv0 = "prepare-gliner2-entity-cleanup-cache", .main_fn = prepare_gliner2_entity_cleanup_cache.main },
    .{ .domain = "dataset", .action = "prepare", .subject = "gliner2-boundary-cache", .adapter_argv0 = "prepare-gliner2-top-layer-boundary-cache", .main_fn = prepare_gliner2_top_layer_boundary_cache.main },
    .{ .domain = "dataset", .action = "prepare", .subject = "reranker-pooled-cache", .adapter_argv0 = "prepare-reranker-pooled-cache", .main_fn = prepare_reranker_pooled_cache.main },
    .{ .domain = "dataset", .action = "prepare", .subject = "reranker-top-layer-cache", .adapter_argv0 = "prepare-reranker-top-layer-cache", .main_fn = prepare_reranker_top_layer_cache.main },
    .{ .domain = "dataset", .action = "materialize", .subject = "gemma4-teacher-targets", .adapter_argv0 = "materialize-gemma4-teacher-targets", .main_fn = materialize_gemma4_teacher_targets.main },

    .{ .domain = "adapter", .action = "bootstrap", .subject = "gemma4", .adapter_argv0 = "bootstrap-gemma4-lora", .main_fn = bootstrap_gemma4_lora.main },
    .{ .domain = "adapter", .action = "bootstrap", .subject = "gliner2", .adapter_argv0 = "bootstrap-gliner2-lora", .main_fn = bootstrap_gliner2_lora.main },
    .{ .domain = "adapter", .action = "bootstrap", .subject = "colqwen2", .adapter_argv0 = "bootstrap-colqwen2-lora", .main_fn = bootstrap_colqwen2_lora.main },
    .{ .domain = "adapter", .action = "bootstrap", .subject = "layoutlmv3", .adapter_argv0 = "bootstrap-layoutlmv3-lora", .main_fn = bootstrap_layoutlmv3_lora.main },
    .{ .domain = "adapter", .action = "bootstrap", .subject = "reranker", .adapter_argv0 = "bootstrap-reranker-lora", .main_fn = bootstrap_reranker_lora.main },
    .{ .domain = "adapter", .action = "compose", .subject = "lora", .adapter_argv0 = "compose-lora-adapters", .main_fn = compose_lora_adapters.main },
    .{ .domain = "adapter", .action = "inspect", .subject = "gemma4", .adapter_argv0 = "inspect-gemma4-lora-bundle", .main_fn = inspect_gemma4_lora_bundle.main },
    .{ .domain = "adapter", .action = "inspect", .subject = "gliner2", .adapter_argv0 = "inspect-gliner2-lora-bundle", .main_fn = inspect_gliner2_lora_bundle.main },
    .{ .domain = "adapter", .action = "inspect", .subject = "gliner2-checkpoint", .adapter_argv0 = "inspect-gliner2-checkpoint", .main_fn = inspect_gliner2_checkpoint.main },
    .{ .domain = "adapter", .action = "inspect", .subject = "colqwen2", .adapter_argv0 = "inspect-colqwen2-lora-bundle", .main_fn = inspect_colqwen2_lora_bundle.main },
    .{ .domain = "adapter", .action = "inspect", .subject = "colqwen2-checkpoint", .adapter_argv0 = "inspect-colqwen2-checkpoint", .main_fn = inspect_colqwen2_checkpoint.main },
    .{ .domain = "adapter", .action = "inspect", .subject = "layoutlmv3", .adapter_argv0 = "inspect-layoutlmv3-lora-bundle", .main_fn = inspect_layoutlmv3_lora_bundle.main },
    .{ .domain = "adapter", .action = "inspect", .subject = "layoutlmv3-bundle", .adapter_argv0 = "inspect-layoutlmv3-bundle", .main_fn = inspect_layoutlmv3_bundle.main },
    .{ .domain = "adapter", .action = "inspect", .subject = "reranker", .adapter_argv0 = "inspect-reranker-lora-bundle", .main_fn = inspect_reranker_lora_bundle.main },
    .{ .domain = "adapter", .action = "materialize", .subject = "gemma4", .adapter_argv0 = "materialize-gemma4-lora", .main_fn = materialize_gemma4_lora.main },
    .{ .domain = "adapter", .action = "materialize", .subject = "gemma4-recursive-base", .adapter_argv0 = "materialize-gemma4-recursive-base", .main_fn = materialize_gemma4_recursive_base.main },
    .{ .domain = "adapter", .action = "materialize", .subject = "gliner2", .adapter_argv0 = "materialize-gliner2-lora", .main_fn = materialize_gliner2_lora.main },
    .{ .domain = "adapter", .action = "materialize", .subject = "colqwen2", .adapter_argv0 = "materialize-colqwen2-lora", .main_fn = materialize_colqwen2_lora.main },
    .{ .domain = "adapter", .action = "materialize", .subject = "layoutlmv3", .adapter_argv0 = "materialize-layoutlmv3-checkpoint", .main_fn = materialize_layoutlmv3_checkpoint.main },
    .{ .domain = "adapter", .action = "materialize", .subject = "reranker-head", .adapter_argv0 = "materialize-reranker-head", .main_fn = materialize_reranker_head.main },
    .{ .domain = "adapter", .action = "materialize", .subject = "reranker", .adapter_argv0 = "materialize-reranker-lora", .main_fn = materialize_reranker_lora.main },

    .{ .domain = "train", .action = "run", .subject = "gemma4-lora", .adapter_argv0 = "train-eval-gemma4-lora-bundle", .main_fn = train_eval_gemma4_lora_bundle.main },
    .{ .domain = "train", .action = "run", .subject = "gliner2-lora", .adapter_argv0 = "train-eval-gliner2-lora-bundle", .main_fn = train_eval_gliner2_lora_bundle.main },
    .{ .domain = "train", .action = "run", .subject = "gliner2-autodiff", .adapter_argv0 = "train-gliner2-autodiff", .main_fn = train_gliner2_autodiff.main },
    .{ .domain = "train", .action = "run", .subject = "gliner2-boundary-head", .adapter_argv0 = "train-eval-gliner2-top-layer-boundary-head", .main_fn = train_eval_gliner2_boundary_head.main },
    .{ .domain = "train", .action = "run", .subject = "gliner2-boundary-task-head", .adapter_argv0 = "train-eval-gliner2-top-layer-boundary-task-head", .main_fn = train_eval_gliner2_boundary_task_head.main },
    .{ .domain = "train", .action = "run", .subject = "entity-cleanup-head", .adapter_argv0 = "train-eval-entity-cleanup-head", .main_fn = train_eval_entity_cleanup_head.main },
    .{ .domain = "train", .action = "run", .subject = "reranker-head", .adapter_argv0 = "train-eval-reranker-head", .main_fn = train_eval_reranker_head.main },
    .{ .domain = "train", .action = "run", .subject = "reranker-head-cached", .adapter_argv0 = "train-eval-reranker-head-cached", .main_fn = train_eval_reranker_head_cached.main },
    .{ .domain = "train", .action = "run", .subject = "reranker-head-top-layer-cached", .adapter_argv0 = "train-eval-reranker-head-top-layer-cached", .main_fn = train_eval_reranker_head_top_layer_cached.main },
    .{ .domain = "train", .action = "run", .subject = "reranker-lora-surrogate", .adapter_argv0 = "train-eval-reranker-lora-surrogate", .main_fn = train_eval_reranker_lora_surrogate.main },
    .{ .domain = "train", .action = "run", .subject = "reranker-lora-surrogate-cached", .adapter_argv0 = "train-eval-reranker-lora-surrogate-cached", .main_fn = train_eval_reranker_lora_surrogate_cached.main },
    .{ .domain = "train", .action = "run", .subject = "reranker-lora-top-layer-cached-surrogate", .adapter_argv0 = "train-eval-reranker-lora-top-layer-cached-surrogate", .main_fn = train_eval_reranker_lora_top_layer_cached_surrogate.main },
    .{ .domain = "train", .action = "run", .subject = "colqwen2-lora", .adapter_argv0 = "train-eval-colqwen2-lora-bundle", .main_fn = train_eval_colqwen2_lora_bundle.main },
    .{ .domain = "train", .action = "run", .subject = "layoutlmv3-lora-one-step", .adapter_argv0 = "train-layoutlmv3-lora-one-step", .main_fn = train_layoutlmv3_lora_one_step.main },
    .{ .domain = "train", .action = "run", .subject = "layoutlmv3-lora-sequence", .adapter_argv0 = "train-eval-layoutlmv3-lora-sequence", .main_fn = train_eval_layoutlmv3_lora_sequence.main },
    .{ .domain = "train", .action = "run", .subject = "layoutlmv3-lora-token", .adapter_argv0 = "train-eval-layoutlmv3-lora-token", .main_fn = train_eval_layoutlmv3_lora_token.main },

    .{ .domain = "eval", .action = "run", .subject = "reranker-checkpoint", .adapter_argv0 = "eval-reranker-checkpoint", .main_fn = eval_reranker_checkpoint.main },
    .{ .domain = "eval", .action = "run", .subject = "fused-chunker", .adapter_argv0 = "eval-fused-chunker", .main_fn = eval_fused_chunker.main },
    .{ .domain = "eval", .action = "run", .subject = "gliner2-boundary-head", .adapter_argv0 = "eval-gliner2-top-layer-boundary-head", .main_fn = eval_gliner2_boundary_head.main },
    .{ .domain = "eval", .action = "run", .subject = "gliner2-boundary-task-head", .adapter_argv0 = "eval-gliner2-top-layer-boundary-task-head", .main_fn = eval_gliner2_boundary_task_head.main },

    .{ .domain = "workflow", .action = "run", .subject = "gemma4-pilot", .adapter_argv0 = "run-gemma4-lora-pilot-workflow", .main_fn = run_gemma4_lora_pilot_workflow.main },
    .{ .domain = "workflow", .action = "run", .subject = "gemma4-recursive-lora-smoke", .adapter_argv0 = "run-gemma4-recursive-lora-smoke-workflow", .main_fn = run_gemma4_recursive_lora_smoke_workflow.main },
    .{ .domain = "workflow", .action = "run", .subject = "gemma4-recursive-lora-sweep", .adapter_argv0 = "run-gemma4-recursive-lora-sweep", .main_fn = run_gemma4_recursive_lora_sweep.main },
    .{ .domain = "workflow", .action = "run", .subject = "gemma4-recursive-lora-sweep-analyze", .adapter_argv0 = "analyze-gemma4-recursive-lora-sweep", .main_fn = analyze_gemma4_recursive_lora_sweep.main },
    .{ .domain = "workflow", .action = "run", .subject = "gliner2-boundary-task-head-smoke", .adapter_argv0 = "run-gliner2-boundary-task-head-smoke-workflow", .main_fn = run_gliner2_boundary_task_head_smoke_workflow.main },
    .{ .domain = "workflow", .action = "run", .subject = "gliner2-entity-cleanup-smoke", .adapter_argv0 = "run-gliner2-entity-cleanup-smoke-workflow", .main_fn = run_gliner2_entity_cleanup_smoke_workflow.main },
    .{ .domain = "workflow", .action = "run", .subject = "layoutlmv3-lora-smoke", .adapter_argv0 = "run-layoutlmv3-lora-smoke-workflow", .main_fn = run_layoutlmv3_lora_smoke_workflow.main },
};

pub fn main(init: std.process.Init, args: []const []const u8) !void {
    if (args.len == 0 or isHelp(args[0])) {
        usage();
        return;
    }

    const domain = args[0];
    if (std.mem.eql(u8, domain, "run") or std.mem.eql(u8, domain, "smoke-fast")) {
        return recipe.main(init.gpa, init.io, args);
    }

    if (std.mem.eql(u8, domain, "train") or std.mem.eql(u8, domain, "eval") or std.mem.eql(u8, domain, "workflow")) {
        if (args.len < 2) return usageError();
        if (std.mem.eql(u8, args[1], "run")) {
            if (args.len < 3) return usageError();
            return dispatch(init, domain, "run", args[2], args[3..]);
        }
        return dispatch(init, domain, "run", args[1], args[2..]);
    }

    if (args.len < 3) return usageError();
    return dispatch(init, domain, args[1], args[2], args[3..]);
}

fn dispatch(
    init: std.process.Init,
    domain: []const u8,
    action: []const u8,
    subject: []const u8,
    args: []const []const u8,
) !void {
    for (commands) |command| {
        if (std.mem.eql(u8, command.domain, domain) and
            std.mem.eql(u8, command.action, action) and
            std.mem.eql(u8, command.subject, subject))
        {
            if (std.mem.eql(u8, domain, "dataset") and std.mem.eql(u8, action, "generate")) {
                if (std.mem.eql(u8, subject, "gemma4-pilot")) {
                    var normalized = try normalizeGemma4PilotArgs(init.gpa, args);
                    defer normalized.deinit(init.gpa);
                    return runCommand(init, command.adapter_argv0, command.main_fn, normalized.items);
                }
                if (std.mem.eql(u8, subject, "gemma4-multimodal-pilot")) {
                    var normalized = try normalizeGemma4MultimodalPilotArgs(init.gpa, args);
                    defer normalized.deinit(init.gpa);
                    return runCommand(init, command.adapter_argv0, command.main_fn, normalized.items);
                }
            }
            return runCommand(init, command.adapter_argv0, command.main_fn, args);
        }
    }

    std.debug.print("unknown finetune command: {s} {s} {s}\n\n", .{ domain, action, subject });
    return usageError();
}

fn normalizeGemma4PilotArgs(allocator: std.mem.Allocator, args: []const []const u8) !std.ArrayListUnmanaged([]const u8) {
    if (args.len == 0) return usageError();

    const out = args[0];
    var count: ?[]const u8 = null;
    var split: ?[]const u8 = null;
    var positional: usize = 0;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--count")) {
            i += 1;
            if (i >= args.len) return usageError();
            count = args[i];
        } else if (std.mem.eql(u8, args[i], "--split")) {
            i += 1;
            if (i >= args.len) return usageError();
            split = args[i];
        } else if (positional == 0) {
            count = args[i];
            positional += 1;
        } else if (positional == 1) {
            split = args[i];
            positional += 1;
        } else {
            return usageError();
        }
    }

    var normalized = std.ArrayListUnmanaged([]const u8).empty;
    errdefer normalized.deinit(allocator);
    try normalized.append(allocator, out);
    if (count) |value| try normalized.append(allocator, value);
    if (split) |value| {
        if (count == null) try normalized.append(allocator, "100");
        try normalized.append(allocator, value);
    }
    return normalized;
}

fn normalizeGemma4MultimodalPilotArgs(allocator: std.mem.Allocator, args: []const []const u8) !std.ArrayListUnmanaged([]const u8) {
    if (args.len == 0) return usageError();

    const out = args[0];
    var count: ?[]const u8 = null;
    var image_path: ?[]const u8 = null;
    var split: ?[]const u8 = null;
    var positional: usize = 0;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--count")) {
            i += 1;
            if (i >= args.len) return usageError();
            count = args[i];
        } else if (std.mem.eql(u8, args[i], "--image-path")) {
            i += 1;
            if (i >= args.len) return usageError();
            image_path = args[i];
        } else if (std.mem.eql(u8, args[i], "--split")) {
            i += 1;
            if (i >= args.len) return usageError();
            split = args[i];
        } else if (positional == 0) {
            count = args[i];
            positional += 1;
        } else if (positional == 1) {
            image_path = args[i];
            positional += 1;
        } else if (positional == 2) {
            split = args[i];
            positional += 1;
        } else {
            return usageError();
        }
    }

    const image = image_path orelse return usageError();
    var normalized = std.ArrayListUnmanaged([]const u8).empty;
    errdefer normalized.deinit(allocator);
    try normalized.append(allocator, out);
    try normalized.append(allocator, count orelse "100");
    try normalized.append(allocator, image);
    if (split) |value| try normalized.append(allocator, value);
    return normalized;
}

fn runCommand(init: std.process.Init, argv0: []const u8, main_fn: CommandMain, args: []const []const u8) !void {
    if (builtin.os.tag == .windows) {
        @compileError("termite finetune command dispatch needs Windows Args vector construction");
    }

    const allocator = init.gpa;
    var owned = try allocator.alloc([:0]u8, args.len + 1);
    defer {
        for (owned) |arg| allocator.free(arg);
        allocator.free(owned);
    }
    var vector = try allocator.alloc([*:0]const u8, args.len + 1);
    defer allocator.free(vector);

    owned[0] = try allocator.dupeSentinel(u8, argv0, 0);
    vector[0] = owned[0].ptr;
    for (args, 0..) |arg, idx| {
        owned[idx + 1] = try allocator.dupeSentinel(u8, arg, 0);
        vector[idx + 1] = owned[idx + 1].ptr;
    }

    var command_init = init;
    command_init.minimal.args = .{ .vector = vector };
    return main_fn(command_init);
}

fn isHelp(arg: []const u8) bool {
    return std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "help");
}

fn usageError() error{InvalidArguments} {
    usage();
    return error.InvalidArguments;
}

fn usage() void {
    std.debug.print(
        \\usage:
        \\  antfly inference finetune run <recipe.json> [--dry-run]
        \\  antfly inference finetune smoke-fast [--out-root <path>]
        \\  antfly inference finetune dataset inspect <gliner2|reranker> ...
        \\  antfly inference finetune dataset generate <gemma4-pilot|gemma4-multimodal-pilot> ...
        \\  antfly inference finetune dataset prepare <family-or-cache> ...
        \\  antfly inference finetune dataset materialize gemma4-teacher-targets ...
        \\  antfly inference finetune adapter bootstrap <family> ...
        \\  antfly inference finetune adapter inspect <family> ...
        \\  antfly inference finetune adapter materialize <family> ...
        \\  antfly inference finetune adapter compose lora ...
        \\  antfly inference finetune train <task> ...
        \\  antfly inference finetune eval <task> ...
        \\  antfly inference finetune workflow <workflow> ...
        \\
        \\examples:
        \\  antfly inference finetune run /tmp/recipe.json
        \\  antfly inference finetune dataset generate gemma4-pilot /tmp/pilot.jsonl --count 1000 --split train
        \\  antfly inference finetune dataset prepare gemma4-lora /models/gemma4 /tmp/pilot.jsonl train /tmp/prepared.json
        \\  antfly inference finetune adapter bootstrap gemma4 /models/gemma4 /tmp/adapter --rank 16 --alpha 32 --target-preset all-linear
        \\  antfly inference finetune train gemma4-lora /models/gemma4 /tmp/adapter /tmp/prepared.json /tmp/out --trainer autodiff
        \\  antfly inference finetune workflow gemma4-pilot text /models/gemma4 /tmp/pilot-run --count 1000 --backend mlx
        \\
    , .{});
}

test "finetune cli command table has entries" {
    try std.testing.expect(commands.len > 0);
}

test "finetune cli command table has unique canonical commands and adapter argv labels" {
    for (commands, 0..) |command, idx| {
        try std.testing.expect(command.domain.len > 0);
        try std.testing.expect(command.action.len > 0);
        try std.testing.expect(command.subject.len > 0);
        try std.testing.expect(command.adapter_argv0.len > 0);

        for (commands[idx + 1 ..]) |other| {
            const same_canonical = std.mem.eql(u8, command.domain, other.domain) and
                std.mem.eql(u8, command.action, other.action) and
                std.mem.eql(u8, command.subject, other.subject);
            try std.testing.expect(!same_canonical);
            try std.testing.expect(!std.mem.eql(u8, command.adapter_argv0, other.adapter_argv0));
        }
    }
}
