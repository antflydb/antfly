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
const recipe_mod = @import("inference_internal").finetune.recipe;

test "GLiNER2 recipe plans the production lifecycle" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const recipe = recipe_mod.Recipe{
        .recipe = "lora-sft",
        .model = .{ .path = "/models/gliner2", .family = "gliner2" },
        .dataset = .{
            .train_path = "/data/train.jsonl",
            .labels = "person,organization",
            .max_seq_len = 128,
            .eval_max_examples = 20,
        },
        .optimizer = .{ .gradient_accumulation_steps = 4 },
        .eval = .{
            .path = "/data/eval.jsonl",
            .every_epochs = 2,
            .batch_size = 8,
            .early_stopping_patience = 3,
            .improvement_threshold = 0.001,
            .entity_minimums = .{
                .precision = 0.5,
                .recall = 0.4,
                .f1 = 0.45,
                .exact_match = 0.25,
            },
            .full_task_minimums = .{
                .classifications_micro_f1 = 0.6,
                .classifications_exact_match = 0.5,
                .json_structures_micro_f1 = 0.55,
                .json_structures_exact_match = 0.4,
                .relations_micro_f1 = 0.5,
                .relations_exact_match = 0.35,
                .count_accuracy = 0.7,
            },
        },
        .checkpoint = .{
            .every_epochs = 2,
            .keep_last = 4,
            .resume_path = "/runs/prior/checkpoints/epoch-4.safetensors",
        },
        .runtime = .{
            .compiled_required = true,
            .graph_cache_capacity = 4,
        },
        .backend = "metal",
        .artifacts = .{
            .root = "/runs/gliner2",
            .trained_adapter_dir = "/runs/gliner2/adapter",
            .materialized_dir = "/runs/gliner2/model",
            .validation_report_path = "/runs/gliner2/validation.json",
            .evaluation_report_path = "/runs/gliner2/eval.json",
            .reload_report_path = "/runs/gliner2/reload.json",
        },
    };

    const plan = try recipe_mod.buildPlan(allocator, recipe);
    defer recipe_mod.freePlan(allocator, plan);
    const expected_commands = [_][]const u8{
        "train-gliner2-autodiff",
        "validate-gliner2-autodiff-run",
        "eval-gliner2-autodiff-adapter-dataset",
        "materialize-gliner2-lora",
        "inspect-gliner2-checkpoint",
    };
    try std.testing.expectEqual(expected_commands.len, plan.steps.len);
    for (expected_commands, 0..) |expected, idx| {
        try std.testing.expectEqualStrings(expected, plan.steps[idx].argv[0]);
    }
    try expectArgValue(plan.steps[0].argv, "--eval-data", "/data/eval.jsonl");
    try expectArgValue(plan.steps[0].argv, "--checkpoint-every-epochs", "2");
    try expectArgValue(plan.steps[0].argv, "--resume-checkpoint", "/runs/prior/checkpoints/epoch-4.safetensors");
    try expectArgValue(plan.steps[0].argv, "--graph-cache-capacity", "4");
    try expectArg(plan.steps[0].argv, "--compiled-required");
    try std.testing.expectEqualStrings("/runs/gliner2/validation.json", plan.steps[1].argv[3]);
    try std.testing.expectEqualStrings("/runs/gliner2/eval.json", plan.steps[2].argv[8]);
    try expectArgValue(plan.steps[2].argv, "--backend", "native");
    try expectArgValue(plan.steps[2].argv, "--min-precision", "0.5");
    try expectArgValue(plan.steps[2].argv, "--min-recall", "0.4");
    try expectArgValue(plan.steps[2].argv, "--min-f1", "0.45");
    try expectArgValue(plan.steps[2].argv, "--min-exact-match", "0.25");
    try expectArgPair(plan.steps[2].argv, "--min-task-metric", "classifications.micro_f1=0.6");
    try expectArgPair(plan.steps[2].argv, "--min-task-metric", "classifications.exact_match=0.5");
    try expectArgPair(plan.steps[2].argv, "--min-task-metric", "json_structures.micro_f1=0.55");
    try expectArgPair(plan.steps[2].argv, "--min-task-metric", "json_structures.exact_match=0.4");
    try expectArgPair(plan.steps[2].argv, "--min-task-metric", "relations.micro_f1=0.5");
    try expectArgPair(plan.steps[2].argv, "--min-task-metric", "relations.exact_match=0.35");
    try expectArgPair(plan.steps[2].argv, "--min-task-metric", "count.accuracy=0.7");
    try std.testing.expectEqualStrings("/runs/gliner2/model", plan.steps[3].argv[3]);
    try std.testing.expectEqualStrings("/runs/gliner2/reload.json", plan.steps[4].argv[3]);
}

test "GLiNER2 recipe rejects ungated heldout evaluation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const recipe = recipe_mod.Recipe{
        .recipe = "lora-sft",
        .model = .{ .path = "/models/gliner2", .family = "gliner2" },
        .dataset = .{ .train_path = "/data/train.jsonl" },
        .eval = .{ .path = "/data/eval.jsonl" },
    };
    try std.testing.expectError(error.MissingFullTaskQualityThreshold, recipe_mod.buildPlan(allocator, recipe));
}

test "GLiNER2 recipe rejects heldout evaluation without entity release floors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const recipe = recipe_mod.Recipe{
        .recipe = "lora-sft",
        .model = .{ .path = "/models/gliner2", .family = "gliner2" },
        .dataset = .{ .train_path = "/data/train.jsonl" },
        .eval = .{
            .path = "/data/eval.jsonl",
            .full_task_minimums = .{
                .classifications_micro_f1 = 0.6,
                .classifications_exact_match = 0.5,
                .json_structures_micro_f1 = 0.55,
                .json_structures_exact_match = 0.4,
                .relations_micro_f1 = 0.5,
                .relations_exact_match = 0.35,
                .count_accuracy = 0.7,
            },
        },
    };
    try std.testing.expectError(error.MissingEntityQualityThreshold, recipe_mod.buildPlan(allocator, recipe));
}

fn expectArgValue(args: []const []const u8, flag: []const u8, expected: []const u8) !void {
    for (args[0..args.len -| 1], 0..) |arg, idx| {
        if (std.mem.eql(u8, arg, flag)) {
            try std.testing.expectEqualStrings(expected, args[idx + 1]);
            return;
        }
    }
    return error.MissingExpectedArgument;
}

fn expectArg(args: []const []const u8, expected: []const u8) !void {
    for (args) |arg| {
        if (std.mem.eql(u8, arg, expected)) return;
    }
    return error.MissingExpectedArgument;
}

fn expectArgPair(args: []const []const u8, flag: []const u8, expected: []const u8) !void {
    for (args[0..args.len -| 1], 0..) |arg, idx| {
        if (std.mem.eql(u8, arg, flag) and std.mem.eql(u8, args[idx + 1], expected)) return;
    }
    return error.MissingExpectedArgument;
}
