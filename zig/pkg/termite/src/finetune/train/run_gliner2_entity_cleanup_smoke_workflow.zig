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
const termite = @import("termite_internal");
const compat = termite.io.compat;
const gliner2 = termite.finetune.gliner2;
const cleanup_data = termite.finetune.entity_cleanup_data;
const cleanup_gliner_cache = termite.finetune.entity_cleanup_gliner_cache;
const cleanup_model = termite.finetune.entity_cleanup_model;
const text_encoder_boundary = termite.finetune.text_encoder_boundary;
const run_contract = termite.run.contract;
const artifact_writer = termite.run.artifact_writer;

const WorkflowSummary = struct {
    artifact_family_version: []const u8,
    cleanup_artifact_family_version: []const u8,
    model_dir: []const u8,
    adapter_dir: []const u8,
    train_input: []const u8,
    eval_input: []const u8,
    train_split: ?[]const u8 = null,
    eval_split: ?[]const u8 = null,
    output_root: []const u8,
    requested_backend: []const u8,
    train_cache_path: []const u8,
    eval_cache_path: []const u8,
    trained_dir: []const u8,
    materialized_dir: []const u8,
    train_dataset_stats: cleanup_data.Stats,
    eval_dataset_stats: cleanup_data.Stats,
    train_used_stats: cleanup_data.Stats,
    eval_used_stats: cleanup_data.Stats,
    train_cache_feature_dim: usize,
    eval_cache_feature_dim: usize,
    train_cache_mentions: usize,
    eval_cache_mentions: usize,
    initial_cleanup_head_present: bool,
    trained_cleanup_head_present: bool,
    materialized_cleanup_head_present: bool,
    materialize_copied_cleanup_head: bool,
    cleanup_train_eval: cleanup_model.TrainEvalSummary,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usageError();
    const adapter_dir = args.next() orelse return usageError();
    const train_input = args.next() orelse return usageError();
    const eval_input = args.next() orelse return usageError();
    const output_root = args.next() orelse return usageError();
    const train_split_arg = args.next() orelse "none";
    const eval_split_arg = args.next() orelse "none";
    const backend = parseBackend(args.next() orelse "native") orelse return error.InvalidBackend;
    const max_train_examples = try std.fmt.parseUnsigned(usize, args.next() orelse "64", 10);
    const max_eval_examples = try std.fmt.parseUnsigned(usize, args.next() orelse "32", 10);
    const max_length = try std.fmt.parseUnsigned(usize, args.next() orelse "256", 10);
    const max_span_width = try std.fmt.parseUnsigned(usize, args.next() orelse "8", 10);
    const top_layer_count = try std.fmt.parseUnsigned(usize, args.next() orelse "1", 10);
    const epochs = try std.fmt.parseUnsigned(usize, args.next() orelse "3", 10);
    const learning_rate = try std.fmt.parseFloat(f32, args.next() orelse "0.05");
    const embedding_learning_rate = try std.fmt.parseFloat(f32, args.next() orelse "0.01");
    const embedding_dim = try std.fmt.parseUnsigned(usize, args.next() orelse "32", 10);

    const train_split = if (std.mem.eql(u8, train_split_arg, "none")) null else train_split_arg;
    const eval_split = if (std.mem.eql(u8, eval_split_arg, "none")) null else eval_split_arg;

    try compat.cwd().createDirPath(compat.io(), output_root);
    const train_cache_path = try std.fs.path.join(allocator, &.{ output_root, "train_cleanup_cache.json" });
    defer allocator.free(train_cache_path);
    const eval_cache_path = try std.fs.path.join(allocator, &.{ output_root, "eval_cleanup_cache.json" });
    defer allocator.free(eval_cache_path);
    const trained_dir = try std.fs.path.join(allocator, &.{ output_root, "trained" });
    defer allocator.free(trained_dir);
    const materialized_dir = try std.fs.path.join(allocator, &.{ output_root, "materialized" });
    defer allocator.free(materialized_dir);
    const workflow_report_path = try std.fs.path.join(allocator, &.{ output_root, "smoke_workflow_report.json" });
    defer allocator.free(workflow_report_path);
    const training_config_path = try std.fs.path.join(allocator, &.{ output_root, "training_config.json" });
    defer allocator.free(training_config_path);
    const training_report_path = try std.fs.path.join(allocator, &.{ output_root, "training_report.json" });
    defer allocator.free(training_report_path);
    const run_status_path = try std.fs.path.join(allocator, &.{ output_root, "run_status.json" });
    defer allocator.free(run_status_path);

    try artifact_writer.writeJsonFile(allocator, training_config_path, .{
        .contract_version = run_contract.training_config_version,
        .artifact_family_version = gliner2.artifact_family_version,
        .task = "gliner2_entity_cleanup_smoke_workflow",
        .run_plan = .{
            .workflow = "bounded_native_smoke",
            .requested_backend = @tagName(backend),
        },
        .inputs = .{
            .model_dir = model_dir,
            .adapter_dir = adapter_dir,
            .train_input = train_input,
            .eval_input = eval_input,
            .train_split = train_split,
            .eval_split = eval_split,
        },
        .training = .{
            .max_train_examples = max_train_examples,
            .max_eval_examples = max_eval_examples,
            .max_length = max_length,
            .max_span_width = max_span_width,
            .top_layer_count = top_layer_count,
            .epochs = epochs,
            .learning_rate = learning_rate,
            .embedding_learning_rate = embedding_learning_rate,
            .embedding_dim = embedding_dim,
        },
        .output_root = output_root,
    });
    try artifact_writer.writeJsonFile(allocator, run_status_path, .{
        .contract_version = run_contract.run_status_version,
        .status = "running",
        .task = "gliner2_entity_cleanup_smoke_workflow",
        .out_dir = output_root,
        .resume_from = @as(?[]const u8, null),
        .actions = @as(?[]const u8, null),
        .derived = .{
            .outcome_code = "running",
            .alerts = [_]u8{},
            .metric_summary = .{
                .requested_backend = @tagName(backend),
            },
        },
        .artifacts = .{
            .report = training_report_path,
            .best = trained_dir,
            .latest = trained_dir,
            .final = materialized_dir,
        },
    });
    errdefer artifact_writer.writeJsonFile(allocator, run_status_path, .{
        .contract_version = run_contract.run_status_version,
        .status = "failed",
        .task = "gliner2_entity_cleanup_smoke_workflow",
        .out_dir = output_root,
        .resume_from = @as(?[]const u8, null),
        .actions = @as(?[]const u8, null),
        .derived = .{
            .outcome_code = "failed",
            .alerts = [_]u8{},
            .metric_summary = .{
                .requested_backend = @tagName(backend),
            },
        },
        .artifacts = .{
            .report = training_report_path,
            .best = trained_dir,
            .latest = trained_dir,
            .final = materialized_dir,
        },
    }) catch {};

    var train_loaded = try cleanup_data.loadExamples(allocator, train_input, train_split);
    defer train_loaded.deinit();
    var eval_loaded = try cleanup_data.loadExamples(allocator, eval_input, eval_split);
    defer eval_loaded.deinit();
    const train_dataset_stats = cleanup_data.computeStats(train_loaded.examples);
    const eval_dataset_stats = cleanup_data.computeStats(eval_loaded.examples);
    const train_used_examples = train_loaded.examples[0..@min(train_loaded.examples.len, max_train_examples)];
    const eval_used_examples = eval_loaded.examples[0..@min(eval_loaded.examples.len, max_eval_examples)];
    const train_used_stats = cleanup_data.computeStats(train_used_examples);
    const eval_used_stats = cleanup_data.computeStats(eval_used_examples);

    var initial_inspect = try gliner2.inspectLoRABundle(allocator, model_dir, adapter_dir);
    defer gliner2.freeLoRABundleInspectionSummary(allocator, &initial_inspect);

    var train_cache = try cleanup_gliner_cache.prepareCachedSummary(
        allocator,
        model_dir,
        train_input,
        train_split,
        train_loaded.examples,
        backend,
        max_train_examples,
        max_length,
        max_span_width,
        top_layer_count,
    );
    defer cleanup_model.freeCachedSummary(allocator, &train_cache);
    try cleanup_model.saveCachedSummary(allocator, train_cache_path, train_cache);

    var eval_cache = try cleanup_gliner_cache.prepareCachedSummary(
        allocator,
        model_dir,
        eval_input,
        eval_split,
        eval_loaded.examples,
        backend,
        max_eval_examples,
        max_length,
        max_span_width,
        top_layer_count,
    );
    defer cleanup_model.freeCachedSummary(allocator, &eval_cache);
    try cleanup_model.saveCachedSummary(allocator, eval_cache_path, eval_cache);

    var bundle = try gliner2.loadLoRABundle(allocator, model_dir, adapter_dir);
    defer bundle.deinit();
    try gliner2.saveLoRABundle(&bundle, trained_dir);

    const cleanup_train_eval = try cleanup_model.trainEvalCached(allocator, &train_cache, &eval_cache, trained_dir, .{
        .epochs = epochs,
        .learning_rate = learning_rate,
        .embedding_learning_rate = embedding_learning_rate,
        .embedding_dim = embedding_dim,
    });

    var trained_inspect = try gliner2.inspectLoRABundle(allocator, model_dir, trained_dir);
    defer gliner2.freeLoRABundleInspectionSummary(allocator, &trained_inspect);

    var materialize = try gliner2.materializeMergedModel(allocator, model_dir, trained_dir, materialized_dir);
    defer freeMaterializeSummary(allocator, &materialize);

    var summary = WorkflowSummary{
        .artifact_family_version = try allocator.dupe(u8, gliner2.artifact_family_version),
        .cleanup_artifact_family_version = try allocator.dupe(u8, cleanup_model.artifact_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .adapter_dir = try allocator.dupe(u8, adapter_dir),
        .train_input = try allocator.dupe(u8, train_input),
        .eval_input = try allocator.dupe(u8, eval_input),
        .train_split = if (train_split) |value| try allocator.dupe(u8, value) else null,
        .eval_split = if (eval_split) |value| try allocator.dupe(u8, value) else null,
        .output_root = try allocator.dupe(u8, output_root),
        .requested_backend = try allocator.dupe(u8, @tagName(backend)),
        .train_cache_path = try allocator.dupe(u8, train_cache_path),
        .eval_cache_path = try allocator.dupe(u8, eval_cache_path),
        .trained_dir = try allocator.dupe(u8, trained_dir),
        .materialized_dir = try allocator.dupe(u8, materialized_dir),
        .train_dataset_stats = train_dataset_stats,
        .eval_dataset_stats = eval_dataset_stats,
        .train_used_stats = train_used_stats,
        .eval_used_stats = eval_used_stats,
        .train_cache_feature_dim = train_cache.feature_dim,
        .eval_cache_feature_dim = eval_cache.feature_dim,
        .train_cache_mentions = train_cache.mentions.len,
        .eval_cache_mentions = eval_cache.mentions.len,
        .initial_cleanup_head_present = initial_inspect.cleanup_head_present,
        .trained_cleanup_head_present = trained_inspect.cleanup_head_present,
        .materialized_cleanup_head_present = materialize.copied_cleanup_head,
        .materialize_copied_cleanup_head = materialize.copied_cleanup_head,
        .cleanup_train_eval = cleanup_train_eval,
    };
    defer freeWorkflowSummary(allocator, &summary);

    try artifact_writer.writeJsonFile(allocator, workflow_report_path, summary);
    try artifact_writer.writeJsonFile(allocator, training_report_path, .{
        .contract_version = run_contract.training_report_version,
        .artifact_family_version = gliner2.artifact_family_version,
        .task = "gliner2_entity_cleanup_smoke_workflow",
        .backend_policy = .{
            .selected = @tagName(backend),
            .preferred = @tagName(backend),
        },
        .summary = summary,
    });
    try artifact_writer.writeJsonFile(allocator, run_status_path, .{
        .contract_version = run_contract.run_status_version,
        .status = "completed",
        .task = "gliner2_entity_cleanup_smoke_workflow",
        .out_dir = output_root,
        .resume_from = @as(?[]const u8, null),
        .actions = @as(?[]const u8, null),
        .derived = .{
            .outcome_code = "completed",
            .alerts = [_]u8{},
            .metric_summary = .{
                .requested_backend = @tagName(backend),
                .train_examples = summary.train_used_stats.num_examples,
                .eval_examples = summary.eval_used_stats.num_examples,
                .train_dataset_examples = summary.train_dataset_stats.num_examples,
                .eval_dataset_examples = summary.eval_dataset_stats.num_examples,
                .trained_cleanup_head_present = summary.trained_cleanup_head_present,
                .materialized_cleanup_head_present = summary.materialized_cleanup_head_present,
            },
        },
        .artifacts = .{
            .report = training_report_path,
            .best = trained_dir,
            .latest = trained_dir,
            .final = materialized_dir,
        },
    });

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseBackend(value: []const u8) ?text_encoder_boundary.BackendChoice {
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    if (std.mem.eql(u8, value, "auto")) return .auto;
    return null;
}

fn freeMaterializeSummary(allocator: std.mem.Allocator, summary: *gliner2.MaterializeSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.base_model_dir);
    allocator.free(summary.adapter_model_dir);
    allocator.free(summary.output_dir);
    allocator.free(summary.output_checkpoint_path);
    summary.* = undefined;
}

fn freeWorkflowSummary(allocator: std.mem.Allocator, summary: *WorkflowSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.cleanup_artifact_family_version);
    allocator.free(summary.model_dir);
    allocator.free(summary.adapter_dir);
    allocator.free(summary.train_input);
    allocator.free(summary.eval_input);
    if (summary.train_split) |value| allocator.free(value);
    if (summary.eval_split) |value| allocator.free(value);
    allocator.free(summary.output_root);
    allocator.free(summary.requested_backend);
    allocator.free(summary.train_cache_path);
    allocator.free(summary.eval_cache_path);
    allocator.free(summary.trained_dir);
    allocator.free(summary.materialized_dir);
    summary.* = undefined;
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: run-gliner2-entity-cleanup-smoke-workflow <model_dir> <adapter_dir> <train_jsonl_or_dir> <eval_jsonl_or_dir> <output_root> [train_split] [eval_split] [backend] [max_train_examples] [max_eval_examples] [max_length] [max_span_width] [top_layer_count] [epochs] [learning_rate] [embedding_learning_rate] [embedding_dim]
        \\example: run-gliner2-entity-cleanup-smoke-workflow /tmp/gliner2_base /tmp/gliner2_adapter /tmp/train_cleanup.jsonl /tmp/eval_cleanup.jsonl /tmp/out train eval native 64 32 256 8 1 3 0.05 0.01 32
        \\
    , .{});
    return error.InvalidArguments;
}
