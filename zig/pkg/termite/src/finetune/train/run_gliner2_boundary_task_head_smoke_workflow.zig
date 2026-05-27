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
const gliner2_boundary = termite.finetune.gliner2_boundary;
const gliner2_data = termite.finetune.gliner2_data;
const text_encoder_boundary = termite.finetune.text_encoder_boundary;
const run_contract = termite.run.contract;
const artifact_writer = termite.run.artifact_writer;

const WorkflowSummary = struct {
    artifact_family_version: []const u8,
    model_dir: []const u8,
    adapter_dir: []const u8,
    train_input: []const u8,
    eval_input: []const u8,
    train_split: ?[]const u8 = null,
    eval_split: ?[]const u8 = null,
    output_root: []const u8,
    requested_backend: []const u8,
    entity_types: []const []const u8,
    train_cache_path: []const u8,
    eval_cache_path: []const u8,
    trained_dir: []const u8,
    materialized_dir: []const u8,
    train_stats: gliner2_data.DatasetStats,
    eval_stats: gliner2_data.DatasetStats,
    initial_inspect: gliner2.LoRABundleInspectionSummary,
    train_cache: gliner2_boundary.CachedBoundarySummary,
    eval_cache: gliner2_boundary.CachedBoundarySummary,
    task_head: gliner2_boundary.TaskHeadTrainEvalSummary,
    trained_inspect: gliner2.LoRABundleInspectionSummary,
    materialize: gliner2.MaterializeSummary,
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
    const entity_types_csv = args.next() orelse return usageError();
    const output_root = args.next() orelse return usageError();
    const train_split_arg = args.next() orelse "none";
    const eval_split_arg = args.next() orelse "none";
    const backend = parseBackend(args.next() orelse "native") orelse return error.InvalidBackend;
    const max_train_examples = try std.fmt.parseUnsigned(usize, args.next() orelse "64", 10);
    const max_eval_examples = try std.fmt.parseUnsigned(usize, args.next() orelse "32", 10);
    const max_length = try std.fmt.parseUnsigned(usize, args.next() orelse "256", 10);
    const max_span_width = try std.fmt.parseUnsigned(usize, args.next() orelse "8", 10);
    const top_layer_count = try std.fmt.parseUnsigned(usize, args.next() orelse "1", 10);
    const learning_rate = try std.fmt.parseFloat(f32, args.next() orelse "0.001");
    const epochs = try std.fmt.parseUnsigned(usize, args.next() orelse "2", 10);

    const train_split = if (std.mem.eql(u8, train_split_arg, "none")) null else train_split_arg;
    const eval_split = if (std.mem.eql(u8, eval_split_arg, "none")) null else eval_split_arg;

    const entity_types = try parseEntityTypesCsv(allocator, entity_types_csv);
    defer freeStringSlice(allocator, entity_types);
    if (entity_types.len == 0) return error.EmptyEntityTypes;

    try compat.cwd().createDirPath(compat.io(), output_root);
    const train_cache_path = try std.fs.path.join(allocator, &.{ output_root, "train_boundary_cache.json" });
    defer allocator.free(train_cache_path);
    const eval_cache_path = try std.fs.path.join(allocator, &.{ output_root, "eval_boundary_cache.json" });
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
        .task = "gliner2_boundary_task_head_smoke_workflow",
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
        .entity_types = entity_types,
        .training = .{
            .max_train_examples = max_train_examples,
            .max_eval_examples = max_eval_examples,
            .max_length = max_length,
            .max_span_width = max_span_width,
            .top_layer_count = top_layer_count,
            .learning_rate = learning_rate,
            .epochs = epochs,
        },
        .output_root = output_root,
    });
    try artifact_writer.writeJsonFile(allocator, run_status_path, .{
        .contract_version = run_contract.run_status_version,
        .status = "running",
        .task = "gliner2_boundary_task_head_smoke_workflow",
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
        .task = "gliner2_boundary_task_head_smoke_workflow",
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

    var train_loaded = try gliner2_data.loadExamples(allocator, train_input, train_split);
    defer train_loaded.deinit();
    var eval_loaded = try gliner2_data.loadExamples(allocator, eval_input, eval_split);
    defer eval_loaded.deinit();
    const train_stats = try gliner2_data.computeStats(allocator, train_loaded.examples);
    const eval_stats = try gliner2_data.computeStats(allocator, eval_loaded.examples);

    var initial_inspect = try gliner2.inspectLoRABundle(allocator, model_dir, adapter_dir);
    errdefer gliner2.freeLoRABundleInspectionSummary(allocator, &initial_inspect);

    var train_cache = try gliner2_boundary.prepareCachedBoundarySummary(
        allocator,
        model_dir,
        train_input,
        train_split,
        train_loaded.examples,
        entity_types,
        backend,
        max_train_examples,
        max_length,
        max_span_width,
        top_layer_count,
    );
    errdefer gliner2_boundary.freeCachedBoundarySummary(allocator, &train_cache);
    try gliner2_boundary.saveCachedBoundarySummary(allocator, train_cache_path, train_cache);

    var eval_cache = try gliner2_boundary.prepareCachedBoundarySummary(
        allocator,
        model_dir,
        eval_input,
        eval_split,
        eval_loaded.examples,
        entity_types,
        backend,
        max_eval_examples,
        max_length,
        max_span_width,
        top_layer_count,
    );
    errdefer gliner2_boundary.freeCachedBoundarySummary(allocator, &eval_cache);
    try gliner2_boundary.saveCachedBoundarySummary(allocator, eval_cache_path, eval_cache);

    var bundle = try gliner2.loadLoRABundle(allocator, model_dir, adapter_dir);
    defer bundle.deinit();
    try gliner2.saveLoRABundle(&bundle, trained_dir);

    var task_head = try gliner2_boundary.trainEvalBoundaryTaskHead(
        allocator,
        model_dir,
        &train_cache,
        &eval_cache,
        backend,
        trained_dir,
        .{ .learning_rate = learning_rate, .epochs = epochs },
    );
    errdefer gliner2_boundary.freeTaskHeadTrainEvalSummary(allocator, &task_head);

    var trained_inspect = try gliner2.inspectLoRABundle(allocator, model_dir, trained_dir);
    errdefer gliner2.freeLoRABundleInspectionSummary(allocator, &trained_inspect);

    var materialize = try gliner2.materializeMergedModel(allocator, model_dir, trained_dir, materialized_dir);
    errdefer freeMaterializeSummary(allocator, &materialize);

    var summary = WorkflowSummary{
        .artifact_family_version = try allocator.dupe(u8, gliner2.artifact_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .adapter_dir = try allocator.dupe(u8, adapter_dir),
        .train_input = try allocator.dupe(u8, train_input),
        .eval_input = try allocator.dupe(u8, eval_input),
        .train_split = if (train_split) |value| try allocator.dupe(u8, value) else null,
        .eval_split = if (eval_split) |value| try allocator.dupe(u8, value) else null,
        .output_root = try allocator.dupe(u8, output_root),
        .requested_backend = try allocator.dupe(u8, @tagName(backend)),
        .entity_types = try dupeStringSlice(allocator, entity_types),
        .train_cache_path = try allocator.dupe(u8, train_cache_path),
        .eval_cache_path = try allocator.dupe(u8, eval_cache_path),
        .trained_dir = try allocator.dupe(u8, trained_dir),
        .materialized_dir = try allocator.dupe(u8, materialized_dir),
        .train_stats = train_stats,
        .eval_stats = eval_stats,
        .initial_inspect = initial_inspect,
        .train_cache = train_cache,
        .eval_cache = eval_cache,
        .task_head = task_head,
        .trained_inspect = trained_inspect,
        .materialize = materialize,
    };
    defer freeWorkflowSummary(allocator, &summary);

    try artifact_writer.writeJsonFile(allocator, workflow_report_path, summary);
    try artifact_writer.writeJsonFile(allocator, training_report_path, .{
        .contract_version = run_contract.training_report_version,
        .artifact_family_version = gliner2.artifact_family_version,
        .task = "gliner2_boundary_task_head_smoke_workflow",
        .backend_policy = .{
            .selected = @tagName(backend),
            .preferred = @tagName(backend),
        },
        .summary = summary,
    });
    try artifact_writer.writeJsonFile(allocator, run_status_path, .{
        .contract_version = run_contract.run_status_version,
        .status = "completed",
        .task = "gliner2_boundary_task_head_smoke_workflow",
        .out_dir = output_root,
        .resume_from = @as(?[]const u8, null),
        .actions = @as(?[]const u8, null),
        .derived = .{
            .outcome_code = "completed",
            .alerts = [_]u8{},
            .metric_summary = .{
                .requested_backend = @tagName(backend),
                .train_examples = summary.train_stats.num_examples,
                .eval_examples = summary.eval_stats.num_examples,
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
    if (std.mem.eql(u8, value, "blas")) return .native;
    if (std.mem.eql(u8, value, "mlx")) return .mlx;
    if (std.mem.eql(u8, value, "auto")) return .auto;
    return null;
}

fn parseEntityTypesCsv(allocator: std.mem.Allocator, csv: []const u8) ![][]const u8 {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (parts.items) |item| allocator.free(item);
        parts.deinit(allocator);
    }
    var iter = std.mem.splitScalar(u8, csv, ',');
    while (iter.next()) |item| {
        const trimmed = std.mem.trim(u8, item, " \t\r\n");
        if (trimmed.len == 0) continue;
        try parts.append(allocator, try allocator.dupe(u8, trimmed));
    }
    return try parts.toOwnedSlice(allocator);
}

fn freeStringSlice(allocator: std.mem.Allocator, items: []const []const u8) void {
    for (items) |item| allocator.free(item);
    allocator.free(items);
}

fn dupeStringSlice(allocator: std.mem.Allocator, items: []const []const u8) ![][]const u8 {
    const out = try allocator.alloc([]const u8, items.len);
    errdefer allocator.free(out);
    for (items, 0..) |item, idx| out[idx] = try allocator.dupe(u8, item);
    return out;
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
    allocator.free(summary.model_dir);
    allocator.free(summary.adapter_dir);
    allocator.free(summary.train_input);
    allocator.free(summary.eval_input);
    if (summary.train_split) |value| allocator.free(value);
    if (summary.eval_split) |value| allocator.free(value);
    allocator.free(summary.output_root);
    allocator.free(summary.requested_backend);
    freeStringSlice(allocator, summary.entity_types);
    allocator.free(summary.train_cache_path);
    allocator.free(summary.eval_cache_path);
    allocator.free(summary.trained_dir);
    allocator.free(summary.materialized_dir);
    gliner2.freeLoRABundleInspectionSummary(allocator, &summary.initial_inspect);
    gliner2_boundary.freeCachedBoundarySummary(allocator, &summary.train_cache);
    gliner2_boundary.freeCachedBoundarySummary(allocator, &summary.eval_cache);
    gliner2_boundary.freeTaskHeadTrainEvalSummary(allocator, &summary.task_head);
    gliner2.freeLoRABundleInspectionSummary(allocator, &summary.trained_inspect);
    freeMaterializeSummary(allocator, &summary.materialize);
    summary.* = undefined;
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: run-gliner2-boundary-task-head-smoke-workflow <model_dir> <adapter_dir> <train_jsonl_or_dir> <eval_jsonl_or_dir> <entity_types_csv> <output_root> [train_split] [eval_split] [backend] [max_train_examples] [max_eval_examples] [max_length] [max_span_width] [top_layer_count] [learning_rate] [epochs]
        \\example: run-gliner2-boundary-task-head-smoke-workflow /tmp/gliner2_base /tmp/gliner2_adapter /tmp/train /tmp/eval person,organization,location /tmp/out train eval native 64 32 256 8 1 0.001 2
        \\
    , .{});
    return error.InvalidArguments;
}
