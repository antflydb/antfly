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
const compat = @import("../io/compat.zig");
const gliner2 = @import("gliner2.zig");
const safetensors = @import("../models/safetensors.zig");

pub const manifest_file_name = "training_manifest.json";
pub const metrics_file_name = "training_metrics.jsonl";
pub const adapter_file_suffix = ".bin";
pub const expected_manifest_schema_version = "gliner2_autodiff_training/v1";
pub const expected_artifact_family_version = "gliner2_autodiff_adapter/v1";

pub const ValidationOptions = struct {
    require_loss_decrease: bool = false,
    min_supervised_tokens_per_second: ?f64 = null,
    max_avg_step_wall_ms: ?f64 = null,
    max_total_execute_ms: ?f64 = null,
    max_peak_resident_bytes: ?usize = null,
    min_examples: ?usize = null,
    min_steps: ?usize = null,
    min_entity_labels: ?usize = null,
    min_supervised_tokens: ?usize = null,
    min_entity_tokens: ?usize = null,
};

pub const RunValidationSummary = struct {
    output_dir: []const u8,
    manifest_path: []const u8,
    metrics_path: []const u8,
    peft_adapter_checkpoint_path: []const u8,
    peft_adapter_config_path: []const u8,
    task_head_checkpoint_path: []const u8,
    adapter_file_count: usize,
    peft_adapter_tensor_count: usize,
    task_head_tensor_count: usize,
    task_head_num_classes: usize,
    task_head_hidden_size: usize,
    manifest_adapter_file_count: usize,
    manifest_peft_adapter_tensor_count: usize,
    manifest_task_head_tensor_count: usize,
    manifest_epochs: usize,
    manifest_example_count: usize,
    manifest_total_steps: usize,
    manifest_batch_size: usize,
    manifest_seq_len: usize,
    manifest_entity_label_count: usize,
    metric_record_count: usize,
    step_record_count: usize,
    epoch_record_count: usize,
    supervised_token_count: usize,
    entity_token_count: usize,
    ignored_token_count: usize,
    total_step_wall_ms: f64,
    avg_step_wall_ms: f64,
    supervised_tokens_per_second: f64,
    total_graph_build_ms: f64,
    total_runtime_input_ms: f64,
    total_autodiff_ms: f64,
    total_execute_ms: f64,
    total_extract_ms: f64,
    total_optimizer_update_ms: f64,
    max_peak_resident_bytes: usize,
    first_step_loss: ?f64 = null,
    final_step_loss: ?f64 = null,
    all_step_losses_finite: bool,
    loss_decreased: bool,
};

pub fn validateRun(
    allocator: std.mem.Allocator,
    out_dir: []const u8,
    options: ValidationOptions,
) !RunValidationSummary {
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, manifest_file_name });
    errdefer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, metrics_file_name });
    errdefer allocator.free(metrics_path);
    const peft_adapter_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, gliner2.adapter_checkpoint_file_name });
    errdefer allocator.free(peft_adapter_checkpoint_path);
    const peft_adapter_config_path = try std.fs.path.join(allocator, &.{ out_dir, gliner2.adapter_config_file_name });
    errdefer allocator.free(peft_adapter_config_path);
    const task_head_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, gliner2.task_head_checkpoint_file_name });
    errdefer allocator.free(task_head_checkpoint_path);

    const manifest_bytes = try compat.cwd().readFileAlloc(compat.io(), manifest_path, allocator, .limited(4 * 1024 * 1024));
    defer allocator.free(manifest_bytes);
    var manifest_parsed = try std.json.parseFromSlice(std.json.Value, allocator, manifest_bytes, .{ .ignore_unknown_fields = true });
    defer manifest_parsed.deinit();
    if (manifest_parsed.value != .object) return error.InvalidTrainingManifest;
    const manifest = inspectManifest(manifest_parsed.value.object) catch return error.InvalidTrainingManifest;

    const metrics_bytes = try compat.cwd().readFileAlloc(compat.io(), metrics_path, allocator, .limited(64 * 1024 * 1024));
    defer allocator.free(metrics_bytes);
    const metrics = try inspectMetricsJsonl(allocator, metrics_bytes);
    if (metrics.step_record_count == 0) return error.NoStepMetrics;
    if (metrics.step_record_count != manifest.total_steps) return error.TrainingManifestMetricsMismatch;
    if (metrics.epoch_record_count != manifest.epochs) return error.TrainingManifestMetricsMismatch;
    if (!metrics.all_step_losses_finite) return error.NonFiniteLoss;
    if (metrics.supervised_token_count == 0) return error.NoSupervisedTokens;
    if (metrics.entity_token_count == 0) return error.NoEntityPositiveTokens;
    if (metrics.total_step_wall_ms <= 0 or metrics.supervised_tokens_per_second <= 0) return error.InvalidPerformanceMetrics;
    if (metrics.total_autodiff_ms <= 0 or metrics.total_execute_ms <= 0 or metrics.max_peak_resident_bytes == 0) return error.InvalidPerformanceMetrics;
    if (options.min_supervised_tokens_per_second) |min_tps| {
        if (!std.math.isFinite(min_tps) or min_tps < 0) return error.InvalidPerformanceThreshold;
        if (metrics.supervised_tokens_per_second < min_tps) return error.ThroughputBelowThreshold;
    }
    if (options.max_avg_step_wall_ms) |max_ms| {
        if (!std.math.isFinite(max_ms) or max_ms <= 0) return error.InvalidPerformanceThreshold;
        if (metrics.avg_step_wall_ms > max_ms) return error.AvgStepWallAboveThreshold;
    }
    if (options.max_total_execute_ms) |max_ms| {
        if (!std.math.isFinite(max_ms) or max_ms <= 0) return error.InvalidPerformanceThreshold;
        if (metrics.total_execute_ms > max_ms) return error.TotalExecuteMsAboveThreshold;
    }
    if (options.max_peak_resident_bytes) |max_bytes| {
        if (max_bytes == 0) return error.InvalidPerformanceThreshold;
        if (metrics.max_peak_resident_bytes > max_bytes) return error.PeakResidentBytesAboveThreshold;
    }
    if (options.min_examples) |min_examples| {
        if (manifest.example_count < min_examples) return error.ExampleCountBelowThreshold;
    }
    if (options.min_steps) |min_steps| {
        if (manifest.total_steps < min_steps) return error.StepCountBelowThreshold;
    }
    if (options.min_entity_labels) |min_entity_labels| {
        if (manifest.entity_label_count < min_entity_labels) return error.EntityLabelCountBelowThreshold;
    }
    if (options.min_supervised_tokens) |min_supervised_tokens| {
        if (metrics.supervised_token_count < min_supervised_tokens) return error.SupervisedTokenCountBelowThreshold;
    }
    if (options.min_entity_tokens) |min_entity_tokens| {
        if (metrics.entity_token_count < min_entity_tokens) return error.EntityTokenCountBelowThreshold;
    }
    if (options.require_loss_decrease and !metrics.loss_decreased) return error.LossDidNotDecrease;

    const adapter_file_count = try countAdapterParameterFiles(out_dir);
    if (adapter_file_count == 0) return error.NoAdapterParameterFiles;
    const peft_adapter_tensor_count = try validatePeftAdapterBundle(allocator, peft_adapter_checkpoint_path, peft_adapter_config_path, manifest);
    if (peft_adapter_tensor_count == 0) return error.NoPeftAdapterTensors;
    const task_head = try validateTaskHeadCheckpoint(allocator, task_head_checkpoint_path, .{
        .num_classes = manifest.num_classes,
        .hidden_size = manifest.hidden_size,
    });
    const task_head_tensor_count = task_head.tensor_count;
    if (task_head_tensor_count < 2) return error.MissingTaskHeadTensors;
    if (manifest.adapter_parameter_file_count != adapter_file_count) return error.TrainingManifestArtifactMismatch;
    if (manifest.peft_adapter_tensor_count != peft_adapter_tensor_count) return error.TrainingManifestArtifactMismatch;
    if (manifest.regular_trainable_tensor_count != task_head_tensor_count) return error.TrainingManifestArtifactMismatch;

    return .{
        .output_dir = try allocator.dupe(u8, out_dir),
        .manifest_path = manifest_path,
        .metrics_path = metrics_path,
        .peft_adapter_checkpoint_path = peft_adapter_checkpoint_path,
        .peft_adapter_config_path = peft_adapter_config_path,
        .task_head_checkpoint_path = task_head_checkpoint_path,
        .adapter_file_count = adapter_file_count,
        .peft_adapter_tensor_count = peft_adapter_tensor_count,
        .task_head_tensor_count = task_head_tensor_count,
        .task_head_num_classes = task_head.num_classes,
        .task_head_hidden_size = task_head.hidden_size,
        .manifest_adapter_file_count = manifest.adapter_parameter_file_count,
        .manifest_peft_adapter_tensor_count = manifest.peft_adapter_tensor_count,
        .manifest_task_head_tensor_count = manifest.regular_trainable_tensor_count,
        .manifest_epochs = manifest.epochs,
        .manifest_example_count = manifest.example_count,
        .manifest_total_steps = manifest.total_steps,
        .manifest_batch_size = manifest.batch_size,
        .manifest_seq_len = manifest.seq_len,
        .manifest_entity_label_count = manifest.entity_label_count,
        .metric_record_count = metrics.metric_record_count,
        .step_record_count = metrics.step_record_count,
        .epoch_record_count = metrics.epoch_record_count,
        .supervised_token_count = metrics.supervised_token_count,
        .entity_token_count = metrics.entity_token_count,
        .ignored_token_count = metrics.ignored_token_count,
        .total_step_wall_ms = metrics.total_step_wall_ms,
        .avg_step_wall_ms = metrics.avg_step_wall_ms,
        .supervised_tokens_per_second = metrics.supervised_tokens_per_second,
        .total_graph_build_ms = metrics.total_graph_build_ms,
        .total_runtime_input_ms = metrics.total_runtime_input_ms,
        .total_autodiff_ms = metrics.total_autodiff_ms,
        .total_execute_ms = metrics.total_execute_ms,
        .total_extract_ms = metrics.total_extract_ms,
        .total_optimizer_update_ms = metrics.total_optimizer_update_ms,
        .max_peak_resident_bytes = metrics.max_peak_resident_bytes,
        .first_step_loss = metrics.first_step_loss,
        .final_step_loss = metrics.final_step_loss,
        .all_step_losses_finite = metrics.all_step_losses_finite,
        .loss_decreased = metrics.loss_decreased,
    };
}

pub fn freeRunValidationSummary(allocator: std.mem.Allocator, summary: *RunValidationSummary) void {
    allocator.free(summary.output_dir);
    allocator.free(summary.manifest_path);
    allocator.free(summary.metrics_path);
    allocator.free(summary.peft_adapter_checkpoint_path);
    allocator.free(summary.peft_adapter_config_path);
    allocator.free(summary.task_head_checkpoint_path);
    summary.* = undefined;
}

const MetricsInspection = struct {
    metric_record_count: usize = 0,
    step_record_count: usize = 0,
    epoch_record_count: usize = 0,
    supervised_token_count: usize = 0,
    entity_token_count: usize = 0,
    ignored_token_count: usize = 0,
    total_step_wall_ms: f64 = 0,
    total_graph_build_ms: f64 = 0,
    total_runtime_input_ms: f64 = 0,
    total_autodiff_ms: f64 = 0,
    total_execute_ms: f64 = 0,
    total_extract_ms: f64 = 0,
    total_optimizer_update_ms: f64 = 0,
    max_peak_resident_bytes: usize = 0,
    first_step_loss: ?f64 = null,
    final_step_loss: ?f64 = null,
    all_step_losses_finite: bool = true,

    fn lossDecreased(self: MetricsInspection) bool {
        const first = self.first_step_loss orelse return false;
        const final = self.final_step_loss orelse return false;
        return final < first;
    }

    fn avgStepWallMs(self: MetricsInspection) f64 {
        if (self.step_record_count == 0) return 0;
        return self.total_step_wall_ms / @as(f64, @floatFromInt(self.step_record_count));
    }

    fn supervisedTokensPerSecond(self: MetricsInspection) f64 {
        if (self.supervised_token_count == 0 or self.total_step_wall_ms <= 0) return 0;
        const seconds = self.total_step_wall_ms / 1000.0;
        return @as(f64, @floatFromInt(self.supervised_token_count)) / seconds;
    }
};

const ManifestInspection = struct {
    adapter_parameter_file_count: usize,
    peft_adapter_tensor_count: usize,
    regular_trainable_tensor_count: usize,
    num_classes: usize,
    hidden_size: usize,
    model_dir: []const u8,
    lora_rank: usize,
    lora_alpha: f64,
    lora_targets: []const u8,
    epochs: usize,
    batch_size: usize,
    seq_len: usize,
    example_count: usize,
    total_steps: usize,
    final_avg_loss: f64,
    entity_label_count: usize,
};

fn inspectManifest(obj: std.json.ObjectMap) !ManifestInspection {
    const schema_version = jsonString(obj.get("schema_version")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, schema_version, expected_manifest_schema_version)) return error.InvalidTrainingManifest;
    const artifact_family = jsonString(obj.get("artifact_family_version")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, artifact_family, expected_artifact_family_version)) return error.InvalidTrainingManifest;

    const metrics_file = jsonString(obj.get("metrics_file")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, metrics_file, metrics_file_name)) return error.InvalidTrainingManifest;
    const peft_checkpoint = jsonString(obj.get("peft_adapter_checkpoint")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, peft_checkpoint, gliner2.adapter_checkpoint_file_name)) return error.InvalidTrainingManifest;
    const peft_config = jsonString(obj.get("peft_adapter_config")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, peft_config, gliner2.adapter_config_file_name)) return error.InvalidTrainingManifest;
    const task_head_checkpoint = jsonString(obj.get("regular_trainable_checkpoint")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, task_head_checkpoint, gliner2.task_head_checkpoint_file_name)) return error.InvalidTrainingManifest;

    const num_classes = jsonUsize(obj.get("num_classes")) orelse return error.InvalidTrainingManifest;
    const hidden_size = jsonUsize(obj.get("hidden_size")) orelse return error.InvalidTrainingManifest;
    if (num_classes == 0 or hidden_size == 0) return error.InvalidTrainingManifest;
    const model_dir = jsonString(obj.get("model_dir")) orelse return error.InvalidTrainingManifest;
    if (std.mem.trim(u8, model_dir, " \t\r\n").len == 0) return error.InvalidTrainingManifest;
    const lora_rank = jsonUsize(obj.get("lora_rank")) orelse return error.InvalidTrainingManifest;
    const lora_alpha = jsonF64(obj.get("lora_alpha")) orelse return error.InvalidTrainingManifest;
    const lora_targets = jsonString(obj.get("lora_targets")) orelse return error.InvalidTrainingManifest;
    if (lora_rank == 0 or !std.math.isFinite(lora_alpha) or lora_alpha <= 0) return error.InvalidTrainingManifest;
    if (countCsvTargets(lora_targets) == 0 or hasEmptyCsvTarget(lora_targets)) return error.InvalidTrainingManifest;
    const epochs = jsonUsize(obj.get("epochs")) orelse return error.InvalidTrainingManifest;
    const batch_size = jsonUsize(obj.get("batch_size")) orelse return error.InvalidTrainingManifest;
    const seq_len = jsonUsize(obj.get("seq_len")) orelse return error.InvalidTrainingManifest;
    const example_count = jsonUsize(obj.get("example_count")) orelse return error.InvalidTrainingManifest;
    const total_steps = jsonUsize(obj.get("total_steps")) orelse return error.InvalidTrainingManifest;
    const final_avg_loss = jsonF64(obj.get("final_avg_loss")) orelse return error.InvalidTrainingManifest;
    const entity_label_count = try inspectEntityLabels(obj.get("entity_labels"));
    if (epochs == 0 or batch_size == 0 or seq_len == 0 or example_count == 0 or total_steps == 0) return error.InvalidTrainingManifest;
    if (!std.math.isFinite(final_avg_loss)) return error.InvalidTrainingManifest;
    if (entity_label_count == 0 or entity_label_count + 1 > num_classes) return error.InvalidTrainingManifest;
    if (jsonUsize(obj.get("entity_label_count"))) |recorded_count| {
        if (recorded_count != entity_label_count) return error.InvalidTrainingManifest;
    }

    return .{
        .adapter_parameter_file_count = jsonUsize(obj.get("adapter_parameter_file_count")) orelse return error.InvalidTrainingManifest,
        .peft_adapter_tensor_count = jsonUsize(obj.get("peft_adapter_tensor_count")) orelse return error.InvalidTrainingManifest,
        .regular_trainable_tensor_count = jsonUsize(obj.get("regular_trainable_tensor_count")) orelse return error.InvalidTrainingManifest,
        .num_classes = num_classes,
        .hidden_size = hidden_size,
        .model_dir = model_dir,
        .lora_rank = lora_rank,
        .lora_alpha = lora_alpha,
        .lora_targets = lora_targets,
        .epochs = epochs,
        .batch_size = batch_size,
        .seq_len = seq_len,
        .example_count = example_count,
        .total_steps = total_steps,
        .final_avg_loss = final_avg_loss,
        .entity_label_count = entity_label_count,
    };
}

fn inspectEntityLabels(value: ?std.json.Value) !usize {
    const v = value orelse return error.InvalidTrainingManifest;
    if (v != .array) return error.InvalidTrainingManifest;
    const labels = v.array.items;
    if (labels.len == 0) return error.InvalidTrainingManifest;
    for (labels, 0..) |entry, idx| {
        if (entry != .string) return error.InvalidTrainingManifest;
        if (std.mem.trim(u8, entry.string, " \t\r\n").len == 0) return error.InvalidTrainingManifest;
        for (labels[0..idx]) |previous| {
            if (std.mem.eql(u8, previous.string, entry.string)) return error.InvalidTrainingManifest;
        }
    }
    return labels.len;
}

fn inspectMetricsJsonl(allocator: std.mem.Allocator, bytes: []const u8) !struct {
    metric_record_count: usize,
    step_record_count: usize,
    epoch_record_count: usize,
    supervised_token_count: usize,
    entity_token_count: usize,
    ignored_token_count: usize,
    total_step_wall_ms: f64,
    avg_step_wall_ms: f64,
    supervised_tokens_per_second: f64,
    total_graph_build_ms: f64,
    total_runtime_input_ms: f64,
    total_autodiff_ms: f64,
    total_execute_ms: f64,
    total_extract_ms: f64,
    total_optimizer_update_ms: f64,
    max_peak_resident_bytes: usize,
    first_step_loss: ?f64,
    final_step_loss: ?f64,
    all_step_losses_finite: bool,
    loss_decreased: bool,
} {
    var inspection = MetricsInspection{};
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r\n");
        if (line.len == 0) continue;

        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidMetricsRecord;
        const obj = parsed.value.object;
        inspection.metric_record_count += 1;

        const event = jsonString(obj.get("event")) orelse return error.InvalidMetricsRecord;
        if (std.mem.eql(u8, event, "step")) {
            inspection.step_record_count += 1;
            const loss = jsonF64(obj.get("loss")) orelse return error.InvalidMetricsRecord;
            if (!std.math.isFinite(loss)) inspection.all_step_losses_finite = false;
            if (inspection.first_step_loss == null) inspection.first_step_loss = loss;
            inspection.final_step_loss = loss;
            inspection.supervised_token_count += jsonUsize(obj.get("supervised_token_count")) orelse return error.InvalidMetricsRecord;
            inspection.entity_token_count += jsonUsize(obj.get("entity_token_count")) orelse return error.InvalidMetricsRecord;
            inspection.ignored_token_count += jsonUsize(obj.get("ignored_token_count")) orelse return error.InvalidMetricsRecord;
            const target_build_ms = jsonF64(obj.get("target_build_ms")) orelse return error.InvalidMetricsRecord;
            const train_step_ms = jsonF64(obj.get("train_step_ms")) orelse return error.InvalidMetricsRecord;
            const step_wall_ms = jsonF64(obj.get("step_wall_ms")) orelse return error.InvalidMetricsRecord;
            const graph_build_ms = jsonF64(obj.get("graph_build_ms")) orelse return error.InvalidMetricsRecord;
            const runtime_input_ms = jsonF64(obj.get("runtime_input_ms")) orelse return error.InvalidMetricsRecord;
            const autodiff_ms = jsonF64(obj.get("autodiff_ms")) orelse return error.InvalidMetricsRecord;
            const execute_ms = jsonF64(obj.get("execute_ms")) orelse return error.InvalidMetricsRecord;
            const extract_ms = jsonF64(obj.get("extract_ms")) orelse return error.InvalidMetricsRecord;
            const optimizer_update_ms = jsonF64(obj.get("optimizer_update_ms")) orelse return error.InvalidMetricsRecord;
            const trainer_total_ms = jsonF64(obj.get("trainer_total_ms")) orelse return error.InvalidMetricsRecord;
            const peak_resident_bytes = jsonUsize(obj.get("peak_resident_bytes")) orelse return error.InvalidMetricsRecord;
            const supervised_tokens_per_second = jsonF64(obj.get("supervised_tokens_per_second")) orelse return error.InvalidMetricsRecord;
            if (!std.math.isFinite(target_build_ms) or target_build_ms < 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(train_step_ms) or train_step_ms < 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(step_wall_ms) or step_wall_ms <= 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(graph_build_ms) or graph_build_ms < 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(runtime_input_ms) or runtime_input_ms < 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(autodiff_ms) or autodiff_ms <= 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(execute_ms) or execute_ms <= 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(extract_ms) or extract_ms < 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(optimizer_update_ms) or optimizer_update_ms < 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(trainer_total_ms) or trainer_total_ms <= 0) return error.InvalidPerformanceMetrics;
            if (peak_resident_bytes == 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(supervised_tokens_per_second) or supervised_tokens_per_second <= 0) return error.InvalidPerformanceMetrics;
            inspection.total_step_wall_ms += step_wall_ms;
            inspection.total_graph_build_ms += graph_build_ms;
            inspection.total_runtime_input_ms += runtime_input_ms;
            inspection.total_autodiff_ms += autodiff_ms;
            inspection.total_execute_ms += execute_ms;
            inspection.total_extract_ms += extract_ms;
            inspection.total_optimizer_update_ms += optimizer_update_ms;
            inspection.max_peak_resident_bytes = @max(inspection.max_peak_resident_bytes, peak_resident_bytes);
        } else if (std.mem.eql(u8, event, "epoch")) {
            inspection.epoch_record_count += 1;
            const avg_loss = jsonF64(obj.get("avg_loss")) orelse return error.InvalidMetricsRecord;
            if (!std.math.isFinite(avg_loss)) inspection.all_step_losses_finite = false;
        }
    }

    return .{
        .metric_record_count = inspection.metric_record_count,
        .step_record_count = inspection.step_record_count,
        .epoch_record_count = inspection.epoch_record_count,
        .supervised_token_count = inspection.supervised_token_count,
        .entity_token_count = inspection.entity_token_count,
        .ignored_token_count = inspection.ignored_token_count,
        .total_step_wall_ms = inspection.total_step_wall_ms,
        .avg_step_wall_ms = inspection.avgStepWallMs(),
        .supervised_tokens_per_second = inspection.supervisedTokensPerSecond(),
        .total_graph_build_ms = inspection.total_graph_build_ms,
        .total_runtime_input_ms = inspection.total_runtime_input_ms,
        .total_autodiff_ms = inspection.total_autodiff_ms,
        .total_execute_ms = inspection.total_execute_ms,
        .total_extract_ms = inspection.total_extract_ms,
        .total_optimizer_update_ms = inspection.total_optimizer_update_ms,
        .max_peak_resident_bytes = inspection.max_peak_resident_bytes,
        .first_step_loss = inspection.first_step_loss,
        .final_step_loss = inspection.final_step_loss,
        .all_step_losses_finite = inspection.all_step_losses_finite,
        .loss_decreased = inspection.lossDecreased(),
    };
}

fn countAdapterParameterFiles(out_dir: []const u8) !usize {
    var dir = try compat.cwd().openDir(compat.io(), out_dir, .{ .iterate = true });
    defer dir.close(compat.io());

    var count: usize = 0;
    var iter = dir.iterate();
    while (try iter.next(compat.io())) |entry| {
        if (entry.kind != .file) continue;
        if (std.mem.endsWith(u8, entry.name, adapter_file_suffix)) count += 1;
    }
    return count;
}

fn validatePeftAdapterBundle(
    allocator: std.mem.Allocator,
    adapter_checkpoint_path: []const u8,
    adapter_config_path: []const u8,
    manifest: ManifestInspection,
) !usize {
    const config_bytes = try compat.cwd().readFileAlloc(compat.io(), adapter_config_path, allocator, .limited(4 * 1024 * 1024));
    defer allocator.free(config_bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, config_bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidPeftAdapterConfig;
    try inspectPeftAdapterConfig(parsed.value.object, manifest);

    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, adapter_checkpoint_path);
    defer reader.deinit();
    return reader.header.tensors.count();
}

fn inspectPeftAdapterConfig(obj: std.json.ObjectMap, manifest: ManifestInspection) !void {
    const peft_type = jsonString(obj.get("peft_type")) orelse return error.InvalidPeftAdapterConfig;
    if (!std.mem.eql(u8, peft_type, "LORA")) return error.InvalidPeftAdapterConfig;
    const task_type = jsonString(obj.get("task_type")) orelse return error.InvalidPeftAdapterConfig;
    if (!std.mem.eql(u8, task_type, "TOKEN_CLS")) return error.InvalidPeftAdapterConfig;
    const base_model_name_or_path = jsonString(obj.get("base_model_name_or_path")) orelse return error.InvalidPeftAdapterConfig;
    if (!std.mem.eql(u8, base_model_name_or_path, manifest.model_dir)) return error.InvalidPeftAdapterConfig;
    const rank = jsonUsize(obj.get("r")) orelse return error.InvalidPeftAdapterConfig;
    if (rank != manifest.lora_rank) return error.InvalidPeftAdapterConfig;
    const lora_alpha = jsonF64(obj.get("lora_alpha")) orelse return error.InvalidPeftAdapterConfig;
    if (!std.math.isFinite(lora_alpha) or @abs(lora_alpha - manifest.lora_alpha) > 1e-6) return error.InvalidPeftAdapterConfig;
    const use_dora = jsonBool(obj.get("use_dora")) orelse return error.InvalidPeftAdapterConfig;
    if (use_dora) return error.InvalidPeftAdapterConfig;

    const target_modules_value = obj.get("target_modules") orelse return error.InvalidPeftAdapterConfig;
    if (target_modules_value != .array) return error.InvalidPeftAdapterConfig;
    const target_modules = target_modules_value.array.items;
    if (target_modules.len != countCsvTargets(manifest.lora_targets)) return error.InvalidPeftAdapterConfig;
    var expected = std.mem.tokenizeScalar(u8, manifest.lora_targets, ',');
    var idx: usize = 0;
    while (expected.next()) |raw_expected| : (idx += 1) {
        if (idx >= target_modules.len) return error.InvalidPeftAdapterConfig;
        const want = std.mem.trim(u8, raw_expected, " \t\r\n");
        if (want.len == 0) return error.InvalidPeftAdapterConfig;
        if (target_modules[idx] != .string) return error.InvalidPeftAdapterConfig;
        if (!std.mem.eql(u8, target_modules[idx].string, want)) return error.InvalidPeftAdapterConfig;
    }
    if (idx != target_modules.len) return error.InvalidPeftAdapterConfig;
}

fn countCsvTargets(value: []const u8) usize {
    var count: usize = 0;
    var iter = std.mem.tokenizeScalar(u8, value, ',');
    while (iter.next()) |raw| {
        if (std.mem.trim(u8, raw, " \t\r\n").len > 0) count += 1;
    }
    return count;
}

fn hasEmptyCsvTarget(value: []const u8) bool {
    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |raw| {
        if (std.mem.trim(u8, raw, " \t\r\n").len == 0) return true;
    }
    return false;
}

const TaskHeadExpectedShape = struct {
    num_classes: usize,
    hidden_size: usize,
};

const TaskHeadInspection = struct {
    tensor_count: usize,
    num_classes: usize,
    hidden_size: usize,
};

fn validateTaskHeadCheckpoint(
    allocator: std.mem.Allocator,
    task_head_checkpoint_path: []const u8,
    expected: TaskHeadExpectedShape,
) !TaskHeadInspection {
    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, task_head_checkpoint_path);
    defer reader.deinit();
    const weight = reader.header.tensors.get("classifier.weight") orelse return error.MissingTaskHeadTensors;
    const bias = reader.header.tensors.get("classifier.bias") orelse return error.MissingTaskHeadTensors;
    if (weight.dtype != .f32 or bias.dtype != .f32) return error.InvalidTaskHeadTensorDType;
    if (weight.shape.len != 2 or bias.shape.len != 1) return error.InvalidTaskHeadTensorShape;

    const weight_classes = positiveShapeDim(weight.shape[0]) orelse return error.InvalidTaskHeadTensorShape;
    const weight_hidden = positiveShapeDim(weight.shape[1]) orelse return error.InvalidTaskHeadTensorShape;
    const bias_classes = positiveShapeDim(bias.shape[0]) orelse return error.InvalidTaskHeadTensorShape;
    if (weight_classes != bias_classes) return error.InvalidTaskHeadTensorShape;
    if (weight_classes != expected.num_classes or weight_hidden != expected.hidden_size) return error.InvalidTaskHeadTensorShape;

    return .{
        .tensor_count = reader.header.tensors.count(),
        .num_classes = weight_classes,
        .hidden_size = weight_hidden,
    };
}

fn positiveShapeDim(dim: i64) ?usize {
    if (dim <= 0) return null;
    return @intCast(dim);
}

fn jsonString(value: ?std.json.Value) ?[]const u8 {
    const v = value orelse return null;
    return switch (v) {
        .string => |s| s,
        else => null,
    };
}

fn jsonF64(value: ?std.json.Value) ?f64 {
    const v = value orelse return null;
    return switch (v) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        else => null,
    };
}

fn jsonUsize(value: ?std.json.Value) ?usize {
    const v = value orelse return null;
    return switch (v) {
        .integer => |i| if (i >= 0) @intCast(i) else null,
        else => null,
    };
}

fn jsonBool(value: ?std.json.Value) ?bool {
    const v = value orelse return null;
    return switch (v) {
        .bool => |b| b,
        else => null,
    };
}
