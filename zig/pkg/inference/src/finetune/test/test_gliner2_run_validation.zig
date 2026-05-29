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
const validation = @import("inference_internal").finetune.gliner2_run_validation;

test "GLiNER2 autodiff run validator accepts complete output" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const out_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(out_dir);
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, validation.manifest_file_name });
    defer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, validation.metrics_file_name });
    defer allocator.free(metrics_path);
    const adapter_path = try std.fs.path.join(allocator, &.{ out_dir, "encoder.layer.0.attention.self.query_proj.lora_A.bin" });
    defer allocator.free(adapter_path);
    const peft_config_path = try std.fs.path.join(allocator, &.{ out_dir, "adapter_config.json" });
    defer allocator.free(peft_config_path);
    const peft_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "adapter_model.safetensors" });
    defer allocator.free(peft_checkpoint_path);
    const task_head_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "task_head.safetensors" });
    defer allocator.free(task_head_checkpoint_path);

    try writeManifestWithRun(allocator, manifest_path, 1, 1, 2, 4, 1, 1, 2, 2, 1.0);
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = metrics_path,
        .data =
        \\{"event":"step","step":1,"loss":1.5,"grad_norm":0.2,"optimizer_stepped":true,"supervised_token_count":10,"entity_token_count":2,"ignored_token_count":3,"entity_token_rate":0.2,"target_build_ms":1.0,"train_step_ms":999.0,"step_wall_ms":1000.0,"graph_build_ms":10.0,"runtime_input_ms":20.0,"autodiff_ms":300.0,"execute_ms":600.0,"extract_ms":5.0,"optimizer_update_ms":2.0,"device_optimizer_ms":0.25,"optimizer_backend":"metal","device_resident_transfer_count":0,"device_trainable_bytes":128,"trainer_total_ms":950.0,"peak_resident_bytes":4096,"supervised_tokens_per_second":10.0}
        \\{"event":"step","step":2,"loss":1.0,"grad_norm":0.1,"optimizer_stepped":true,"supervised_token_count":8,"entity_token_count":1,"ignored_token_count":5,"entity_token_rate":0.125,"target_build_ms":1.0,"train_step_ms":1999.0,"step_wall_ms":2000.0,"graph_build_ms":1.0,"runtime_input_ms":20.0,"autodiff_ms":500.0,"execute_ms":1400.0,"extract_ms":5.0,"optimizer_update_ms":2.0,"device_optimizer_ms":0.75,"optimizer_backend":"metal","device_resident_transfer_count":0,"device_trainable_bytes":128,"trainer_total_ms":1950.0,"peak_resident_bytes":8192,"supervised_tokens_per_second":4.0}
        \\{"event":"epoch","epoch":1,"avg_loss":1.25,"supervised_token_count":18,"entity_token_count":3,"ignored_token_count":8,"entity_token_rate":0.16666666666666666,"epoch_wall_ms":3000.0,"supervised_tokens_per_second":6.0}
        \\
        ,
    });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = adapter_path, .data = "LORA" });
    try writePeftConfig(peft_config_path);
    try writeOneTensorSafetensors(allocator, peft_checkpoint_path);
    try writeTaskHeadSafetensors(allocator, task_head_checkpoint_path);

    var summary = try validation.validateRun(allocator, out_dir, .{
        .require_loss_decrease = true,
        .require_backend = "Metal",
        .require_optimizer_backend = "metal",
        .max_device_resident_transfer_count = 0,
        .min_device_trainable_bytes = 128,
    });
    defer validation.freeRunValidationSummary(allocator, &summary);
    try std.testing.expectEqual(@as(usize, 1), summary.adapter_file_count);
    try std.testing.expectEqual(@as(usize, 1), summary.peft_adapter_tensor_count);
    try std.testing.expectEqual(@as(usize, 2), summary.task_head_tensor_count);
    try std.testing.expectEqual(@as(usize, 4), summary.task_head_num_classes);
    try std.testing.expectEqual(@as(usize, 1), summary.task_head_hidden_size);
    try std.testing.expectEqual(@as(usize, 1), summary.manifest_epochs);
    try std.testing.expectEqual(@as(usize, 2), summary.manifest_example_count);
    try std.testing.expectEqual(@as(usize, 2), summary.manifest_total_steps);
    try std.testing.expectEqual(@as(usize, 1), summary.manifest_batch_size);
    try std.testing.expectEqual(@as(usize, 64), summary.manifest_seq_len);
    try std.testing.expectEqual(@as(usize, 3), summary.manifest_entity_label_count);
    try std.testing.expectEqualStrings("Metal", summary.manifest_backend);
    try std.testing.expectEqual(@as(usize, 2), summary.step_record_count);
    try std.testing.expectEqual(@as(usize, 18), summary.supervised_token_count);
    try std.testing.expectEqual(@as(usize, 3), summary.entity_token_count);
    try std.testing.expectEqual(@as(usize, 8), summary.ignored_token_count);
    try std.testing.expectApproxEqAbs(@as(f64, 3000.0), summary.total_step_wall_ms, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 1500.0), summary.avg_step_wall_ms, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 6.0), summary.supervised_tokens_per_second, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 11.0), summary.total_graph_build_ms, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 40.0), summary.total_runtime_input_ms, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 800.0), summary.total_autodiff_ms, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 2000.0), summary.total_execute_ms, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 10.0), summary.total_extract_ms, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 4.0), summary.total_optimizer_update_ms, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), summary.total_device_optimizer_ms, 0.001);
    try std.testing.expectEqual(@as(u64, 0), summary.max_device_resident_transfer_count);
    try std.testing.expectEqual(@as(usize, 128), summary.max_device_trainable_bytes);
    try std.testing.expectEqual(@as(usize, 8192), summary.max_peak_resident_bytes);
    try std.testing.expect(summary.loss_decreased);
}

test "GLiNER2 autodiff run validator accepts smoothed loss decrease" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const out_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(out_dir);
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, validation.manifest_file_name });
    defer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, validation.metrics_file_name });
    defer allocator.free(metrics_path);
    const adapter_path = try std.fs.path.join(allocator, &.{ out_dir, "encoder.layer.0.attention.self.query_proj.lora_A.bin" });
    defer allocator.free(adapter_path);
    const peft_config_path = try std.fs.path.join(allocator, &.{ out_dir, "adapter_config.json" });
    defer allocator.free(peft_config_path);
    const peft_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "adapter_model.safetensors" });
    defer allocator.free(peft_checkpoint_path);
    const task_head_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "task_head.safetensors" });
    defer allocator.free(task_head_checkpoint_path);

    try writeManifestWithRun(allocator, manifest_path, 1, 1, 2, 4, 1, 1, 41, 41, 0.3);
    var metrics: std.Io.Writer.Allocating = .init(allocator);
    defer metrics.deinit();
    for (1..42) |step| {
        const loss: f64 = if (step == 1) 0.2 else if (step <= 20) 1.0 else 0.3;
        try metrics.writer.print(
            "{{\"event\":\"step\",\"step\":{},\"loss\":{d},\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":0,\"target_build_ms\":1.0,\"train_step_ms\":9.0,\"step_wall_ms\":10.0,\"graph_build_ms\":1.0,\"runtime_input_ms\":1.0,\"autodiff_ms\":2.0,\"execute_ms\":3.0,\"extract_ms\":1.0,\"optimizer_update_ms\":1.0,\"device_optimizer_ms\":0.5,\"optimizer_backend\":\"metal\",\"device_resident_transfer_count\":0,\"device_trainable_bytes\":128,\"trainer_total_ms\":8.0,\"peak_resident_bytes\":1024,\"supervised_tokens_per_second\":400.0}}\n",
            .{ step, loss },
        );
    }
    try metrics.writer.writeAll("{\"event\":\"epoch\",\"avg_loss\":0.3}\n");
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metrics_path, .data = metrics.written() });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = adapter_path, .data = "LORA" });
    try writePeftConfig(peft_config_path);
    try writeOneTensorSafetensors(allocator, peft_checkpoint_path);
    try writeTaskHeadSafetensors(allocator, task_head_checkpoint_path);

    var summary = try validation.validateRun(allocator, out_dir, .{
        .require_loss_decrease = true,
        .require_backend = "Metal",
        .require_optimizer_backend = "metal",
        .max_device_resident_transfer_count = 0,
        .min_device_trainable_bytes = 128,
    });
    defer validation.freeRunValidationSummary(allocator, &summary);
    try std.testing.expectEqual(@as(usize, 41), summary.step_record_count);
    try std.testing.expect(summary.final_step_loss.? > summary.first_step_loss.?);
    try std.testing.expect(summary.loss_decreased);
}

test "GLiNER2 autodiff run validator rejects manifest metrics mismatch" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const out_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(out_dir);
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, validation.manifest_file_name });
    defer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, validation.metrics_file_name });
    defer allocator.free(metrics_path);

    try writeManifestWithRun(allocator, manifest_path, 1, 1, 2, 4, 1, 1, 2, 2, 1.0);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metrics_path, .data = "{\"event\":\"step\",\"loss\":1.0,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":0,\"target_build_ms\":1.0,\"train_step_ms\":9.0,\"step_wall_ms\":10.0,\"graph_build_ms\":1.0,\"runtime_input_ms\":1.0,\"autodiff_ms\":2.0,\"execute_ms\":3.0,\"extract_ms\":1.0,\"optimizer_update_ms\":1.0,\"trainer_total_ms\":8.0,\"peak_resident_bytes\":1024,\"supervised_tokens_per_second\":400.0}\n{\"event\":\"epoch\",\"avg_loss\":1.0}\n" });

    try std.testing.expectError(error.TrainingManifestMetricsMismatch, validation.validateRun(allocator, out_dir, .{}));
}

test "GLiNER2 autodiff run validator rejects missing adapter files" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const out_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(out_dir);
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, validation.manifest_file_name });
    defer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, validation.metrics_file_name });
    defer allocator.free(metrics_path);

    try writeManifest(allocator, manifest_path, 1, 1, 2);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metrics_path, .data = "{\"event\":\"step\",\"loss\":1.0,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":0,\"target_build_ms\":1.0,\"train_step_ms\":9.0,\"step_wall_ms\":10.0,\"graph_build_ms\":1.0,\"runtime_input_ms\":1.0,\"autodiff_ms\":2.0,\"execute_ms\":3.0,\"extract_ms\":1.0,\"optimizer_update_ms\":1.0,\"trainer_total_ms\":8.0,\"peak_resident_bytes\":1024,\"supervised_tokens_per_second\":400.0}\n{\"event\":\"epoch\",\"avg_loss\":1.0}\n" });

    try std.testing.expectError(error.NoAdapterParameterFiles, validation.validateRun(allocator, out_dir, .{}));
}

test "GLiNER2 autodiff run validator rejects manifest artifact mismatch" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const out_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(out_dir);
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, validation.manifest_file_name });
    defer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, validation.metrics_file_name });
    defer allocator.free(metrics_path);
    const adapter_path = try std.fs.path.join(allocator, &.{ out_dir, "encoder.layer.0.attention.self.query_proj.lora_A.bin" });
    defer allocator.free(adapter_path);
    const peft_config_path = try std.fs.path.join(allocator, &.{ out_dir, "adapter_config.json" });
    defer allocator.free(peft_config_path);
    const peft_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "adapter_model.safetensors" });
    defer allocator.free(peft_checkpoint_path);
    const task_head_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "task_head.safetensors" });
    defer allocator.free(task_head_checkpoint_path);

    try writeManifest(allocator, manifest_path, 2, 1, 2);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metrics_path, .data = "{\"event\":\"step\",\"loss\":1.0,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":0,\"target_build_ms\":1.0,\"train_step_ms\":9.0,\"step_wall_ms\":10.0,\"graph_build_ms\":1.0,\"runtime_input_ms\":1.0,\"autodiff_ms\":2.0,\"execute_ms\":3.0,\"extract_ms\":1.0,\"optimizer_update_ms\":1.0,\"trainer_total_ms\":8.0,\"peak_resident_bytes\":1024,\"supervised_tokens_per_second\":400.0}\n{\"event\":\"epoch\",\"avg_loss\":1.0}\n" });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = adapter_path, .data = "LORA" });
    try writePeftConfig(peft_config_path);
    try writeOneTensorSafetensors(allocator, peft_checkpoint_path);
    try writeTaskHeadSafetensors(allocator, task_head_checkpoint_path);

    try std.testing.expectError(error.TrainingManifestArtifactMismatch, validation.validateRun(allocator, out_dir, .{}));
}

test "GLiNER2 autodiff run validator rejects PEFT config mismatch" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const out_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(out_dir);
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, validation.manifest_file_name });
    defer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, validation.metrics_file_name });
    defer allocator.free(metrics_path);
    const adapter_path = try std.fs.path.join(allocator, &.{ out_dir, "encoder.layer.0.attention.self.query_proj.lora_A.bin" });
    defer allocator.free(adapter_path);
    const peft_config_path = try std.fs.path.join(allocator, &.{ out_dir, "adapter_config.json" });
    defer allocator.free(peft_config_path);
    const peft_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "adapter_model.safetensors" });
    defer allocator.free(peft_checkpoint_path);
    const task_head_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "task_head.safetensors" });
    defer allocator.free(task_head_checkpoint_path);

    try writeManifest(allocator, manifest_path, 1, 1, 2);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metrics_path, .data = "{\"event\":\"step\",\"loss\":1.0,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":0,\"target_build_ms\":1.0,\"train_step_ms\":9.0,\"step_wall_ms\":10.0,\"graph_build_ms\":1.0,\"runtime_input_ms\":1.0,\"autodiff_ms\":2.0,\"execute_ms\":3.0,\"extract_ms\":1.0,\"optimizer_update_ms\":1.0,\"trainer_total_ms\":8.0,\"peak_resident_bytes\":1024,\"supervised_tokens_per_second\":400.0}\n{\"event\":\"epoch\",\"avg_loss\":1.0}\n" });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = adapter_path, .data = "LORA" });
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = peft_config_path,
        .data =
        \\{
        \\  "base_model_name_or_path": "base-model",
        \\  "peft_type": "LORA",
        \\  "task_type": "TOKEN_CLS",
        \\  "r": 16,
        \\  "lora_alpha": 32,
        \\  "target_modules": ["query_proj", "key_proj"],
        \\  "use_dora": false
        \\}
        \\
        ,
    });
    try writeOneTensorSafetensors(allocator, peft_checkpoint_path);
    try writeTaskHeadSafetensors(allocator, task_head_checkpoint_path);

    try std.testing.expectError(error.InvalidPeftAdapterConfig, validation.validateRun(allocator, out_dir, .{}));
}

test "GLiNER2 autodiff run validator rejects no entity-positive supervision" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const out_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(out_dir);
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, validation.manifest_file_name });
    defer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, validation.metrics_file_name });
    defer allocator.free(metrics_path);

    try writeManifest(allocator, manifest_path, 1, 1, 2);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metrics_path, .data = "{\"event\":\"step\",\"loss\":1.0,\"supervised_token_count\":4,\"entity_token_count\":0,\"ignored_token_count\":0,\"target_build_ms\":1.0,\"train_step_ms\":9.0,\"step_wall_ms\":10.0,\"graph_build_ms\":1.0,\"runtime_input_ms\":1.0,\"autodiff_ms\":2.0,\"execute_ms\":3.0,\"extract_ms\":1.0,\"optimizer_update_ms\":1.0,\"trainer_total_ms\":8.0,\"peak_resident_bytes\":1024,\"supervised_tokens_per_second\":400.0}\n{\"event\":\"epoch\",\"avg_loss\":1.0}\n" });

    try std.testing.expectError(error.NoEntityPositiveTokens, validation.validateRun(allocator, out_dir, .{}));
}

test "GLiNER2 autodiff run validator rejects missing performance metrics" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const out_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(out_dir);
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, validation.manifest_file_name });
    defer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, validation.metrics_file_name });
    defer allocator.free(metrics_path);

    try writeManifest(allocator, manifest_path, 1, 1, 2);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metrics_path, .data = "{\"event\":\"step\",\"loss\":1.0,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":0}\n" });

    try std.testing.expectError(error.InvalidMetricsRecord, validation.validateRun(allocator, out_dir, .{}));
}

test "GLiNER2 autodiff run validator rejects throughput below requested threshold" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const out_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(out_dir);
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, validation.manifest_file_name });
    defer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, validation.metrics_file_name });
    defer allocator.free(metrics_path);
    const adapter_path = try std.fs.path.join(allocator, &.{ out_dir, "encoder.layer.0.attention.self.query_proj.lora_A.bin" });
    defer allocator.free(adapter_path);
    const peft_config_path = try std.fs.path.join(allocator, &.{ out_dir, "adapter_config.json" });
    defer allocator.free(peft_config_path);
    const peft_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "adapter_model.safetensors" });
    defer allocator.free(peft_checkpoint_path);
    const task_head_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "task_head.safetensors" });
    defer allocator.free(task_head_checkpoint_path);

    try writeManifest(allocator, manifest_path, 1, 1, 2);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metrics_path, .data = "{\"event\":\"step\",\"loss\":1.0,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":0,\"target_build_ms\":1.0,\"train_step_ms\":999.0,\"step_wall_ms\":1000.0,\"graph_build_ms\":1.0,\"runtime_input_ms\":1.0,\"autodiff_ms\":300.0,\"execute_ms\":600.0,\"extract_ms\":1.0,\"optimizer_update_ms\":1.0,\"trainer_total_ms\":950.0,\"peak_resident_bytes\":1024,\"supervised_tokens_per_second\":4.0}\n{\"event\":\"epoch\",\"avg_loss\":1.0}\n" });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = adapter_path, .data = "LORA" });
    try writePeftConfig(peft_config_path);
    try writeOneTensorSafetensors(allocator, peft_checkpoint_path);
    try writeTaskHeadSafetensors(allocator, task_head_checkpoint_path);

    try std.testing.expectError(error.ThroughputBelowThreshold, validation.validateRun(allocator, out_dir, .{
        .min_supervised_tokens_per_second = 5.0,
    }));
}

test "GLiNER2 autodiff run validator rejects run above requested performance ceilings" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const out_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(out_dir);
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, validation.manifest_file_name });
    defer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, validation.metrics_file_name });
    defer allocator.free(metrics_path);

    try writeManifest(allocator, manifest_path, 1, 1, 2);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metrics_path, .data = "{\"event\":\"step\",\"loss\":1.0,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":0,\"target_build_ms\":1.0,\"train_step_ms\":9.0,\"step_wall_ms\":10.0,\"graph_build_ms\":1.0,\"runtime_input_ms\":1.0,\"autodiff_ms\":2.0,\"execute_ms\":3.0,\"extract_ms\":1.0,\"optimizer_update_ms\":1.0,\"trainer_total_ms\":8.0,\"peak_resident_bytes\":1024,\"supervised_tokens_per_second\":400.0}\n{\"event\":\"epoch\",\"avg_loss\":1.0}\n" });

    try std.testing.expectError(error.AvgStepWallAboveThreshold, validation.validateRun(allocator, out_dir, .{
        .max_avg_step_wall_ms = 9.0,
    }));
    try std.testing.expectError(error.TotalExecuteMsAboveThreshold, validation.validateRun(allocator, out_dir, .{
        .max_total_execute_ms = 2.0,
    }));
    try std.testing.expectError(error.PeakResidentBytesAboveThreshold, validation.validateRun(allocator, out_dir, .{
        .max_peak_resident_bytes = 1023,
    }));
    try std.testing.expectError(error.InvalidPerformanceThreshold, validation.validateRun(allocator, out_dir, .{
        .max_avg_step_wall_ms = 0,
    }));
    try std.testing.expectError(error.InvalidPerformanceThreshold, validation.validateRun(allocator, out_dir, .{
        .max_peak_resident_bytes = 0,
    }));
}

test "GLiNER2 autodiff run validator rejects non-resident Metal optimizer metrics" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const out_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(out_dir);
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, validation.manifest_file_name });
    defer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, validation.metrics_file_name });
    defer allocator.free(metrics_path);

    try writeManifest(allocator, manifest_path, 1, 1, 2);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metrics_path, .data = "{\"event\":\"step\",\"loss\":1.0,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":0,\"target_build_ms\":1.0,\"train_step_ms\":9.0,\"step_wall_ms\":10.0,\"graph_build_ms\":1.0,\"runtime_input_ms\":1.0,\"autodiff_ms\":2.0,\"execute_ms\":3.0,\"extract_ms\":1.0,\"optimizer_update_ms\":1.0,\"device_optimizer_ms\":0.0,\"optimizer_backend\":\"host\",\"device_resident_transfer_count\":0,\"device_trainable_bytes\":128,\"trainer_total_ms\":8.0,\"peak_resident_bytes\":1024,\"supervised_tokens_per_second\":400.0}\n{\"event\":\"epoch\",\"avg_loss\":1.0}\n" });

    try std.testing.expectError(error.OptimizerBackendMismatch, validation.validateRun(allocator, out_dir, .{
        .require_optimizer_backend = "metal",
    }));
    try std.testing.expectError(error.OptimizerBackendMismatch, validation.validateRun(allocator, out_dir, .{
        .require_optimizer_backend = "mlx",
    }));

    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metrics_path, .data = "{\"event\":\"step\",\"loss\":1.0,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":0,\"target_build_ms\":1.0,\"train_step_ms\":9.0,\"step_wall_ms\":10.0,\"graph_build_ms\":1.0,\"runtime_input_ms\":1.0,\"autodiff_ms\":2.0,\"execute_ms\":3.0,\"extract_ms\":1.0,\"optimizer_update_ms\":1.0,\"device_optimizer_ms\":0.5,\"optimizer_backend\":\"metal\",\"device_resident_transfer_count\":1,\"device_trainable_bytes\":128,\"trainer_total_ms\":8.0,\"peak_resident_bytes\":1024,\"supervised_tokens_per_second\":400.0}\n{\"event\":\"epoch\",\"avg_loss\":1.0}\n" });

    try std.testing.expectError(error.DeviceResidentTransferCountAboveThreshold, validation.validateRun(allocator, out_dir, .{
        .require_optimizer_backend = "metal",
        .max_device_resident_transfer_count = 0,
    }));

    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metrics_path, .data = "{\"event\":\"step\",\"loss\":1.0,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":0,\"target_build_ms\":1.0,\"train_step_ms\":9.0,\"step_wall_ms\":10.0,\"graph_build_ms\":1.0,\"runtime_input_ms\":1.0,\"autodiff_ms\":2.0,\"execute_ms\":3.0,\"extract_ms\":1.0,\"optimizer_update_ms\":1.0,\"device_optimizer_ms\":0.5,\"optimizer_backend\":\"metal\",\"device_resident_transfer_count\":0,\"device_trainable_bytes\":0,\"trainer_total_ms\":8.0,\"peak_resident_bytes\":1024,\"supervised_tokens_per_second\":400.0}\n{\"event\":\"epoch\",\"avg_loss\":1.0}\n" });

    try std.testing.expectError(error.DeviceTrainableBytesBelowThreshold, validation.validateRun(allocator, out_dir, .{
        .require_optimizer_backend = "metal",
        .max_device_resident_transfer_count = 0,
        .min_device_trainable_bytes = 1,
    }));
}

test "GLiNER2 autodiff run validator rejects run below requested cardinality thresholds" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const out_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(out_dir);
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, validation.manifest_file_name });
    defer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, validation.metrics_file_name });
    defer allocator.free(metrics_path);
    const adapter_path = try std.fs.path.join(allocator, &.{ out_dir, "encoder.layer.0.attention.self.query_proj.lora_A.bin" });
    defer allocator.free(adapter_path);
    const peft_config_path = try std.fs.path.join(allocator, &.{ out_dir, "adapter_config.json" });
    defer allocator.free(peft_config_path);
    const peft_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "adapter_model.safetensors" });
    defer allocator.free(peft_checkpoint_path);
    const task_head_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "task_head.safetensors" });
    defer allocator.free(task_head_checkpoint_path);

    try writeManifest(allocator, manifest_path, 1, 1, 2);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metrics_path, .data = "{\"event\":\"step\",\"loss\":1.0,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":0,\"target_build_ms\":1.0,\"train_step_ms\":9.0,\"step_wall_ms\":10.0,\"graph_build_ms\":1.0,\"runtime_input_ms\":1.0,\"autodiff_ms\":2.0,\"execute_ms\":3.0,\"extract_ms\":1.0,\"optimizer_update_ms\":1.0,\"trainer_total_ms\":8.0,\"peak_resident_bytes\":1024,\"supervised_tokens_per_second\":400.0}\n{\"event\":\"epoch\",\"avg_loss\":1.0}\n" });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = adapter_path, .data = "LORA" });
    try writePeftConfig(peft_config_path);
    try writeOneTensorSafetensors(allocator, peft_checkpoint_path);
    try writeTaskHeadSafetensors(allocator, task_head_checkpoint_path);

    try std.testing.expectError(error.ExampleCountBelowThreshold, validation.validateRun(allocator, out_dir, .{
        .min_examples = 2,
    }));
    try std.testing.expectError(error.StepCountBelowThreshold, validation.validateRun(allocator, out_dir, .{
        .min_steps = 2,
    }));
    try std.testing.expectError(error.EntityLabelCountBelowThreshold, validation.validateRun(allocator, out_dir, .{
        .min_entity_labels = 4,
    }));
    try std.testing.expectError(error.SupervisedTokenCountBelowThreshold, validation.validateRun(allocator, out_dir, .{
        .min_supervised_tokens = 5,
    }));
    try std.testing.expectError(error.EntityTokenCountBelowThreshold, validation.validateRun(allocator, out_dir, .{
        .min_entity_tokens = 2,
    }));
}

test "GLiNER2 autodiff run validator rejects task head class mismatch" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const out_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(out_dir);
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, validation.manifest_file_name });
    defer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, validation.metrics_file_name });
    defer allocator.free(metrics_path);
    const adapter_path = try std.fs.path.join(allocator, &.{ out_dir, "encoder.layer.0.attention.self.query_proj.lora_A.bin" });
    defer allocator.free(adapter_path);
    const peft_config_path = try std.fs.path.join(allocator, &.{ out_dir, "adapter_config.json" });
    defer allocator.free(peft_config_path);
    const peft_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "adapter_model.safetensors" });
    defer allocator.free(peft_checkpoint_path);
    const task_head_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, "task_head.safetensors" });
    defer allocator.free(task_head_checkpoint_path);

    try writeManifestWithShape(allocator, manifest_path, 1, 1, 2, 4, 1);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metrics_path, .data = "{\"event\":\"step\",\"loss\":1.0,\"supervised_token_count\":4,\"entity_token_count\":1,\"ignored_token_count\":0,\"target_build_ms\":1.0,\"train_step_ms\":9.0,\"step_wall_ms\":10.0,\"graph_build_ms\":1.0,\"runtime_input_ms\":1.0,\"autodiff_ms\":2.0,\"execute_ms\":3.0,\"extract_ms\":1.0,\"optimizer_update_ms\":1.0,\"trainer_total_ms\":8.0,\"peak_resident_bytes\":1024,\"supervised_tokens_per_second\":400.0}\n{\"event\":\"epoch\",\"avg_loss\":1.0}\n" });
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = adapter_path, .data = "LORA" });
    try writePeftConfig(peft_config_path);
    try writeOneTensorSafetensors(allocator, peft_checkpoint_path);
    try writeTaskHeadSafetensorsWithShape(allocator, task_head_checkpoint_path, 3, 1);

    try std.testing.expectError(error.InvalidTaskHeadTensorShape, validation.validateRun(allocator, out_dir, .{}));
}

fn writeManifest(
    allocator: std.mem.Allocator,
    path: []const u8,
    adapter_count: usize,
    peft_tensor_count: usize,
    task_head_tensor_count: usize,
) !void {
    try writeManifestWithShape(allocator, path, adapter_count, peft_tensor_count, task_head_tensor_count, 4, 1);
}

fn writeManifestWithShape(
    allocator: std.mem.Allocator,
    path: []const u8,
    adapter_count: usize,
    peft_tensor_count: usize,
    task_head_tensor_count: usize,
    num_classes: usize,
    hidden_size: usize,
) !void {
    try writeManifestWithRun(allocator, path, adapter_count, peft_tensor_count, task_head_tensor_count, num_classes, hidden_size, 1, 1, 1, 1.0);
}

fn writeManifestWithRun(
    allocator: std.mem.Allocator,
    path: []const u8,
    adapter_count: usize,
    peft_tensor_count: usize,
    task_head_tensor_count: usize,
    num_classes: usize,
    hidden_size: usize,
    epochs: usize,
    example_count: usize,
    total_steps: usize,
    final_avg_loss: f64,
) !void {
    const data = try std.fmt.allocPrint(allocator,
        \\{{
        \\  "schema_version": "gliner2_autodiff_training/v1",
        \\  "artifact_family_version": "gliner2_autodiff_adapter/v1",
        \\  "backend": "Metal",
        \\  "metrics_file": "training_metrics.jsonl",
        \\  "adapter_parameter_file_count": {},
        \\  "peft_adapter_checkpoint": "adapter_model.safetensors",
        \\  "peft_adapter_config": "adapter_config.json",
        \\  "peft_adapter_tensor_count": {},
        \\  "regular_trainable_checkpoint": "task_head.safetensors",
        \\  "regular_trainable_tensor_count": {},
        \\  "num_classes": {},
        \\  "hidden_size": {},
        \\  "model_dir": "base-model",
        \\  "lora_rank": 16,
        \\  "lora_alpha": 32,
        \\  "lora_targets": "query_proj,value_proj",
        \\  "entity_labels": ["location", "organization", "person"],
        \\  "entity_label_count": 3,
        \\  "epochs": {},
        \\  "batch_size": 1,
        \\  "seq_len": 64,
        \\  "example_count": {},
        \\  "total_steps": {},
        \\  "final_avg_loss": {d}
        \\}}
        \\
    , .{ adapter_count, peft_tensor_count, task_head_tensor_count, num_classes, hidden_size, epochs, example_count, total_steps, final_avg_loss });
    defer allocator.free(data);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = data });
}

fn writePeftConfig(path: []const u8) !void {
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{
        .sub_path = path,
        .data =
        \\{
        \\  "base_model_name_or_path": "base-model",
        \\  "peft_type": "LORA",
        \\  "task_type": "TOKEN_CLS",
        \\  "r": 16,
        \\  "lora_alpha": 32,
        \\  "target_modules": ["query_proj", "value_proj"],
        \\  "use_dora": false
        \\}
        \\
        ,
    });
}

fn writeOneTensorSafetensors(allocator: std.mem.Allocator, path: []const u8) !void {
    var buf: std.Io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    const header = "{\"adapter.test.lora_A.weight\":{\"dtype\":\"F32\",\"shape\":[1],\"data_offsets\":[0,4]}}";
    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, header.len, .little);
    try buf.writer.writeAll(&len_buf);
    try buf.writer.writeAll(header);
    var data_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &data_buf, 0, .little);
    try buf.writer.writeAll(&data_buf);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = buf.written() });
}

fn writeTaskHeadSafetensors(allocator: std.mem.Allocator, path: []const u8) !void {
    try writeTaskHeadSafetensorsWithShape(allocator, path, 4, 1);
}

fn writeTaskHeadSafetensorsWithShape(allocator: std.mem.Allocator, path: []const u8, num_classes: usize, hidden_size: usize) !void {
    var buf: std.Io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    const weight_bytes = num_classes * hidden_size * @sizeOf(f32);
    const bias_bytes = num_classes * @sizeOf(f32);
    const header = try std.fmt.allocPrint(
        allocator,
        "{{\"classifier.weight\":{{\"dtype\":\"F32\",\"shape\":[{},{}],\"data_offsets\":[0,{}]}}," ++
            "\"classifier.bias\":{{\"dtype\":\"F32\",\"shape\":[{}],\"data_offsets\":[{},{}]}}}}",
        .{ num_classes, hidden_size, weight_bytes, num_classes, weight_bytes, weight_bytes + bias_bytes },
    );
    defer allocator.free(header);
    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, header.len, .little);
    try buf.writer.writeAll(&len_buf);
    try buf.writer.writeAll(header);
    for (0..weight_bytes + bias_bytes) |_| try buf.writer.writeByte(0);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = buf.written() });
}
