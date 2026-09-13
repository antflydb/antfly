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
const assets = @import("assets/gliner2.zig");

const compat = @import("../io/compat.zig");
const entity_cleanup_model = @import("entity_cleanup_model.zig");

// Checkpoint types and operations are owned by the offline asset layer.
pub const artifact_family_version = assets.artifact_family_version;
pub const legacy_artifact_family_version = assets.legacy_artifact_family_version;
pub const checkpoint_file_name = assets.checkpoint_file_name;
pub const config_file_name = assets.config_file_name;
pub const encoder_config_file_name = assets.encoder_config_file_name;
pub const adapter_checkpoint_file_name = assets.adapter_checkpoint_file_name;
pub const adapter_config_file_name = assets.adapter_config_file_name;
pub const task_head_checkpoint_file_name = assets.task_head_checkpoint_file_name;
pub const tokenizer_file_name = assets.tokenizer_file_name;
pub const tokenizer_config_file_name = assets.tokenizer_config_file_name;
pub const special_tokens_map_file_name = assets.special_tokens_map_file_name;
pub const added_tokens_file_name = assets.added_tokens_file_name;
pub const sentencepiece_model_file_name = assets.sentencepiece_model_file_name;
pub const materialization_manifest_file_name = assets.materialization_manifest_file_name;
pub const materialization_schema_version = assets.materialization_schema_version;
pub const isSupportedArtifactFamilyVersion = assets.isSupportedArtifactFamilyVersion;
pub const default_lora_target_modules = assets.default_lora_target_modules;
pub const default_lora_dropout = assets.default_lora_dropout;
pub const expanded_encoder_lora_target_modules = assets.expanded_encoder_lora_target_modules;
pub const BackboneConfig = assets.BackboneConfig;
pub const ArtifactPaths = assets.ArtifactPaths;
pub const CheckpointInspection = assets.CheckpointInspection;
pub const AdapterConfig = assets.AdapterConfig;
pub const LoRATargetTensor = assets.LoRATargetTensor;
pub const BootstrapOptions = assets.BootstrapOptions;
pub const BootstrapSummary = assets.BootstrapSummary;
pub const LoRATensorSummary = assets.LoRATensorSummary;
pub const LoRABundleInspectionSummary = assets.LoRABundleInspectionSummary;
pub const LoadedLoRALayer = assets.LoadedLoRALayer;
pub const LoadedPassthroughTensor = assets.LoadedPassthroughTensor;
pub const LoadedLoRABundle = assets.LoadedLoRABundle;
pub const validateLoRADropout = assets.validateLoRADropout;
pub const expandLoRATargetModules = assets.expandLoRATargetModules;
pub const AutodiffAdapterParam = assets.AutodiffAdapterParam;
pub const AutodiffAdapterExportSummary = assets.AutodiffAdapterExportSummary;
pub const AutodiffRegularParamExportSummary = assets.AutodiffRegularParamExportSummary;
pub const MaterializeSummary = assets.MaterializeSummary;
const MaterializationInventory = assets.MaterializationInventory;
const MaterializationManifest = assets.MaterializationManifest;
pub const ClassifierTaskHead = assets.ClassifierTaskHead;
pub const resolveArtifactPaths = assets.resolveArtifactPaths;
pub const loadBackboneConfig = assets.loadBackboneConfig;
pub const resolveLoRACheckpointPath = assets.resolveLoRACheckpointPath;
pub const inspectCheckpoint = assets.inspectCheckpoint;
pub const bootstrapLoRABundle = assets.bootstrapLoRABundle;
pub const inspectLoRABundle = assets.inspectLoRABundle;
pub const loadLoRABundle = assets.loadLoRABundle;
pub const saveLoRABundle = assets.saveLoRABundle;
pub const exportAutodiffAdaptersAsPeftBundle = assets.exportAutodiffAdaptersAsPeftBundle;
pub const exportAutodiffRegularParamsAsSafetensors = assets.exportAutodiffRegularParamsAsSafetensors;
pub const loadClassifierTaskHead = assets.loadClassifierTaskHead;
pub const materializeMergedModel = assets.materializeMergedModel;
pub const freeCheckpointInspection = assets.freeCheckpointInspection;
pub const freeBootstrapSummary = assets.freeBootstrapSummary;
pub const freeLoRABundleInspectionSummary = assets.freeLoRABundleInspectionSummary;
pub const freeAutodiffAdapterExportSummary = assets.freeAutodiffAdapterExportSummary;
pub const freeAutodiffRegularParamExportSummary = assets.freeAutodiffRegularParamExportSummary;
pub const freeMaterializeSummary = assets.freeMaterializeSummary;
pub const autodiffParamNameToPeftName = assets.autodiffParamNameToPeftName;
const writeHeaderAndTensorsF32 = assets.writeHeaderAndTensorsF32;
const doraMagnitudeTensorName = assets.doraMagnitudeTensorName;
const materializationStagingPath = assets.materializationStagingPath;
const pathExists = assets.pathExists;
const writeSyncedJsonFile = assets.writeSyncedJsonFile;
const openTensorAccessForFile = assets.openTensorAccessForFile;
const loadTensorAsF32 = assets.loadTensorAsF32;
const stringSliceContains = assets.stringSliceContains;

test "gliner2 checkpoint inspection reads config and tensor summary" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_gliner2_inspect_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};
    const encoder_dir = try std.fs.path.join(allocator, &.{ root, "encoder_config" });
    defer allocator.free(encoder_dir);
    try compat.cwd().createDirPath(compat.io(), encoder_dir);
    const config_path = try std.fs.path.join(allocator, &.{ root, "config.json" });
    defer allocator.free(config_path);
    const encoder_config_path = try std.fs.path.join(allocator, &.{ root, "encoder_config", "config.json" });
    defer allocator.free(encoder_config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_name":"urchade/gliner2","model_type":"gliner2","counting_layer":"count_embed","token_pooling":"first","max_width":12,"count_embed_dim":128,"count_embed_layers":2,"count_embed_heads":4,"count_embed_ffn":256,"max_count_embed":20}
        ,
    });
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = encoder_config_path,
        .data =
        \\{"vocab_size":30522,"hidden_size":128,"num_hidden_layers":2,"num_attention_heads":4,"intermediate_size":256,"max_position_embeddings":512,"type_vocab_size":2,"position_buckets":32,"relative_attention":true,"hidden_dropout_prob":0.1,"attention_probs_dropout_prob":0.1,"layer_norm_eps":1e-7}
        ,
    });
    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "encoder.embeddings.word_embeddings.weight", .shape = &.{ 4, 128 }, .data = &[_]f32{0} ** (4 * 128) },
        .{ .name = "encoder.encoder.rel_embeddings.weight", .shape = &.{ 32, 32 }, .data = &[_]f32{0} ** (32 * 32) },
        .{ .name = "encoder.encoder.LayerNorm.weight", .shape = &.{128}, .data = &[_]f32{0} ** 128 },
        .{ .name = "encoder.encoder.layer.0.attention.self.query_proj.weight", .shape = &.{ 128, 128 }, .data = &[_]f32{0} ** (128 * 128) },
        .{ .name = "encoder.encoder.layer.0.attention.self.key_proj.weight", .shape = &.{ 128, 128 }, .data = &[_]f32{0} ** (128 * 128) },
        .{ .name = "encoder.encoder.layer.0.attention.self.value_proj.weight", .shape = &.{ 128, 128 }, .data = &[_]f32{0} ** (128 * 128) },
        .{ .name = "encoder.encoder.layer.1.attention.self.query_proj.weight", .shape = &.{ 128, 128 }, .data = &[_]f32{0} ** (128 * 128) },
        .{ .name = "encoder.encoder.layer.1.attention.self.key_proj.weight", .shape = &.{ 128, 128 }, .data = &[_]f32{0} ** (128 * 128) },
        .{ .name = "encoder.encoder.layer.1.attention.self.value_proj.weight", .shape = &.{ 128, 128 }, .data = &[_]f32{0} ** (128 * 128) },
        .{ .name = "span_rep.span_rep_layer.project_start.0.weight", .shape = &.{ 32, 128 }, .data = &[_]f32{0} ** (32 * 128) },
        .{ .name = "count_embed.pos_embedding.weight", .shape = &.{ 8, 128 }, .data = &[_]f32{0} ** (8 * 128) },
    });

    var summary = try inspectCheckpoint(allocator, root, null);
    defer freeCheckpointInspection(allocator, &summary);
    try std.testing.expect(summary.word_embeddings_found);
    try std.testing.expect(summary.rel_embeddings_found);
    try std.testing.expect(summary.final_layernorm_found);
    try std.testing.expectEqual(@as(usize, 2), summary.query_proj_weights_found);
    try std.testing.expect(summary.core_backbone_loadable);
}

test "gliner2 upstream lora target groups expand to concrete modules" {
    const allocator = std.testing.allocator;
    const expanded = try expandLoRATargetModules(allocator, default_lora_target_modules[0..]);
    defer {
        for (expanded) |item| allocator.free(item);
        allocator.free(expanded);
    }

    try std.testing.expect(stringSliceContains(expanded, "query_proj"));
    try std.testing.expect(stringSliceContains(expanded, "key_proj"));
    try std.testing.expect(stringSliceContains(expanded, "value_proj"));
    try std.testing.expect(stringSliceContains(expanded, "attention.output.dense"));
    try std.testing.expect(stringSliceContains(expanded, "intermediate.dense"));
    try std.testing.expect(stringSliceContains(expanded, "output.dense"));
    try std.testing.expect(stringSliceContains(expanded, "span_rep.span_rep_layer.project_start.0"));
    try std.testing.expect(stringSliceContains(expanded, "span_rep.span_rep_layer.project_start.3"));
    try std.testing.expect(stringSliceContains(expanded, "span_rep.span_rep_layer.project_end.0"));
    try std.testing.expect(stringSliceContains(expanded, "span_rep.span_rep_layer.project_end.3"));
    try std.testing.expect(stringSliceContains(expanded, "span_rep.span_rep_layer.out_project.0"));
    try std.testing.expect(stringSliceContains(expanded, "span_rep.span_rep_layer.out_project.3"));
    try std.testing.expect(stringSliceContains(expanded, "classifier.0"));
    try std.testing.expect(stringSliceContains(expanded, "classifier.2"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.in_projector"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.transformer.layers.0.linear1"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.transformer.layers.0.linear2"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.transformer.layers.1.linear1"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.transformer.layers.1.linear2"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.out_projector.0"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.out_projector.2"));
    try std.testing.expect(stringSliceContains(expanded, "count_embed.transformer.out_projector.4"));
    try std.testing.expect(stringSliceContains(expanded, "count_pred.0"));
    try std.testing.expect(stringSliceContains(expanded, "count_pred.2"));
    try std.testing.expect(!stringSliceContains(expanded, "count_embed.gru"));
    try std.testing.expect(!stringSliceContains(expanded, "count_embed.transformer.transformer.layers.0.self_attn.out_proj"));
    try std.testing.expect(!stringSliceContains(expanded, "count_embed.transformer.transformer.layers.1.self_attn.out_proj"));
    try std.testing.expect(!stringSliceContains(expanded, "self_attn.in_proj_weight"));
    try std.testing.expect(!stringSliceContains(expanded, "task_classifier"));
}

test "gliner2 lora dropout validates python-compatible range" {
    try validateLoRADropout(0.0);
    try validateLoRADropout(0.1);
    try std.testing.expectError(error.InvalidLoRADropout, validateLoRADropout(-0.1));
    try std.testing.expectError(error.InvalidLoRADropout, validateLoRADropout(1.0));
}

test "gliner2 lora artifact family writes v1 and accepts legacy v1alpha1" {
    try std.testing.expectEqualStrings("gliner2_lora/v1", artifact_family_version);
    try std.testing.expect(isSupportedArtifactFamilyVersion(artifact_family_version));
    try std.testing.expect(isSupportedArtifactFamilyVersion(legacy_artifact_family_version));
    try std.testing.expect(!isSupportedArtifactFamilyVersion("gliner2_lora/v2"));
}

test "gliner2 bootstrap and inspect lora bundle" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_gliner2_bootstrap_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};
    const encoder_dir = try std.fs.path.join(allocator, &.{ root, "encoder_config" });
    defer allocator.free(encoder_dir);
    try compat.cwd().createDirPath(compat.io(), encoder_dir);
    const config_path = try std.fs.path.join(allocator, &.{ root, "config.json" });
    defer allocator.free(config_path);
    const encoder_config_path = try std.fs.path.join(allocator, &.{ root, "encoder_config", "config.json" });
    defer allocator.free(encoder_config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data = "{\"model_name\":\"urchade/gliner2\",\"model_type\":\"gliner2\",\"counting_layer\":\"count_embed\",\"token_pooling\":\"first\",\"max_width\":12}",
    });
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = encoder_config_path,
        .data = "{\"hidden_size\":128,\"num_hidden_layers\":1,\"num_attention_heads\":4}",
    });
    for ([_][]const u8{
        tokenizer_file_name,
        tokenizer_config_file_name,
        special_tokens_map_file_name,
        added_tokens_file_name,
    }) |file_name| {
        const tokenizer_artifact_path = try std.fs.path.join(allocator, &.{ root, file_name });
        defer allocator.free(tokenizer_artifact_path);
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = tokenizer_artifact_path, .data = "{}" });
    }
    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "encoder.embeddings.word_embeddings.weight", .shape = &.{ 4, 128 }, .data = &[_]f32{0} ** (4 * 128) },
        .{ .name = "encoder.encoder.rel_embeddings.weight", .shape = &.{ 32, 32 }, .data = &[_]f32{0} ** (32 * 32) },
        .{ .name = "encoder.encoder.LayerNorm.weight", .shape = &.{128}, .data = &[_]f32{0} ** 128 },
        .{ .name = "encoder.encoder.layer.0.attention.self.query_proj.weight", .shape = &.{ 128, 128 }, .data = &[_]f32{0} ** (128 * 128) },
        .{ .name = "encoder.encoder.layer.0.attention.self.key_proj.weight", .shape = &.{ 128, 128 }, .data = &[_]f32{0} ** (128 * 128) },
        .{ .name = "encoder.encoder.layer.0.attention.self.value_proj.weight", .shape = &.{ 128, 128 }, .data = &[_]f32{0} ** (128 * 128) },
    });

    const out_dir = try std.fs.path.join(allocator, &.{ root, "lora" });
    defer allocator.free(out_dir);
    var bootstrap = try bootstrapLoRABundle(allocator, root, out_dir, .{ .rank = 8, .alpha = 16 });
    defer freeBootstrapSummary(allocator, &bootstrap);
    try std.testing.expectEqual(@as(usize, 3), bootstrap.resolved_tensors.len);

    var bundle = try loadLoRABundle(allocator, root, out_dir);
    defer bundle.deinit();
    for (bundle.layers) |*layer| {
        const magnitude = try allocator.alloc(f32, layer.output_dim);
        @memset(magnitude, 1.0);
        layer.dora_magnitude = magnitude;
        layer.dora_magnitude_tensor_name = try doraMagnitudeTensorName(allocator, layer.base_tensor_name);
    }
    try saveLoRABundle(&bundle, out_dir);
    const task_head_path = try std.fs.path.join(allocator, &.{ out_dir, task_head_checkpoint_file_name });
    defer allocator.free(task_head_path);
    const classifier_weight = [_]f32{0.25} ** (3 * 128);
    const classifier_bias = [_]f32{ 0.5, -0.25, 0.75 };
    try writeHeaderAndTensorsF32(allocator, task_head_path, &.{
        .{ .name = "classifier.weight", .shape = &.{ 3, 128 }, .data = &classifier_weight },
        .{ .name = "classifier.bias", .shape = &.{3}, .data = &classifier_bias },
    });

    var cleanup_head = try entity_cleanup_model.CleanupHead.init(allocator, 8, 4, 0);
    defer cleanup_head.deinit();
    try entity_cleanup_model.saveHead(allocator, &cleanup_head, out_dir);

    var inspect = try inspectLoRABundle(allocator, root, out_dir);
    defer freeLoRABundleInspectionSummary(allocator, &inspect);
    try std.testing.expectEqual(@as(usize, 3), inspect.resolved_tensor_count);
    try std.testing.expectEqual(@as(?usize, 8), inspect.lora_rank);
    try std.testing.expectEqual(@as(usize, 3), inspect.dora_magnitude_tensor_count);
    try std.testing.expectEqual(@as(usize, 384), inspect.dora_magnitude_parameter_count);
    try std.testing.expectEqual(@as(?bool, true), inspect.use_dora);
    try std.testing.expect(inspect.cleanup_head_present);

    const materialized_dir = try std.fs.path.join(allocator, &.{ root, "materialized" });
    defer allocator.free(materialized_dir);
    var materialize = try materializeMergedModel(allocator, root, out_dir, materialized_dir);
    defer freeMaterializeSummary(allocator, &materialize);
    try std.testing.expect(materialize.copied_cleanup_head);
    try std.testing.expectEqual(@as(usize, 3), materialize.merged_dora_tensor_count);
    try std.testing.expectEqual(@as(usize, 2), materialize.attached_task_head_tensor_count);
    try std.testing.expectError(error.OutputDirectoryAlreadyExists, materializeMergedModel(allocator, root, out_dir, materialized_dir));

    const completion_path = try std.fs.path.join(allocator, &.{ materialized_dir, materialization_manifest_file_name });
    defer allocator.free(completion_path);
    const completion_bytes = try compat.cwd().readFileAlloc(compat.io(), completion_path, allocator, .limited(64 * 1024));
    defer allocator.free(completion_bytes);
    var completion = try std.json.parseFromSlice(std.json.Value, allocator, completion_bytes, .{});
    defer completion.deinit();
    try std.testing.expectEqualStrings("complete", completion.value.object.get("status").?.string);
    try std.testing.expectEqualStrings(artifact_family_version, completion.value.object.get("artifact_family_version").?.string);
    const inventory = completion.value.object.get("supporting_artifacts").?.object;
    try std.testing.expect(inventory.get("tokenizer").?.bool);
    try std.testing.expect(inventory.get("added_tokens").?.bool);
    try std.testing.expect(!inventory.get("sentencepiece_model").?.bool);

    const manifest_inventory = MaterializationInventory{
        .config = true,
        .encoder_config = true,
        .tokenizer = true,
        .tokenizer_config = true,
        .special_tokens_map = true,
        .added_tokens = true,
        .sentencepiece_model = false,
    };
    try writeSyncedJsonFile(allocator, completion_path, MaterializationManifest{
        .artifact_family_version = legacy_artifact_family_version,
        .base_model_dir = root,
        .adapter_model_dir = out_dir,
        .merged_lora_tensor_count = materialize.merged_lora_tensor_count,
        .merged_dora_tensor_count = materialize.merged_dora_tensor_count,
        .task_head_passthrough_tensor_count = materialize.task_head_passthrough_tensor_count,
        .attached_task_head_tensor_count = materialize.attached_task_head_tensor_count,
        .copied_boundary_head = materialize.copied_boundary_head,
        .copied_boundary_task_head = materialize.copied_boundary_task_head,
        .copied_cleanup_head = materialize.copied_cleanup_head,
        .supporting_artifacts = manifest_inventory,
    });
    var legacy_reload = try inspectCheckpoint(allocator, materialized_dir, null);
    defer freeCheckpointInspection(allocator, &legacy_reload);
    try std.testing.expectEqualStrings(legacy_artifact_family_version, legacy_reload.artifact_family_version);

    try writeSyncedJsonFile(allocator, completion_path, MaterializationManifest{
        .artifact_family_version = "gliner2_lora/v2",
        .base_model_dir = root,
        .adapter_model_dir = out_dir,
        .merged_lora_tensor_count = materialize.merged_lora_tensor_count,
        .merged_dora_tensor_count = materialize.merged_dora_tensor_count,
        .task_head_passthrough_tensor_count = materialize.task_head_passthrough_tensor_count,
        .attached_task_head_tensor_count = materialize.attached_task_head_tensor_count,
        .copied_boundary_head = materialize.copied_boundary_head,
        .copied_boundary_task_head = materialize.copied_boundary_task_head,
        .copied_cleanup_head = materialize.copied_cleanup_head,
        .supporting_artifacts = manifest_inventory,
    });
    try std.testing.expectError(error.UnsupportedArtifactFamilyVersion, inspectCheckpoint(allocator, materialized_dir, null));

    var materialized_access = try openTensorAccessForFile(allocator, materialize.output_checkpoint_path);
    defer materialized_access.deinit();
    var materialized_classifier_weight = try loadTensorAsF32(allocator, materialized_access, "classifier.weight");
    defer materialized_classifier_weight.deinit();
    var materialized_classifier_bias = try loadTensorAsF32(allocator, materialized_access, "classifier.bias");
    defer materialized_classifier_bias.deinit();
    try std.testing.expectEqualSlices(i64, &.{ 3, 128 }, materialized_classifier_weight.shape);
    try std.testing.expectEqualSlices(i64, &.{3}, materialized_classifier_bias.shape);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), materialized_classifier_weight.asFloat32()[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -0.25), materialized_classifier_bias.asFloat32()[1], 1e-6);

    var adapter_head = try loadClassifierTaskHead(allocator, task_head_path);
    defer adapter_head.deinit();
    var materialized_head = try loadClassifierTaskHead(allocator, materialize.output_checkpoint_path);
    defer materialized_head.deinit();
    try std.testing.expectEqual(adapter_head.num_classes, materialized_head.num_classes);
    try std.testing.expectEqual(adapter_head.hidden_size, materialized_head.hidden_size);
    try std.testing.expectEqualSlices(f32, adapter_head.weight, materialized_head.weight);
    try std.testing.expectEqualSlices(f32, adapter_head.bias, materialized_head.bias);

    const hidden_rows = [_]f32{0.5} ** (2 * 128);
    const adapter_logits = try adapter_head.scoreRowsAlloc(allocator, &hidden_rows);
    defer allocator.free(adapter_logits);
    const materialized_logits = try materialized_head.scoreRowsAlloc(allocator, &hidden_rows);
    defer allocator.free(materialized_logits);
    try std.testing.expectEqualSlices(f32, adapter_logits, materialized_logits);
    try std.testing.expectApproxEqAbs(@as(f32, 16.5), adapter_logits[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 15.75), adapter_logits[1], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 16.75), adapter_logits[2], 1e-5);

    // A missing required inventory file must leave neither a published model
    // nor the same-process staging directory behind.
    const added_tokens_path = try std.fs.path.join(allocator, &.{ root, added_tokens_file_name });
    defer allocator.free(added_tokens_path);
    try compat.cwd().deleteFile(compat.io(), added_tokens_path);
    const failed_materialized_dir = try std.fs.path.join(allocator, &.{ root, "materialized-missing-inventory" });
    defer allocator.free(failed_materialized_dir);
    const failed_staging_dir = try materializationStagingPath(allocator, failed_materialized_dir);
    defer allocator.free(failed_staging_dir);
    try std.testing.expectError(
        error.RequiredSupportingArtifactMissing,
        materializeMergedModel(allocator, root, out_dir, failed_materialized_dir),
    );
    try std.testing.expect(!pathExists(failed_materialized_dir));
    try std.testing.expect(!pathExists(failed_staging_dir));
}

test "gliner2 exports autodiff adapter params as inspectable PEFT bundle" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_gliner2_autodiff_export_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};
    const encoder_dir = try std.fs.path.join(allocator, &.{ root, "encoder_config" });
    defer allocator.free(encoder_dir);
    try compat.cwd().createDirPath(compat.io(), encoder_dir);
    const config_path = try std.fs.path.join(allocator, &.{ root, config_file_name });
    defer allocator.free(config_path);
    const encoder_config_path = try std.fs.path.join(allocator, &.{ root, encoder_config_file_name });
    defer allocator.free(encoder_config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data = "{\"model_name\":\"urchade/gliner2\",\"model_type\":\"gliner2\",\"counting_layer\":\"count_embed\",\"token_pooling\":\"first\",\"max_width\":12}",
    });
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = encoder_config_path,
        .data = "{\"hidden_size\":128,\"num_hidden_layers\":1,\"num_attention_heads\":4}",
    });
    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "encoder.embeddings.word_embeddings.weight", .shape = &.{ 4, 128 }, .data = &[_]f32{0} ** (4 * 128) },
        .{ .name = "encoder.encoder.rel_embeddings.weight", .shape = &.{ 32, 32 }, .data = &[_]f32{0} ** (32 * 32) },
        .{ .name = "encoder.encoder.LayerNorm.weight", .shape = &.{128}, .data = &[_]f32{0} ** 128 },
        .{ .name = "encoder.encoder.layer.0.attention.self.query_proj.weight", .shape = &.{ 128, 128 }, .data = &[_]f32{0} ** (128 * 128) },
        .{ .name = "encoder.encoder.layer.0.attention.self.key_proj.weight", .shape = &.{ 128, 128 }, .data = &[_]f32{0} ** (128 * 128) },
        .{ .name = "encoder.encoder.layer.0.attention.self.value_proj.weight", .shape = &.{ 128, 128 }, .data = &[_]f32{0} ** (128 * 128) },
    });

    const out_dir = try std.fs.path.join(allocator, &.{ root, "autodiff_lora" });
    defer allocator.free(out_dir);
    const a_data = [_]f32{0.01} ** (2 * 128);
    const b_data = [_]f32{0.02} ** (128 * 2);
    const params = [_]AutodiffAdapterParam{
        .{
            .name = "encoder.layer.0.attention.self.query_proj.weight.lora_A",
            .dims = &.{ 2, 128 },
            .weights = &a_data,
        },
        .{
            .name = "encoder.layer.0.attention.self.query_proj.weight.lora_B",
            .dims = &.{ 128, 2 },
            .weights = &b_data,
        },
    };
    var exported = try exportAutodiffAdaptersAsPeftBundle(
        allocator,
        out_dir,
        root,
        2,
        4,
        0.0,
        &.{"query"},
        &params,
    );
    defer freeAutodiffAdapterExportSummary(allocator, &exported);
    try std.testing.expectEqual(@as(usize, 2), exported.exported_tensor_count);

    const config_bytes = try compat.cwd().readFileAlloc(compat.io(), exported.adapter_config_path, allocator, .limited(64 * 1024));
    defer allocator.free(config_bytes);
    var config = try std.json.parseFromSlice(std.json.Value, allocator, config_bytes, .{});
    defer config.deinit();
    try std.testing.expect(config.value.object.get("task_type").? == .null);
    try std.testing.expectEqualStrings("Extractor", config.value.object.get("auto_mapping").?.object.get("base_model_class").?.string);
    try std.testing.expectEqualStrings("encoder.encoder.layer.0.attention.self.query_proj", config.value.object.get("target_modules").?.array.items[0].string);

    var inspected = try inspectLoRABundle(allocator, root, out_dir);
    defer freeLoRABundleInspectionSummary(allocator, &inspected);
    try std.testing.expectEqual(@as(usize, 1), inspected.resolved_tensor_count);
    try std.testing.expectEqual(@as(usize, 512), inspected.trainable_parameter_count);
    try std.testing.expectEqualStrings("encoder.encoder.layer.0.attention.self.query_proj.weight", inspected.tensors[0].base_tensor_name);
    try std.testing.expectEqualStrings("base_model.model.encoder.encoder.layer.0.attention.self.query_proj.lora_A.weight", inspected.tensors[0].adapter_a_tensor_name);
}

test "gliner2 classifier task head reloads and scores golden hidden rows" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_gliner2_task_head_score_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const checkpoint_path = try std.fs.path.join(allocator, &.{ root, task_head_checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    const weight = [_]f32{
        1.0,  0.0,  0.5,  -1.0,
        0.25, 0.25, 0.25, 0.25,
        -1.0, 1.0,  0.0,  0.5,
    };
    const bias = [_]f32{ 0.1, -0.2, 0.0 };
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "classifier.weight", .shape = &.{ 3, 4 }, .data = &weight },
        .{ .name = "classifier.bias", .shape = &.{3}, .data = &bias },
    });

    var head = try loadClassifierTaskHead(allocator, checkpoint_path);
    defer head.deinit();
    try std.testing.expectEqual(@as(usize, 3), head.num_classes);
    try std.testing.expectEqual(@as(usize, 4), head.hidden_size);

    const hidden = [_]f32{
        2.0,  -1.0, 0.5, 1.0,
        -2.0, 3.0,  0.0, 2.0,
    };
    const logits = try head.scoreRowsAlloc(allocator, &hidden);
    defer allocator.free(logits);
    try std.testing.expectApproxEqAbs(@as(f32, 1.35), logits[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.425), logits[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -2.5), logits[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -3.9), logits[3], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.55), logits[4], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 6.0), logits[5], 1e-6);

    const predictions = try head.predictRowsAlloc(allocator, &hidden);
    defer allocator.free(predictions);
    try std.testing.expectEqualSlices(usize, &.{ 0, 2 }, predictions);
}
