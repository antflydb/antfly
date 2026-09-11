// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const job = @import("gliner_boundary_training_job.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const sources = @import("gliner_boundary_training_source.zig");
const files = @import("../runtime/file_snapshot.zig");
const memory = @import("../runtime/tier/memory.zig");
const compat = @import("../io/compat.zig");
const Execution = @import("seeded_gradient_trainer.zig").Execution;
const mib = 1024 * 1024;

fn pin(value: std.json.Value) !bundle.Digest {
    const size = value.object.get("size_bytes").?.integer;
    const digest = value.object.get("sha256").?.string;
    if (size < 0 or digest.len != 64) return error.InvalidTestFixture;
    return .{ .size_bytes = @intCast(size), .sha256 = digest[0..64].* };
}

test "boundary training job published small heads matches uninterrupted durable resume and portable source reload" {
    const source_path = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_TRAINING_JOB_MODEL_DIR") orelse return error.SkipZigTest;
    try publishedSmallHeads(source_path, .native);
}

test "boundary training job resident Metal published small heads matches uninterrupted durable resume and portable source reload" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const source_path = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_TRAINING_JOB_METAL_MODEL_DIR") orelse return error.SkipZigTest;
    try publishedSmallHeads(source_path, .resident_metal);
}

fn publishedSmallHeads(source_path: []const u8, execution: Execution) !void {
    const a = std.testing.allocator;
    const io = compat.io();
    const manifest_bytes = try files.read(a, io, compat.cwd(), "testdata/gliner25/training_job_small_v1/manifest.json", 64 * 1024, null);
    defer a.free(manifest_bytes);
    var manifest = try std.json.parseFromSlice(std.json.Value, a, manifest_bytes, .{});
    defer manifest.deinit();
    const model_files = manifest.value.object.get("model_reference").?.object.get("files").?.object;
    const expected = bundle.Identity{ .backbone = .small, .precision = .fp32, .weight = try pin(model_files.get("model.safetensors").?), .sidecars = .{
        try pin(model_files.get("config.json").?), try pin(model_files.get("encoder_config/config.json").?), try pin(model_files.get("tokenizer.json").?), try pin(model_files.get("tokenizer_config.json").?),
    } };
    const train_path = try compat.cwd().realPathFileAlloc(io, "testdata/gliner25/training_job_small_v1/train.jsonl", a);
    defer a.free(train_path);
    const validation_path = try compat.cwd().realPathFileAlloc(io, "testdata/gliner25/training_job_small_v1/validation.jsonl", a);
    defer a.free(validation_path);
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const relative = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{temporary.sub_path});
    defer a.free(relative);
    const parent = try compat.cwd().realPathFileAlloc(io, relative, a);
    defer a.free(parent);
    const full_path = try std.fs.path.join(a, &.{ parent, "uninterrupted" });
    defer a.free(full_path);
    const pause_path = try std.fs.path.join(a, &.{ parent, "paused" });
    defer a.free(pause_path);
    const resume_path = try std.fs.path.join(a, &.{ parent, "resumed" });
    defer a.free(resume_path);
    const rejected_path = try std.fs.path.join(a, &.{ parent, "rejected" });
    defer a.free(rejected_path);
    var config = job.Config{
        .version = 1,
        .source_dir = source_path,
        .train_file = train_path,
        .calibration_file = validation_path,
        .output_dir = full_path,
        .expected_source = expected,
        .execution = execution,
        .run = .{ .mode = .heads, .epochs = 2, .batch_size = 1, .accumulation = 2, .scheduler = .constant, .seed = 42, .shuffle = false, .warmup_steps = 0 },
        .memory = .{ .host_bytes = 512 * mib, .backend_bytes = 512 * mib, .combined_bytes = 2 * 1024 * mib, .optimizer_state_bytes = 256 * mib, .optimizer_transaction_bytes = 256 * mib },
        .source_limits = .{ .max_auxiliary_bytes = 128 * mib },
        .dataset_limits = .{ .max_file_bytes = mib, .max_host_bytes = 32 * mib },
        .timeout_seconds = 600,
    };
    if (execution == .resident_metal) {
        config.memory.backend_bytes = 1024 * mib;
        config.memory.backend_metadata_bytes = 64 * mib;
        config.memory.combined_bytes = 3 * 1024 * mib;
    }
    var admission = memory.AdmissionController{};
    defer admission.deinit();
    const uninterrupted = try job.execute(a, io, config, &admission, .{}, null);
    try std.testing.expectEqual(.complete, uninterrupted.status);
    try std.testing.expectEqual(@as(u64, 2), uninterrupted.identity.optimizer_step);
    try std.testing.expectEqual(@as(u64, 4), uninterrupted.identity.microbatch_step);
    try std.testing.expectEqual(@as(usize, 0), admission.snapshot().hostTotalBytes());
    try std.testing.expectEqual(@as(usize, 0), admission.snapshot().backendTotalBytes());
    config.output_dir = pause_path;
    const paused = try job.execute(a, io, config, &admission, .{ .stop_after_microbatches = 1 }, null);
    try std.testing.expectEqual(.paused, paused.status);
    try std.testing.expectEqual(@as(u32, 1), paused.accumulated_microbatches);
    try std.testing.expect(paused.portable_model == null);
    const paused_result_path = try std.fs.path.join(a, &.{ pause_path, "result.json" });
    defer a.free(paused_result_path);
    const paused_bytes = try files.read(a, io, compat.cwd(), paused_result_path, 64 * 1024, null);
    defer a.free(paused_bytes);
    var paused_disk = try std.json.parseFromSlice(job.Result, a, paused_bytes, .{});
    defer paused_disk.deinit();
    try std.testing.expectEqual(paused, paused_disk.value);
    const checkpoint_path = try std.fs.path.join(a, &.{ pause_path, "latest.safetensors" });
    defer a.free(checkpoint_path);
    config.resume_from = checkpoint_path;
    config.output_dir = rejected_path;
    config.expected_restore_state_sha256 = @splat(0);
    try std.testing.expectError(error.TrainingRestoreStateMismatch, job.execute(a, io, config, &admission, .{}, null));
    try std.testing.expectEqual(@as(usize, 0), admission.snapshot().hostTotalBytes());
    try std.testing.expectEqual(@as(usize, 0), admission.snapshot().backendTotalBytes());
    config.output_dir = resume_path;
    config.expected_restore_state_sha256 = paused.state_sha256;
    const resumed = try job.execute(a, io, config, &admission, .{}, null);
    try std.testing.expectEqual(.complete, resumed.status);
    try std.testing.expectEqual(uninterrupted.identity, resumed.identity);
    try std.testing.expectEqual(uninterrupted.run_fingerprint, resumed.run_fingerprint);
    try std.testing.expectEqual(uninterrupted.state_sha256, resumed.state_sha256);
    try std.testing.expectEqual(uninterrupted.portable_model.?.weights, resumed.portable_model.?.weights);
    try std.testing.expectEqual(@as(usize, 0), admission.snapshot().hostTotalBytes());
    try std.testing.expectEqual(@as(usize, 0), admission.snapshot().backendTotalBytes());
    const model_path = try std.fs.path.join(a, &.{ resume_path, "model" });
    defer a.free(model_path);
    const reloaded = try sources.Source.open(a, io, model_path, .{}, null);
    defer reloaded.deinit();
    try std.testing.expectEqual(resumed.portable_model.?.weights, reloaded.identity.weight);
    try std.testing.expectEqual(expected.sidecars, reloaded.identity.sidecars);
    try std.testing.expectEqual(@as(usize, 334), reloaded.parameters.len);
    try std.testing.expect(!std.meta.eql(expected.weight, reloaded.identity.weight));
    std.debug.print("boundary real small heads job ({s}): four microbatches, two updates; resumed state/model exact; model_sha256={s}; state_sha256={s}\n", .{ @tagName(execution), resumed.portable_model.?.weights.sha256, std.fmt.bytesToHex(resumed.state_sha256, .lower) });
}
