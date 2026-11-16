// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Codec/layout preparation around public compiled image stages. No learned
//! projector arithmetic runs on the host preparation backend.
const std = @import("std");
const Session = @import("../backends/session.zig").Session;
const Tensor = @import("../backends/tensor.zig").Tensor;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const factory = @import("../architectures/session_factory.zig");
const gpt = @import("../models/gpt.zig");
const companion = @import("../architectures/gemma4_projector.zig");
const patch = @import("../architectures/qwen3vl_projector.zig");
const plan = @import("../architectures/qwen3vl_plan.zig");

pub fn companionImages(session: Session, a: std.mem.Allocator, images: []const []const u8, control: ?Control) !companion.ProjectedImages {
    if (session.backend() != .pjrt or images.len > 8) return error.UnsupportedModelProgram;
    const metadata = factory.getAuxiliaryProjectorMetadata(session) orelse return error.MissingProjector;
    var values: std.ArrayList(f32) = .empty;
    defer values.deinit(a);
    const counts = try a.alloc(usize, images.len);
    errdefer a.free(counts);
    var hidden: usize = 0;
    for (images, counts) |bytes, *count| {
        if (control) |active| try active.check();
        const prepared = try companion.prepareProjectorImage(a, metadata, bytes);
        defer prepared.deinit();
        var output = try imageFeatures(session, a, prepared, control);
        defer output.deinit();
        count.* = @intCast(output.shape[1]);
        const width: usize = @intCast(output.shape[2]);
        if (hidden != 0 and hidden != width) return error.InvalidTensorShape;
        hidden = width;
        try values.appendSlice(a, output.asFloat32());
    }
    return .{ .allocator = a, .embeddings = try values.toOwnedSlice(a), .tokens_per_image = counts, .hidden_size = hidden, .batches_executed = images.len, .max_batch_executed = @intFromBool(images.len != 0) };
}

pub fn patchImages(session: Session, a: std.mem.Allocator, config: gpt.Config, images: []const []const u8, limits: patch.Limits, control: ?Control) !patch.ProjectedImages {
    if (session.backend() != .pjrt or images.len == 0 or images.len > limits.max_images) return error.UnsupportedModelProgram;
    var values: std.ArrayList(f32) = .empty;
    defer values.deinit(a);
    const counts = try a.alloc(usize, images.len);
    errdefer a.free(counts);
    const grids = try a.alloc(plan.VisionGrid, images.len);
    errdefer a.free(grids);
    for (images, counts, grids) |bytes, *count, *grid| {
        if (control) |active| try active.check();
        var prepared = try patch.prepareProjectedImageResident(a, config, bytes, limits, .{ .context = &control, .check_fn = checkImageControl });
        defer prepared.deinit();
        var input = try Tensor.initFloat32(a, "patch_values", &.{ 1, @intCast(prepared.geometry.patchCount()), @intCast(3 * config.vision_patch_size * config.vision_patch_size) }, prepared.patch_rows);
        defer input.deinit();
        var layout = try Tensor.initInt64(a, "image_grid", &.{2}, &.{ @intCast(prepared.geometry.grid_y), @intCast(prepared.geometry.grid_x) });
        defer layout.deinit();
        const outputs = try session.runWithControl(&.{ input, layout }, a, control);
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        const output = try imageOutput(outputs, 2);
        count.* = @intCast(output.shape[0]);
        if (count.* != prepared.geometry.tokenCount() or output.shape[1] != config.hidden_size) return error.InvalidTensorShape;
        grid.* = .{ .temporal = 1, .height = @intCast(prepared.geometry.grid_y), .width = @intCast(prepared.geometry.grid_x) };
        try values.appendSlice(a, output.asFloat32());
    }
    const empty_deepstack = try a.alloc(f32, 0);
    errdefer a.free(empty_deepstack);
    const empty_evidence = try a.alloc(patch.PreprocessEvidence, 0);
    errdefer a.free(empty_evidence);
    const empty_patches = try a.alloc(f32, 0);
    errdefer a.free(empty_patches);
    return .{ .allocator = a, .embeddings = try values.toOwnedSlice(a), .tokens_per_image = counts, .grids = grids, .hidden_size = config.hidden_size, .deepstack_layer_count = 0, .deepstack_embeddings = empty_deepstack, .preprocess_evidence = empty_evidence, .preprocess_spatial_patches = empty_patches };
}

fn imageOutput(outputs: []const Tensor, rank: usize) !Tensor {
    if (outputs.len != 1 or !std.mem.eql(u8, outputs[0].name, "image_features") or outputs[0].dtype != .f32 or outputs[0].shape.len != rank) return error.InvalidModelProgramOutput;
    for (outputs[0].shape) |axis| if (axis <= 0) return error.InvalidTensorShape;
    return outputs[0];
}

fn checkImageControl(raw: ?*const anyopaque) !void {
    const control: *const ?Control = @ptrCast(@alignCast(raw orelse return));
    if (control.*) |active| try active.check();
}

pub fn companionAudio(session: Session, a: std.mem.Allocator, clips: []const companion.AudioInput, control: ?Control) !companion.ProjectedAudio {
    if (session.backend() != .pjrt or clips.len == 0 or clips.len > 8) return error.UnsupportedModelProgram;
    const metadata = factory.getAuxiliaryProjectorMetadata(session) orelse return error.MissingProjector;
    var values: std.ArrayList(f32) = .empty;
    defer values.deinit(a);
    const counts = try a.alloc(usize, clips.len);
    errdefer a.free(counts);
    var hidden: usize = 0;
    for (clips, counts) |clip, *count| {
        if (control) |active| try active.check();
        var prepared = try companion.prepareNamedAudio(a, metadata, clip);
        defer prepared.deinit();
        const layout = try companion.namedAudioLayout(metadata, prepared.frames, prepared.mel_bins);
        if (hidden != 0 and hidden != layout.hidden) return error.InvalidTensorShape;
        hidden = layout.hidden;
        var features = try Tensor.initFloat32(a, "audio_values", &.{ 1, @intCast(prepared.frames), @intCast(prepared.mel_bins) }, prepared.data);
        defer features.deinit();
        const bits = try a.alloc(i64, prepared.frames);
        defer a.free(bits);
        for (bits, prepared.mask) |*bit, valid| bit.* = @intFromBool(valid);
        var mask = try Tensor.initInt64(a, "audio_mask", &.{ 1, @intCast(prepared.frames) }, bits);
        defer mask.deinit();
        const outputs = try session.runWithControl(&.{ features, mask }, a, control);
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        if (outputs.len != 1 or outputs[0].dtype != .f32 or !std.mem.eql(u8, outputs[0].name, "audio_features") or outputs[0].shape.len != 2 or outputs[0].shape[0] <= 0 or outputs[0].shape[1] != hidden) return error.InvalidModelProgramOutput;
        count.* = @intCast(outputs[0].shape[0]);
        try values.appendSlice(a, outputs[0].asFloat32());
    }
    return .{ .allocator = a, .embeddings = try values.toOwnedSlice(a), .tokens_per_audio = counts, .hidden_size = hidden };
}

fn imageFeatures(session: Session, a: std.mem.Allocator, prepared: companion.PreparedImage, control: ?Control) !Tensor {
    var input = try Tensor.initFloat32(a, "pixel_values", &.{ 1, 3, @intCast(prepared.geometry.height), @intCast(prepared.geometry.width) }, prepared.pixels);
    defer input.deinit();
    const outputs = try session.runWithControl(&.{input}, a, control);
    defer a.free(outputs);
    errdefer for (outputs) |*output| output.deinit();
    return imageOutput(outputs, 3);
}

/// Each decoded frame reuses the exact still-image semantic stage, retaining
/// the existing video processor's smaller grid and presentation timestamps.
pub fn companionVideoFrames(session: Session, cb: *const @import("../ops/ops.zig").ComputeBackend, a: std.mem.Allocator, prepared: []const ?companion.PreparedImage, timestamps: []const i64, control: ?Control) !companion.ProjectedVideo {
    if (session.backend() != .pjrt or prepared.len != timestamps.len or prepared.len == 0 or prepared.len > 32) return error.InvalidVideoFrame;
    var values: std.ArrayList(f32) = .empty;
    defer values.deinit(a);
    var counts: std.ArrayList(usize) = .empty;
    defer counts.deinit(a);
    var times: std.ArrayList(i64) = .empty;
    defer times.deinit(a);
    var hidden: usize = 0;
    var rows: usize = 0;
    for (prepared, timestamps) |item, timestamp| if (item) |pixels| {
        if (control) |active| try active.check();
        var output = try imageFeatures(session, a, pixels, control);
        defer output.deinit();
        const count: usize = @intCast(output.shape[1]);
        const width: usize = @intCast(output.shape[2]);
        if (hidden != 0 and hidden != width) return error.InvalidTensorShape;
        hidden = width;
        rows = try std.math.add(usize, rows, count);
        try values.appendSlice(a, output.asFloat32());
        try counts.append(a, count);
        try times.append(a, timestamp);
    };
    if (rows == 0) return error.InvalidVideoFrame;
    const tensor = try cb.fromFloat32Shape(values.items, &.{ @intCast(rows), @intCast(hidden) });
    errdefer cb.free(tensor);
    const owned_counts = try counts.toOwnedSlice(a);
    errdefer a.free(owned_counts);
    const owned_times = try times.toOwnedSlice(a);
    return .{ .images = .{ .allocator = a, .backend = cb, .embeddings = tensor, .tokens_per_image = owned_counts, .hidden_size = hidden, .batches_executed = owned_counts.len, .max_batch_executed = 1 }, .timestamps_us = owned_times, .stats = .{ .decoded_frames = owned_counts.len, .selected_frames = owned_counts.len } };
}
