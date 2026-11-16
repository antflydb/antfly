// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime spatial patch rows with a checkpoint-owned vision tower. Pixel
//! decoding and grid selection precede this stage; learned arithmetic stays
//! in the shared tensor program.
const std = @import("std");
const capture = @import("capture.zig");
const ops = @import("../ops/ops.zig");
const Config = @import("../models/gpt.zig").Config;
const projector = @import("../architectures/qwen3vl_projector.zig");

pub fn image(config: *Config) capture.Forward {
    return .{ .context = config, .fingerprint = 7, .output_names = &.{"image_features"}, .validate = validate, .run = run };
}

fn validate(raw: *anyopaque, inputs: []const capture.Input) !void {
    const config: *Config = @ptrCast(@alignCast(raw));
    if (inputs.len != 2) return error.UnexpectedModelProgramInput;
    var patches: ?capture.Input = null;
    var grid: ?capture.Input = null;
    for (inputs) |input| {
        _ = try capture.inputShape(input.tensor);
        if (std.mem.eql(u8, input.tensor.name, "patch_values")) {
            if (patches != null) return error.DuplicateInputs;
            patches = input;
        } else if (std.mem.eql(u8, input.tensor.name, "image_grid")) {
            if (grid != null) return error.DuplicateInputs;
            grid = input;
        } else return error.UnexpectedModelProgramInput;
    }
    const p = (patches orelse return error.MissingInputs).tensor;
    const g = (grid orelse return error.MissingInputs).tensor;
    if (p.dtype != .f32 or !p.isAlignedFor(f32) or p.shape.len != 3 or p.shape[0] != 1 or g.dtype != .i64 or !g.isAlignedFor(i64) or g.shape.len != 1 or g.shape[0] != 2) return error.InvalidInputShape;
    const geometry = try projector.patchTensorGeometry(config.*, g.asInt64());
    if (p.shape[1] != geometry.patchCount() or p.shape[2] != 3 * config.vision_patch_size * config.vision_patch_size) return error.InvalidInputShape;
}

fn run(raw: *anyopaque, cb: *const ops.ComputeBackend, a: std.mem.Allocator, inputs: []const capture.Input) ![]capture.Output {
    try validate(raw, inputs);
    const config: *Config = @ptrCast(@alignCast(raw));
    const p = if (std.mem.eql(u8, inputs[0].tensor.name, "patch_values")) inputs[0].tensor else inputs[1].tensor;
    const g = if (std.mem.eql(u8, inputs[0].tensor.name, "image_grid")) inputs[0].tensor else inputs[1].tensor;
    const geometry = try projector.patchTensorGeometry(config.*, g.asInt64());
    const input = try cb.fromFloat32Shape(p.asFloat32(), &.{ @intCast(p.shape[1]), @intCast(p.shape[2]) });
    defer cb.free(input);
    const output = try projector.encodeNamedPatchTensorResident(cb, a, config.*, input, geometry);
    errdefer cb.free(output);
    const outputs = try a.alloc(capture.Output, 1);
    outputs[0] = .{ .name = "image_features", .value = output };
    return outputs;
}
