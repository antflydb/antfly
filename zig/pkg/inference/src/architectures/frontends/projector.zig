// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Companion projector stage. Metadata describes the architecture; named
//! operands come from the loaded session's shared immutable inventory.
const std = @import("std");
const capture = @import("../../execution/capture.zig");
const Tensor = @import("../../backends/tensor.zig").Tensor;
const Store = @import("../../models/tensor_store.zig").GgufStore;
const projector = @import("../gemma4_projector.zig");
const ops = @import("../../ops/ops.zig");

pub fn image(metadata: *Store) capture.Forward {
    return .{ .context = metadata, .fingerprint = 6, .output_names = &.{"image_features"}, .input_info = &.{.{ .name = "pixel_values", .dtype = .f32, .shape = &.{ -1, 3, -1, -1 } }}, .validate = validateImage, .run = imageForward };
}

fn pixels(metadata: *Store, inputs: []const capture.Input) !Tensor {
    if (inputs.len != 1 or !std.mem.eql(u8, inputs[0].tensor.name, "pixel_values")) return error.UnexpectedModelProgramInput;
    const input = inputs[0].tensor;
    _ = try capture.inputShape(input);
    if (input.dtype != .f32 or !input.isAlignedFor(f32) or input.shape.len != 4 or input.shape[0] <= 0 or input.shape[1] != 3 or input.shape[2] <= 0 or input.shape[3] <= 0) return error.InvalidInputShape;
    _ = try projector.imageTensorLayout(&metadata.parsed, @intCast(input.shape[3]), @intCast(input.shape[2]), @intCast(input.shape[0]));
    return input;
}

fn validateImage(raw: *anyopaque, inputs: []const capture.Input) !void {
    const metadata: *Store = @ptrCast(@alignCast(raw));
    _ = try pixels(metadata, inputs);
}

fn imageForward(raw: *anyopaque, cb: *const ops.ComputeBackend, a: std.mem.Allocator, inputs: []const capture.Input) ![]capture.Output {
    const metadata: *Store = @ptrCast(@alignCast(raw));
    const input = try pixels(metadata, inputs);
    const batch: usize = @intCast(input.shape[0]);
    const height: usize = @intCast(input.shape[2]);
    const width: usize = @intCast(input.shape[3]);
    const layout = try projector.imageTensorLayout(&metadata.parsed, width, height, batch);
    const source = try cb.fromFloat32Shape(input.asFloat32(), &.{ @intCast(batch), 3, @intCast(height), @intCast(width) });
    defer cb.free(source);
    const encoded = try projector.encodeNamedPixelTensorResident(cb, a, metadata, source, width, height, batch);
    return capture.publishReshapedOutput(a, cb, encoded.tensor, "image_features", &.{ @intCast(batch), @intCast(encoded.tokens / batch), @intCast(layout.hidden) });
}

pub fn audio(metadata: *Store) capture.Forward {
    return .{ .context = metadata, .fingerprint = 9, .output_names = &.{"audio_features"}, .validate = validateAudio, .run = audioForward };
}

fn audioInputs(metadata: *Store, inputs: []const capture.Input) !struct { features: Tensor, mask: Tensor } {
    if (inputs.len != 2) return error.UnexpectedModelProgramInput;
    try capture.validateHostInputs(inputs);
    var features: ?Tensor = null;
    var mask: ?Tensor = null;
    for (inputs) |input| {
        const slot = if (std.mem.eql(u8, input.tensor.name, "audio_values")) &features else if (std.mem.eql(u8, input.tensor.name, "audio_mask")) &mask else return error.UnexpectedModelProgramInput;
        if (slot.* != null) return error.DuplicateInputs;
        slot.* = input.tensor;
    }
    const f = features orelse return error.MissingInputs;
    const m = mask orelse return error.MissingInputs;
    if (f.dtype != .f32 or !f.isAlignedFor(f32) or f.shape.len != 3 or f.shape[0] != 1 or f.shape[1] <= 0 or f.shape[2] <= 0 or m.dtype != .i64 or !m.isAlignedFor(i64) or m.shape.len != 2 or m.shape[0] != 1 or m.shape[1] != f.shape[1]) return error.InvalidInputShape;
    _ = try projector.namedAudioLayout(&metadata.parsed, @intCast(f.shape[1]), @intCast(f.shape[2]));
    var valid: usize = 0;
    for (m.asInt64()) |bit| {
        if (bit != 0 and bit != 1) return error.InvalidAttentionMask;
        valid += @intFromBool(bit != 0);
    }
    if (valid == 0) return error.EmptyEmbeddingInput;
    return .{ .features = f, .mask = m };
}
fn validateAudio(raw: *anyopaque, inputs: []const capture.Input) !void {
    const metadata: *Store = @ptrCast(@alignCast(raw));
    _ = try audioInputs(metadata, inputs);
}
fn audioForward(raw: *anyopaque, cb: *const ops.ComputeBackend, a: std.mem.Allocator, inputs: []const capture.Input) ![]capture.Output {
    const metadata: *Store = @ptrCast(@alignCast(raw));
    const source = try audioInputs(metadata, inputs);
    const frames: usize = @intCast(source.features.shape[1]);
    const width: usize = @intCast(source.features.shape[2]);
    const mask = try a.alloc(bool, frames);
    defer a.free(mask);
    for (mask, source.mask.asInt64()) |*valid, bit| valid.* = bit != 0;
    const input = try cb.fromFloat32Shape(source.features.asFloat32(), &.{ 1, @intCast(frames), @intCast(width) });
    defer cb.free(input);
    const encoded = try projector.encodeNamedAudioTensorResident(cb, a, metadata, input, mask, frames, width);
    return capture.publishOutput(a, cb, "audio_features", encoded.embeddings);
}
