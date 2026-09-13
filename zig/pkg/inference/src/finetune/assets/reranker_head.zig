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

//! Offline checkpoint operations shared by tools and training.

const std = @import("std");
const compat = @import("../../io/compat.zig");
const Tensor = @import("../../backends/tensor.zig").Tensor;
const manifest_mod = @import("../../models/manifest.zig");
const tensor_access = @import("../../models/tensor_access.zig");
const weight_source = @import("../../models/weight_source.zig");

pub const head_checkpoint_file_name = "legacy_reranker_head.safetensors";

pub const head_config_file_name = "legacy_reranker_head_config.json";

pub const merged_head_checkpoint_file_name = "model.safetensors";

pub const RerankerHead = struct {
    allocator: std.mem.Allocator,
    hidden_size: usize,
    weight: []f32,
    bias: f32,

    pub fn deinit(self: *RerankerHead) void {
        self.allocator.free(self.weight);
        self.* = undefined;
    }
};

pub fn resolveModelHiddenSize(allocator: std.mem.Allocator, model_dir: []const u8) !usize {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    if (manifest.hidden_size == 0) return error.MissingHiddenSize;
    return manifest.hidden_size;
}

pub fn initHeadFromModelDir(allocator: std.mem.Allocator, model_dir: []const u8) !RerankerHead {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    const hidden_size: usize = manifest.hidden_size;
    if (hidden_size == 0) return error.MissingHiddenSize;

    const weight = try allocator.alloc(f32, hidden_size);
    errdefer allocator.free(weight);
    @memset(weight, 0.0);
    var bias: f32 = 0.0;

    var access = try tensor_access.openFromManifest(allocator, manifest);
    defer access.deinit();

    var out_proj_weight = loadOptionalTensorAsF32(allocator, access, "classifier.out_proj.weight") catch null;
    if (out_proj_weight) |*tensor| {
        defer tensor.deinit();
        if (tensor.shape.len == 2 and tensor.shape[1] == hidden_size and tensor.shape[0] >= 1) {
            @memcpy(weight, tensor.asFloat32()[0..hidden_size]);
            var bias_tensor = try loadTensorAsF32(allocator, access, "classifier.out_proj.bias");
            defer bias_tensor.deinit();
            if (bias_tensor.shape.len >= 1 and bias_tensor.asFloat32().len >= 1) bias = bias_tensor.asFloat32()[0];
        }
    } else {
        var linear_weight = loadOptionalTensorAsF32(allocator, access, "classifier.weight") catch null;
        if (linear_weight) |*tensor| {
            defer tensor.deinit();
            if (tensor.shape.len == 2 and tensor.shape[1] == hidden_size and tensor.shape[0] >= 1) {
                @memcpy(weight, tensor.asFloat32()[0..hidden_size]);
                var bias_tensor = try loadTensorAsF32(allocator, access, "classifier.bias");
                defer bias_tensor.deinit();
                if (bias_tensor.shape.len >= 1 and bias_tensor.asFloat32().len >= 1) bias = bias_tensor.asFloat32()[0];
            }
        }
    }

    return .{
        .allocator = allocator,
        .hidden_size = hidden_size,
        .weight = weight,
        .bias = bias,
    };
}

pub fn saveHead(allocator: std.mem.Allocator, head: *const RerankerHead, out_dir: []const u8) !void {
    try compat.cwd().createDirPath(compat.io(), out_dir);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, head_checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    const config_path = try std.fs.path.join(allocator, &.{ out_dir, head_config_file_name });
    defer allocator.free(config_path);

    const bias = [_]f32{head.bias};
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "legacy_reranker.classifier.weight", .shape = &.{ 1, head.hidden_size }, .data = head.weight },
        .{ .name = "legacy_reranker.classifier.bias", .shape = &.{1}, .data = &bias },
    });

    var file = try compat.cwd().createFile(compat.io(), config_path, .{ .truncate = true });
    defer file.close(compat.io());
    var buf: [256]u8 = undefined;
    var writer = file.writerStreaming(compat.io(), &buf);
    try std.json.Stringify.value(.{
        .task = "reranker_regression",
        .hidden_size = head.hidden_size,
        .checkpoint = head_checkpoint_file_name,
    }, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

pub fn materializeMergedHead(allocator: std.mem.Allocator, head: *const RerankerHead, out_dir: []const u8) !void {
    try compat.cwd().createDirPath(compat.io(), out_dir);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, merged_head_checkpoint_file_name });
    defer allocator.free(checkpoint_path);

    const bias = [_]f32{head.bias};
    try writeHeaderAndTensorsF32(allocator, checkpoint_path, &.{
        .{ .name = "classifier.out_proj.weight", .shape = &.{ 1, head.hidden_size }, .data = head.weight },
        .{ .name = "classifier.out_proj.bias", .shape = &.{1}, .data = &bias },
    });
}

pub fn loadHeadIfPresent(allocator: std.mem.Allocator, model_dir: []const u8, hidden_size: usize) !?RerankerHead {
    const checkpoint_path = try std.fs.path.join(allocator, &.{ model_dir, head_checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    _ = compat.cwd().statFile(compat.io(), checkpoint_path, .{}) catch return null;

    var access = try openTensorAccessForFile(allocator, checkpoint_path);
    defer access.deinit();
    var weight_tensor = try loadTensorAsF32(allocator, access, "legacy_reranker.classifier.weight");
    defer weight_tensor.deinit();
    var bias_tensor = try loadTensorAsF32(allocator, access, "legacy_reranker.classifier.bias");
    defer bias_tensor.deinit();
    if (weight_tensor.shape.len != 2 or weight_tensor.shape[0] < 1 or weight_tensor.shape[1] != hidden_size) return error.ShapeMismatch;

    const weight = try allocator.dupe(f32, weight_tensor.asFloat32()[0..hidden_size]);
    return .{
        .allocator = allocator,
        .hidden_size = hidden_size,
        .weight = weight,
        .bias = if (bias_tensor.asFloat32().len > 0) bias_tensor.asFloat32()[0] else 0.0,
    };
}

pub fn loadHeadFromInput(allocator: std.mem.Allocator, input_path: []const u8, hidden_size: usize) !?RerankerHead {
    const stat = compat.cwd().statFile(compat.io(), input_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    return switch (stat.kind) {
        .directory => loadHeadIfPresent(allocator, input_path, hidden_size),
        .file => try loadHeadFromCheckpointPath(allocator, input_path, hidden_size),
        else => error.InvalidHeadInput,
    };
}

pub fn materializeHeadFromDir(allocator: std.mem.Allocator, model_dir: []const u8, head_dir: []const u8, out_dir: []const u8) !void {
    const hidden_size = try resolveModelHiddenSize(allocator, model_dir);
    var head = (try loadHeadFromInput(allocator, head_dir, hidden_size)) orelse return error.MissingRerankerHead;
    defer head.deinit();
    try materializeMergedHead(allocator, &head, out_dir);
}

fn loadHeadFromCheckpointPath(allocator: std.mem.Allocator, checkpoint_path: []const u8, hidden_size: usize) !RerankerHead {
    var access = try openTensorAccessForFile(allocator, checkpoint_path);
    defer access.deinit();
    var weight_tensor = try loadTensorAsF32(allocator, access, "legacy_reranker.classifier.weight");
    defer weight_tensor.deinit();
    var bias_tensor = try loadTensorAsF32(allocator, access, "legacy_reranker.classifier.bias");
    defer bias_tensor.deinit();
    if (weight_tensor.shape.len != 2 or weight_tensor.shape[0] < 1 or weight_tensor.shape[1] != hidden_size) return error.ShapeMismatch;

    const weight = try allocator.dupe(f32, weight_tensor.asFloat32()[0..hidden_size]);
    return .{
        .allocator = allocator,
        .hidden_size = hidden_size,
        .weight = weight,
        .bias = if (bias_tensor.asFloat32().len > 0) bias_tensor.asFloat32()[0] else 0.0,
    };
}

fn loadOptionalTensorAsF32(allocator: std.mem.Allocator, access: tensor_access.TensorAccess, name: []const u8) !?Tensor {
    return loadTensorAsF32(allocator, access, name) catch |err| switch (err) {
        error.TensorNotFound => null,
        else => err,
    };
}

fn loadTensorAsF32(allocator: std.mem.Allocator, access: tensor_access.TensorAccess, name: []const u8) !Tensor {
    var record = try access.getRecord(allocator, name);
    defer record.deinit();
    var tensor = (try record.materializeDense(allocator)) orelse return error.UnsupportedTensorEncoding;
    if (tensor.dtype == .f16 or tensor.dtype == .bf16) {
        const converted = try weight_source.convertToF32(allocator, &tensor);
        tensor.deinit();
        return converted;
    }
    if (tensor.dtype != .f32) {
        tensor.deinit();
        return error.UnsupportedTensorType;
    }
    return tensor;
}

fn openTensorAccessForFile(allocator: std.mem.Allocator, path: []const u8) !tensor_access.TensorAccess {
    if (std.mem.endsWith(u8, path, ".index.json")) {
        const access = try tensor_access.ShardedSafetensorsAccess.initAbsolute(allocator, path);
        return access.tensorAccess();
    }
    const access = try tensor_access.SafetensorsAccess.initAbsolute(allocator, path);
    return access.tensorAccess();
}

const WriteTensorF32 = struct {
    name: []const u8,
    shape: []const usize,
    data: []const f32,
};

fn writeHeaderAndTensorsF32(allocator: std.mem.Allocator, path: []const u8, tensors: []const WriteTensorF32) !void {
    var header_buf: std.Io.Writer.Allocating = .init(allocator);
    defer header_buf.deinit();
    const writer = &header_buf.writer;

    try writer.writeByte('{');
    var offset: u64 = 0;
    for (tensors, 0..) |tensor, idx| {
        if (idx != 0) try writer.writeByte(',');
        const byte_len = tensor.data.len * @sizeOf(f32);
        try writer.print("\"{s}\":{{\"dtype\":\"F32\",\"shape\":[", .{tensor.name});
        for (tensor.shape, 0..) |dim, dim_idx| {
            if (dim_idx != 0) try writer.writeByte(',');
            try writer.print("{}", .{dim});
        }
        try writer.print("],\"data_offsets\":[{},{}]}}", .{ offset, offset + byte_len });
        offset += byte_len;
    }
    try writer.writeByte('}');

    const io = compat.io();
    var file = try compat.cwd().createFile(io, path, .{ .truncate = true });
    defer file.close(io);

    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, header_buf.written().len, .little);
    try file.writeStreamingAll(io, &len_buf);
    try file.writeStreamingAll(io, header_buf.written());
    for (tensors) |tensor| {
        for (tensor.data) |item| {
            const bits: u32 = @bitCast(item);
            var bits_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &bits_buf, bits, .little);
            try file.writeStreamingAll(io, &bits_buf);
        }
    }
}
