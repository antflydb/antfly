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

const std = @import("std");
const ops = @import("../ops/ops.zig");
const gguf_metadata = @import("../gguf/metadata.zig");
const gguf_format = @import("../gguf/format.zig");
const gguf_mod = @import("../gguf/root.zig");
const tensor_store_mod = @import("../models/tensor_store.zig");
const weight_source_mod = @import("../models/weight_source.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const compat = @import("../io/compat.zig");
const gpt_mod = @import("../models/gpt.zig");
const projector_format_mod = @import("projector_format.zig");
const encoder = @import("patch_encoder_projector.zig");

const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;

const ProjectorConfig = struct {
    text_hidden: usize,
    vision_hidden: usize,
    intermediate_size: usize,
    block_count: usize,
    head_count: usize,
    image_size: usize,
    patch_size: usize,
    mm_tokens_per_image: usize,
};

const LoadedF32 = struct {
    store: *tensor_store_mod.GgufStore,
    name: []const u8,
    weight: weight_source_mod.LoadedWeight,
    converted: ?Tensor = null,
    data: []const f32,
    shape: []const i64,

    pub fn deinit(self: *LoadedF32) void {
        if (self.converted) |*converted| converted.deinit();
        self.weight.deinit();
        self.store.discardTensorFileCache(self.name);
    }
};

pub fn isSupportedProjectorPath(allocator: std.mem.Allocator, projector_path: []const u8) !bool {
    return try projector_format_mod.detectPath(allocator, projector_path) == .antfly_gemma3;
}

pub fn isSupportedProjectorFile(file: *const gguf_format.File) bool {
    return projector_format_mod.detectFile(file) == .antfly_gemma3;
}

pub fn encodeProjectedImageTokens(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    projector_path: []const u8,
    runtime_cfg: gpt_mod.Config,
    pixel_values: []const f32,
    batch: usize,
) ![]f32 {
    var store = try tensor_store_mod.GgufStore.initAbsolute(allocator, projector_path);
    defer store.tensorStore().deinit();

    return encodeProjectedImageTokensFromStore(cb, allocator, store, runtime_cfg, pixel_values, batch);
}

pub fn encodeProjectedImageTokensFromStore(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    runtime_cfg: gpt_mod.Config,
    pixel_values: []const f32,
    batch: usize,
) ![]f32 {
    const cfg = try parseProjectorConfig(&store.parsed, runtime_cfg);
    return encoder.encode(cb, allocator, Weights{ .cb = cb, .allocator = allocator, .store = store }, .{
        .text_hidden = cfg.text_hidden,
        .vision_hidden = cfg.vision_hidden,
        .intermediate_size = cfg.intermediate_size,
        .block_count = cfg.block_count,
        .head_count = cfg.head_count,
        .image_size = cfg.image_size,
        .patch_size = cfg.patch_size,
        .mm_tokens_per_image = cfg.mm_tokens_per_image,
        .norm_weight_offset = runtime_cfg.norm_weight_offset,
    }, pixel_values, batch, false);
}

const Weights = struct {
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    pub fn get(self: @This(), name: []const u8) !CT {
        return loadWeightCt(self.cb, self.allocator, self.store, name);
    }
    pub fn positions(self: @This()) !LoadedF32 {
        return loadTensorF32(self.store, "vision_tower.vision_model.embeddings.position_embedding.weight");
    }
    pub fn projection(self: @This(), in_dim: usize, out_dim: usize) !CT {
        return loadLinearWeightCt(self.cb, self.allocator, self.store, "multi_modal_projector.mm_input_projection_weight", in_dim, out_dim);
    }
};

fn parseProjectorConfig(file: *const gguf_format.File, runtime_cfg: gpt_mod.Config) !ProjectorConfig {
    if (!isSupportedProjectorFile(file)) return error.InvalidGgufProjector;
    const view = gguf_metadata.View.init(file);
    const cfg: ProjectorConfig = .{
        .text_hidden = @intCast(view.getU64("inference.projector.text_hidden_size") orelse return error.InvalidGgufProjector),
        .vision_hidden = @intCast(view.getU64("inference.projector.vision_hidden_size") orelse return error.InvalidGgufProjector),
        .intermediate_size = @intCast(view.getU64("inference.projector.vision_feed_forward_length") orelse return error.InvalidGgufProjector),
        .block_count = @intCast(view.getU64("inference.projector.vision_block_count") orelse return error.InvalidGgufProjector),
        .head_count = @intCast(view.getU64("inference.projector.vision_attention_head_count") orelse return error.InvalidGgufProjector),
        .image_size = @intCast(view.getU64("inference.projector.vision_image_size") orelse return error.InvalidGgufProjector),
        .patch_size = @intCast(view.getU64("inference.projector.vision_patch_size") orelse return error.InvalidGgufProjector),
        .mm_tokens_per_image = @intCast(view.getU64("inference.projector.mm_tokens_per_image") orelse return error.InvalidGgufProjector),
    };
    if (cfg.text_hidden != runtime_cfg.hidden_size) return error.InvalidGgufProjector;
    if (runtime_cfg.mm_tokens_per_image > 0 and cfg.mm_tokens_per_image != runtime_cfg.mm_tokens_per_image) return error.InvalidGgufProjector;
    return cfg;
}

fn loadWeightCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    name: []const u8,
) !CT {
    var tensor = try loadTensorF32(store, name);
    defer tensor.deinit();
    const shape = try shapeI32(allocator, tensor.shape);
    defer allocator.free(shape);
    return cb.fromFloat32Shape(tensor.data, shape);
}

fn loadLinearWeightCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    name: []const u8,
    in_dim: usize,
    out_dim: usize,
) !CT {
    var tensor = try loadTensorF32(store, name);
    defer tensor.deinit();
    if (tensor.shape.len != 2) return error.InvalidTensorShape;
    const rows: usize = @intCast(tensor.shape[0]);
    const cols: usize = @intCast(tensor.shape[1]);
    if (rows == out_dim and cols == in_dim) {
        const shape = [_]i32{ @intCast(out_dim), @intCast(in_dim) };
        return cb.fromFloat32Shape(tensor.data, &shape);
    }
    if (rows == in_dim and cols == out_dim) {
        const transposed = try encoder.transposeMatrix(allocator, tensor.data, in_dim, out_dim);
        defer allocator.free(transposed);
        const shape = [_]i32{ @intCast(out_dim), @intCast(in_dim) };
        return cb.fromFloat32Shape(transposed, &shape);
    }
    return error.InvalidTensorShape;
}

fn loadTensorF32(store: *tensor_store_mod.GgufStore, name: []const u8) !LoadedF32 {
    var tensor_ref = try store.tensorStore().describeTensor(store.allocator, name);
    defer tensor_ref.deinit(store.allocator);
    var loaded = try store.tensorStore().loadTensorRef(&tensor_ref);
    errdefer loaded.deinit();

    if (loaded.tensor.dtype == .f32) {
        return .{
            .store = store,
            .name = name,
            .weight = loaded,
            .data = loaded.tensor.asFloat32(),
            .shape = loaded.tensor.shape,
        };
    }

    if (loaded.tensor.dtype == .f16 or loaded.tensor.dtype == .bf16) {
        const converted = try weight_source_mod.convertToF32(store.allocator, &loaded.tensor);
        errdefer converted.deinit();
        return .{
            .store = store,
            .name = name,
            .weight = loaded,
            .converted = converted,
            .data = converted.asFloat32(),
            .shape = converted.shape,
        };
    }

    return error.UnsupportedTensorType;
}

fn shapeI32(allocator: std.mem.Allocator, shape: []const i64) ![]i32 {
    const out = try allocator.alloc(i32, shape.len);
    for (shape, 0..) |dim, i| out[i] = @intCast(dim);
    return out;
}

test "supports termite gemma3 projector metadata" {
    const allocator = std.testing.allocator;
    const path = try std.fs.path.join(allocator, &.{ "/tmp", "termite-gemma3-projector-classify.gguf" });
    defer allocator.free(path);
    defer std.Io.Dir.cwd().deleteFile(compat.testingIo(), path) catch {};

    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "antfly-projector" } },
        .{ .key = "inference.projector.source_architecture", .value = .{ .string = "gemma3" } },
        .{ .key = "inference.projector.text_hidden_size", .value = .{ .u32 = 4 } },
        .{ .key = "inference.projector.vision_hidden_size", .value = .{ .u32 = 8 } },
        .{ .key = "inference.projector.vision_feed_forward_length", .value = .{ .u32 = 16 } },
        .{ .key = "inference.projector.vision_block_count", .value = .{ .u32 = 3 } },
        .{ .key = "inference.projector.vision_attention_head_count", .value = .{ .u32 = 4 } },
        .{ .key = "inference.projector.vision_image_size", .value = .{ .u32 = 224 } },
        .{ .key = "inference.projector.vision_patch_size", .value = .{ .u32 = 14 } },
        .{ .key = "inference.projector.mm_tokens_per_image", .value = .{ .u32 = 256 } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    try std.Io.Dir.cwd().writeFile(compat.testingIo(), .{ .sub_path = path, .data = layout.header_bytes });

    try std.testing.expect(try isSupportedProjectorPath(allocator, path));
}

test "rejects clip projector metadata" {
    const allocator = std.testing.allocator;
    const path = try std.fs.path.join(allocator, &.{ "/tmp", "termite-gemma3-projector-reject.gguf" });
    defer allocator.free(path);
    defer std.Io.Dir.cwd().deleteFile(compat.testingIo(), path) catch {};

    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4v" } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    try std.Io.Dir.cwd().writeFile(compat.testingIo(), .{ .sub_path = path, .data = layout.header_bytes });

    try std.testing.expect(!(try isSupportedProjectorPath(allocator, path)));
}

const EncoderFixture = @import("testing/patch_encoder_fixture.zig");
fn writeEncoderFixture(a: std.mem.Allocator, path: []const u8, output_first: bool) !void {
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "antfly-projector" } },
        .{ .key = "inference.projector.source_architecture", .value = .{ .string = "gemma3" } },
        .{ .key = "inference.projector.text_hidden_size", .value = .{ .u32 = 3 } },
        .{ .key = "inference.projector.vision_hidden_size", .value = .{ .u32 = 2 } },
        .{ .key = "inference.projector.vision_feed_forward_length", .value = .{ .u32 = 2 } },
        .{ .key = "inference.projector.vision_block_count", .value = .{ .u32 = 1 } },
        .{ .key = "inference.projector.vision_attention_head_count", .value = .{ .u32 = 1 } },
        .{ .key = "inference.projector.vision_image_size", .value = .{ .u32 = 2 } },
        .{ .key = "inference.projector.vision_patch_size", .value = .{ .u32 = 1 } },
        .{ .key = "inference.projector.mm_tokens_per_image", .value = .{ .u32 = 1 } },
    };
    var dimensions: [EncoderFixture.weights.len][4]u64 = undefined;
    var specs: [EncoderFixture.weights.len]gguf_mod.writer.TensorSpec = undefined;
    for (EncoderFixture.weights, 0..) |weight, index| {
        for (weight.shape, 0..) |_, axis| dimensions[index][axis] = @intCast(weight.shape[weight.shape.len - 1 - axis]);
        if (output_first and index == EncoderFixture.weights.len - 1) dimensions[index][0..2].* = .{ 2, 3 };
        specs[index] = .{ .name = weight.name, .dimensions = dimensions[index][0..weight.shape.len], .tensor_type = .{ .known = .F32 } };
    }
    var layout = try gguf_mod.writer.buildLayout(a, &metadata, &specs);
    defer layout.deinit(a);
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(a);
    try bytes.appendSlice(a, layout.header_bytes);
    const start = std.mem.alignForward(usize, layout.header_bytes.len, @intCast(layout.alignment));
    try bytes.appendNTimes(a, 0, start - bytes.items.len);
    const transposed = [_]f32{ 1, 4, 2, 6, 3, 9 };
    for (EncoderFixture.weights, layout.offsets, 0..) |weight, offset, index| {
        const position = try std.math.add(usize, start, @intCast(offset));
        try bytes.appendNTimes(a, 0, position - bytes.items.len);
        const values = if (output_first and index == EncoderFixture.weights.len - 1) &transposed else weight.values;
        try bytes.appendSlice(a, std.mem.sliceAsBytes(values));
    }
    try std.Io.Dir.cwd().writeFile(compat.testingIo(), .{ .sub_path = path, .data = bytes.items });
}
fn checkActualProjectorStore(cb_value: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const cb = &cb_value;
    for ([_]bool{ false, true }) |output_first| {
        const path = try std.fmt.allocPrint(a, "/tmp/antfly-patch-encoder219-{d}-{}.gguf", .{ std.posix.system.getpid(), output_first });
        defer a.free(path);
        defer std.Io.Dir.cwd().deleteFile(compat.testingIo(), path) catch {};
        try writeEncoderFixture(a, path, output_first);
        var store = try tensor_store_mod.GgufStore.initAbsolute(a, path);
        defer store.tensorStore().deinit();
        const actual = try encodeProjectedImageTokensFromStore(cb, a, store, EncoderFixture.config(), &EncoderFixture.pixels, 1);
        defer a.free(actual);
        try EncoderFixture.expectProjected(actual);
        // The retained result and actual reader cache remain valid across a
        // second full execution with the same real store and source format.
        const held = try a.dupe(f32, actual);
        defer a.free(held);
        const later = try encodeProjectedImageTokensFromStore(cb, a, store, EncoderFixture.config(), &EncoderFixture.pixels, 1);
        defer a.free(later);
        try std.testing.expectEqualSlices(f32, actual, later);
        try std.testing.expectEqualSlices(f32, held, actual);
    }
}
test "Native patch encoder projector reads real GGUF input first and output first projection weights" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onNative(helper.noWeights, checkActualProjectorStore);
}
test "Metal patch encoder projector reads real GGUF input first and output first projection weights" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onMetal(helper.noWeights, checkActualProjectorStore);
}
