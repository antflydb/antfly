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
const c_file = @import("../util/c_file.zig");

pub const ImageResample = enum(u8) { nearest = 0, bilinear = 2, bicubic = 3 };

pub const PreprocessorConfig = struct {
    resample: ImageResample = .bicubic,
    do_resize: bool = true,
    do_rescale: bool = true,
    do_normalize: bool = true,
    do_convert_rgb: bool = true,
    rescale_factor: f32 = 1.0 / 255.0,
    image_mean: [3]f32 = .{ 0.48145466, 0.4578275, 0.40821073 },
    image_std: [3]f32 = .{ 0.26862954, 0.26130258, 0.27577711 },
    min_pixels: u32 = 56 * 56,
    max_pixels: u32 = 28 * 28 * 1280,
    patch_size: u32 = 14,
    temporal_patch_size: u32 = 2,
    merge_size: u32 = 2,
};

pub const PreparedImage = struct {
    allocator: std.mem.Allocator,
    pixel_values: []f32,
    resized_width: u32,
    resized_height: u32,
    image_grid_thw: [3]u32,
    image_token_count: usize,

    pub fn deinit(self: *PreparedImage) void {
        self.allocator.free(self.pixel_values);
    }
};

/// Version execution semantics separately from checkpoint bytes so corrected
/// preprocessing cannot reuse embeddings produced by the earlier fixed filter.
pub const preprocessing_recipe = "qwen2vl-declared-resample-scale-v2";

pub fn preprocessingRevision(artifacts: [32]u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(preprocessing_recipe);
    hash.update(&.{0});
    hash.update(&artifacts);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

pub fn loadPreprocessorConfig(allocator: std.mem.Allocator, model_dir: []const u8) !PreprocessorConfig {
    if (c_file.readFileFromDir(allocator, model_dir, "preprocessor_config.json")) |bytes| {
        defer allocator.free(bytes);
        return parsePreprocessorConfig(allocator, bytes);
    } else |_| {}

    const bytes = try c_file.readFileFromDir(allocator, model_dir, "processor_config.json");
    defer allocator.free(bytes);
    return parsePreprocessorConfig(allocator, bytes);
}

pub fn parsePreprocessorConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !PreprocessorConfig {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidPreprocessorConfig;
    const root = parsed.value.object;
    const obj = blk: {
        if (root.get("image_processor")) |value| {
            if (value == .object) break :blk value.object;
        }
        break :blk root;
    };

    var config = PreprocessorConfig{};
    if (obj.get("resample")) |value| {
        if (value != .integer) return error.UnsupportedQwen2VlPreprocessorConfig;
        const code = std.math.cast(u8, value.integer) orelse return error.UnsupportedQwen2VlPreprocessorConfig;
        config.resample = std.enums.fromInt(ImageResample, code) orelse return error.UnsupportedQwen2VlPreprocessorConfig;
    }
    if (obj.get("do_resize")) |v| {
        if (jsonBool(v)) |value| config.do_resize = value;
    }
    if (obj.get("do_rescale")) |v| {
        if (jsonBool(v)) |value| config.do_rescale = value;
    }
    if (obj.get("do_normalize")) |v| {
        if (jsonBool(v)) |value| config.do_normalize = value;
    }
    if (obj.get("do_convert_rgb")) |v| {
        if (jsonBool(v)) |value| config.do_convert_rgb = value;
    }
    if (obj.get("rescale_factor")) |v| {
        if (jsonF32(v)) |value| config.rescale_factor = value;
    }
    if (obj.get("min_pixels")) |v| {
        if (jsonU32(v)) |value| config.min_pixels = value;
    }
    if (obj.get("max_pixels")) |v| {
        if (jsonU32(v)) |value| config.max_pixels = value;
    }
    if (obj.get("size")) |v| {
        if (v == .object) {
            if (v.object.get("shortest_edge")) |edge| {
                if (jsonU32(edge)) |value| config.min_pixels = value;
            }
            if (v.object.get("longest_edge")) |edge| {
                if (jsonU32(edge)) |value| config.max_pixels = value;
            }
        }
    }
    if (obj.get("patch_size")) |v| {
        if (jsonU32(v)) |value| config.patch_size = value;
    }
    if (obj.get("temporal_patch_size")) |v| {
        if (jsonU32(v)) |value| config.temporal_patch_size = value;
    }
    if (obj.get("merge_size")) |v| {
        if (jsonU32(v)) |value| config.merge_size = value;
    }
    if (obj.get("image_mean")) |v| {
        if (parseRgbTriple(v)) |value| config.image_mean = value;
    }
    if (obj.get("image_std")) |v| {
        if (parseRgbTriple(v)) |value| config.image_std = value;
    }
    return config;
}

/// Exact upper bound for admission before image dimensions are known.
pub fn maxImageTokenCount(config: PreprocessorConfig) !usize {
    if (config.patch_size == 0 or config.merge_size == 0 or
        config.max_pixels == 0 or config.min_pixels > config.max_pixels)
    {
        return error.InvalidPreprocessorConfig;
    }
    const factor = std.math.mul(usize, config.patch_size, config.merge_size) catch
        return error.InvalidPreprocessorConfig;
    const pixels_per_token = std.math.mul(usize, factor, factor) catch
        return error.InvalidPreprocessorConfig;
    const token_count = config.max_pixels / pixels_per_token;
    if (token_count == 0) return error.InvalidPreprocessorConfig;
    return token_count;
}

fn parseRgbTriple(v: std.json.Value) ?[3]f32 {
    if (v != .array or v.array.items.len != 3) return null;
    var result: [3]f32 = undefined;
    for (v.array.items, 0..) |item, idx| {
        result[idx] = switch (item) {
            .float => |f| @floatCast(f),
            .integer => |i| @floatFromInt(i),
            else => return null,
        };
    }
    return result;
}

fn jsonBool(v: std.json.Value) ?bool {
    return switch (v) {
        .bool => |b| b,
        else => null,
    };
}
fn jsonU32(v: std.json.Value) ?u32 {
    return switch (v) {
        .integer => |i| @intCast(i),
        else => null,
    };
}
fn jsonF32(v: std.json.Value) ?f32 {
    return switch (v) {
        .float => |f| @floatCast(f),
        .integer => |i| @floatFromInt(i),
        else => null,
    };
}

test "model processor resampling execution identity versions preprocessing semantics" {
    const artifacts: [32]u8 = @splat(1);
    const revision = preprocessingRevision(artifacts);
    try std.testing.expect(!std.mem.eql(u8, &artifacts, &revision));
    try std.testing.expectEqual(revision, preprocessingRevision(artifacts));
    const other = preprocessingRevision(@splat(2));
    try std.testing.expect(!std.mem.eql(u8, &revision, &other));
}
