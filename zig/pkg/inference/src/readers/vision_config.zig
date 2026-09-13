// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

const std = @import("std");
const c_file = @import("../util/c_file.zig");
const image = @import("../pipelines/image.zig");
const encoder_decoder = @import("../pipelines/encoder_decoder.zig");

pub const PreprocessorConfig = struct {
    image_size: usize = 384,
    image_seq_length: usize = 0,
    resample: image.Resample = .bilinear,
    image_mean: [3]f32 = .{ 0.5, 0.5, 0.5 },
    image_std: [3]f32 = .{ 0.5, 0.5, 0.5 },
    pix2struct_max_patches: usize = 0,
    pix2struct_patch_height: usize = 0,
    pix2struct_patch_width: usize = 0,
    pix2struct_do_normalize: bool = false,
};

/// Immutable request-hot-path metadata owned by one loaded model generation.
/// Sidecars are parsed exactly once before the generation is published.
pub const RuntimeConfig = struct {
    decoder: encoder_decoder.DecoderConfig,
    preprocessor: PreprocessorConfig,
    final_logits_bias_zero: bool,
};

pub fn loadPreprocessorConfig(allocator: std.mem.Allocator, model_dir: []const u8) PreprocessorConfig {
    const path = std.fmt.allocPrint(allocator, "{s}/preprocessor_config.json", .{model_dir}) catch return .{};
    defer allocator.free(path);
    return loadPreprocessorConfigFile(allocator, path) catch .{};
}

pub fn loadPreprocessorConfigFile(allocator: std.mem.Allocator, path: []const u8) !PreprocessorConfig {
    const data = try c_file.readFile(allocator, path);
    defer allocator.free(data);

    var config = PreprocessorConfig{};
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, data, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidPreprocessorConfig;

    const obj = parsed.value.object;
    if (obj.get("size")) |size_val| {
        if (jsonValueGetSize(size_val)) |v| config.image_size = v;
    } else if (obj.get("crop_size")) |crop_val| {
        if (jsonValueGetSize(crop_val)) |v| config.image_size = v;
    }
    if (obj.get("image_seq_length")) |v| {
        if (jsonValueGetUsize(v)) |parsed_int| config.image_seq_length = parsed_int;
    }
    if (obj.get("resample")) |v| {
        if (jsonValueGetUsize(v)) |parsed_int| {
            config.resample = switch (parsed_int) {
                3 => .bicubic,
                2 => .bilinear,
                0 => .nearest,
                else => .bilinear,
            };
        }
    }
    if (obj.get("image_mean")) |v| {
        if (jsonValueGetFloatArray3(v)) |mean| config.image_mean = mean;
    }
    if (obj.get("image_std")) |v| {
        if (jsonValueGetFloatArray3(v)) |stddev| config.image_std = stddev;
    }
    if (obj.get("max_patches")) |v| {
        if (jsonValueGetUsize(v)) |parsed_int| config.pix2struct_max_patches = parsed_int;
    }
    if (obj.get("do_normalize")) |v| {
        switch (v) {
            .bool => |parsed_bool| config.pix2struct_do_normalize = parsed_bool,
            else => {},
        }
    }
    if (obj.get("patch_size")) |v| {
        if (v == .object) {
            if (v.object.get("height")) |height_val| {
                if (jsonValueGetUsize(height_val)) |parsed_int| config.pix2struct_patch_height = parsed_int;
            }
            if (v.object.get("width")) |width_val| {
                if (jsonValueGetUsize(width_val)) |parsed_int| config.pix2struct_patch_width = parsed_int;
            }
        }
    }

    return config;
}

fn jsonValueGetSize(val: std.json.Value) ?usize {
    return switch (val) {
        .integer => jsonValueGetUsize(val),
        .object => |obj| blk: {
            if (obj.get("height")) |h| {
                if (jsonValueGetUsize(h)) |parsed| break :blk parsed;
            }
            if (obj.get("width")) |w| {
                if (jsonValueGetUsize(w)) |parsed| break :blk parsed;
            }
            break :blk null;
        },
        else => null,
    };
}

fn jsonValueGetUsize(val: std.json.Value) ?usize {
    return switch (val) {
        .integer => |i| std.math.cast(usize, i),
        else => null,
    };
}

fn jsonValueGetFloatArray3(val: std.json.Value) ?[3]f32 {
    if (val != .array or val.array.items.len < 3) return null;
    var result: [3]f32 = undefined;
    for (0..3) |i| {
        result[i] = switch (val.array.items[i]) {
            .float => |f| @floatCast(f),
            .integer => |n| @floatFromInt(n),
            else => return null,
        };
    }
    return result;
}

test "preprocessor integer conversion rejects negative sizes" {
    try std.testing.expectEqual(@as(?usize, null), jsonValueGetUsize(.{ .integer = -1 }));
    try std.testing.expectEqual(@as(?usize, null), jsonValueGetSize(.{ .integer = -1 }));
    try std.testing.expectEqual(@as(?usize, 384), jsonValueGetSize(.{ .integer = 384 }));
}
