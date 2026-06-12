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
const c_file = @import("../util/c_file.zig");
const safetensors = @import("../models/safetensors.zig");
const image = @import("image.zig");
const common = @import("document_shared.zig");

// Internal implementation for document/page classification head logic.
// Prefer `document_classification.zig` for external callers.

pub const ClassificationResult = common.ClassificationResult;

pub const default_checkpoint_name = "layoutdoc_sequence_head.safetensors";
pub const default_prefix = "layoutdoc_sequence_head";
pub const legacy_checkpoint_name = "sequence_head.safetensors";

pub const SequenceExampleInput = struct {
    image_path: []const u8,
    num_tokens: usize,
};

pub const SequenceFeatures = struct {
    num_tokens: usize,
    image_width: u32,
    image_height: u32,
    image_components: u8,
    mean_darkness: f32,
    std_darkness: f32,
    top_darkness: f32,
    bottom_darkness: f32,
    left_darkness: f32,
    right_darkness: f32,
    center_darkness: f32,
};

pub const SequenceHead = struct {
    allocator: std.mem.Allocator,
    feature_dim: usize,
    hidden_dim: usize,
    num_labels: usize,
    w1: []f32,
    b1: []f32,
    w2: []f32,
    b2: []f32,

    pub fn deinit(self: *SequenceHead) void {
        self.allocator.free(self.w1);
        self.allocator.free(self.b1);
        self.allocator.free(self.w2);
        self.allocator.free(self.b2);
        self.* = undefined;
    }

    pub fn load(allocator: std.mem.Allocator, checkpoint_path: []const u8, prefix: []const u8) !SequenceHead {
        var reader = try safetensors.MMapReader.openFileAbsolute(allocator, checkpoint_path);
        defer reader.deinit();

        var w1 = try common.readTensorWithFallback(&reader, allocator, prefix, "w1", default_prefix, "classifier.dense.weight");
        defer w1.deinit();
        var b1 = try common.readTensorWithFallback(&reader, allocator, prefix, "b1", default_prefix, "classifier.dense.bias");
        defer b1.deinit();
        var w2 = try common.readTensorWithFallback(&reader, allocator, prefix, "w2", default_prefix, "classifier.out_proj.weight");
        defer w2.deinit();
        var b2 = try common.readTensorWithFallback(&reader, allocator, prefix, "b2", default_prefix, "classifier.out_proj.bias");
        defer b2.deinit();

        if (w1.dtype != .f32 or b1.dtype != .f32 or w2.dtype != .f32 or b2.dtype != .f32) return error.UnsupportedDType;
        if (w1.shape.len != 2 or b1.shape.len != 1 or w2.shape.len != 2 or b2.shape.len != 1) return error.UnsupportedTensorShape;
        if (w1.shape[0] != b1.shape[0]) return error.ShapeMismatch;
        if (w2.shape[0] != b2.shape[0]) return error.ShapeMismatch;
        if (w1.shape[0] != w2.shape[1]) return error.ShapeMismatch;

        const feature_dim: usize = @intCast(w1.shape[1]);
        const hidden_dim: usize = @intCast(w1.shape[0]);
        const num_labels: usize = @intCast(w2.shape[0]);

        return .{
            .allocator = allocator,
            .feature_dim = feature_dim,
            .hidden_dim = hidden_dim,
            .num_labels = num_labels,
            .w1 = try common.dupTensorF32(allocator, &w1),
            .b1 = try common.dupTensorF32(allocator, &b1),
            .w2 = try common.dupTensorF32(allocator, &w2),
            .b2 = try common.dupTensorF32(allocator, &b2),
        };
    }
};

pub fn resolveCheckpointPath(allocator: std.mem.Allocator, model_input: []const u8) ![]const u8 {
    return common.resolveCheckpointPath(allocator, model_input, default_checkpoint_name, legacy_checkpoint_name);
}

pub fn extractFeatures(allocator: std.mem.Allocator, input: SequenceExampleInput) !SequenceFeatures {
    const stats = try computeVisualStatsForPath(allocator, input.image_path);
    return .{
        .num_tokens = input.num_tokens,
        .image_width = stats.width,
        .image_height = stats.height,
        .image_components = stats.components,
        .mean_darkness = stats.mean_darkness,
        .std_darkness = stats.std_darkness,
        .top_darkness = stats.top_darkness,
        .bottom_darkness = stats.bottom_darkness,
        .left_darkness = stats.left_darkness,
        .right_darkness = stats.right_darkness,
        .center_darkness = stats.center_darkness,
    };
}

pub fn classify(
    allocator: std.mem.Allocator,
    head: *const SequenceHead,
    labels: []const []const u8,
    input: SequenceExampleInput,
) ![]ClassificationResult {
    if (labels.len != head.num_labels) return error.LabelCountMismatch;
    const features = try extractFeatures(allocator, input);
    const probs = try predictProbabilities(allocator, head, features);
    defer allocator.free(probs);

    const results = try allocator.alloc(ClassificationResult, labels.len);
    for (labels, probs, 0..) |label, prob, idx| {
        results[idx] = .{
            .label = label,
            .score = prob,
        };
    }
    common.sortByScoreDesc(results);
    return results;
}

fn predictProbabilities(
    allocator: std.mem.Allocator,
    head: *const SequenceHead,
    features: SequenceFeatures,
) ![]f32 {
    const input = buildFeatureVector(features);
    if (head.feature_dim != input.len) return error.FeatureDimMismatch;

    const hidden = try allocator.alloc(f32, head.hidden_dim);
    defer allocator.free(hidden);
    computeHidden(head, input[0..], hidden);
    for (hidden) |*v| v.* = if (v.* > 0) v.* else 0;

    const logits = try allocator.alloc(f32, head.num_labels);
    errdefer allocator.free(logits);
    computeLogits(head, hidden, logits);
    common.softmaxInPlace(logits);
    return logits;
}

fn computeHidden(head: *const SequenceHead, features: []const f32, out: []f32) void {
    for (0..head.hidden_dim) |h| {
        const base = h * head.feature_dim;
        var value = head.b1[h];
        for (0..head.feature_dim) |f| value += head.w1[base + f] * features[f];
        out[h] = value;
    }
}

fn computeLogits(head: *const SequenceHead, hidden: []const f32, out: []f32) void {
    for (0..head.num_labels) |j| {
        const base = j * head.hidden_dim;
        var logit = head.b2[j];
        for (0..head.hidden_dim) |h| logit += head.w2[base + h] * hidden[h];
        out[j] = logit;
    }
}

fn buildFeatureVector(features: SequenceFeatures) [8]f32 {
    return .{
        1.0,
        @as(f32, @floatFromInt(features.num_tokens)) / 512.0,
        @as(f32, @floatFromInt(features.image_width)) / 2000.0,
        @as(f32, @floatFromInt(features.image_height)) / 2000.0,
        features.mean_darkness,
        features.std_darkness,
        features.top_darkness - features.bottom_darkness,
        features.left_darkness - features.right_darkness,
    };
}

const VisualStats = struct {
    width: u32,
    height: u32,
    components: u8,
    mean_darkness: f32,
    std_darkness: f32,
    top_darkness: f32,
    bottom_darkness: f32,
    left_darkness: f32,
    right_darkness: f32,
    center_darkness: f32,
};

fn computeVisualStatsForPath(allocator: std.mem.Allocator, path: []const u8) !VisualStats {
    const image_bytes = try c_file.readFileMax(allocator, path, 128 * 1024 * 1024);
    defer allocator.free(image_bytes);
    const decoded = try image.decode(allocator, image_bytes);
    defer decoded.deinit(allocator);
    const pixel_len = @as(usize, decoded.width) * @as(usize, decoded.height) * @as(usize, decoded.channels);
    const pixels = decoded.data[0..pixel_len];
    return computeVisualStatsFromPixels(decoded.width, decoded.height, @intCast(decoded.channels), pixels);
}

fn computeVisualStatsFromPixels(width: u32, height: u32, components: u8, pixels: []const u8) VisualStats {
    var sum: f32 = 0;
    var sum_sq: f32 = 0;
    var top: f32 = 0;
    var bottom: f32 = 0;
    var left: f32 = 0;
    var right: f32 = 0;
    var center: f32 = 0;

    var y: usize = 0;
    while (y < 16) : (y += 1) {
        var x: usize = 0;
        while (x < 16) : (x += 1) {
            const darkness = samplePixelDarkness(width, height, components, pixels, x, y);
            sum += darkness;
            sum_sq += darkness * darkness;
            if (y < 8) top += darkness else bottom += darkness;
            if (x < 8) left += darkness else right += darkness;
            if (x >= 4 and x < 12 and y >= 4 and y < 12) center += darkness;
        }
    }

    const n: f32 = 256.0;
    const mean = sum / n;
    const variance = @max(0.0, (sum_sq / n) - (mean * mean));
    return .{
        .width = width,
        .height = height,
        .components = components,
        .mean_darkness = clamp01(mean),
        .std_darkness = clamp01(@sqrt(variance)),
        .top_darkness = clamp01(top / 128.0),
        .bottom_darkness = clamp01(bottom / 128.0),
        .left_darkness = clamp01(left / 128.0),
        .right_darkness = clamp01(right / 128.0),
        .center_darkness = clamp01(center / 64.0),
    };
}

fn samplePixelDarkness(width_u32: u32, height_u32: u32, components: u8, pixels: []const u8, x16: usize, y16: usize) f32 {
    const width = @as(usize, width_u32);
    const height = @as(usize, height_u32);
    if (width == 0 or height == 0) return 0;

    const src_x = if (width == 1) 0 else @min((x16 * width) / 16, width - 1);
    const src_y = if (height == 1) 0 else @min((y16 * height) / 16, height - 1);
    const base = (src_y * width + src_x) * components;

    const r, const g, const b = if (components == 1) blk: {
        const gray = @as(f32, @floatFromInt(pixels[base])) / 255.0;
        break :blk .{ gray, gray, gray };
    } else blk: {
        const rr = @as(f32, @floatFromInt(pixels[base])) / 255.0;
        const gg = @as(f32, @floatFromInt(pixels[base + 1])) / 255.0;
        const bb = @as(f32, @floatFromInt(pixels[base + 2])) / 255.0;
        break :blk .{ rr, gg, bb };
    };
    return clamp01(1.0 - ((r + g + b) / 3.0));
}

fn clamp01(value: f32) f32 {
    return std.math.clamp(value, 0.0, 1.0);
}

test "focused layoutdoc sequence feature vector matches training shape" {
    const features = buildFeatureVector(.{
        .num_tokens = 12,
        .image_width = 1000,
        .image_height = 800,
        .image_components = 3,
        .mean_darkness = 0.5,
        .std_darkness = 0.2,
        .top_darkness = 0.6,
        .bottom_darkness = 0.4,
        .left_darkness = 0.7,
        .right_darkness = 0.3,
        .center_darkness = 0.5,
    });
    try std.testing.expectEqual(@as(usize, 8), features.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), features[0], 1e-6);
}
