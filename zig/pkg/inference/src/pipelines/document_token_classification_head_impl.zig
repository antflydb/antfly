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
const common = @import("document_shared.zig");

// Internal implementation for document token-classification head logic.
// Prefer `document_token_classification.zig` for external callers.

pub const default_checkpoint_name = "layoutdoc_token_head.safetensors";
pub const default_prefix = "layoutdoc_token_head";
pub const legacy_checkpoint_name = "token_head.safetensors";
pub const legacy_prefix = "classifier";

pub const TokenBox = common.OcrToken;

pub const TokenFeatures = struct {
    text_length: usize,
    bbox: [4]i32,
    width: f32,
    height: f32,
    relative_position: f32,
    bbox_phase_sin: f32,
};

pub const ClassificationResult = common.ClassificationResult;

pub const TokenPrediction = struct {
    token_index: usize,
    text: []const u8,
    bbox: [4]i32,
    features: TokenFeatures,
    best: ?ClassificationResult,
    scores: []ClassificationResult,
};

pub const TokenHead = struct {
    allocator: std.mem.Allocator,
    feature_dim: usize,
    num_labels: usize,
    weights: []f32,
    bias: []f32,

    pub fn deinit(self: *TokenHead) void {
        self.allocator.free(self.weights);
        self.allocator.free(self.bias);
        self.* = undefined;
    }

    pub fn load(allocator: std.mem.Allocator, checkpoint_path: []const u8, prefix: []const u8) !TokenHead {
        var reader = try safetensors.MMapReader.openFileAbsolute(allocator, checkpoint_path);
        defer reader.deinit();

        const legacy_weight = try std.fmt.allocPrint(allocator, "{s}.weight", .{legacy_prefix});
        defer allocator.free(legacy_weight);
        const legacy_bias = try std.fmt.allocPrint(allocator, "{s}.bias", .{legacy_prefix});
        defer allocator.free(legacy_bias);

        var weight = try common.readTensorWithFallback(&reader, allocator, prefix, "weight", default_prefix, legacy_weight);
        defer weight.deinit();
        var bias = try common.readTensorWithFallback(&reader, allocator, prefix, "bias", default_prefix, legacy_bias);
        defer bias.deinit();

        if (weight.dtype != .f32 or bias.dtype != .f32) return error.UnsupportedDType;
        if (weight.shape.len != 2 or bias.shape.len != 1) return error.UnsupportedTensorShape;
        if (weight.shape[0] != bias.shape[0]) return error.ShapeMismatch;

        return .{
            .allocator = allocator,
            .feature_dim = @intCast(weight.shape[1]),
            .num_labels = @intCast(weight.shape[0]),
            .weights = try common.dupTensorF32(allocator, &weight),
            .bias = try common.dupTensorF32(allocator, &bias),
        };
    }
};

pub fn resolveCheckpointPath(allocator: std.mem.Allocator, model_input: []const u8) ![]const u8 {
    return common.resolveCheckpointPath(allocator, model_input, default_checkpoint_name, legacy_checkpoint_name);
}

pub fn extractFeatures(allocator: std.mem.Allocator, tokens: []const TokenBox) ![]TokenFeatures {
    const out = try allocator.alloc(TokenFeatures, tokens.len);
    errdefer allocator.free(out);
    for (tokens, 0..) |tok, idx| {
        out[idx] = computeTokenFeatures(tok, idx, tokens.len);
    }
    return out;
}

pub fn classifyTokens(
    allocator: std.mem.Allocator,
    head: *const TokenHead,
    labels: []const []const u8,
    tokens: []const TokenBox,
) ![]TokenPrediction {
    if (labels.len != head.num_labels) return error.LabelCountMismatch;

    const predictions = try allocator.alloc(TokenPrediction, tokens.len);
    errdefer {
        for (predictions[0..]) |pred| allocator.free(pred.scores);
        allocator.free(predictions);
    }

    for (tokens, 0..) |tok, idx| {
        const features = computeTokenFeatures(tok, idx, tokens.len);
        const probs = try predictProbabilities(allocator, head, features);
        defer allocator.free(probs);

        const scores = try allocator.alloc(ClassificationResult, labels.len);
        for (labels, probs, 0..) |label, prob, label_idx| {
            scores[label_idx] = .{ .label = label, .score = prob };
        }
        common.sortByScoreDesc(scores);

        predictions[idx] = .{
            .token_index = idx,
            .text = tok.text,
            .bbox = tok.bbox,
            .features = features,
            .best = if (scores.len > 0) scores[0] else null,
            .scores = scores,
        };
    }

    return predictions;
}

fn predictProbabilities(
    allocator: std.mem.Allocator,
    head: *const TokenHead,
    features: TokenFeatures,
) ![]f32 {
    const input = buildFeatureVector(features);
    if (head.feature_dim != input.len) return error.FeatureDimMismatch;

    const logits = try allocator.alloc(f32, head.num_labels);
    errdefer allocator.free(logits);
    computeTokenLogits(head, input[0..], logits);
    common.softmaxInPlace(logits);
    return logits;
}

fn computeTokenLogits(head: *const TokenHead, features: []const f32, out: []f32) void {
    for (0..head.num_labels) |j| {
        const base = j * head.feature_dim;
        var logit = head.bias[j];
        for (0..@min(head.feature_dim, features.len)) |f| {
            logit += head.weights[base + f] * features[f];
        }
        out[j] = logit;
    }
}

fn computeTokenFeatures(tok: TokenBox, token_idx: usize, token_count: usize) TokenFeatures {
    const x0 = @as(f32, @floatFromInt(tok.bbox[0])) / 1000.0;
    const y0 = @as(f32, @floatFromInt(tok.bbox[1])) / 1000.0;
    const x1 = @as(f32, @floatFromInt(tok.bbox[2])) / 1000.0;
    const y1 = @as(f32, @floatFromInt(tok.bbox[3])) / 1000.0;
    return .{
        .text_length = tok.text.len,
        .bbox = tok.bbox,
        .width = x1 - x0,
        .height = y1 - y0,
        .relative_position = @as(f32, @floatFromInt(token_idx)) / @as(f32, @floatFromInt(@max(token_count, 1))),
        .bbox_phase_sin = @sin((x0 + y0 + x1 + y1) * 3.1415927),
    };
}

fn buildFeatureVector(features: TokenFeatures) [6]f32 {
    return .{
        1.0,
        @as(f32, @floatFromInt(features.text_length)) / 64.0,
        features.width,
        features.height,
        features.relative_position,
        features.bbox_phase_sin,
    };
}

test "layoutdoc token feature vector matches training shape" {
    const features = buildFeatureVector(.{
        .text_length = 12,
        .bbox = .{ 0, 0, 100, 50 },
        .width = 0.1,
        .height = 0.05,
        .relative_position = 0.25,
        .bbox_phase_sin = 0.5,
    });
    try std.testing.expectEqual(@as(usize, 6), features.len);
}
