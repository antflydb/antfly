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
const Tensor = @import("../backends/tensor.zig").Tensor;

/// Legacy shared helper module for document-task compatibility shims.
/// Prefer `document_preprocessing.zig`, `document_classification.zig`, and
/// `document_token_classification.zig` for new call sites.
pub const ClassificationResult = struct {
    label: []const u8,
    score: f32,
};

/// OCR token with text and bounding box used by the legacy layoutdoc/layoutlmv3
/// modules and re-exported through `document_preprocessing.zig`.
pub const OcrToken = struct {
    text: []const u8,
    bbox: [4]i32,
};

/// Sort a ClassificationResult slice by score descending (in-place).
pub fn sortByScoreDesc(results: []ClassificationResult) void {
    std.mem.sort(ClassificationResult, results, {}, struct {
        fn lessThan(_: void, lhs: ClassificationResult, rhs: ClassificationResult) bool {
            return lhs.score > rhs.score;
        }
    }.lessThan);
}

/// In-place softmax over logits, writing probabilities into `out`.
pub fn softmax(logits: []const f32, out: []f32) void {
    var max_logit = logits[0];
    for (logits[1..]) |value| {
        if (value > max_logit) max_logit = value;
    }
    var sum: f32 = 0;
    for (logits, 0..) |value, idx| {
        out[idx] = @exp(value - max_logit);
        sum += out[idx];
    }
    if (sum > 0) {
        for (out) |*value| value.* /= sum;
    }
}

/// In-place softmax: reads from and writes back to the same slice.
pub fn softmaxInPlace(logits: []f32) void {
    var max_logit: f32 = -std.math.inf(f32);
    for (logits) |logit| max_logit = @max(max_logit, logit);
    var exp_sum: f32 = 0;
    for (logits) |*logit| {
        logit.* = @exp(logit.* - max_logit);
        exp_sum += logit.*;
    }
    if (exp_sum > 0) {
        for (logits) |*logit| logit.* /= exp_sum;
    }
}

/// Resolve a safetensors checkpoint path. Tries `default_name` then `legacy_name`
/// as filenames within `model_input` (if it's a directory), or returns `model_input`
/// directly if it already ends in `.safetensors`.
pub fn resolveCheckpointPath(
    allocator: std.mem.Allocator,
    model_input: []const u8,
    default_name: []const u8,
    legacy_name: []const u8,
) ![]const u8 {
    if (std.mem.endsWith(u8, model_input, ".safetensors")) return allocator.dupe(u8, model_input);
    const direct = try std.fs.path.join(allocator, &.{ model_input, default_name });
    if (c_file.fileExists(allocator, direct)) return direct;
    allocator.free(direct);
    const legacy = try std.fs.path.join(allocator, &.{ model_input, legacy_name });
    if (c_file.fileExists(allocator, legacy)) return legacy;
    allocator.free(legacy);
    return error.CheckpointNotFound;
}

/// Read a named tensor from a safetensors reader, trying `{prefix}.{suffix}` first,
/// falling back to `legacy_name` if the prefix matches `default_prefix`.
pub fn readTensorWithFallback(
    reader: *safetensors.MMapReader,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    suffix: []const u8,
    default_prefix: []const u8,
    legacy_name: []const u8,
) !Tensor {
    const canonical_name = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ prefix, suffix });
    defer allocator.free(canonical_name);
    return reader.readTensor(canonical_name) catch |err| {
        if (!std.mem.eql(u8, prefix, default_prefix)) return err;
        return reader.readTensor(legacy_name);
    };
}

/// Duplicate a safetensors tensor's data as an owned `[]f32` slice.
/// Only supports f32 dtype tensors.
pub fn dupTensorF32(allocator: std.mem.Allocator, tensor: *const Tensor) ![]f32 {
    if (tensor.dtype != .f32) return error.UnsupportedDType;
    const count = tensor.elementCount();
    if (tensor.data.len != count * @sizeOf(f32)) return error.UnsupportedTensorShape;
    const out = try allocator.alloc(f32, count);
    errdefer allocator.free(out);
    for (0..count) |idx| {
        const start = idx * @sizeOf(f32);
        const word: *const [4]u8 = @ptrCast(tensor.data[start .. start + 4].ptr);
        const bits = std.mem.readInt(u32, word, .little);
        out[idx] = @bitCast(bits);
    }
    return out;
}

/// Append token IDs and attention mask values from `values` into `ids`/`mask`
/// starting at `start`, respecting `limit`. Returns the new position.
pub fn appendSliceInto(ids: []i32, mask: []i32, start: usize, limit: usize, values: []const i32) usize {
    var pos = start;
    for (values) |value| {
        if (pos >= limit) break;
        ids[pos] = value;
        mask[pos] = 1;
        pos += 1;
    }
    return pos;
}

test "softmax produces valid probability distribution" {
    var logits = [_]f32{ 1.0, 2.0, 3.0 };
    var out: [3]f32 = undefined;
    softmax(&logits, &out);
    var sum: f32 = 0;
    for (out) |p| {
        try std.testing.expect(p >= 0 and p <= 1);
        sum += p;
    }
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), sum, 1e-6);
    try std.testing.expect(out[2] > out[1]);
    try std.testing.expect(out[1] > out[0]);
}

test "softmaxInPlace matches softmax" {
    const logits = [_]f32{ 1.0, 2.0, 3.0 };
    var out: [3]f32 = undefined;
    softmax(&logits, &out);

    var in_place = [_]f32{ 1.0, 2.0, 3.0 };
    softmaxInPlace(&in_place);
    for (0..3) |i| {
        try std.testing.expectApproxEqAbs(out[i], in_place[i], 1e-6);
    }
}

test "sortByScoreDesc orders highest first" {
    var results = [_]ClassificationResult{
        .{ .label = "a", .score = 0.1 },
        .{ .label = "b", .score = 0.9 },
        .{ .label = "c", .score = 0.5 },
    };
    sortByScoreDesc(&results);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), results[0].score, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), results[1].score, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.1), results[2].score, 1e-6);
}

test "resolveCheckpointPath returns direct safetensors path" {
    const allocator = std.testing.allocator;
    const result = try resolveCheckpointPath(allocator, "/some/model.safetensors", "default.safetensors", "legacy.safetensors");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("/some/model.safetensors", result);
}

test "appendSliceInto respects limit" {
    var ids: [4]i32 = .{ 0, 0, 0, 0 };
    var mask: [4]i32 = .{ 0, 0, 0, 0 };
    const values = [_]i32{ 10, 20, 30, 40, 50 };
    const pos = appendSliceInto(&ids, &mask, 1, 3, &values);
    try std.testing.expectEqual(@as(usize, 3), pos);
    try std.testing.expectEqual(@as(i32, 10), ids[1]);
    try std.testing.expectEqual(@as(i32, 20), ids[2]);
    try std.testing.expectEqual(@as(i32, 0), ids[3]);
}
