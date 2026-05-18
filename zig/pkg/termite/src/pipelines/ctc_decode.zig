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
const backends = @import("../backends/backends.zig");

pub const Result = struct {
    allocator: std.mem.Allocator,
    text: []u8,
    confidence: f64,

    pub fn deinit(self: Result) void {
        self.allocator.free(self.text);
    }
};

pub fn decode(
    allocator: std.mem.Allocator,
    logits: []const f32,
    time_steps: usize,
    vocab_size: usize,
    char_dict: []const []const u8,
) !Result {
    const required = time_steps * vocab_size;
    if (required == 0 or logits.len < required) {
        return .{
            .allocator = allocator,
            .text = try allocator.dupe(u8, ""),
            .confidence = 0,
        };
    }

    var indices = std.ArrayListUnmanaged(usize).empty;
    defer indices.deinit(allocator);
    var confidences = std.ArrayListUnmanaged(f64).empty;
    defer confidences.deinit(allocator);

    var prev_idx: isize = -1;
    for (0..time_steps) |t| {
        const offset = t * vocab_size;
        var best_idx: usize = 0;
        var best_val: f32 = -std.math.floatMax(f32);
        for (0..vocab_size) |v| {
            const value = logits[offset + v];
            if (value > best_val) {
                best_val = value;
                best_idx = v;
            }
        }

        if (best_idx != 0 and @as(isize, @intCast(best_idx)) != prev_idx) {
            try indices.append(allocator, best_idx);
            try confidences.append(allocator, best_val);
        }
        prev_idx = @intCast(best_idx);
    }

    if (indices.items.len == 0) {
        return .{
            .allocator = allocator,
            .text = try allocator.dupe(u8, ""),
            .confidence = 0,
        };
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    for (indices.items) |idx| {
        const dict_idx = idx - 1;
        if (dict_idx < char_dict.len) try out.appendSlice(allocator, char_dict[dict_idx]);
    }

    var avg_conf: f64 = 0;
    for (confidences.items) |conf| avg_conf += conf;
    avg_conf /= @as(f64, @floatFromInt(confidences.items.len));

    return .{
        .allocator = allocator,
        .text = try out.toOwnedSlice(allocator),
        .confidence = avg_conf,
    };
}

pub fn decodeFromTensor(
    allocator: std.mem.Allocator,
    output: *const backends.Tensor,
    char_dict: []const []const u8,
) !Result {
    if (output.dtype != .f32) return error.UnsupportedTensorType;
    if (output.shape.len != 3) return error.UnexpectedOutputShape;
    if (output.shape[0] <= 0 or output.shape[1] < 0 or output.shape[2] <= 0) return error.UnexpectedOutputShape;

    const time_steps: usize = @intCast(output.shape[1]);
    const vocab_size: usize = @intCast(output.shape[2]);
    const logits = output.asFloat32IfAligned() orelse return error.UnalignedTensorData;
    return decode(allocator, logits, time_steps, vocab_size, char_dict);
}

pub fn loadCharDictFile(allocator: std.mem.Allocator, path: []const u8) ![][]u8 {
    const c_file = @import("../util/c_file.zig");
    const bytes = try c_file.readFileMax(allocator, path, 1024 * 1024);
    defer allocator.free(bytes);
    return loadCharDictBytes(allocator, bytes);
}

pub fn loadCharDictBytes(allocator: std.mem.Allocator, bytes: []const u8) ![][]u8 {
    var dict = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (dict.items) |entry| allocator.free(entry);
        dict.deinit(allocator);
    }

    var it = std.mem.splitScalar(u8, bytes, '\n');
    while (it.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, "\r");
        if (line.len == 0) continue;
        try dict.append(allocator, try allocator.dupe(u8, line));
    }

    return try dict.toOwnedSlice(allocator);
}

pub fn freeCharDict(allocator: std.mem.Allocator, dict: [][]u8) void {
    for (dict) |entry| allocator.free(entry);
    allocator.free(dict);
}

test "decode collapses blanks and repeated indices" {
    const allocator = std.testing.allocator;
    const char_dict = [_][]const u8{ "A", "B" };
    const logits = [_]f32{
        0.1, 0.9, 0.0,
        0.2, 0.8, 0.1,
        0.9, 0.1, 0.0,
        0.1, 0.2, 0.9,
    };

    const result = try decode(allocator, &logits, 4, 3, &char_dict);
    defer result.deinit();

    try std.testing.expectEqualStrings("AB", result.text);
    try std.testing.expectApproxEqAbs(@as(f64, (0.9 + 0.9) / 2.0), result.confidence, 1e-6);
}

test "loadCharDictBytes trims windows newlines" {
    const allocator = std.testing.allocator;
    const dict = try loadCharDictBytes(allocator, "a\r\nb\r\n");
    defer freeCharDict(allocator, dict);

    try std.testing.expectEqual(@as(usize, 2), dict.len);
    try std.testing.expectEqualStrings("a", dict[0]);
    try std.testing.expectEqualStrings("b", dict[1]);
}

test "decodeFromTensor validates output tensor shape" {
    const allocator = std.testing.allocator;
    var tensor = try backends.Tensor.initFloat32(allocator, "logits", &.{ 1, 2, 3 }, &.{
        0.1, 0.9, 0.0,
        0.1, 0.2, 0.9,
    });
    defer tensor.deinit();

    const char_dict = [_][]const u8{ "A", "B" };
    const result = try decodeFromTensor(allocator, &tensor, &char_dict);
    defer result.deinit();

    try std.testing.expectEqualStrings("AB", result.text);
}
