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
    if (vocab_size == 0 or vocab_size - 1 != char_dict.len)
        return error.InvalidCharacterDictionary;
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
        try out.appendSlice(allocator, char_dict[dict_idx]);
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

pub fn loadCharDictFile(allocator: std.mem.Allocator, path: []const u8, use_space_char: bool) ![][]u8 {
    const c_file = @import("../util/c_file.zig");
    const bytes = try c_file.readFileMax(allocator, path, 1024 * 1024);
    defer allocator.free(bytes);
    return loadCharDictBytes(allocator, bytes, use_space_char);
}

pub fn loadCharDictBytes(allocator: std.mem.Allocator, bytes: []const u8, use_space_char: bool) ![][]u8 {
    var dict = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (dict.items) |entry| allocator.free(entry);
        dict.deinit(allocator);
    }

    var it = std.mem.splitScalar(u8, bytes, '\n');
    while (it.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, "\r");
        if (line.len == 0) continue;
        try dict.ensureUnusedCapacity(allocator, 1);
        dict.appendAssumeCapacity(try allocator.dupe(u8, line));
    }
    if (use_space_char) {
        try dict.ensureUnusedCapacity(allocator, 1);
        dict.appendAssumeCapacity(try allocator.dupe(u8, " "));
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

test "decode rejects unmapped vocabulary instead of dropping recognized characters" {
    try std.testing.expectError(
        error.InvalidCharacterDictionary,
        decode(std.testing.allocator, &.{ 0, 0, 0, 1 }, 1, 4, &.{ "A", "B" }),
    );
}

test "loadCharDictBytes trims windows newlines" {
    const allocator = std.testing.allocator;
    const dict = try loadCharDictBytes(allocator, "a\r\nb\r\n", false);
    defer freeCharDict(allocator, dict);

    try std.testing.expectEqual(@as(usize, 2), dict.len);
    try std.testing.expectEqualStrings("a", dict[0]);
    try std.testing.expectEqualStrings("b", dict[1]);
}

test "CTC dictionary preserves spaces UTF8 and ownership across allocation failures" {
    const Case = struct {
        fn run(allocator: std.mem.Allocator) !void {
            const dict = try loadCharDictBytes(allocator, "a\r\né\r\n", true);
            defer freeCharDict(allocator, dict);
            const result = try decode(allocator, &.{
                0, 1, 0, 0,
                0, 0, 0, 1,
                0, 0, 0, 1,
                1, 0, 0, 0,
                0, 0, 1, 0,
            }, 5, 4, dict);
            defer result.deinit();
            try std.testing.expectEqualStrings("a é", result.text);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
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

/// Reduce resident CTC logits to one class and score per timestep. Text
/// assembly remains on CPU; the vocabulary-sized output stays on the device.
/// Returns null only when the backend lacks the required reduction/gather ops.
pub fn decodeResident(allocator: std.mem.Allocator, cb: *const @import("../ops/ops.zig").ComputeBackend, output: @import("../ops/ops.zig").CT, char_dict: []const []const u8) !?Result {
    if (cb.vtable.argmaxRows == null or cb.vtable.takeRows == null or cb.vtable.reshapeOp == null) return null;
    const shape = try cb.tensorShape(output, allocator);
    defer allocator.free(shape);
    if (shape.len != 3 or shape[0] != 1 or shape[1] < 0 or shape[2] <= 0) return error.UnexpectedOutputShape;
    const time: usize = @intCast(shape[1]);
    const vocab: usize = @intCast(shape[2]);
    if (vocab - 1 != char_dict.len) return error.InvalidCharacterDictionary;
    if (time == 0) return .{ .allocator = allocator, .text = try allocator.dupe(u8, ""), .confidence = 0 };
    const total = std.math.mul(usize, time, vocab) catch return error.UnexpectedOutputShape;
    if (total > std.math.maxInt(u32)) return error.UnexpectedOutputShape;
    const logits = try cb.primReshape(output, &.{ @intCast(time), @intCast(vocab) });
    defer cb.free(logits);
    const choices = (cb.argmaxRows(logits, 0, time, vocab, allocator) catch |err| switch (err) {
        error.UnsupportedTensorType, error.UnsupportedPrimitiveOp, error.UnsupportedOperation, error.UnsupportedShape => return null,
        else => return err,
    }) orelse return null;
    defer allocator.free(choices);
    if (choices.len != time) return error.UnexpectedOutputShape;
    const indices = try allocator.alloc(u32, time);
    defer allocator.free(indices);
    for (choices, 0..) |choice, row| {
        if (choice >= vocab) return error.UnexpectedOutputShape;
        indices[row] = @intCast(row * vocab + choice);
    }
    const flat = try cb.primReshape(logits, &.{ @intCast(total), 1 });
    defer cb.free(flat);
    const selected = try cb.takeRows(flat, indices, time, 1) orelse return null;
    defer cb.free(selected);
    const scores = try cb.toFloat32(selected, allocator);
    defer allocator.free(scores);
    if (scores.len != time) return error.UnexpectedOutputShape;
    return try decodeChoices(allocator, choices, scores, char_dict);
}

fn decodeChoices(allocator: std.mem.Allocator, choices: []const u32, scores: []const f32, char_dict: []const []const u8) !Result {
    if (choices.len != scores.len) return error.UnexpectedOutputShape;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    var previous: ?u32 = null;
    var sum: f64 = 0;
    var count: usize = 0;
    for (choices, scores) |choice, score| {
        if (choice > char_dict.len) return error.InvalidCharacterDictionary;
        if (choice != 0 and choice != previous) {
            try out.appendSlice(allocator, char_dict[choice - 1]);
            sum += score;
            count += 1;
        }
        previous = choice;
    }
    return .{ .allocator = allocator, .text = try out.toOwnedSlice(allocator), .confidence = if (count > 0) sum / @as(f64, @floatFromInt(count)) else 0 };
}

fn checkCtcResidency(cb: @import("../ops/ops.zig").ComputeBackend, _: @import("../ops/ops.zig").ComputeBackend) !void {
    const allocator = std.testing.allocator;
    const dict = [_][]const u8{ "A", "é", " " };
    const data = [_]f32{ 0.1, 0.9, 0, 0, 0.1, 0.9, 0, 0, 1, 0, 0, 0, 0, 0, 0.8, 0, 0, 0, 0, 0.7, 0, 0, 0, 0.7, 0, 0.9, 0, 0 };
    const input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&data, &.{ 1, 7, 4 }));
    defer cb.free(input);
    const expected = try decode(allocator, &data, 7, 4, &dict);
    defer expected.deinit();
    const fixture = @import("../ops/media_residency_test.zig");
    const before = fixture.Transfers.snapshot(cb);
    const actual = (try decodeResident(allocator, &cb, input, &dict)) orelse return error.MissingResidentCtc;
    defer actual.deinit();
    const after = fixture.Transfers.snapshot(cb);
    if (cb.kind() == .metal) try std.testing.expect(after.downloads - before.downloads <= 7 * 8);
    if (cb.kind() == .cuda) try std.testing.expect(after.cuda_bytes - before.cuda_bytes <= 7 * 8);
    try std.testing.expectEqualStrings(expected.text, actual.text);
    try std.testing.expectApproxEqAbs(expected.confidence, actual.confidence, 1e-6);
    var limited_table = cb.vtable.*;
    limited_table.takeRows = null;
    const limited = @import("../ops/ops.zig").ComputeBackend{ .ptr = cb.ptr, .vtable = &limited_table };
    try std.testing.expectEqual(@as(?Result, null), try decodeResident(allocator, &limited, input, &dict));
}

test "media residency OCR CTC native parity" {
    const fixture = @import("../ops/media_residency_test.zig");
    try fixture.onNative(fixture.noWeights, checkCtcResidency);
}

test "media residency OCR CTC Metal parity" {
    const fixture = @import("../ops/media_residency_test.zig");
    try fixture.onMetal(fixture.noWeights, checkCtcResidency);
}

test "media residency OCR CTC CUDA parity" {
    const fixture = @import("../ops/media_residency_test.zig");
    try fixture.onCuda(fixture.noWeights, checkCtcResidency);
}
