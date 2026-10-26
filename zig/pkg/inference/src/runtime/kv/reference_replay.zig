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

//! Deterministic, bounded token plan for one post-ingestion calibration replay.
//! The caller supplies eligible token intervals established by provenance; this
//! module never infers payload ownership from text or inserts marker tokens.
//! These tokens belong to a private reference sequence, not the live sampler,
//! grammar, history, output, or user token accounting.
const std = @import("std");

pub const max_additional_tokens: usize = 8192;
pub const max_excerpts: usize = 16;
pub const instruction = "\nRepeat the supplied document excerpts exactly, preserving their details.\n";
pub const instruction_position = std.math.maxInt(u32);
pub const Range = struct { start: u32, end: u32 };

pub const Plan = struct {
    allocator: std.mem.Allocator,
    tokens: []i64,
    /// Original prompt position for copied payload; sentinel for instruction.
    source_positions: []u32,
    payload_tokens: usize,
    excerpt_count: usize,
    entire_payload: bool,

    pub fn deinit(self: *Plan) void {
        self.allocator.free(self.tokens);
        self.allocator.free(self.source_positions);
        self.* = undefined;
    }
};

/// Includes the fixed instruction in both the 8192-token and positional limits.
/// Ranges must be ordered, nonoverlapping, nonempty, and already exclude role
/// delimiters, instructions, special tokens, and protected prefix/tail tokens.
/// Never truncate the instruction or silently admit an empty replay payload.
pub fn prepare(allocator: std.mem.Allocator, prompt: []const i64, eligible: []const Range, instruction_tokens: []const i64, position_limit: usize) !Plan {
    if (prompt.len == 0 or prompt.len >= position_limit or position_limit > std.math.maxInt(u32) or
        eligible.len == 0 or instruction_tokens.len == 0) return error.InvalidReferenceReplay;
    const budget = @min(max_additional_tokens, position_limit - prompt.len);
    if (instruction_tokens.len >= budget) return error.ContextReplayBudgetExceeded;
    for (instruction_tokens) |id| if (id < 0) return error.InvalidReferenceReplay;
    var previous_end: usize = 0;
    var payload_size: usize = 0;
    for (eligible) |range| {
        if (range.start < previous_end or range.start >= range.end or range.end > prompt.len) return error.InvalidReferenceReplay;
        payload_size += range.end - range.start; // Disjoint ranges bound sum by prompt.len.
        previous_end = range.end;
        for (prompt[range.start..range.end]) |id| if (id < 0) return error.InvalidReferenceReplay;
    }
    const payload_tokens = @min(payload_size, budget - instruction_tokens.len);
    const count = payload_tokens + instruction_tokens.len;
    const tokens = try allocator.alloc(i64, count);
    errdefer allocator.free(tokens);
    const positions = try allocator.alloc(u32, count);
    errdefer allocator.free(positions);
    @memcpy(tokens[0..instruction_tokens.len], instruction_tokens);
    @memset(positions[0..instruction_tokens.len], instruction_position);
    const excerpts = if (payload_tokens == payload_size) 1 else @min(max_excerpts, @max(@as(usize, 1), payload_tokens / 128));
    var copied: usize = 0;
    for (0..excerpts) |excerpt| {
        const size = payload_tokens / excerpts + @intFromBool(excerpt < payload_tokens % excerpts);
        // Distribute the omitted tokens between excerpts. The first and last
        // excerpt reach the payload endpoints; a single short excerpt is
        // centered. Arithmetic uses u64 to support 32-bit builds safely.
        const gap: usize = if (excerpts == 1) (payload_size - payload_tokens) / 2 else @intCast(@as(u64, @intCast(payload_size - payload_tokens)) * excerpt / (excerpts - 1));
        var skip = copied + gap;
        var remaining = size;
        for (eligible) |range| {
            const length = range.end - range.start;
            if (skip >= length) {
                skip -= length;
                continue;
            }
            const take = @min(remaining, length - skip);
            const begin = range.start + skip;
            const dest = instruction_tokens.len + copied;
            @memcpy(tokens[dest..][0..take], prompt[begin..][0..take]);
            for (positions[dest..][0..take], 0..) |*position, offset| position.* = @intCast(begin + offset);
            copied += take;
            remaining -= take;
            skip = 0;
            if (remaining == 0) break;
        }
        std.debug.assert(remaining == 0);
    }
    std.debug.assert(copied == payload_tokens);
    return .{ .allocator = allocator, .tokens = tokens, .source_positions = positions, .payload_tokens = payload_tokens, .excerpt_count = excerpts, .entire_payload = payload_tokens == payload_size };
}

test "context reference replay preserves selected tokens and excludes gaps" {
    var plan = try prepare(std.testing.allocator, &.{ 0, 11, 12, 0, 21, 22, 0 }, &.{ .{ .start = 1, .end = 3 }, .{ .start = 4, .end = 6 } }, &.{ 100, 101 }, 100);
    defer plan.deinit();
    try std.testing.expectEqualSlices(i64, &.{ 100, 101, 11, 12, 21, 22 }, plan.tokens);
    try std.testing.expectEqualSlices(u32, &.{ instruction_position, instruction_position, 1, 2, 4, 5 }, plan.source_positions);
    try std.testing.expect(plan.entire_payload);
    try std.testing.expectEqual(@as(usize, 4), plan.payload_tokens);
}

test "context reference replay bounds and deterministically distributes excerpts" {
    const prompt = try std.testing.allocator.alloc(i64, 20000);
    defer std.testing.allocator.free(prompt);
    for (prompt, 0..) |*id, i| id.* = @intCast(i);
    const ranges = [_]Range{ .{ .start = 256, .end = 9000 }, .{ .start = 10000, .end = 19000 } };
    for ([_]usize{ 20002, 20300, 24000, 100000 }) |limit| {
        var plan = try prepare(std.testing.allocator, prompt, &ranges, &.{101}, limit);
        defer plan.deinit();
        var repeat = try prepare(std.testing.allocator, prompt, &ranges, &.{101}, limit);
        defer repeat.deinit();
        try std.testing.expectEqual(@min(max_additional_tokens, limit - prompt.len), plan.tokens.len);
        try std.testing.expectEqualSlices(i64, plan.tokens, repeat.tokens);
        try std.testing.expect(!plan.entire_payload);
        var previous: u32 = 0;
        for (plan.source_positions[1..], plan.tokens[1..]) |position, token| {
            try std.testing.expect(position > previous);
            try std.testing.expect((position >= 256 and position < 9000) or (position >= 10000 and position < 19000));
            try std.testing.expectEqual(@as(i64, position), token);
            previous = position;
        }
        if (plan.excerpt_count > 1) {
            try std.testing.expectEqual(@as(u32, 256), plan.source_positions[1]);
            try std.testing.expectEqual(@as(u32, 18999), plan.source_positions[plan.source_positions.len - 1]);
        }
    }
}

test "context reference replay rejects invalid intervals and insufficient position budget" {
    const prompt = [_]i64{ 1, 2, 3, 4 };
    for ([_][]const Range{ &.{}, &.{.{ .start = 2, .end = 2 }}, &.{.{ .start = 0, .end = 5 }}, &.{ .{ .start = 0, .end = 3 }, .{ .start = 2, .end = 4 } } }) |ranges|
        try std.testing.expectError(error.InvalidReferenceReplay, prepare(std.testing.allocator, &prompt, ranges, &.{1}, 10));
    try std.testing.expectError(error.ContextReplayBudgetExceeded, prepare(std.testing.allocator, &prompt, &.{.{ .start = 1, .end = 3 }}, &.{ 1, 2 }, 6));
    try std.testing.expectError(error.InvalidReferenceReplay, prepare(std.testing.allocator, &prompt, &.{.{ .start = 1, .end = 3 }}, &.{1}, 4));
}

test "context reference replay allocation failure leaves no plan" {
    const Probe = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var plan = try prepare(allocator, &.{ 1, 2, 3 }, &.{.{ .start = 1, .end = 3 }}, &.{4}, 10);
            plan.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}
