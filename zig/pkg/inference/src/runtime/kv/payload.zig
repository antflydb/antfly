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

//! Explicit payload selection and byte-to-token eligibility. Ownership comes
//! from the renderer and tokenizer, never from matching prompt substrings.
const std = @import("std");
const replay = @import("reference_replay.zig");
pub const Target = struct { message_index: u32 = 0, part_index: u32 = 0 };
pub const max_targets = 64;
pub const Targets = struct {
    items: [max_targets]Target = @splat(.{}),
    count: u8 = 0,

    pub fn slice(self: *const Targets) []const Target {
        return self.items[0..@min(self.count, max_targets)];
    }
    pub fn add(self: *Targets, target: Target) !void {
        if (self.count >= max_targets) return error.InvalidCompactionTarget;
        for (self.slice()) |old| if (std.meta.eql(old, target)) return error.InvalidCompactionTarget;
        self.items[self.count] = target;
        self.count += 1;
    }
    pub fn parse(self: *Targets, value: []const u8) !void {
        var parts = std.mem.splitScalar(u8, value, ':');
        const message = parts.next() orelse return error.InvalidCompactionTarget;
        const part = parts.next() orelse return error.InvalidCompactionTarget;
        if (parts.next() != null or message.len == 0 or part.len == 0) return error.InvalidCompactionTarget;
        for (message) |c| if (!std.ascii.isDigit(c)) return error.InvalidCompactionTarget;
        for (part) |c| if (!std.ascii.isDigit(c)) return error.InvalidCompactionTarget;
        try self.add(.{
            .message_index = std.fmt.parseInt(u32, message, 10) catch return error.InvalidCompactionTarget,
            .part_index = std.fmt.parseInt(u32, part, 10) catch return error.InvalidCompactionTarget,
        });
    }
    pub fn label(self: *const Targets, message: usize, part: usize) u32 {
        for (self.slice(), 0..) |target, i| if (target.message_index == message and target.part_index == part) return @intCast(i + 1);
        return 0;
    }
    pub fn validateMessages(self: *const Targets, messages: anytype) !void {
        if (self.count == 0 or self.count > max_targets) return error.InvalidCompactionTarget;
        for (self.slice(), 0..) |target, i| {
            for (self.slice()[0..i]) |old| if (std.meta.eql(old, target)) return error.InvalidCompactionTarget;
            if (target.message_index >= messages.len) return error.InvalidCompactionTarget;
            const message = messages[target.message_index];
            if (!std.mem.eql(u8, message.role, "user") and !std.mem.eql(u8, message.role, "tool")) return error.InvalidCompactionTarget;
            if (message.content_parts) |parts| {
                if (target.part_index >= parts.len or parts[target.part_index] != .text or parts[target.part_index].text.len == 0) return error.InvalidCompactionTarget;
            } else if (target.part_index != 0 or message.content_is_null or message.content.len == 0) return error.InvalidCompactionTarget;
        }
    }
};

pub const Eligible = struct {
    allocator: std.mem.Allocator,
    labels: []u32,
    ranges: []replay.Range,
    blocks: []replay.Range,
    pub fn deinit(self: *Eligible) void {
        self.allocator.free(self.labels);
        self.allocator.free(self.ranges);
        self.allocator.free(self.blocks);
    }
};

/// Require one nonzero part label across every byte of a token. Cross-part,
/// cross-role, zero-span, special, prefix and tail tokens are always protected.
/// Full 512-token blocks are anchored within each contiguous eligible part.
pub fn tokenizeEligibility(allocator: std.mem.Allocator, byte_labels: []const u32, offsets: []const [2]u32, ids: []const i32, specials: []const u32, target_count: usize, prefix: usize, recent: usize) !Eligible {
    if (ids.len != offsets.len or target_count == 0 or target_count > max_targets or ids.len > std.math.maxInt(u32)) return error.InvalidPayloadProvenance;
    var seen: [max_targets]bool = @splat(false);
    for (byte_labels) |label| {
        if (label > target_count) return error.InvalidPayloadProvenance;
        if (label != 0) seen[label - 1] = true;
    }
    for (seen[0..target_count]) |present| if (!present) return error.UnrenderedCompactionTarget;
    const labels = try allocator.alloc(u32, ids.len);
    errdefer allocator.free(labels);
    @memset(labels, 0);
    for (offsets, ids, 0..) |span, id, i| {
        if (id < 0 or span[0] > span[1] or span[1] > byte_labels.len) return error.InvalidPayloadProvenance;
        if (span[0] == span[1] or i < prefix or i >= ids.len -| recent or std.mem.indexOfScalar(u32, specials, @intCast(id)) != null) continue;
        const label = byte_labels[span[0]];
        if (label != 0 and std.mem.allEqual(u32, byte_labels[span[0]..span[1]], label)) labels[i] = label;
    }
    var ranges: std.ArrayListUnmanaged(replay.Range) = .empty;
    defer ranges.deinit(allocator);
    var blocks: std.ArrayListUnmanaged(replay.Range) = .empty;
    defer blocks.deinit(allocator);
    var i: usize = 0;
    while (i < labels.len) {
        if (labels[i] == 0) {
            i += 1;
            continue;
        }
        const start = i;
        const label = labels[i];
        while (i < labels.len and labels[i] == label) : (i += 1) {}
        try ranges.append(allocator, .{ .start = @intCast(start), .end = @intCast(i) });
        var block = start;
        while (i - block >= 512) : (block += 512) try blocks.append(allocator, .{ .start = @intCast(block), .end = @intCast(block + 512) });
    }
    const owned_ranges = try ranges.toOwnedSlice(allocator);
    errdefer allocator.free(owned_ranges);
    return .{ .allocator = allocator, .labels = labels, .ranges = owned_ranges, .blocks = try blocks.toOwnedSlice(allocator) };
}

test "context payload protects cross-part tokens specials prefix and tail" {
    var result = try tokenizeEligibility(std.testing.allocator, &.{ 0, 1, 1, 1, 2, 2, 2, 2 }, &.{ .{ 0, 1 }, .{ 1, 3 }, .{ 3, 5 }, .{ 5, 6 }, .{ 6, 7 }, .{ 7, 8 } }, &.{ 0, 1, 2, 3, 4, 5 }, &.{3}, 2, 1, 1);
    defer result.deinit();
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 0, 0, 2, 0 }, result.labels);
    try std.testing.expectEqual(@as(usize, 2), result.ranges.len);
    try std.testing.expectEqual(@as(usize, 0), result.blocks.len);
    try std.testing.expectError(error.UnrenderedCompactionTarget, tokenizeEligibility(std.testing.allocator, &.{1}, &.{.{ 0, 1 }}, &.{1}, &.{}, 2, 0, 0));
}

test "context payload full blocks remain within each selected part" {
    var labels: [1536]u32 = undefined;
    @memset(labels[0..513], 1);
    @memset(labels[513..], 2);
    var offsets: [labels.len][2]u32 = undefined;
    const ids: [labels.len]i32 = @splat(7);
    for (&offsets, 0..) |*span, i| span.* = .{ @intCast(i), @intCast(i + 1) };
    var result = try tokenizeEligibility(std.testing.allocator, &labels, &offsets, &ids, &.{}, 2, 0, 0);
    defer result.deinit();
    try std.testing.expectEqualSlices(replay.Range, &.{ .{ .start = 0, .end = 512 }, .{ .start = 513, .end = 1025 } }, result.blocks);
}

test "context payload selector syntax duplicate bounds and allocation rollback" {
    var targets = Targets{};
    try targets.parse("2:0");
    try std.testing.expectError(error.InvalidCompactionTarget, targets.parse("2:0"));
    for ([_][]const u8{ "-1:0", "1", "1:2:3", "1:+2", "4294967296:0" }) |value| try std.testing.expectError(error.InvalidCompactionTarget, targets.parse(value));
    const Probe = struct {
        fn run(a: std.mem.Allocator) !void {
            var result = try tokenizeEligibility(a, &.{1}, &.{.{ 0, 1 }}, &.{1}, &.{}, 1, 0, 0);
            result.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}
