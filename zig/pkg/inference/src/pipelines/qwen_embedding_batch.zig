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

//! Length-aware planning for dynamic Qwen embedding rows. Planning never
//! substitutes for atomic memory admission immediately before execution.
const std = @import("std");

pub const max_padded_tokens: usize = 2048;

pub fn bucket(length: usize) usize {
    return std.math.ceilPowerOfTwo(usize, @max(length, 32)) catch std.math.maxInt(usize);
}

pub fn order(allocator: std.mem.Allocator, lengths: []const usize) ![]usize {
    const indices = try allocator.alloc(usize, lengths.len);
    for (indices, 0..) |*index, i| index.* = i;
    std.mem.sort(usize, indices, lengths, struct {
        fn less(rows: []const usize, a: usize, b: usize) bool {
            const a_bucket = bucket(rows[a]);
            const b_bucket = bucket(rows[b]);
            return if (a_bucket == b_bucket) a < b else a_bucket < b_bucket;
        }
    }.less);
    return indices;
}

pub fn chunkLength(lengths: []const usize, indices: []const usize, max_items: usize) usize {
    if (indices.len == 0) return 0;
    var maximum = lengths[indices[0]];
    var count: usize = 1;
    while (count < indices.len and count < @max(max_items, 1)) : (count += 1) {
        if (bucket(lengths[indices[count]]) != bucket(lengths[indices[0]])) break;
        const next_maximum = @max(maximum, lengths[indices[count]]);
        const padded = std.math.mul(usize, count + 1, next_maximum) catch break;
        if (padded > max_padded_tokens) break;
        maximum = next_maximum;
    }
    // A long singleton is governed by memory and context admission, rather
    // than being rejected by the microbatch throughput target.
    return count;
}

test "Qwen embedding planner bounds passage chunks and preserves identities" {
    const lengths: [32]usize = @splat(256);
    const indices = try order(std.testing.allocator, &lengths);
    defer std.testing.allocator.free(indices);
    for (indices, 0..) |index, i| try std.testing.expectEqual(i, index);
    try std.testing.expectEqual(@as(usize, 8), chunkLength(&lengths, indices, 32));
}

test "Qwen embedding planner reduces ragged padding and admits long singletons" {
    const lengths = [_]usize{ 1, 2, 8, 15, 18, 11, 226, 2082 };
    const indices = try order(std.testing.allocator, &lengths);
    defer std.testing.allocator.free(indices);
    var cursor: usize = 0;
    var padded: usize = 0;
    while (cursor < indices.len) {
        const count = chunkLength(&lengths, indices[cursor..], 32);
        var maximum: usize = 0;
        for (indices[cursor..][0..count]) |index| maximum = @max(maximum, lengths[index]);
        padded += count * maximum;
        cursor += count;
    }
    try std.testing.expectEqual(@as(usize, 2416), padded);
    try std.testing.expectEqual(@as(usize, 1), chunkLength(&.{32768}, &.{0}, 32));
    try std.testing.expectEqual(@as(usize, 32), chunkLength(&@as([32]usize, @splat(20)), indicesForShort(), 32));
}

fn indicesForShort() []const usize {
    return &.{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31 };
}
