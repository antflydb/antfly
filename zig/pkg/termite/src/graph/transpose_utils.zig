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
const ml = @import("ml");

const max_rank = ml.graph.shape.max_rank;
const TransposeAttrs = ml.graph.node.TransposeAttrs;

/// Return the effective transpose permutation.
///
/// ONNX represents a missing `perm` attribute as an empty permutation, which
/// means "reverse all axes". Termite's graph stores that as `num_axes == 0`.
pub fn effectivePerm(attrs: TransposeAttrs, rank: usize, buf: *[max_rank]u8) []const u8 {
    if (attrs.num_axes != 0) {
        @memcpy(buf[0..attrs.num_axes], attrs.perm[0..attrs.num_axes]);
        return buf[0..attrs.num_axes];
    }
    for (0..rank) |axis| {
        buf[axis] = @intCast(rank - 1 - axis);
    }
    return buf[0..rank];
}

pub fn isValidPermutation(perm: []const u8, rank: usize) bool {
    if (rank > max_rank or perm.len != rank) return false;
    var seen: [max_rank]bool = [_]bool{false} ** max_rank;
    for (perm) |axis| {
        if (axis >= rank or seen[axis]) return false;
        seen[axis] = true;
    }
    return true;
}

test "effective transpose perm defaults to reverse axes" {
    var attrs = TransposeAttrs{};
    var buf: [max_rank]u8 = undefined;
    try std.testing.expectEqualSlices(u8, &.{ 2, 1, 0 }, effectivePerm(attrs, 3, &buf));

    attrs.num_axes = 3;
    attrs.perm = .{ 0, 2, 1, 0, 0, 0, 0, 0 };
    try std.testing.expectEqualSlices(u8, &.{ 0, 2, 1 }, effectivePerm(attrs, 3, &buf));
}
