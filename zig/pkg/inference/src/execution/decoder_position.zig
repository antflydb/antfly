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

/// Borrowed new tokens for an append-only decoder. Positions are zero-based.
pub const Suffix = struct {
    position: usize,
    tokens: []const i64,

    pub fn endAt(self: Suffix, committed: usize) !usize {
        if (self.position != committed or self.tokens.len == 0) return error.InvalidDecoderPosition;
        return std.math.add(usize, self.position, self.tokens.len) catch error.InvalidDecoderPosition;
    }
};

test "decoder suffix accepts only a nonempty append at the committed position" {
    const suffix: Suffix = .{ .position = 3, .tokens = &.{ 9, 10 } };
    try std.testing.expectEqual(@as(usize, 5), try suffix.endAt(3));
    try std.testing.expectError(error.InvalidDecoderPosition, suffix.endAt(2));
    try std.testing.expectError(error.InvalidDecoderPosition, suffix.endAt(4));
    const empty: Suffix = .{ .position = 3, .tokens = &.{} };
    try std.testing.expectError(error.InvalidDecoderPosition, empty.endAt(3));
    const overflowing: Suffix = .{ .position = std.math.maxInt(usize), .tokens = &.{9} };
    try std.testing.expectError(error.InvalidDecoderPosition, overflowing.endAt(overflowing.position));
}
