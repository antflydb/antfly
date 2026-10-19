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

//! Lowercase hex encoding for digests and binary keys.
const std = @import("std");

/// Write `bytes` as lowercase hex into the first `2 * bytes.len` bytes of `out`.
pub fn encodeHexLower(out: []u8, bytes: []const u8) void {
    std.debug.assert(out.len >= bytes.len * 2);
    for (bytes, 0..) |byte, index| {
        out[index * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[index * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
}

/// Return `bytes` as a newly allocated lowercase hex string.
pub fn allocHexLower(alloc: std.mem.Allocator, bytes: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, bytes.len * 2);
    encodeHexLower(out, bytes);
    return out;
}

test "hex encoding is lowercase and two characters per byte" {
    var out: [6]u8 = undefined;
    encodeHexLower(&out, &.{ 0x00, 0xab, 0x9f });
    try std.testing.expectEqualStrings("00ab9f", &out);
    const allocated = try allocHexLower(std.testing.allocator, &.{0xfe});
    defer std.testing.allocator.free(allocated);
    try std.testing.expectEqualStrings("fe", allocated);
}
