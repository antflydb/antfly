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
const antfly_image = @import("antfly_image");

test {
    std.testing.refAllDecls(antfly_image);
}

test "CCITT Group 3 accepts zero fill before row end markers" {
    // Two white/black striped rows. EOLs have 4, 8, and 4 leading fill bits;
    // row codes are white(2), black(2), white(2), black(2).
    const encoded = [_]u8{ 0x00, 0x01, 0x7d, 0xf0, 0x00, 0x01, 0x7d, 0xf0, 0x00, 0x10 };
    const pixels = try antfly_image.ccitt.decodeGrayAlloc(std.testing.allocator, &encoded, .msb, .group3, 8, 2, .{});
    defer std.testing.allocator.free(pixels);
    try std.testing.expectEqualSlices(u8, &.{
        255, 255, 0, 0, 255, 255, 0, 0,
        255, 255, 0, 0, 255, 255, 0, 0,
    }, pixels);
}

test "CCITT Group 3 rejects short or unterminated end markers" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.MissingCcittEol, antfly_image.ccitt.decodeGrayAlloc(alloc, &.{0x01}, .msb, .group3, 8, 1, .{}));
    try std.testing.expectError(error.EndOfStream, antfly_image.ccitt.decodeGrayAlloc(alloc, &.{ 0, 0, 0 }, .msb, .group3, 8, 1, .{}));
}
