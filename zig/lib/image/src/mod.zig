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
const conformance_impl = @import("conformance.zig");

pub const png = @import("png.zig");
pub const jpeg = @import("jpeg.zig");
pub const jpeg2000 = @import("jpeg2000/mod.zig");
pub const gif = @import("gif.zig");
pub const ccitt = @import("ccitt.zig");
pub const processing = @import("processing.zig");
pub const conformance = conformance_impl;

pub const Format = enum { png, jpeg, jpeg2000_jp2, jpeg2000_j2k, gif, unknown };

pub fn detectFormat(bytes: []const u8) Format {
    if (png.hasSignature(bytes)) return .png;
    if (jpeg2000.box.hasSignature(bytes)) return .jpeg2000_jp2;
    if (jpeg2000.codestream.hasSoc(bytes)) return .jpeg2000_j2k;
    if (jpeg.hasSignature(bytes)) return .jpeg;
    if (gif.hasSignature(bytes)) return .gif;
    return .unknown;
}

test {
    _ = png;
    _ = jpeg;
    _ = jpeg2000;
    _ = gif;
    _ = ccitt;
    _ = processing;
    _ = conformance;
    _ = @import("test_support.zig");
}

test "detectFormat signatures" {
    try std.testing.expectEqual(Format.png, detectFormat(&.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }));
    try std.testing.expectEqual(Format.jpeg, detectFormat(&.{ 0xFF, 0xD8, 0xFF, 0xE0 }));
    try std.testing.expectEqual(Format.gif, detectFormat("GIF89a\x00"));
    try std.testing.expectEqual(Format.jpeg2000_j2k, detectFormat(&.{ 0xFF, 0x4F, 0xFF, 0x51 }));
    try std.testing.expectEqual(Format.jpeg2000_jp2, detectFormat(&.{ 0x00, 0x00, 0x00, 0x0C, 'j', 'P', ' ', ' ', 0x0D, 0x0A, 0x87, 0x0A }));
    try std.testing.expectEqual(Format.unknown, detectFormat("hello"));
}
