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

pub const DecodeLimits = struct {
    max_pixels: usize,
    max_rgba_bytes: usize,

    pub const inference_default = DecodeLimits{
        .max_pixels = 32 * 1024 * 1024,
        .max_rgba_bytes = 128 * 1024 * 1024,
    };

    pub fn validate(self: DecodeLimits, width: u32, height: u32) !void {
        const pixels_u64 = @as(u64, width) * @as(u64, height);
        if (pixels_u64 > std.math.maxInt(usize)) return error.ImageTooLarge;
        const pixels: usize = @intCast(pixels_u64);
        if (pixels > self.max_pixels) return error.ImageTooLarge;
        if (pixels > std.math.maxInt(usize) / 4) return error.ImageTooLarge;
        if (pixels * 4 > self.max_rgba_bytes) return error.ImageTooLarge;
    }
};

test "decode limits reject oversized dimensions" {
    const limits = DecodeLimits{
        .max_pixels = 4,
        .max_rgba_bytes = 16,
    };
    try limits.validate(2, 2);
    try std.testing.expectError(error.ImageTooLarge, limits.validate(3, 2));
    try std.testing.expectError(error.ImageTooLarge, limits.validate(2, 3));
}
