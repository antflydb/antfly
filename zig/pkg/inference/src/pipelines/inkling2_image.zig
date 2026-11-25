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
const image = @import("image.zig");

pub fn decodeRgb(allocator: std.mem.Allocator, data: []const u8) !image.Image {
    _ = try image.inspectEncodedForInference(data, 4096);
    const rgba = try image.decodeRgba(allocator, data);
    defer rgba.deinit(allocator);
    const rgb = try allocator.alloc(u8, @as(usize, rgba.width) * rgba.height * 3);
    for (0..rgba.height) |y| for (0..rgba.width) |x| {
        const i = y * rgba.width + x;
        const alpha: u32 = rgba.data[4 * i + 3];
        const checker: u32 = if ((x / 8 + y / 8) % 2 == 0) 204 else 153;
        for (0..3) |channel| {
            const color: u32 = rgba.data[4 * i + channel];
            rgb[3 * i + channel] = @intCast((alpha * color + (255 - alpha) * checker + 127) / 255);
        }
    };
    return .{ .data = rgb, .width = rgba.width, .height = rgba.height, .channels = 3 };
}
