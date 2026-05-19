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

pub const WindowPack = struct {
    data: []f32,
    padded_h: usize,
    padded_w: usize,
    window_count: usize,
    window_area: usize,
};

pub fn padTokensToWindows(
    allocator: std.mem.Allocator,
    tokens: []const f32,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
    window_size: usize,
) !WindowPack {
    const pad_w = (window_size - (width % window_size)) % window_size;
    const pad_h = (window_size - (height % window_size)) % window_size;
    const padded_h = height + pad_h;
    const padded_w = width + pad_w;
    const windows_h = padded_h / window_size;
    const windows_w = padded_w / window_size;
    const window_count = batch * windows_h * windows_w;
    const window_area = window_size * window_size;

    const data = try allocator.alloc(f32, window_count * window_area * dim);
    @memset(data, 0.0);

    for (0..batch) |b| {
        for (0..windows_h) |wh| {
            for (0..windows_w) |ww| {
                const window_index = ((b * windows_h) + wh) * windows_w + ww;
                for (0..window_size) |dy| {
                    for (0..window_size) |dx| {
                        const src_y = wh * window_size + dy;
                        const src_x = ww * window_size + dx;
                        if (src_y >= height or src_x >= width) continue;
                        const src_token = (b * height * width + src_y * width + src_x) * dim;
                        const dst_token = (window_index * window_area + dy * window_size + dx) * dim;
                        @memcpy(data[dst_token..][0..dim], tokens[src_token..][0..dim]);
                    }
                }
            }
        }
    }

    return .{
        .data = data,
        .padded_h = padded_h,
        .padded_w = padded_w,
        .window_count = window_count,
        .window_area = window_area,
    };
}

pub fn unpadWindowTokens(
    allocator: std.mem.Allocator,
    window_tokens: []const f32,
    window_pack: WindowPack,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
) ![]f32 {
    const window_size = std.math.sqrt(window_pack.window_area);
    const windows_h = window_pack.padded_h / window_size;
    const windows_w = window_pack.padded_w / window_size;
    const result = try allocator.alloc(f32, batch * height * width * dim);

    for (0..batch) |b| {
        for (0..windows_h) |wh| {
            for (0..windows_w) |ww| {
                const window_index = ((b * windows_h) + wh) * windows_w + ww;
                for (0..window_size) |dy| {
                    for (0..window_size) |dx| {
                        const dst_y = wh * window_size + dy;
                        const dst_x = ww * window_size + dx;
                        if (dst_y >= height or dst_x >= width) continue;
                        const src_token = (window_index * window_pack.window_area + dy * window_size + dx) * dim;
                        const dst_token = (b * height * width + dst_y * width + dst_x) * dim;
                        @memcpy(result[dst_token..][0..dim], window_tokens[src_token..][0..dim]);
                    }
                }
            }
        }
    }

    return result;
}

test "padTokensToWindows and unpadWindowTokens round-trip" {
    const allocator = std.testing.allocator;
    const tokens = [_]f32{
        1,  2,
        3,  4,
        5,  6,
        7,  8,
        9,  10,
        11, 12,
    };
    const pack = try padTokensToWindows(allocator, &tokens, 1, 2, 3, 2, 2);
    defer allocator.free(pack.data);
    const restored = try unpadWindowTokens(allocator, pack.data, pack, 1, 2, 3, 2);
    defer allocator.free(restored);
    try std.testing.expectEqualSlices(f32, &tokens, restored);
}
