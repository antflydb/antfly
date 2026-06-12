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

pub const PreprocessorConfig = struct {
    do_resize: bool = true,
    do_rescale: bool = true,
    do_normalize: bool = true,
    do_convert_rgb: bool = true,
    rescale_factor: f32 = 1.0 / 255.0,
    image_mean: [3]f32 = .{ 0.48145466, 0.4578275, 0.40821073 },
    image_std: [3]f32 = .{ 0.26862954, 0.26130258, 0.27577711 },
    min_pixels: u32 = 56 * 56,
    max_pixels: u32 = 28 * 28 * 1280,
    patch_size: u32 = 14,
    temporal_patch_size: u32 = 2,
    merge_size: u32 = 2,
};

pub const PreparedImage = struct {
    allocator: std.mem.Allocator,
    pixel_values: []f32,
    resized_width: u32,
    resized_height: u32,
    image_grid_thw: [3]u32,
    image_token_count: usize,

    pub fn deinit(self: *PreparedImage) void {
        self.allocator.free(self.pixel_values);
    }
};
