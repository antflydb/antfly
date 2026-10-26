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

//! Executable preprocessing is model metadata, never inferred from tensor names.
const std = @import("std");
// Metadata is also read by offline tools that have no image runtime. Keep
// the persisted policy here and translate it at the preprocessing boundary.
pub const Resample = enum(u32) {
    nearest = 0,
    lanczos = 1,
    bilinear = 2,
    bicubic = 3,
    pillow_bicubic = 4,
    torchvision_bicubic = 5,
};

pub const Profile = struct {
    input: enum { waveform, pixels },
    score_transform: enum { softmax, sigmoid },
    sample_rate: u32 = 16000,
    normalize_waveform: bool = true,
    max_audio_ns: i64 = 30 * std.time.ns_per_s,
    width: u32 = 0,
    height: u32 = 0,
    mean: [3]f32 = .{ 0, 0, 0 },
    std_dev: [3]f32 = .{ 1, 1, 1 },
    resample: Resample = .bilinear,

    pub fn validate(self: Profile) !void {
        if (self.sample_rate < 8000 or self.sample_rate > 192000 or self.max_audio_ns <= 0 or self.max_audio_ns > 300 * std.time.ns_per_s) return error.InvalidMediaClassifierProfile;
        if (self.input == .pixels and (self.width == 0 or self.height == 0 or self.width > 4096 or self.height > 4096)) return error.InvalidMediaClassifierProfile;
        for (self.mean, self.std_dev) |mean, deviation| if (!std.math.isFinite(mean) or !std.math.isFinite(deviation) or deviation <= 0) return error.InvalidMediaClassifierProfile;
    }
};
