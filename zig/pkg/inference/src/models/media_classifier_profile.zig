// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Executable preprocessing is model metadata, never inferred from tensor names.
const std = @import("std");
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
    resample: @import("antfly_image").processing.Resample = .bilinear,

    pub fn validate(self: Profile) !void {
        if (self.sample_rate < 8000 or self.sample_rate > 192000 or self.max_audio_ns <= 0 or self.max_audio_ns > 300 * std.time.ns_per_s) return error.InvalidMediaClassifierProfile;
        if (self.input == .pixels and (self.width == 0 or self.height == 0 or self.width > 4096 or self.height > 4096)) return error.InvalidMediaClassifierProfile;
        for (self.mean, self.std_dev) |mean, deviation| if (!std.math.isFinite(mean) or !std.math.isFinite(deviation) or deviation <= 0) return error.InvalidMediaClassifierProfile;
    }
};
