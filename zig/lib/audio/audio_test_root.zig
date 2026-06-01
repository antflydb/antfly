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
const inference_audio = @import("inference_audio");

const noise_stereo_aac_44k_tns_bytes = @embedFile("testdata/codec-corpus/noise-stereo-44k-tns.aac");

test {
    std.testing.refAllDecls(inference_audio);
}

test "zig mp3 backend decodes l3-he_mode smoke vector" {
    const fixture = @embedFile("testdata/mp3-corpus/l3-he_mode.bit");

    const decoded = try inference_audio.mp3.zig_backend.decodeMono(std.testing.allocator, fixture);
    defer std.testing.allocator.free(decoded.samples);

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 147456), decoded.samples.len);
}

test "zig aac backend decodes low-bitrate stereo tns cpe fixture" {
    var decoded = try inference_audio.aac.decodeInterleavedStereoAdtsAlloc(std.testing.allocator, noise_stereo_aac_44k_tns_bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 92160), decoded.samples.len);

    try std.testing.expectEqual(@as(usize, 45), decoded.frame_count);
}
