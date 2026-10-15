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

pub const live_coordinator = @import("live_coordinator.zig");
pub const live_windows = @import("live_windows.zig");
pub const transport_avc = @import("transport_avc.zig");
pub const h264_pictures = @import("h264_pictures.zig");
pub const webm_pictures = @import("webm_pictures.zig");
pub const codecs = @import("codecs/decoder.zig");
pub const vp8 = @import("vp8.zig");
pub const software = @import("software.zig");
pub const h264_dynamic = @import("h264_dynamic.zig");
pub const h264 = @import("h264.zig");
pub const h264_sparse = @import("h264_sparse.zig");
pub const mjpeg_metal = @import("mjpeg_metal.zig");
pub const mjpeg = @import("mjpeg.zig");
pub const capabilities = @import("capabilities.zig");
pub const avc = @import("avc.zig");
pub const preparation = @import("preparation.zig");
pub const apple = @import("backends/apple.zig");
pub const decoding = @import("decoding.zig");
pub const apple_webm = @import("backends/apple_webm.zig");
pub const decode_plan = @import("decode_plan.zig");
pub const windows = @import("windows.zig");
pub const apple_jobs = @import("apple_jobs.zig");
pub const tone = @import("tone.zig");
pub const display = @import("display.zig");
pub const sampling = @import("sampling.zig");
test {
    _ = live_coordinator;
    _ = live_windows;
    _ = transport_avc;
    _ = codecs;
    _ = @import("codecs/test.zig");
    _ = webm_pictures;
    _ = h264_pictures;
    _ = avc;
    _ = vp8;
    _ = h264_sparse;
    _ = @import("h264_test.zig");
    _ = @import("h264_fuzz_test.zig");
    _ = @import("mjpeg_test.zig");
    _ = @import("mjpeg_metal_test.zig");
    _ = @import("scheduling_test.zig");
    _ = @import("h264_transform4.zig");
    _ = @import("h264_pixels.zig");
    _ = preparation;
    _ = @import("preparation_test.zig");
    _ = apple;
    _ = decoding;
    _ = apple_webm;
    _ = @import("backends/apple_test.zig");
    _ = sampling;
    _ = display;
    _ = tone;
    _ = @import("sampling_test.zig");
}
