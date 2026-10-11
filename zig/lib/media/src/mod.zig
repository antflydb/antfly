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

pub const isobmff = @import("isobmff.zig");
pub const ebml = @import("ebml.zig");
pub const mp4_audio = @import("mp4_audio.zig");
pub const webm_audio = @import("webm_audio.zig");
pub const objectstore = @import("objectstore.zig");
pub const remote = @import("remote.zig");
pub const admission = @import("admission.zig");
pub const live_mp4 = @import("live_mp4.zig");
pub const http_stream = @import("http_stream.zig");
pub const hls_live = @import("hls_live.zig");
pub const hls = @import("hls.zig");
pub const stream = @import("stream.zig");
pub const source = @import("source.zig");
pub const timeline = @import("timeline.zig");
pub const webm = @import("webm.zig");
pub const mp4 = @import("mp4.zig");
test {
    _ = isobmff;
    _ = ebml;
    _ = source;
    _ = stream;
    _ = live_mp4;
    _ = hls;
    _ = http_stream;
    _ = hls_live;
    _ = admission;
    _ = remote;
    _ = timeline;
    _ = mp4;
    _ = webm;
    _ = @import("webm_test.zig");
    _ = @import("mp4_test.zig");
}
