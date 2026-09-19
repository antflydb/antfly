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

//! Test root for lib/audio. Every codec module is referenced here so that a
//! `--test-filter` can reach its tests: Zig only collects tests from files
//! that analyzed code references, and a filtered run analyzes none of the
//! mod.zig tests that would otherwise pull the codec modules in.

const audio = @import("src/mod.zig");
const aac = @import("src/aac.zig");
const aiff = @import("src/aiff.zig");
const alac = @import("src/alac.zig");
const au = @import("src/au.zig");
const caf = @import("src/caf.zig");
const conformance = @import("src/conformance.zig");
const flac = @import("src/flac.zig");
const imdct = @import("src/imdct.zig");
const mp3 = @import("src/mp3.zig");
const mp4 = @import("src/mp4.zig");
const ogg = @import("src/ogg.zig");
const opus = @import("src/opus.zig");
const opus_celt = @import("src/opus_celt.zig");
const opus_silk = @import("src/opus_silk.zig");
const vorbis = @import("src/vorbis.zig");
const wav = @import("src/wav.zig");
const webm = @import("src/webm.zig");

test {
    _ = audio;
    _ = aac;
    _ = aiff;
    _ = alac;
    _ = au;
    _ = caf;
    _ = conformance;
    _ = flac;
    _ = imdct;
    _ = mp3;
    _ = mp4;
    _ = ogg;
    _ = opus;
    _ = opus_celt;
    _ = opus_silk;
    _ = vorbis;
    _ = wav;
    _ = webm;
}
