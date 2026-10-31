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

//! Recognition capabilities are backend facts, shared by dispatch and discovery.
const std = @import("std");
const Options = @import("antfly_documents").types.AudioReadOptions;
pub const Capabilities = struct {
    forced_language: bool,
    detected_language: bool,
    timestamps: bool,
    diarization: bool,
    windowing: bool,
    max_duration_ms: ?u64,

    pub fn validate(self: @This(), options: Options) !void {
        if ((options.language != null and !self.forced_language) or
            ((options.timestamps orelse false) and !self.timestamps) or
            ((options.diarization orelse false) and !self.diarization)) return error.UnsupportedAudioReadCapability;
        if (options.language) |language| if (std.mem.trim(u8, language, " \t\r\n").len == 0) return error.InvalidTranscriptionLanguage;
    }
    pub fn validateDuration(self: @This(), duration_ms: u64) !void {
        if (self.max_duration_ms) |maximum| if (duration_ms > maximum) return error.AudioTooLong;
    }
};
pub fn capabilities(whisper: bool) Capabilities {
    return .{ .forced_language = whisper, .detected_language = whisper, .timestamps = whisper, .diarization = whisper, .windowing = whisper, .max_duration_ms = if (whisper) null else 30_000 };
}

test "audio read backend features reject downgrades and bound only generator duration" {
    const asr = capabilities(true);
    const generator = capabilities(false);
    try asr.validate(.{ .language = "es", .timestamps = true, .diarization = true });
    try asr.validateDuration(3_600_000);
    try generator.validate(.{ .timestamps = false, .diarization = false });
    try generator.validateDuration(30_000);
    try std.testing.expectError(error.AudioTooLong, generator.validateDuration(30_001));
    try std.testing.expectError(error.UnsupportedAudioReadCapability, generator.validate(.{ .timestamps = true }));
    try std.testing.expectError(error.UnsupportedAudioReadCapability, generator.validate(.{ .diarization = true }));
    try std.testing.expectError(error.UnsupportedAudioReadCapability, generator.validate(.{ .language = "en" }));
    try std.testing.expectError(error.InvalidTranscriptionLanguage, asr.validate(.{ .language = " " }));
}
