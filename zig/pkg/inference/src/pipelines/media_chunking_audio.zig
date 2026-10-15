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

//! Window providers retain one PCM window while preserving original sample clocks.
const std = @import("std");
const audio = @import("audio.zig");
const media = @import("antfly_media");
pub const Lease = struct {
    pcm: audio.PcmAudio,
    owned: ?[]f32 = null,
    pub fn deinit(self: *Lease, a: std.mem.Allocator) void {
        if (self.owned) |samples| a.free(samples);
    }
};
pub const Provider = struct {
    context: *anyopaque,
    samples: usize,
    sample_rate: u32,
    read: *const fn (*anyopaque, std.mem.Allocator, usize, usize) anyerror!Lease,
};
pub const Pcm = struct {
    pcm: audio.PcmAudio,
    pub fn provider(self: *@This()) Provider {
        return .{ .context = self, .samples = self.pcm.samples.len, .sample_rate = self.pcm.sample_rate, .read = read };
    }
    fn read(raw: *anyopaque, _: std.mem.Allocator, first: usize, last: usize) !Lease {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (first > last or last > self.pcm.samples.len) return error.InvalidAudioWindow;
        return .{ .pcm = .{ .samples = self.pcm.samples[first..last], .sample_rate = self.pcm.sample_rate } };
    }
};
pub const Wav = struct {
    parsed: audio.wav.ParsedData,
    pub fn provider(self: *@This()) Provider {
        return .{ .context = self, .samples = self.parsed.data.len / self.frameBytes(), .sample_rate = self.parsed.format.sample_rate, .read = read };
    }
    fn frameBytes(self: @This()) usize {
        return @as(usize, self.parsed.format.bits_per_sample / 8) * self.parsed.channels;
    }
    fn read(raw: *anyopaque, a: std.mem.Allocator, first: usize, last: usize) !Lease {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const bytes = self.frameBytes();
        if (first > last or last > self.parsed.data.len / bytes) return error.InvalidAudioWindow;
        var window = self.parsed;
        window.data = self.parsed.data[first * bytes .. last * bytes];
        const decoded = try audio.wav.decodeMonoParsed(a, window);
        return .{ .pcm = .{ .samples = decoded.samples, .sample_rate = decoded.format.sample_rate }, .owned = decoded.samples };
    }
};
pub const SourceWav = struct {
    layout: audio.wav.SourceLayout,
    source: *media.source.Source,
    pub fn provider(self: *@This()) !Provider {
        return .{ .context = self, .samples = std.math.cast(usize, self.layout.frames()) orelse return error.ResourceLimitExceeded, .sample_rate = self.layout.format.sample_rate, .read = read };
    }
    fn read(raw: *anyopaque, a: std.mem.Allocator, first: usize, last: usize) !Lease {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const samples = try self.layout.readMono(a, self.source, first, last);
        return .{ .pcm = .{ .samples = samples, .sample_rate = self.layout.format.sample_rate }, .owned = samples };
    }
};
