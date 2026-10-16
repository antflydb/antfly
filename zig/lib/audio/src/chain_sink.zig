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

//! Bounded linear normalization between sequential logical audio streams.
const std = @import("std");
/// Normalize each logical chain to the same rate/channel policy as batch
/// decoding. Only the previous source frame and one output block are retained.
pub const ChainSink = struct {
    sink: @import("pcm_sink.zig").Sink,
    source_rate: u32,
    source_channels: u8,
    target_rate: u32,
    target_channels: u8,
    source_frames: usize,
    target_frames: usize,
    previous: []f32,
    output: []f32,
    received: usize = 0,
    emitted: usize = 0,
    buffered: usize = 0,
    fn sample(self: *@This(), frame: []const f32, channel: usize) f32 {
        return if (channel < self.source_channels) frame[channel] else if (self.source_channels == 1) frame[0] else 0;
    }
    fn emit(self: *@This(), a: []const f32, b: []const f32, fraction: f32) !void {
        for (0..self.target_channels) |ch| {
            const x = self.sample(a, ch);
            const y = self.sample(b, ch);
            self.output[self.buffered] = x + (y - x) * fraction;
            self.buffered += 1;
        }
        self.emitted += 1;
        if (self.buffered == self.output.len) try self.flush();
    }
    fn flush(self: *@This()) !void {
        if (self.buffered != 0) try self.sink.push(self.output[0..self.buffered], self.target_rate, self.target_channels);
        self.buffered = 0;
    }
    pub fn write(raw: *anyopaque, pcm: []const f32, rate: u32, channels: u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (rate != self.source_rate or channels != self.source_channels) return error.UnsupportedAudioFormat;
        var offset: usize = 0;
        while (offset < pcm.len) : (offset += channels) {
            const current = pcm[offset..][0..channels];
            while (self.emitted < self.target_frames) {
                const position = (@as(f64, @floatFromInt(self.emitted)) * @as(f64, @floatFromInt(self.source_rate))) / @as(f64, @floatFromInt(self.target_rate));
                if (position > @as(f64, @floatFromInt(self.received))) break;
                const index: usize = @intFromFloat(@floor(position));
                if (index + 1 < self.received) return error.UnsupportedAudioFormat;
                const a = if (index == self.received) current else self.previous;
                const fraction: f32 = @floatCast(position - @floor(position));
                try self.emit(a, current, fraction);
            }
            @memcpy(self.previous, current);
            self.received += 1;
        }
    }
    pub fn finish(self: *@This()) !void {
        if (self.received != self.source_frames) return error.UnsupportedAudioFormat;
        // Upsampling can leave a fractional position beyond the final frame;
        // batch normalization clamps both neighbours to that last frame.
        while (self.emitted < self.target_frames) try self.emit(self.previous, self.previous, 0);
        try self.flush();
    }
};

pub fn resampledFrameCount(source_frames: usize, source_sample_rate: u32, target_sample_rate: u32) usize {
    if (source_frames == 0 or source_sample_rate == 0 or target_sample_rate == 0) return 0;
    const numerator = @as(u128, source_frames) * target_sample_rate + @divFloor(source_sample_rate, 2);
    return @intCast(@max(@as(u128, 1), @divFloor(numerator, source_sample_rate)));
}
