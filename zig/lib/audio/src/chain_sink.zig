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
    unknown_length: bool = false,
    pending_valid: bool = false,
    pending_frame: [255]f32 = undefined,
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
    fn emitPending(self: *@This()) !void {
        for (self.pending_frame[0..self.target_channels]) |value| {
            self.output[self.buffered] = value;
            self.buffered += 1;
        }
        self.emitted += 1;
        self.pending_valid = false;
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
            const allowed = if (self.unknown_length) resampledFrameCount(self.received + 1, self.source_rate, self.target_rate) else self.target_frames;
            if (self.pending_valid and self.emitted < allowed) try self.emitPending();
            while (!self.pending_valid and (self.unknown_length or self.emitted < self.target_frames)) {
                const position = (@as(f64, @floatFromInt(self.emitted)) * @as(f64, @floatFromInt(self.source_rate))) / @as(f64, @floatFromInt(self.target_rate));
                if (position > @as(f64, @floatFromInt(self.received))) break;
                const index: usize = @intFromFloat(@floor(position));
                if (index + 1 < self.received) return error.UnsupportedAudioFormat;
                const a = if (index == self.received) current else self.previous;
                const fraction: f32 = @floatCast(position - @floor(position));
                if (self.emitted >= allowed) {
                    // A downsample position can become interpolatable before
                    // rounded output length admits it. Retain one candidate,
                    // never an unbounded source tail or a counting PCM pass.
                    for (0..self.target_channels) |ch| {
                        const x = self.sample(a, ch);
                        const y = self.sample(current, ch);
                        self.pending_frame[ch] = x + (y - x) * fraction;
                    }
                    self.pending_valid = true;
                } else try self.emit(a, current, fraction);
            }
            @memcpy(self.previous, current);
            self.received += 1;
        }
    }
    pub fn finish(self: *@This()) !void {
        if (self.unknown_length) {
            self.source_frames = self.received;
            self.target_frames = resampledFrameCount(self.received, self.source_rate, self.target_rate);
        }
        if (self.received != self.source_frames) return error.UnsupportedAudioFormat;
        if (self.pending_valid and self.emitted < self.target_frames) try self.emitPending();
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

test "unknown-length chain normalization matches known totals across ratios and block boundaries" {
    const Collect = struct {
        a: std.mem.Allocator,
        samples: std.ArrayList(f32) = .empty,
        fn write(raw: *anyopaque, pcm: []const f32, _: u32, _: u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.samples.appendSlice(self.a, pcm);
        }
    };
    const a = std.testing.allocator;
    for ([_][2]u32{ .{ 48000, 8000 }, .{ 8000, 48000 }, .{ 44100, 48000 }, .{ 48000, 44100 }, .{ 48000, 48000 } }) |rates| {
        for (1..65) |frames| {
            var expected = Collect{ .a = a };
            defer expected.samples.deinit(a);
            var actual = Collect{ .a = a };
            defer actual.samples.deinit(a);
            var previous: [2]f32 = undefined;
            var unknown_previous: [2]f32 = undefined;
            var output: [14]f32 = undefined;
            var unknown_output: [14]f32 = undefined;
            var known = ChainSink{ .sink = .{ .context = &expected, .write = Collect.write }, .source_rate = rates[0], .source_channels = 2, .target_rate = rates[1], .target_channels = 2, .source_frames = frames, .target_frames = resampledFrameCount(frames, rates[0], rates[1]), .previous = &previous, .output = &output };
            var unknown = ChainSink{ .sink = .{ .context = &actual, .write = Collect.write }, .source_rate = rates[0], .source_channels = 2, .target_rate = rates[1], .target_channels = 2, .source_frames = 0, .target_frames = 0, .unknown_length = true, .previous = &unknown_previous, .output = &unknown_output };
            for (0..frames) |i| {
                const sample = [_]f32{ @as(f32, @floatFromInt(i % 13)) / 13, -@as(f32, @floatFromInt(i % 7)) / 7 };
                try ChainSink.write(&known, &sample, rates[0], 2);
                try ChainSink.write(&unknown, &sample, rates[0], 2);
            }
            try known.finish();
            try unknown.finish();
            try std.testing.expectEqualSlices(f32, expected.samples.items, actual.samples.items);
        }
    }
}
