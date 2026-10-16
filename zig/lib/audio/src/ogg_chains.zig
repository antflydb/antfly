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

//! Logical Ogg chains share a stable PCM clock without retaining decoded audio.
const std = @import("std");
const ogg = @import("ogg.zig");
const opus = @import("opus.zig");
const vorbis = @import("vorbis.zig");
const flac = @import("flac.zig");
const normalizer = @import("chain_sink.zig");
const Sink = @import("pcm_sink.zig").Sink;
const Shape = struct { rate: u32, channels: u8, frames: usize };
const Counter = struct {
    shape: Shape = .{ .rate = 0, .channels = 0, .frames = 0 },
    fn write(raw: *anyopaque, samples: []const f32, rate: u32, channels: u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.shape.rate != 0 and (rate != self.shape.rate or channels != self.shape.channels)) return error.UnsupportedAudioFormat;
        self.shape.rate = rate;
        self.shape.channels = channels;
        self.shape.frames = try std.math.add(usize, self.shape.frames, samples.len / channels);
    }
};
fn shape(a: std.mem.Allocator, stream: ogg.PacketSequence) !Shape {
    if (stream.packets.len == 0) return error.UnsupportedAudioFormat;
    switch (try ogg.sniffFirstPacket(stream.packets[0].bytes)) {
        .opus => {
            var demuxed = try opus.demuxOggPacketsAlloc(a, stream.packets);
            defer demuxed.deinit();
            return .{ .rate = 48000, .channels = demuxed.header.channels, .frames = std.math.cast(usize, demuxed.playable_frames) orelse return error.UnsupportedAudioFormat };
        },
        .vorbis => {
            var demuxed = try vorbis.demuxPacketsAlloc(a, stream.packets);
            defer demuxed.deinit();
            return .{ .rate = demuxed.headers.identification.sample_rate, .channels = demuxed.headers.identification.channels, .frames = std.math.cast(usize, demuxed.playable_samples) orelse return error.UnsupportedAudioFormat };
        },
        .flac => {
            const encoded = try ogg.reconstructFlacPacketsAlloc(a, stream.packets);
            defer a.free(encoded);
            const info = try flac.parseStreamInfo(encoded);
            if (info.total_samples != 0) return .{ .rate = info.sample_rate, .channels = info.channels, .frames = std.math.cast(usize, info.total_samples) orelse return error.UnsupportedAudioFormat };
            var count = Counter{};
            try flac.visitInterleaved(a, encoded, .{ .context = &count, .write = Counter.write });
            return count.shape;
        },
    }
}
fn visitStream(a: std.mem.Allocator, stream: ogg.PacketSequence, sink: Sink) !void {
    switch (try ogg.sniffFirstPacket(stream.packets[0].bytes)) {
        .opus => {
            var demuxed = try opus.demuxOggPacketsAlloc(a, stream.packets);
            defer demuxed.deinit();
            try opus.visitPackets(a, demuxed.packets, demuxed.packet_tocs, demuxed.header.channels, demuxed.header.output_gain_q8, demuxed.header.pre_skip, demuxed.playable_frames, sink);
        },
        .vorbis => {
            var demuxed = try vorbis.demuxPacketsAlloc(a, stream.packets);
            defer demuxed.deinit();
            try vorbis.visitDemuxed(a, demuxed, sink);
        },
        .flac => {
            const encoded = try ogg.reconstructFlacPacketsAlloc(a, stream.packets);
            defer a.free(encoded);
            try flac.visitInterleaved(a, encoded, sink);
        },
    }
}
pub fn visit(a: std.mem.Allocator, bytes: []const u8, sink: Sink) !void {
    var streams = try ogg.parsePacketStreamsAlloc(a, bytes);
    defer streams.deinit();
    if (streams.streams.len == 0) return error.UnsupportedAudioFormat;
    const shapes = try a.alloc(Shape, streams.streams.len);
    defer a.free(shapes);
    var target_rate: u32 = 0;
    var target_channels: u8 = 0;
    for (streams.streams, shapes) |stream, *out| {
        try @import("work_control.zig").check();
        out.* = try shape(a, stream);
        if (out.rate == 0 or out.channels == 0) return error.UnsupportedAudioFormat;
        target_rate = @max(target_rate, out.rate);
        target_channels = @max(target_channels, out.channels);
    }
    for (streams.streams, shapes) |stream, source| {
        if (source.rate == target_rate and source.channels == target_channels) {
            try visitStream(a, stream, sink);
            continue;
        }
        const previous = try a.alloc(f32, source.channels);
        defer a.free(previous);
        const output = try a.alloc(f32, @as(usize, target_channels) * 1024);
        defer a.free(output);
        var normalized = normalizer.ChainSink{ .sink = sink, .source_rate = source.rate, .source_channels = source.channels, .target_rate = target_rate, .target_channels = target_channels, .source_frames = source.frames, .target_frames = normalizer.resampledFrameCount(source.frames, source.rate, target_rate), .previous = previous, .output = output };
        try visitStream(a, stream, .{ .context = &normalized, .write = normalizer.ChainSink.write });
        try normalized.finish();
    }
}
pub const Decoded = struct { samples: []f32, sample_rate: u32, channels: u8, allocator: std.mem.Allocator };
pub fn decode(a: std.mem.Allocator, bytes: []const u8) !Decoded {
    const Collect = struct {
        a: std.mem.Allocator,
        samples: std.ArrayList(f32) = .empty,
        rate: u32 = 0,
        channels: u8 = 0,
        fn write(raw: *anyopaque, samples: []const f32, rate: u32, channels: u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.rate != 0 and (rate != self.rate or channels != self.channels)) return error.UnsupportedAudioFormat;
            self.rate = rate;
            self.channels = channels;
            try self.samples.appendSlice(self.a, samples);
        }
    };
    var collect = Collect{ .a = a };
    defer collect.samples.deinit(a);
    try visit(a, bytes, .{ .context = &collect, .write = Collect.write });
    return .{ .samples = try collect.samples.toOwnedSlice(a), .sample_rate = collect.rate, .channels = collect.channels, .allocator = a };
}
