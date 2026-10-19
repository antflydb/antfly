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
/// Demux/setup belongs to one traversal and is reused for actual decoding.
const Prepared = struct {
    shape: Shape,
    length_known: bool = true,
    data: union(enum) { opus: opus.Demuxed, vorbis: vorbis.Demuxed, flac: []u8 },
    fn init(a: std.mem.Allocator, stream: ogg.PacketSequence) !Prepared {
        if (stream.packets.len == 0) return error.UnsupportedAudioFormat;
        switch (try ogg.sniffFirstPacket(stream.packets[0].bytes)) {
            .opus => {
                var d = try opus.demuxOggPacketsAlloc(a, stream.packets);
                errdefer d.deinit();
                return .{ .shape = .{ .rate = 48000, .channels = d.header.channels, .frames = std.math.cast(usize, d.playable_frames) orelse return error.UnsupportedAudioFormat }, .data = .{ .opus = d } };
            },
            .vorbis => {
                var d = try vorbis.demuxPacketsAlloc(a, stream.packets);
                errdefer d.deinit();
                return .{ .shape = .{ .rate = d.headers.identification.sample_rate, .channels = d.headers.identification.channels, .frames = std.math.cast(usize, d.playable_samples) orelse return error.UnsupportedAudioFormat }, .data = .{ .vorbis = d } };
            },
            .flac => {
                const encoded = try ogg.reconstructFlacPacketsAlloc(a, stream.packets);
                errdefer a.free(encoded);
                const info = try flac.parseStreamInfo(encoded);
                var count = std.math.cast(usize, info.total_samples) orelse return error.UnsupportedAudioFormat;
                if (count == 0) {
                    // Ogg FLAC's terminal granule is the exact sample clock even
                    // when STREAMINFO leaves its total unknown. Validate it
                    // against decoded output rather than decoding to count.
                    const last = stream.packets[stream.packets.len - 1];
                    if (last.is_eos and last.granule_applies and last.page_granule_position != std.math.maxInt(u64)) {
                        count = std.math.cast(usize, last.page_granule_position) orelse return error.UnsupportedAudioFormat;
                    }
                }
                return .{ .shape = .{ .rate = info.sample_rate, .channels = info.channels, .frames = count }, .length_known = count != 0, .data = .{ .flac = encoded } };
            },
        }
    }
    fn deinit(self: *Prepared, a: std.mem.Allocator) void {
        switch (self.data) {
            .opus => |*d| d.deinit(),
            .vorbis => |*d| d.deinit(),
            .flac => |bytes| a.free(bytes),
        }
    }
    fn visit(self: Prepared, a: std.mem.Allocator, sink: Sink) !void {
        const Checked = struct {
            counter: Counter = .{},
            sink: Sink,
            fn write(raw: *anyopaque, samples: []const f32, rate: u32, channels: u8) !void {
                const c: *@This() = @ptrCast(@alignCast(raw));
                try Counter.write(&c.counter, samples, rate, channels);
                try c.sink.push(samples, rate, channels);
            }
        };
        var checked = Checked{ .sink = sink };
        const output = Sink{ .context = &checked, .write = Checked.write };
        switch (self.data) {
            .opus => |d| try opus.visitPackets(a, d.packets, d.packet_tocs, d.header.channels, d.header.output_gain_q8, d.header.pre_skip, d.playable_frames, output),
            .vorbis => |d| try vorbis.visitDemuxed(a, d, output),
            .flac => |bytes| try flac.visitInterleaved(a, bytes, output),
        }
        const actual = checked.counter.shape;
        if (actual.rate != self.shape.rate or actual.channels != self.shape.channels or (self.length_known and actual.frames != self.shape.frames)) return error.UnsupportedAudioFormat;
    }
};
pub fn visit(a: std.mem.Allocator, bytes: []const u8, sink: Sink) !void {
    var streams = try ogg.parsePacketStreamsAlloc(a, bytes);
    defer streams.deinit();
    if (streams.streams.len == 0) return error.UnsupportedAudioFormat;
    const prepared = try a.alloc(Prepared, streams.streams.len);
    var initialized: usize = 0;
    defer {
        for (prepared[0..initialized]) |*item| item.deinit(a);
        a.free(prepared);
    }
    var target_rate: u32 = 0;
    var target_channels: u8 = 0;
    for (streams.streams, prepared) |stream, *out| {
        try @import("work_control.zig").check();
        out.* = try Prepared.init(a, stream);
        initialized += 1;
        if (out.shape.rate == 0 or out.shape.channels == 0) return error.UnsupportedAudioFormat;
        target_rate = @max(target_rate, out.shape.rate);
        target_channels = @max(target_channels, out.shape.channels);
    }
    for (prepared) |item| {
        const source = item.shape;
        if (source.rate == target_rate and source.channels == target_channels) {
            try item.visit(a, sink);
            continue;
        }
        const previous = try a.alloc(f32, source.channels);
        defer a.free(previous);
        const output = try a.alloc(f32, @as(usize, target_channels) * 1024);
        defer a.free(output);
        var normalized = normalizer.ChainSink{ .sink = sink, .source_rate = source.rate, .source_channels = source.channels, .target_rate = target_rate, .target_channels = target_channels, .source_frames = source.frames, .unknown_length = !item.length_known, .target_frames = normalizer.resampledFrameCount(source.frames, source.rate, target_rate), .previous = previous, .output = output };
        try item.visit(a, .{ .context = &normalized, .write = normalizer.ChainSink.write });
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
