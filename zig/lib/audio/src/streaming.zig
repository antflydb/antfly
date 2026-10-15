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

//! Bounded PCM visitors for compressed audio. Packet/configuration metadata is
//! retained; decoded PCM is delivered in codec blocks, including trim/gain.
const std = @import("std");
const audio = @import("mod.zig");
pub const Sink = @import("pcm_sink.zig").Sink;
pub const Info = struct { samples: usize, sample_rate: u32 };
const Mono = struct {
    a: std.mem.Allocator,
    sink: Sink,
    scale: f32 = 1,
    skip: u64 = 0,
    remaining: ?u64 = null,
    fn write(raw: *anyopaque, interleaved: []const f32, rate: u32, channels: u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const frames = interleaved.len / channels;
        const dropped: usize = @intCast(@min(frames, self.skip));
        self.skip -= dropped;
        const count: usize = @intCast(@min(frames - dropped, self.remaining orelse std.math.maxInt(u64)));
        const mono = try self.a.alloc(f32, count);
        defer self.a.free(mono);
        for (mono, 0..) |*out, frame| {
            var sum: f32 = 0;
            for (interleaved[(dropped + frame) * channels ..][0..channels]) |sample| {
                const scaled = sample * self.scale;
                sum += if (std.math.isFinite(scaled)) std.math.clamp(scaled, -1.0, 1.0) else 0;
            }
            out.* = sum / @as(f32, @floatFromInt(channels));
        }
        if (self.remaining) |remaining| self.remaining = remaining - count;
        if (count != 0) try self.sink.push(mono, rate, 1);
    }
    fn consumer(self: *@This()) Sink {
        return .{ .context = self, .write = write };
    }
};
pub fn supported(a: std.mem.Allocator, bytes: []const u8) !bool {
    const format = audio.detectFormat(bytes) orelse return false;
    return switch (format) {
        .mp3, .flac, .aac, .webm, .aiff, .au, .caf => true,
        .ogg, .opus => blk: {
            if (bytes.len < 4 or !std.mem.eql(u8, bytes[0..4], "OggS")) break :blk false;
            const codec = try audio.ogg.sniffCodec(a, bytes);
            var streams = try audio.ogg.parsePacketStreamsAlloc(a, bytes);
            defer streams.deinit();
            break :blk codec == .vorbis or streams.streams.len == 1;
        },
        .mp4 => blk: {
            var demuxed = try audio.mp4.demux(a, bytes);
            defer demuxed.deinit();
            if (demuxed.codec == .alac) break :blk true;
            const cfg = try audio.aac.parseAudioSpecificConfig(demuxed.decoder_config);
            break :blk !cfg.sbr_present and !cfg.ps_present and cfg.object_type != 5 and cfg.object_type != 29;
        },
        else => false,
    };
}
pub fn visit(a: std.mem.Allocator, bytes: []const u8, control: @import("work_control.zig").Control, sink: Sink) !void {
    const scope = @import("work_control.zig").Scope.enter(control);
    defer scope.deinit();
    var mono = Mono{ .a = a, .sink = sink };
    const format = audio.detectFormat(bytes) orelse return error.UnsupportedAudioFormat;
    switch (format) {
        .webm => try audio.webm.visitInterleaved(a, bytes, mono.consumer()),
        .aiff => try audio.aiff.visitInterleaved(a, bytes, mono.consumer()),
        .au => try audio.au.visitInterleaved(a, bytes, mono.consumer()),
        .caf => {
            if (audio.caf.isPcm(bytes)) {
                try audio.caf.visitInterleaved(a, bytes, mono.consumer());
                return;
            }
            var demuxed = try audio.caf.demux(a, bytes);
            defer demuxed.deinit();
            mono.skip = demuxed.priming_frames;
            if (demuxed.valid_frames < @as(u64, demuxed.priming_frames) + demuxed.remainder_frames) return error.UnsupportedAudioFormat;
            mono.remaining = demuxed.valid_frames - demuxed.priming_frames - demuxed.remainder_frames;
            try audio.alac.visitPacketized(a, demuxed.decoder_config, demuxed.access_units, mono.consumer());
            if (mono.skip != 0 or mono.remaining.? != 0) return error.UnsupportedAudioFormat;
        },
        .mp3 => try audio.mp3.pure_zig.visitInterleaved(a, bytes, mono.consumer()),
        .flac => try audio.flac.visitInterleaved(a, bytes, mono.consumer()),
        .aac => {
            mono.scale = 1.0 / 32768.0;
            try audio.aac.visitAdts(a, bytes, mono.consumer());
        },
        .mp4 => {
            var demuxed = try audio.mp4.demux(a, bytes);
            defer demuxed.deinit();
            mono.skip = demuxed.trim_start_frames;
            mono.remaining = demuxed.playable_frames;
            if (demuxed.codec == .alac) try audio.alac.visitPacketized(a, demuxed.decoder_config, demuxed.access_units, mono.consumer()) else {
                mono.scale = 1.0 / 32768.0;
                try audio.aac.visitAccessUnits(a, demuxed.decoder_config, demuxed.access_units, mono.consumer());
            }
            if (mono.skip != 0 or (mono.remaining != null and mono.remaining.? != 0)) return error.UnsupportedAudioFormat;
        },
        .ogg, .opus => {
            switch (try audio.ogg.sniffCodec(a, bytes)) {
                .opus => {
                    var demuxed = try audio.opus.demuxOggAlloc(a, bytes);
                    defer demuxed.deinit();
                    try audio.opus.visitPackets(a, demuxed.packets, demuxed.packet_tocs, demuxed.header.channels, demuxed.header.output_gain_q8, demuxed.header.pre_skip, demuxed.playable_frames, mono.consumer());
                },
                .vorbis => try audio.vorbis.visitOgg(a, bytes, mono.consumer()),
                .flac => {
                    const encoded = try audio.ogg.reconstructFlacStreamAlloc(a, bytes);
                    defer a.free(encoded);
                    try audio.flac.visitInterleaved(a, encoded, mono.consumer());
                },
            }
        },
        else => return error.UnsupportedStreamingAudioProfile,
    }
}
pub fn inspect(a: std.mem.Allocator, bytes: []const u8, control: @import("work_control.zig").Control) !Info {
    const format = audio.detectFormat(bytes) orelse return error.UnsupportedAudioFormat;
    if (format == .flac) {
        const info = try audio.flac.parseStreamInfo(bytes);
        if (info.total_samples != 0) return .{ .samples = std.math.cast(usize, info.total_samples) orelse return error.AudioTooLarge, .sample_rate = info.sample_rate };
    }
    if (format == .mp3) {
        const plans = try audio.mp3.pure_zig.collectFrameDecodePlans(a, bytes);
        defer {
            for (plans) |*plan| plan.deinit(a);
            a.free(plans);
        }
        if (plans.len == 0) return error.UnsupportedAudioFormat;
        var count: usize = 0;
        for (plans) |plan| if (plan.has_complete_main_data and !plan.is_vbr_tag) {
            count = try std.math.add(usize, count, @as(usize, plan.side_info.granule_count) * 576);
        };
        return .{ .samples = count, .sample_rate = plans[0].header.sample_rate };
    }
    // Containers without a trustworthy duration use a bounded validation pass.
    // This retains no PCM, and the execution pass still uses one codec state.
    const Counter = struct {
        info: Info = .{ .samples = 0, .sample_rate = 0 },
        fn write(raw: *anyopaque, samples: []const f32, rate: u32, _: u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.info.sample_rate != 0 and self.info.sample_rate != rate) return error.UnsupportedAudioFormat;
            self.info.sample_rate = rate;
            self.info.samples = try std.math.add(usize, self.info.samples, samples.len);
        }
    };
    var counter = Counter{};
    try visit(a, bytes, control, .{ .context = &counter, .write = Counter.write });
    return counter.info;
}
