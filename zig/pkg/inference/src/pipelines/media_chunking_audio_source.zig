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

//! Source-backed AAC-LC windows. One immutable MP4 audio index supplies bounded
//! coded windows; codec preroll is discarded before consumers borrow the PCM.
const std = @import("std");
const media = @import("antfly_media");
const audio = @import("inference_audio");
const providers = @import("media_chunking_audio.zig");
const container = media.mp4_audio;
pub const Mp4 = struct {
    allocator: std.mem.Allocator,
    source: *media.source.Source,
    metadata: []u8,
    tables: container.TrackTables,
    offsets: []u64,
    samples: usize,
    leading_samples: usize,
    trim: u64,
    frame_samples: usize,

    pub fn init(a: std.mem.Allocator, source: *media.source.Source, max_metadata_bytes: usize, max_packets: usize) !Mp4 {
        if (source.limits.max_read_bytes == 0 or max_packets == 0) return error.ResourceLimitExceeded;
        var box_count: usize = 0;
        var offset: u64 = 0;
        while (offset < source.length()) {
            box_count += 1;
            if (box_count > max_packets) return error.ResourceLimitExceeded;
            var header: [16]u8 = undefined;
            if (source.length() - offset < 8) return error.UnsupportedAudioFormat;
            try readInto(source, offset, header[0..8]);
            var size: u64 = std.mem.readInt(u32, header[0..4], .big);
            var header_size: u64 = 8;
            if (size == 1) {
                if (source.length() - offset < 16) return error.UnsupportedAudioFormat;
                try readInto(source, offset + 8, header[8..16]);
                size = std.mem.readInt(u64, header[8..16], .big);
                header_size = 16;
            } else if (size == 0) size = source.length() - offset;
            if (size < header_size or size > source.length() - offset) return error.UnsupportedAudioFormat;
            if (std.mem.eql(u8, header[4..8], "moov")) {
                const length = std.math.cast(usize, size - header_size) orelse return error.ResourceLimitExceeded;
                if (length > max_metadata_bytes) return error.ResourceLimitExceeded;
                const metadata = try a.alloc(u8, length);
                errdefer a.free(metadata);
                try readInto(source, offset + header_size, metadata);
                var tables = container.TrackTables{};
                errdefer tables.deinit(a);
                try container.parseMoov(a, metadata, &tables);
                if (tables.codec != .aac or tables.sample_rate == 0 or tables.media_timescale != tables.sample_rate or tables.sample_sizes.items.len == 0 or tables.sample_sizes.items.len > max_packets or tables.stsc_entries.items.len == 0) return error.UnsupportedStreamingAudioProfile;
                const config = try audio.aac.parseAudioSpecificConfig(tables.decoder_config);
                if (config.object_type != 2 or config.sbr_present or config.ps_present or config.depends_on_core_coder or config.sample_rate != tables.sample_rate) return error.UnsupportedStreamingAudioProfile;
                const frame_samples: usize = if (config.frame_length_960) 960 else 1024;
                const total = try std.math.mul(usize, tables.sample_sizes.items.len, frame_samples);
                const trim = try container.resolveEditListTrim(tables);
                if (trim.trim_start_frames > total) return error.UnsupportedAudioFormat;
                const playable = trim.playable_frames orelse (total - trim.trim_start_frames);
                if (playable > total - trim.trim_start_frames) return error.UnsupportedAudioFormat;
                var leading: u64 = 0;
                for (tables.edit_entries.items) |entry| {
                    if (entry.media_time != -1) break;
                    leading = try std.math.add(u64, leading, try container.scaleDurationToMediaFrames(entry.segment_duration, tables.movie_timescale, tables.sample_rate));
                }
                const offsets = try a.alloc(u64, tables.sample_sizes.items.len);
                errdefer a.free(offsets);
                var packet: usize = 0;
                for (tables.chunk_offsets.items, 0..) |chunk_offset, chunk| {
                    const count = container.samplesPerChunkForIndex(tables.stsc_entries.items, @intCast(chunk + 1));
                    var position = chunk_offset;
                    for (0..count) |_| {
                        if (packet >= offsets.len) return error.UnsupportedAudioFormat;
                        const bytes = tables.sample_sizes.items[packet];
                        if (bytes == 0 or position > source.length() or bytes > source.length() - position) return error.UnsupportedAudioFormat;
                        offsets[packet] = position;
                        position = try std.math.add(u64, position, bytes);
                        packet += 1;
                    }
                }
                if (packet != offsets.len) return error.UnsupportedAudioFormat;
                return .{ .allocator = a, .source = source, .metadata = metadata, .tables = tables, .offsets = offsets, .samples = std.math.cast(usize, try std.math.add(u64, leading, playable)) orelse return error.ResourceLimitExceeded, .leading_samples = std.math.cast(usize, leading) orelse return error.ResourceLimitExceeded, .trim = trim.trim_start_frames, .frame_samples = frame_samples };
            }
            offset += size;
        }
        return error.UnsupportedAudioFormat;
    }
    pub fn deinit(self: *Mp4) void {
        self.tables.deinit(self.allocator);
        self.allocator.free(self.offsets);
        self.allocator.free(self.metadata);
    }
    pub fn provider(self: *Mp4) providers.Provider {
        return .{ .context = self, .samples = self.samples, .sample_rate = self.tables.sample_rate, .read = read };
    }
    fn read(raw: *anyopaque, a: std.mem.Allocator, first: usize, requested_last: usize) anyerror!providers.Lease {
        const self: *Mp4 = @ptrCast(@alignCast(raw));
        try self.source.control.check();
        if (requested_last < first) return error.InvalidAudioWindow;
        const last = @min(requested_last, self.samples);
        if (first >= last) return .{ .pcm = .{ .samples = &.{}, .sample_rate = self.tables.sample_rate } };
        const samples = try a.alloc(f32, last - first);
        errdefer a.free(samples);
        @memset(samples, 0);
        if (last <= self.leading_samples) return .{ .pcm = .{ .samples = samples, .sample_rate = self.tables.sample_rate }, .owned = samples };
        const raw_first = try std.math.add(u64, first -| self.leading_samples, self.trim);
        const raw_last = try std.math.add(u64, last - self.leading_samples, self.trim);
        const first_packet: usize = @intCast((raw_first / self.frame_samples) -| 2);
        const stop_packet: usize = @intCast(@min(self.offsets.len, (raw_last + self.frame_samples - 1) / self.frame_samples));
        const units = try a.alloc([]const u8, stop_packet - first_packet);
        defer a.free(units);
        var initialized: usize = 0;
        defer for (units[0..initialized]) |unit| a.free(unit);
        for (units, first_packet..) |*unit, index| {
            const bytes = try a.alloc(u8, self.tables.sample_sizes.items[index]);
            errdefer a.free(bytes);
            try readInto(self.source, self.offsets[index], bytes);
            unit.* = bytes;
            initialized += 1;
        }
        try audio.aac.validateWindowAccessUnits(a, self.tables.decoder_config, units);
        const Capture = struct {
            source: *media.source.Source,
            output: []f32,
            first: u64,
            position: u64 = 0,
            rate: u32,
            filled: usize = 0,
            fn write(ptr: *anyopaque, values: []const f32, rate: u32, channels: u8) anyerror!void {
                const capture: *@This() = @ptrCast(@alignCast(ptr));
                try capture.source.control.check();
                if (rate != capture.rate or channels == 0 or values.len % channels != 0) return error.UnsupportedStreamingAudioProfile;
                const frames = values.len / channels;
                const skip: usize = @intCast(@min(frames, capture.first -| capture.position));
                const count = @min(frames - skip, capture.output.len - capture.filled);
                for (0..count) |i| {
                    var sum: f32 = 0;
                    for (values[(skip + i) * channels ..][0..channels]) |value| sum += std.math.clamp(value / 32768, -1, 1);
                    capture.output[capture.filled + i] = sum / @as(f32, @floatFromInt(channels));
                }
                capture.filled += count;
                capture.position += frames;
            }
        };
        const silent = @min(samples.len, self.leading_samples -| first);
        var capture = Capture{ .source = self.source, .output = samples[silent..], .first = raw_first - first_packet * self.frame_samples, .rate = self.tables.sample_rate };
        try audio.aac.visitAccessUnits(a, self.tables.decoder_config, units, .{ .context = &capture, .write = Capture.write });
        if (capture.filled != capture.output.len) return error.IncompleteAudioStream;
        return .{ .pcm = .{ .samples = samples, .sample_rate = self.tables.sample_rate }, .owned = samples };
    }
};
fn readInto(source: *media.source.Source, offset: u64, out: []u8) !void {
    var position: usize = 0;
    while (position < out.len) {
        var lease = try source.read(offset + position, @min(out.len - position, source.limits.max_read_bytes));
        defer lease.deinit();
        if (lease.bytes.len == 0) return error.UnexpectedEof;
        @memcpy(out[position..][0..lease.bytes.len], lease.bytes);
        position += lease.bytes.len;
    }
}

/// Codec adapters share one borrowed PCM provider. Source-backed MP4 AAC-LC
/// uses bounded seeks; other codec/container profiles use a bounded input spool
/// and a forward codec-block queue, never a full decoded PCM copy.
pub const Track = struct {
    allocator: std.mem.Allocator,
    mp4: ?Mp4 = null,
    stream: ?*providers.Stream = null,
    bytes: ?[]u8 = null,
    decoded: ?audio.Audio = null,
    pcm: providers.Pcm = undefined,
    pub fn init(a: std.mem.Allocator, source: *media.source.Source, control: @import("../execution_control.zig").InferenceExecutionControl, max_bytes: usize, max_packets: usize) !Track {
        var self = Track{ .allocator = a };
        errdefer self.deinit();
        var prefix: [4]u8 = undefined;
        if (source.length() < 4) return error.UnsupportedAudioFormat;
        try readInto(source, 0, &prefix);
        if (!std.mem.eql(u8, &prefix, "\x1a\x45\xdf\xa3")) {
            self.mp4 = Mp4.init(a, source, @min(64 * 1024 * 1024, max_bytes), max_packets) catch |err| switch (err) {
                error.UnsupportedAudioFormat, error.UnsupportedStreamingAudioProfile => null,
                else => return err,
            };
            if (self.mp4 != null) return self;
        }
        const length = std.math.cast(usize, source.length()) orelse return error.AudioTooLarge;
        if (length > max_bytes) return error.AudioTooLarge;
        self.bytes = try a.alloc(u8, length);
        try readInto(source, 0, self.bytes.?);
        self.stream = try providers.Stream.start(a, self.bytes.?, control);
        if (self.stream == null) {
            const check = control.imageWorkControl();
            self.decoded = try audio.decodeBounded(a, self.bytes.?, .{ .control = .{ .context = check.context, .check_fn = check.check_fn } }, max_bytes);
        }
        return self;
    }
    pub fn deinit(self: *Track) void {
        if (self.mp4) |*owner| owner.deinit();
        if (self.stream) |owner| owner.deinit();
        if (self.decoded) |*owner| owner.deinit();
        if (self.bytes) |bytes| self.allocator.free(bytes);
    }
    pub fn provider(self: *Track) providers.Provider {
        if (self.mp4) |*owner| return owner.provider();
        if (self.stream) |owner| return owner.provider();
        self.pcm = .{ .pcm = .{ .samples = self.decoded.?.samples, .sample_rate = self.decoded.?.sample_rate } };
        return self.pcm.provider();
    }
};

test "model aware media MP4 audio windows preserve complete PCM clocks across bounded seeks" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const bytes = std.Io.Dir.cwd().readFileAlloc(io, "lib/audio/testdata/codec-corpus/tone-stereo.mp4", a, .limited(16 * 1024 * 1024)) catch try std.Io.Dir.cwd().readFileAlloc(io, "../../lib/audio/testdata/codec-corpus/tone-stereo.mp4", a, .limited(16 * 1024 * 1024));
    defer a.free(bytes);
    var reference = try audio.decode(a, bytes, .{});
    defer reference.deinit();
    var source = media.source.Source{ .allocator = a, .identity = "fixture", .storage = .{ .borrowed = bytes }, .limits = .{ .max_read_bytes = 4096 } };
    var owner = try Mp4.init(a, &source, 1024 * 1024, 100000);
    defer owner.deinit();
    try std.testing.expectEqual(reference.sample_rate, owner.tables.sample_rate);
    try std.testing.expectEqual(reference.samples.len, owner.samples - owner.leading_samples);
    const provider = owner.provider();
    for ([_]usize{ 0, @min(3 * owner.frame_samples + 127, reference.samples.len / 2), reference.samples.len - @min(1000, reference.samples.len) }) |start| {
        const stop = @min(reference.samples.len, start + 2048);
        var lease = try provider.read(provider.context, a, owner.leading_samples + start, owner.leading_samples + stop);
        defer lease.deinit(a);
        try std.testing.expectEqual(stop - start, lease.pcm.samples.len);
        for (reference.samples[start..stop], lease.pcm.samples) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 0.00001);
    }
    var track = try Track.init(a, &source, .{ .io = io }, 4 * 1024 * 1024, 100000);
    defer track.deinit();
    try std.testing.expect(track.mp4 != null and track.bytes == null and track.stream == null);
}
