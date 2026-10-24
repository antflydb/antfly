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

//! Immutable codec directories with request-local decoding and source control.
const std = @import("std");
const audio = @import("inference_audio");
const media = @import("antfly_media");
const providers = @import("media_chunking_audio.zig");
pub const Index = union(enum) {
    mp3: audio.mp3.pure_zig.WindowIndex,
    flac: audio.flac.WindowIndex,
    pub fn init(a: std.mem.Allocator, bytes: []const u8, max_frames: usize) !Index {
        return switch (audio.detectFormat(bytes) orelse return error.UnsupportedAudioFormat) {
            .mp3 => .{ .mp3 = try audio.mp3.pure_zig.WindowIndex.init(a, bytes, max_frames) },
            .flac => .{ .flac = try audio.flac.WindowIndex.init(a, bytes, max_frames) },
            else => error.UnsupportedAudioFormat,
        };
    }
    pub fn deinit(self: *Index) void {
        switch (self.*) {
            inline else => |*index| index.deinit(),
        }
    }
    pub fn copy(self: Index, a: std.mem.Allocator) !Index {
        return switch (self) {
            .mp3 => |index| .{ .mp3 = try index.copy(a) },
            .flac => |index| .{ .flac = try index.copy(a) },
        };
    }
    pub fn estimatedBytes(self: Index) usize {
        return switch (self) {
            inline else => |index| index.estimatedBytes(),
        };
    }
    pub fn samples(self: Index) usize {
        return switch (self) {
            inline else => |index| index.samples,
        };
    }
    pub fn rate(self: Index) u32 {
        return switch (self) {
            .mp3 => |index| index.sample_rate,
            .flac => |index| index.info.sample_rate,
        };
    }
};
pub const Reader = struct {
    index: *const Index,
    source: ?*media.source.Source = null,
    bytes: []const u8 = &.{},
    control: @import("../execution_control.zig").InferenceExecutionControl = .{},
    pub fn provider(self: *Reader) providers.Provider {
        return .{ .context = self, .samples = self.index.samples(), .sample_rate = self.index.rate(), .read = read };
    }
    fn read(raw: *anyopaque, a: std.mem.Allocator, first: usize, last: usize) !providers.Lease {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.control.check();
        const check = self.control.imageWorkControl();
        const scope = audio.work_control.Scope.enter(.{ .context = check.context, .check_fn = check.check_fn });
        defer scope.deinit();
        if (last < first or last > self.index.samples()) return error.InvalidAudioWindow;
        const samples = try a.alloc(f32, last - first);
        errdefer a.free(samples);
        switch (self.index.*) {
            .mp3 => |*index| {
                const Capture = struct {
                    samples: []f32,
                    at: usize = 0,
                    fn write(ptr: *anyopaque, values: []const f32, _: u32, channels: u8) !void {
                        const capture: *@This() = @ptrCast(@alignCast(ptr));
                        const frames = values.len / channels;
                        if (frames > capture.samples.len - capture.at) return error.InvalidAudioWindow;
                        for (0..frames) |i| capture.samples[capture.at + i] = mono(values[i * channels ..][0..channels]);
                        capture.at += frames;
                    }
                };
                var capture = Capture{ .samples = samples };
                try index.visitWindow(a, first, last, .{ .context = &capture, .write = Capture.write });
                if (capture.at != samples.len) return error.IncompleteAudioStream;
            },
            .flac => |*index| {
                var lo: usize = 0;
                var hi = index.frames.len;
                while (lo < hi) {
                    const mid = lo + (hi - lo) / 2;
                    const frame = index.frames[mid];
                    if (frame.first + frame.samples <= first) lo = mid + 1 else hi = mid;
                }
                var copied: usize = 0;
                for (index.frames[lo..]) |frame| {
                    if (frame.first >= last) break;
                    try self.control.check();
                    const owned = if (self.source != null) try a.alloc(u8, frame.bytes) else null;
                    defer if (owned) |value| a.free(value);
                    const encoded = if (owned) |out| blk: {
                        var at: usize = 0;
                        while (at < out.len) {
                            var lease = try self.source.?.read(frame.offset + at, @min(out.len - at, self.source.?.limits.max_read_bytes));
                            defer lease.deinit();
                            if (lease.bytes.len == 0) return error.UnexpectedEof;
                            @memcpy(out[at..][0..lease.bytes.len], lease.bytes);
                            at += lease.bytes.len;
                        }
                        break :blk out;
                    } else self.bytes[frame.offset..][0..frame.bytes];
                    var decoded = try index.decodeFrame(a, encoded);
                    defer decoded.deinit();
                    if (decoded.consumed != frame.bytes or decoded.frame_samples != frame.samples or decoded.channels != index.info.channels or decoded.sample_rate != index.info.sample_rate) return error.InvalidAudioWindow;
                    const start = first -| frame.first;
                    const stop = @min(frame.samples, last - frame.first);
                    for (start..stop) |i| {
                        samples[copied] = mono(decoded.samples[i * decoded.channels ..][0..decoded.channels]);
                        copied += 1;
                    }
                }
                if (copied != samples.len) return error.IncompleteAudioStream;
            },
        }
        return .{ .pcm = .{ .samples = samples, .sample_rate = self.index.rate() }, .owned = samples };
    }
};
fn mono(values: []const f32) f32 {
    var sum: f32 = 0;
    for (values) |sample| sum += if (std.math.isFinite(sample)) std.math.clamp(sample, -1, 1) else 0;
    return sum / @as(f32, @floatFromInt(values.len));
}

test "model aware media immutable audio windows match complete decoding across MP3 reservoir and FLAC frame boundaries" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const paths = [_][]const u8{ "tone.mp3", "mp3-corpus/lsf-short-8000.mp3", "mp3-corpus/lsf-short-24000.mp3", "mp3-corpus/l3-he_free.bit", "codec-corpus/tone-stereo.flac", "codec-corpus/tone-stereo-24bit.flac" };
    for (paths) |path| {
        const name = try std.fmt.allocPrint(a, "lib/audio/testdata/{s}", .{path});
        defer a.free(name);
        const bytes = std.Io.Dir.cwd().readFileAlloc(io, name, a, .limited(16 * 1024 * 1024)) catch blk: {
            const other = try std.fmt.allocPrint(a, "../../{s}", .{name});
            defer a.free(other);
            break :blk try std.Io.Dir.cwd().readFileAlloc(io, other, a, .limited(16 * 1024 * 1024));
        };
        defer a.free(bytes);
        var index = try Index.init(a, bytes, 1000000);
        defer index.deinit();
        const stream = (try providers.Stream.start(a, bytes, .{})).?;
        defer stream.deinit();
        const full = try stream.provider().read(stream, a, 0, index.samples());
        try stream.finish();
        var reader = Reader{ .index = &index, .bytes = bytes };
        var start: usize = 0;
        while (start < index.samples()) : (start += 997) {
            const last = @min(index.samples(), start + 2049);
            var window = try reader.provider().read(&reader, a, start, last);
            defer window.deinit(a);
            try std.testing.expectEqualSlices(f32, full.pcm.samples[start..last], window.pcm.samples);
        }
    }
}
