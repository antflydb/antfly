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
    opus: @import("media_opus_seek.zig").Index,
    vorbis: audio.vorbis.WindowIndex,
    webm: audio.webm.WindowIndexFor(@import("media_opus_seek.zig").Index),
    pub const Admission = @import("media_opus_seek.zig").Admission;
    pub fn init(a: std.mem.Allocator, bytes: []const u8, max_frames: usize) !Index {
        return initAdmitted(a, bytes, max_frames, null);
    }
    pub fn initAdmitted(a: std.mem.Allocator, bytes: []const u8, max_frames: usize, admission: ?Admission) !Index {
        const format = audio.detectFormat(bytes) orelse return error.UnsupportedAudioFormat;
        if (format == .ogg or format == .opus) {
            var streams = try audio.ogg.parsePacketStreamsAlloc(a, bytes);
            defer streams.deinit();
            if (streams.streams.len != 1) return error.UnsupportedStreamingAudioProfile;
        }
        return switch (format) {
            .mp3 => .{ .mp3 = try audio.mp3.pure_zig.WindowIndex.init(a, bytes, max_frames) },
            .flac => .{ .flac = try audio.flac.WindowIndex.init(a, bytes, max_frames) },
            .webm => blk: {
                var index = try audio.webm.WindowIndexFor(@import("media_opus_seek.zig").Index).init(a, bytes, max_frames);
                errdefer index.deinit();
                if (index.codec == .opus) try index.codec.opus.admit(admission);
                break :blk .{ .webm = index };
            },
            .ogg, .opus => switch (try audio.ogg.sniffCodec(a, bytes)) {
                .opus => .{ .opus = try @import("media_opus_seek.zig").Index.init(bytes, max_frames, admission) },
                .vorbis => .{ .vorbis = try audio.vorbis.WindowIndex.init(a, bytes, max_frames) },
                else => error.UnsupportedAudioFormat,
            },
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
            .opus => |index| .{ .opus = try index.copy(a) },
            .vorbis => |index| .{ .vorbis = try index.copy(a) },
            .webm => |index| .{ .webm = try index.copy(a) },
        };
    }
    pub fn estimatedBytes(self: Index) usize {
        return switch (self) {
            inline else => |index| index.estimatedBytes(),
        };
    }
    pub fn samples(self: Index) usize {
        return switch (self) {
            .opus => |index| index.samples(),
            inline else => |index| index.samples,
        };
    }
    pub fn rate(self: Index) u32 {
        return switch (self) {
            .mp3 => |index| index.sample_rate,
            .flac => |index| index.info.sample_rate,
            .opus => 48000,
            .vorbis => |index| index.demuxed.headers.identification.sample_rate,
            .webm => |index| index.sample_rate,
        };
    }
};
pub const Reader = struct {
    index: *const Index,
    source: ?*media.source.Source = null,
    bytes: []const u8 = &.{},
    lane: u1 = 0,
    fork_allocator: ?std.mem.Allocator = null,
    control: @import("../execution_control.zig").InferenceExecutionControl = .{},
    pub fn provider(self: *Reader) providers.Provider {
        return .{ .context = self, .samples = self.index.samples(), .sample_rate = self.index.rate(), .read = read, .fork = fork };
    }
    fn fork(raw: *anyopaque, a: std.mem.Allocator) !providers.Fork {
        const self: *Reader = @ptrCast(@alignCast(raw));
        const copy = try a.create(Reader);
        copy.* = self.*;
        copy.lane = 1;
        copy.fork_allocator = a;
        return .{ .provider = copy.provider(), .context = copy, .destroy = destroyFork };
    }
    fn destroyFork(raw: *anyopaque) void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        self.fork_allocator.?.destroy(self);
    }
    fn read(raw: *anyopaque, a: std.mem.Allocator, first: usize, last: usize) !providers.Lease {
        const self: *Reader = @ptrCast(@alignCast(raw));
        const lane = @import("media_opus_seek.zig").Lane.enter(self.lane);
        defer lane.deinit();
        try self.control.check();
        const check = self.control.imageWorkControl();
        const scope = audio.work_control.Scope.enter(.{ .context = check.context, .check_fn = check.check_fn });
        defer scope.deinit();
        if (last < first or last > self.index.samples()) return error.InvalidAudioWindow;
        const samples = try a.alloc(f32, last - first);
        errdefer a.free(samples);
        switch (self.index.*) {
            inline .mp3, .opus, .vorbis, .webm => |*index| {
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
    const paths = [_][]const u8{ "tone.mp3", "mp3-corpus/lsf-short-8000.mp3", "mp3-corpus/lsf-short-24000.mp3", "mp3-corpus/l3-he_free.bit", "codec-corpus/tone-stereo.flac", "codec-corpus/tone-stereo-24bit.flac", "codec-corpus/tone-stereo.opus", "codec-corpus/probe-hybrid-stereo-48k.ogg", "codec-corpus/probe-silk-mono-16k.ogg", "codec-corpus/tone-stereo.oga" };
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

test "benchmark media seek windows and AAC metadata" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_][]const u8{ "tone-stereo.opus", "tone-stereo.oga", "probe-hybrid-stereo-48k.ogg", "webm-opus" }) |path| {
        const name = try std.fmt.allocPrint(a, "../../lib/audio/testdata/codec-corpus/{s}", .{if (std.mem.eql(u8, path, "webm-opus")) "tone-stereo.opus" else path});
        defer a.free(name);
        const encoded = try std.Io.Dir.cwd().readFileAlloc(io, name, a, .limited(16 * 1024 * 1024));
        defer a.free(encoded);
        var webm_bytes: ?[]u8 = null;
        defer if (webm_bytes) |owned| a.free(owned);
        const bytes = if (std.mem.eql(u8, path, "webm-opus")) build: {
            var ogg = try audio.ogg.parsePacketsAlloc(a, encoded);
            defer ogg.deinit();
            const packets = try a.alloc([]const u8, ogg.packets.len - 2);
            defer a.free(packets);
            for (packets, ogg.packets[2..]) |*packet, value| packet.* = value.bytes;
            webm_bytes = try audio.webm.buildOpusTestFileAlloc(a, ogg.packets[0].bytes, packets);
            break :build webm_bytes.?;
        } else encoded;
        const started = std.Io.Clock.awake.now(io).toNanoseconds();
        var index = try Index.init(a, bytes, 1000000);
        defer index.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var cached = try index.copy(arena.allocator());
        defer cached.deinit();
        const cold = std.Io.Clock.awake.now(io).toNanoseconds() - started;
        var times: [2]i128 = undefined;
        var checksums: [2]f64 = @splat(0);
        for (0..2) |mode| {
            const start = std.Io.Clock.awake.now(io).toNanoseconds();
            for (0..30) |page| {
                const first = index.samples() * page / 30;
                const last = index.samples() * (page + 1) / 30;
                if (mode == 0) {
                    const stream = (try providers.Stream.start(a, bytes, .{})).?;
                    defer stream.deinit();
                    const window = try stream.provider().read(stream, a, first, last);
                    for (window.pcm.samples) |value| checksums[mode] += value;
                } else {
                    var reader = Reader{ .index = &cached, .bytes = bytes };
                    var window = try reader.provider().read(&reader, a, first, last);
                    defer window.deinit(a);
                    for (window.pcm.samples) |value| checksums[mode] += value;
                }
            }
            times[mode] = std.Io.Clock.awake.now(io).toNanoseconds() - start;
        }
        try std.testing.expectEqual(checksums[0], checksums[1]);
        std.debug.print("STATEFUL_SEEK file={s} samples={d} pages=30 baseline_ns={d} indexed_ns={d} cold_build_and_copy_ns={d}\n", .{ path, index.samples(), times[0], times[1], cold });
    }
    const Source = @import("media_chunking_audio_source.zig");
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, "../../lib/audio/testdata/codec-corpus/tone-stereo.mp4", a, .limited(16 * 1024 * 1024));
    defer a.free(bytes);
    var source = media.source.Source{ .allocator = a, .identity = "aac", .storage = .{ .borrowed = bytes }, .limits = .{ .max_total_bytes = 1024 * 1024 * 1024 } };
    const cold_start = std.Io.Clock.awake.now(io).toNanoseconds();
    var original = try Source.Mp4.init(a, &source, 1024 * 1024, 100000);
    defer original.deinit();
    const Cache = @import("media_index_cache.zig").Cache;
    const Memory = @import("../runtime/tier/memory.zig");
    const Admission = struct {
        fn acquire(raw: *anyopaque, bytes_: usize) !Memory.AdmissionLease {
            const manager: *Memory.AdmissionController = @ptrCast(@alignCast(raw));
            return manager.tryAcquire(.cpu, .{ .host_limit_bytes = Cache.max_bytes }, .{ .host_scratch_bytes = bytes_ }, false);
        }
    };
    var controller: Memory.AdmissionController = .{};
    var cache: Cache = .{};
    defer cache.deinit();
    cache.remember(a, @splat(1), .{ .aac = original }, .{ .context = &controller, .acquire = Admission.acquire });
    const cold_ns = std.Io.Clock.awake.now(io).toNanoseconds() - cold_start;
    var times: [2]i128 = undefined;
    var checksum: [2]usize = @splat(0);
    for (0..2) |mode| {
        const start = std.Io.Clock.awake.now(io).toNanoseconds();
        for (0..1000) |_| {
            const hit = if (mode == 1) cache.get(@splat(1)).? else null;
            defer if (hit) |entry| cache.release(entry);
            var owner = if (mode == 0) try Source.Mp4.init(a, &source, 1024 * 1024, 100000) else hit.?.data.aac.reader(a, &source);
            defer owner.deinit();
            std.mem.doNotOptimizeAway(&owner);
            checksum[mode] +%= owner.offsets.len + owner.samples + owner.tables.decoder_config.len;
        }
        times[mode] = std.Io.Clock.awake.now(io).toNanoseconds() - start;
    }
    try std.testing.expectEqual(checksum[0], checksum[1]);
    std.debug.print("AAC_INDEX pages=1000 baseline_ns={d} cached_ns={d} cold_build_and_copy_ns={d}\n", .{ times[0], times[1], cold_ns });
}
