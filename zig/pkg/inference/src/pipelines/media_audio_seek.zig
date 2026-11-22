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
    flac: @import("media_flac_seek.zig").Index,
    opus: @import("media_opus_seek.zig").Index,
    vorbis: audio.vorbis.WindowIndex,
    webm: audio.webm.WindowIndexFor(@import("media_opus_seek.zig").Index),
    webm_flac: WebmFlac,
    pub const Admission = @import("media_opus_seek.zig").Admission;
    pub fn init(a: std.mem.Allocator, bytes: []const u8, max_frames: usize) !Index {
        return initAdmitted(a, bytes, max_frames, null);
    }
    pub fn initAdmitted(a: std.mem.Allocator, bytes: []const u8, max_frames: usize, admission: ?Admission) !Index {
        const format = audio.detectFormat(bytes) orelse return error.UnsupportedAudioFormat;
        if (format == .ogg or format == .opus) {
            const Progressive = @import("media_opus_seek.zig").Index;
            const owner = try Progressive.create(admission);
            var initial = Progressive{ .owner = owner };
            var release_owner = true;
            defer if (release_owner) initial.deinit();
            const arena = owner.arena.allocator();
            var streams = try audio.ogg.parsePacketStreamsAlloc(arena, bytes);
            defer streams.deinit();
            if (streams.streams.len != 1) return error.UnsupportedStreamingAudioProfile;
            const sequence = &streams.streams[0];
            if (sequence.packets.len == 0) return error.UnsupportedAudioFormat;
            return switch (try audio.ogg.sniffFirstPacket(sequence.packets[0].bytes)) {
                .opus => blk: {
                    owner.demuxed = try audio.opus.takeOggPacketsAlloc(arena, sequence);
                    try Progressive.finish(owner, max_frames);
                    release_owner = false;
                    break :blk .{ .opus = initial };
                },
                .vorbis => blk: {
                    const index = try audio.vorbis.WindowIndex.initPackets(a, sequence.packets, max_frames);
                    // Packet parsing was admitted, but Vorbis owns its independent
                    // metadata and coded bytes after construction.
                    break :blk .{ .vorbis = index };
                },
                else => error.UnsupportedAudioFormat,
            };
        }
        return switch (format) {
            .mp3 => .{ .mp3 = try audio.mp3.pure_zig.WindowIndex.init(a, bytes, max_frames) },
            .flac => .{ .flac = try @import("media_flac_seek.zig").Index.init(bytes, max_frames, admission) },
            .webm => blk: {
                const Progressive = @import("media_opus_seek.zig").Index;
                const owner = try Progressive.create(admission);
                // The returned codec (or retained_owner for Vorbis) takes
                // this reference; failures release all admitted metadata.
                var initial = Progressive{ .owner = owner };
                errdefer initial.deinit();
                var index = try audio.webm.WindowIndexFor(Progressive).initWithBuilder(owner.arena.allocator(), bytes, max_frames, Progressive.Builder{ .owner = owner });
                if (index.codec == .vorbis) index.retained_owner = initial else initial.deinit();
                Progressive.refreshBytes(owner);
                break :blk .{ .webm = index };
            },
            else => error.UnsupportedAudioFormat,
        };
    }
    pub fn initWebmSource(a: std.mem.Allocator, source: *media.source.Source, max_frames: usize, max_bytes: usize, admission: ?Admission) !Index {
        var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = a, .limit = max_bytes };
        const scratch = bounded.allocator();
        const read_limit = @min(source.limits.max_read_bytes, source.limits.max_retained_bytes);
        const directory = media.webm_audio.Directory.init(scratch, source, .{
            .max_packets = max_frames,
            .max_metadata_bytes = @min(max_bytes, read_limit),
            .max_index_bytes = max_bytes,
            .max_packet_bytes = read_limit,
        }) catch |err| return if (err == error.OutOfMemory and bounded.denied) error.AudioTooLarge else err;
        defer directory.deinit();
        if (directory.track.codec == .flac) return .{ .webm_flac = try WebmFlac.init(a, scratch, directory, source, max_frames, admission, max_bytes - bounded.live) };
        const Progressive = @import("media_opus_seek.zig").Index;
        const Webm = audio.webm.WindowIndexFor(Progressive);
        const temporary = Webm.directoryScratchBytes(directory);
        if (temporary >= max_bytes - bounded.live) return error.AudioTooLarge;
        const index_limit = max_bytes - bounded.live - temporary;
        bounded.limit = max_bytes - index_limit;
        const owner = try Progressive.createBounded(admission, index_limit);
        var initial = Progressive{ .owner = owner };
        errdefer initial.deinit();
        var index = Webm.initDirectory(owner.arena.allocator(), scratch, directory, max_frames, Progressive.Builder{ .owner = owner }) catch |err| return if (err == error.OutOfMemory and (bounded.denied or owner.bound.?.denied)) error.AudioTooLarge else err;
        if (index.codec == .vorbis) index.retained_owner = initial else initial.deinit();
        Progressive.refreshBytes(owner);
        return .{ .webm = index };
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
            .webm_flac => |index| .{ .webm_flac = try index.copy(a) },
        };
    }
    pub fn estimatedBytes(self: Index) usize {
        return switch (self) {
            inline else => |index| index.estimatedBytes(),
        };
    }
    pub fn samples(self: Index) usize {
        return switch (self) {
            inline .opus, .flac, .webm_flac => |index| index.samples(),
            inline else => |index| index.samples,
        };
    }
    pub fn rate(self: Index) u32 {
        return switch (self) {
            .mp3 => |index| index.sample_rate,
            .flac => |index| index.rate(),
            .opus => 48000,
            .vorbis => |index| index.demuxed.headers.identification.sample_rate,
            .webm => |index| index.sample_rate,
            .webm_flac => |index| index.flac.rate(),
        };
    }
};
/// A native FLAC byte stream assembled logically from CodecPrivate and
/// packet extents. Only the current request is bound to the underlying source.
const WebmFlac = struct {
    const Flac = @import("media_flac_seek.zig").Index;
    const Extent = struct { logical: u64, physical: u64, size: usize };
    allocator: std.mem.Allocator,
    private: []u8,
    extents: []Extent,
    length: u64,
    leading: usize,
    sample_count: usize,
    flac: Flac,

    fn init(a: std.mem.Allocator, scratch: std.mem.Allocator, directory: media.webm_audio.Directory, input: *media.source.Source, max_frames: usize, admission: ?Index.Admission, remaining: usize) !WebmFlac {
        if (directory.track.codec_private.len +| directory.packets.len *| @sizeOf(Extent) > remaining) return error.AudioTooLarge;
        const private = try a.dupe(u8, directory.track.codec_private);
        errdefer a.free(private);
        const extents = try a.alloc(Extent, directory.packets.len);
        errdefer a.free(extents);
        var length: u64 = private.len;
        for (extents, directory.packets) |*extent, packet| {
            extent.* = .{ .logical = length, .physical = packet.offset, .size = packet.size };
            length = try std.math.add(u64, length, packet.size);
        }
        var map = Mapping{ .input = input, .private = private, .extents = extents };
        var source = map.source(a, length);
        var parsed = try audio.flac.parseStream(private);
        if (parsed.data_offset != private.len) return error.UnsupportedAudioFormat;
        if (parsed.stream_info.total_samples == 0) {
            const packets = media.webm_audio.PacketSource{ .source = input, .packets = directory.packets };
            // FLAC frame numbers provide the duration even when STREAMINFO
            // omits it. Only the last block can contain an unknown tail.
            const first_packet = directory.packets[0];
            var prefix = try input.read(first_packet.offset, @min(16, first_packet.size));
            defer prefix.deinit();
            const first = try audio.flac.frameStart(parsed.stream_info, prefix.bytes);
            var tail = try packets.read(directory.packets.len - 1);
            defer tail.deinit();
            var end = try audio.flac.frameStart(parsed.stream_info, tail.bytes);
            var at: usize = 0;
            while (at < tail.bytes.len) {
                try input.control.check();
                var frame = try audio.flac.decodeFrameAlloc(scratch, parsed.stream_info, tail.bytes[at..]);
                defer frame.deinit();
                at += frame.consumed;
                end = try std.math.add(u64, end, frame.frame_samples);
            }
            if (end <= first) return error.UnsupportedAudioFormat;
            parsed.stream_info.total_samples = end - first;
        }
        var flac = try Flac.initParsed(parsed, std.math.cast(usize, source.length()) orelse return error.ResourceLimitExceeded, max_frames, admission);
        errdefer flac.deinit();
        const rate = flac.rate();
        const offset = frames(directory.packets[0].time_ns, rate) -| frames(directory.track.codec_delay_ns, rate);
        const leading = if (offset > @max(1, rate / 1000)) offset else 0;
        if (leading > @as(usize, rate) * 3600) return error.UnsupportedAudioFormat;
        const count = try std.math.add(usize, flac.samples(), leading);
        const trim = if (directory.discard_padding_ns > 0) frames(@intCast(directory.discard_padding_ns), rate) else 0;
        return .{ .allocator = a, .private = private, .extents = extents, .length = length, .leading = leading, .sample_count = if (trim < count) count - trim else count, .flac = flac };
    }

    fn frames(ns: u64, rate: u32) usize {
        return std.math.cast(usize, @as(u128, ns) * rate / std.time.ns_per_s) orelse std.math.maxInt(usize);
    }

    fn copy(self: WebmFlac, a: std.mem.Allocator) !WebmFlac {
        const private = try a.dupe(u8, self.private);
        errdefer a.free(private);
        const extents = try a.dupe(Extent, self.extents);
        errdefer a.free(extents);
        return .{ .allocator = a, .private = private, .extents = extents, .length = self.length, .leading = self.leading, .sample_count = self.sample_count, .flac = try self.flac.copy(a) };
    }

    fn deinit(self: *WebmFlac) void {
        self.flac.deinit();
        self.allocator.free(self.extents);
        self.allocator.free(self.private);
    }

    fn samples(self: WebmFlac) usize {
        return self.sample_count;
    }

    fn estimatedBytes(self: WebmFlac) usize {
        return self.private.len +| std.mem.sliceAsBytes(self.extents).len +| self.flac.estimatedBytes();
    }

    fn read(self: WebmFlac, a: std.mem.Allocator, first: usize, last: usize, out: []f32, input: *media.source.Source, control: @import("../execution_control.zig").InferenceExecutionControl) !void {
        const silence = @min(last, self.leading) -| first;
        @memset(out[0..silence], 0);
        if (silence == out.len) return;
        var map = Mapping{ .input = input, .private = self.private, .extents = self.extents };
        var source = map.source(a, self.length);
        try self.flac.read(a, first -| self.leading, last - self.leading, out[silence..], &source, &.{}, control);
    }

    const Mapping = struct {
        input: *media.source.Source,
        private: []const u8,
        extents: []const Extent,

        fn source(self: *Mapping, a: std.mem.Allocator, length: u64) media.source.Source {
            return .{
                .allocator = a,
                .identity = self.input.identity,
                .storage = .{ .range = .{ .context = self, .length = length, .read_at = readAt } },
                .limits = .{ .max_total_bytes = std.math.maxInt(u64), .max_reads = std.math.maxInt(usize) },
            };
        }

        fn readAt(raw: *anyopaque, offset: u64, out: []u8, control: media.source.Control) !usize {
            const self: *Mapping = @ptrCast(@alignCast(raw));
            if (offset < self.private.len) {
                const available = self.private[@intCast(offset)..];
                const count = @min(out.len, available.len);
                @memcpy(out[0..count], available[0..count]);
                return count;
            }
            var low: usize = 0;
            var high = self.extents.len;
            while (low < high) {
                const mid = low + (high - low) / 2;
                if (self.extents[mid].logical + self.extents[mid].size <= offset) low = mid + 1 else high = mid;
            }
            if (low == self.extents.len) return 0;
            const extent = self.extents[low];
            const begin: usize = @intCast(offset - extent.logical);
            const size = @min(out.len, extent.size - begin, self.input.limits.max_read_bytes, self.input.limits.max_retained_bytes);
            if (size == 0) return error.ResourceLimitExceeded;
            var lease = try self.input.readControlled(extent.physical + begin, size, control);
            defer lease.deinit();
            @memcpy(out[0..size], lease.bytes);
            return size;
        }
    };
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
                const sink = audio.opus.WindowSink{ .context = &capture, .write = Capture.write };
                if (@hasDecl(@TypeOf(index.*), "visitWindowSource")) {
                    try index.visitWindowSource(a, first, last, sink, self.source);
                } else {
                    try index.visitWindow(a, first, last, sink);
                }
                if (capture.at != samples.len) return error.IncompleteAudioStream;
            },
            .flac => |*index| try index.read(a, first, last, samples, self.source, self.bytes, self.control),
            .webm_flac => |*index| try index.read(a, first, last, samples, self.source orelse return error.UnsupportedAudioFormat, self.control),
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

test "model aware media WebM admission rejects before parsing or copying packets" {
    const Deny = struct {
        calls: usize = 0,
        fn acquire(raw: *anyopaque, _: usize) !@import("../runtime/tier/memory.zig").AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.OutOfMemory;
        }
    };
    var deny: Deny = .{};
    // This has the WebM signature but no valid EBML payload. Admission must
    // reject it before the parser can report its invalid container contents.
    try std.testing.expectError(error.OutOfMemory, Index.initAdmitted(std.testing.allocator, &.{ 0x1a, 0x45, 0xdf, 0xa3, 0xff }, 1000, .{ .context = &deny, .acquire = Deny.acquire }));
    try std.testing.expectEqual(@as(usize, 1), deny.calls);
}

test "model aware media WebM owners roll back every failed admission during construction" {
    const a = std.testing.allocator;
    const audio_bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/codec-corpus/tone-stereo.opus", a, .limited(1024 * 1024));
    defer a.free(audio_bytes);
    var ogg = try audio.ogg.parsePacketsAlloc(a, audio_bytes);
    defer ogg.deinit();
    const packets = try a.alloc([]const u8, ogg.packets.len - 2);
    defer a.free(packets);
    for (packets, ogg.packets[2..]) |*out, packet| out.* = packet.bytes;
    const bytes = try audio.webm.buildOpusTestFileAlloc(a, ogg.packets[0].bytes, packets);
    defer a.free(bytes);
    const memory = @import("../runtime/tier/memory.zig");
    const Quota = struct {
        controller: memory.AdmissionController = .{},
        calls: usize = 0,
        fail_at: ?usize = null,
        fn acquire(raw: *anyopaque, size: usize) !memory.AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.fail_at) |limit| if (self.calls >= limit) return error.OutOfMemory;
            return self.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 64 * 1024 * 1024 }, .{ .host_scratch_bytes = size }, false);
        }
    };
    var quota: Quota = .{};
    var index = try Index.initAdmitted(a, bytes, 100000, .{ .context = &quota, .acquire = Quota.acquire });
    var cloned = try index.copy(a);
    index.deinit();
    const calls = quota.calls;
    cloned.deinit();
    try std.testing.expectEqual(@as(usize, 0), quota.controller.snapshot().hostTotalBytes());
    for (1..calls + 1) |failure| {
        var failed = Quota{ .fail_at = failure };
        try std.testing.expectError(error.OutOfMemory, Index.initAdmitted(a, bytes, 100000, .{ .context = &failed, .acquire = Quota.acquire }));
        try std.testing.expectEqual(@as(usize, 0), failed.controller.snapshot().hostTotalBytes());
    }
}

test "model aware media Ogg admission precedes packet parsing and chained streams fall back" {
    const Denied = struct {
        fn acquire(_: *anyopaque, _: usize) !@import("../runtime/tier/memory.zig").AdmissionLease {
            return error.OutOfMemory;
        }
    };
    var context: u8 = 0;
    try std.testing.expectError(error.OutOfMemory, Index.initAdmitted(std.testing.allocator, "OggS", 100000, .{ .context = &context, .acquire = Denied.acquire }));
    const a = std.testing.allocator;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/codec-corpus/tone-stereo.opus", a, .limited(1024 * 1024));
    defer a.free(bytes);
    const chained = try std.mem.concat(a, u8, &.{ bytes, bytes });
    defer a.free(chained);
    // Use a different logical-stream serial in the second stream.
    var offset = bytes.len;
    while (offset < chained.len) {
        chained[offset + 14] ^= 1;
        const segments = chained[offset + 26];
        var payload: usize = 0;
        for (chained[offset + 27 ..][0..segments]) |size| payload += size;
        offset += 27 + @as(usize, segments) + payload;
    }
    try std.testing.expectError(error.UnsupportedStreamingAudioProfile, Index.init(a, chained, 100000));
}

test "model aware media range WebM audio skips unrelated payload and rebinds cached packet offsets" {
    const a = std.testing.allocator;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/codec-corpus/tone-stereo.opus", a, .limited(1024 * 1024));
    defer a.free(encoded);
    var ogg = try audio.ogg.parsePacketsAlloc(a, encoded);
    defer ogg.deinit();
    const packets = try a.alloc([]const u8, ogg.packets.len - 2);
    defer a.free(packets);
    for (packets, ogg.packets[2..]) |*out, packet| out.* = packet.bytes;
    const webm = try audio.webm.buildOpusTestFileAlloc(a, ogg.packets[0].bytes, packets);
    defer a.free(webm);
    const header = try media.webm_audio.readElementHeader(webm, 0);
    const boundary = header.data_end.?;
    const void_header = [_]u8{ 0xec, 0x30, 0, 0 }; // A one-MiB Void before Segment.
    const Source = struct {
        bytes: []const u8,
        boundary: usize,
        fn read(raw: *anyopaque, offset: u64, out: []u8, _: media.source.Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const start: usize = @intCast(offset);
            if (start >= self.boundary + void_header.len + 1024 * 1024) {
                const logical = start - void_header.len - 1024 * 1024;
                @memcpy(out, self.bytes[logical..][0..out.len]);
                return out.len;
            }
            if (start >= self.boundary + void_header.len) return error.UnexpectedPayloadRead;
            const count = @min(out.len, self.boundary + void_header.len - start);
            const from_file = @min(count, self.boundary -| start);
            @memcpy(out[0..from_file], self.bytes[@min(start, self.boundary)..][0..from_file]);
            if (count > from_file) @memcpy(out[from_file..count], void_header[start + from_file - self.boundary ..][0 .. count - from_file]);
            @memset(out[count..], 0);
            return out.len;
        }
    };
    var context = Source{ .bytes = webm, .boundary = boundary };
    var source = media.source.Source{ .allocator = a, .identity = "ranged-opus", .storage = .{ .range = .{ .context = &context, .read_at = Source.read, .length = webm.len + void_header.len + 1024 * 1024 } }, .limits = .{ .max_total_bytes = webm.len } };

    var track = try @import("media_chunking_audio_source.zig").Track.init(a, &source, .{}, 64 * 1024, 1024);
    defer track.deinit();
    try std.testing.expect(track.bytes == null);
    try std.testing.expect(source.total_bytes < webm.len / 2);
    var baseline = try Index.init(a, webm, 1024);
    defer baseline.deinit();
    try std.testing.expectEqual(baseline.samples(), track.provider().samples);
    var expected_reader = Reader{ .index = &baseline };
    const expected_provider = expected_reader.provider();
    var expected = try expected_provider.read(expected_provider.context, a, 0, 2048);
    defer expected.deinit(a);
    var copied = try track.seek.?.copy(a);
    defer copied.deinit();
    var second = source;
    second.total_bytes = 0;
    var reader = Reader{ .index = &copied, .source = &second };
    const provider = reader.provider();
    var actual = try provider.read(provider.context, a, 0, 2048);
    defer actual.deinit(a);
    try std.testing.expectEqualSlices(f32, expected.pcm.samples, actual.pcm.samples);
    try std.testing.expect(second.total_bytes > 0 and second.total_bytes < webm.len / 2);
    try std.testing.expectEqual(@as(usize, 0), second.retained_bytes);
}

test "model aware media range WebM audio preserves FLAC clocks with absent stream totals" {
    const a = std.testing.allocator;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/codec-corpus/tone-stereo.flac", a, .limited(1024 * 1024));
    defer a.free(encoded);
    var expected = try audio.decode(a, encoded, .{});
    defer expected.deinit();
    for ([_]bool{ false, true }) |unknown_total| {
        const webm = try audio.webm.buildFlacTestFileAlloc(a, encoded, unknown_total);
        defer a.free(webm);
        var source = media.source.Source{ .allocator = a, .identity = "webm-flac", .storage = .{ .borrowed = webm } };
        var index = try Index.initWebmSource(a, &source, 1024, 1024 * 1024, null);
        defer index.deinit();
        try std.testing.expectEqual(expected.sample_rate, index.rate());
        try std.testing.expectEqual(expected.samples.len, index.samples());
        var reader = Reader{ .index = &index, .source = &source };
        const provider = reader.provider();
        var actual = try provider.read(provider.context, a, 512, 2048);
        defer actual.deinit(a);
        try std.testing.expectEqualSlices(f32, expected.samples[512..2048], actual.pcm.samples);
        try std.testing.expectEqual(@as(usize, 0), source.retained_bytes);
    }
}
