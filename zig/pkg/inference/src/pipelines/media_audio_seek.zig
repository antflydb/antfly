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
        const coded_extent_limit = std.math.cast(usize, source.length()) orelse std.math.maxInt(usize);
        const directory = media.webm_audio.Directory.initWithFlacPacketLimit(scratch, source, .{
            .max_packets = max_frames,
            .max_metadata_bytes = @min(max_bytes, read_limit),
            .max_index_bytes = max_bytes,
            .max_packet_bytes = read_limit,
        }, coded_extent_limit) catch |err| return if (err == error.OutOfMemory and bounded.denied) error.AudioTooLarge else err;
        defer directory.deinit();
        return initWebmDirectory(a, directory, source, max_frames, max_bytes, bounded.live, admission);
    }

    pub fn initWebmDirectory(a: std.mem.Allocator, directory: media.webm_audio.Directory, source: *media.source.Source, max_frames: usize, max_bytes: usize, directory_bytes: usize, admission: ?Admission) !Index {
        if (directory.track.codec == .flac) return .{ .webm_flac = try WebmFlac.init(a, directory, source, max_bytes, directory_bytes, admission) };
        const OpusSeek = @import("media_opus_seek.zig");
        if (directory.track.codec == .opus and directory.track.seek_pre_roll_ns > OpusSeek.max_webm_preroll_ns) return error.UnsupportedStreamingAudioProfile;
        const Progressive = OpusSeek.Index;
        const Webm = audio.webm.WindowIndexFor(Progressive);
        const temporary = Webm.directoryScratchBytes(directory);
        const available = max_bytes -| directory_bytes;
        if (temporary >= available) return error.AudioTooLarge;
        const index_limit = available - temporary;
        var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = a, .limit = temporary };
        const scratch = bounded.allocator();
        const owner = try Progressive.createBounded(admission, index_limit);
        var initial = Progressive{ .owner = owner };
        errdefer initial.deinit();
        var index = Webm.initDirectory(owner.arena.allocator(), scratch, directory, max_frames, Progressive.Builder{ .owner = owner, .range_backed = true }) catch |err| return if (err == error.OutOfMemory and (bounded.denied or owner.bound.?.denied)) error.AudioTooLarge else err;
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
            .webm_flac => |index| index.info.sample_rate,
        };
    }
};
/// Each FLAC Block starts on a native frame boundary. Retain its sample
/// coordinate so a cold seek can decode independent frames in that block.
const WebmFlac = struct {
    const Extent = struct { packet: media.webm_audio.Packet, first: usize };
    const Gap = struct { raw: usize, first: usize, count: usize, inserted_before: usize };
    const Cut = struct { first: usize, end: usize, removed_before: usize };
    const FramePoint = struct { block: usize, sample: usize, offset: usize };
    const FrameDirectory = struct {
        refs: std.atomic.Value(usize) = .init(1),
        mutex: std.atomic.Mutex = .unlocked,
        lease: ?@import("../runtime/tier/memory.zig").AdmissionLease,
        points: [128]?FramePoint = @splat(null),
        next: usize = 0,
        final_raw_samples: std.atomic.Value(usize) = .init(0),
        generation: std.atomic.Value(usize) = .init(0),

        fn create(admission: ?Index.Admission) !*FrameDirectory {
            var lease = if (admission) |quota| try quota.acquire(quota.context, @sizeOf(FrameDirectory)) else null;
            errdefer if (lease) |*owned| owned.release();
            const self = try @import("antfly_platform").allocator.concurrentFallback().create(FrameDirectory);
            self.* = .{ .lease = lease };
            return self;
        }

        fn release(self: *FrameDirectory) void {
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            if (self.lease) |*lease| lease.release();
            @import("antfly_platform").allocator.concurrentFallback().destroy(self);
        }

        fn seek(self: *FrameDirectory, block: usize, sample: usize, control: @import("../execution_control.zig").InferenceExecutionControl) !?FramePoint {
            try control.lockWithPoll(&self.mutex, 100 * std.time.ns_per_us);
            defer self.mutex.unlock();
            var best: ?FramePoint = null;
            for (self.points) |maybe| if (maybe) |point| {
                if (point.block == block and point.sample <= sample and (best == null or best.?.sample < point.sample)) best = point;
            };
            return best;
        }

        fn remember(self: *FrameDirectory, point: FramePoint, control: @import("../execution_control.zig").InferenceExecutionControl) !void {
            try control.lockWithPoll(&self.mutex, 100 * std.time.ns_per_us);
            defer self.mutex.unlock();
            for (self.points) |maybe| if (maybe) |known| {
                if (known.block == point.block and known.offset == point.offset) return;
            };
            self.points[self.next] = point;
            self.next = (self.next + 1) % self.points.len;
            _ = self.generation.fetchAdd(1, .release);
        }
    };
    allocator: std.mem.Allocator,
    info: audio.flac.StreamInfo,
    extents: []Extent,
    leading: usize,
    gaps: []Gap,
    sample_count: usize,
    cuts: []Cut,
    tail_trim: usize,
    tail_group_start: usize,
    working_bytes: usize,
    frame_directory: *FrameDirectory,

    fn init(a: std.mem.Allocator, directory: media.webm_audio.Directory, input: *media.source.Source, budget: usize, temporary: usize, admission: ?Index.Admission) !WebmFlac {
        const retained = directory.packets.len *| @sizeOf(Extent) +| directory.padding.len *| @sizeOf(Cut) +| @sizeOf(FrameDirectory);
        if (retained > budget -| temporary) return error.AudioTooLarge;
        const parsed = try audio.flac.parseStream(directory.track.codec_private);
        if (parsed.data_offset != directory.track.codec_private.len) return error.UnsupportedAudioFormat;
        const info = parsed.stream_info;
        const extents = try a.alloc(Extent, directory.packets.len);
        errdefer a.free(extents);
        var origin: u64 = 0;
        for (extents, directory.packets, 0..) |*extent, packet, index| {
            try input.checkControl();
            const sample = try audio.flac.frameStart(info, packet.prefix[0..@min(packet.prefix.len, packet.size)]);
            if (index == 0) origin = sample;
            const start = std.math.cast(usize, std.math.sub(u64, sample, origin) catch return error.UnsupportedAudioFormat) orelse return error.ResourceLimitExceeded;
            if (index != 0 and start <= extents[index - 1].first) return error.UnsupportedAudioFormat;
            extent.* = .{ .packet = packet, .first = start };
        }
        const frame_directory = try FrameDirectory.create(admission);
        errdefer frame_directory.release();
        const tail = extents[extents.len - 1];
        const declared = std.math.cast(usize, info.total_samples) orelse return error.ResourceLimitExceeded;
        if (declared != 0 and declared <= tail.first) return error.UnsupportedAudioFormat;
        // Each frame consumes at least one byte and contributes at most a
        // u16 block size. This is a planning ceiling until the final read.
        const ceiling = if (declared == 0) try std.math.add(usize, tail.first, try std.math.mul(usize, tail.packet.size, std.math.maxInt(u16))) else declared;
        const rate = info.sample_rate;
        const delay = frames(directory.track.codec_delay_ns, rate);
        const leading = media.webm_audio.playbackGap(directory.packets[0].time_ns, delay, rate, 0);
        if (leading > @as(usize, rate) * 3600) return error.UnsupportedAudioFormat;
        const count = try std.math.add(usize, ceiling, leading);
        const cuts = try a.alloc(Cut, directory.padding.len);
        errdefer a.free(cuts);
        var cut_count: usize = 0;
        var removed: usize = 0;
        var tail_trim: usize = 0;
        var tail_group_start: usize = 0;
        for (directory.padding) |padding| {
            const start = extents[padding.first].first;
            const end = if (padding.end < extents.len) extents[padding.end].first else ceiling;
            const drop = @min(end - start, frames(@abs(padding.ns), rate));
            if (drop == 0) continue;
            if (padding.ns > 0 and padding.end == extents.len and declared == 0) {
                tail_trim = drop;
                tail_group_start = start;
                continue;
            }
            const first = if (padding.ns < 0) start else end - drop;
            cuts[cut_count] = .{ .first = first, .end = first + drop, .removed_before = removed };
            cut_count += 1;
            removed += drop;
        }
        var gaps: std.ArrayList(Gap) = .empty;
        defer gaps.deinit(a);
        var inserted: usize = 0;
        if (directory.padding.len != 0 or for (extents[1..]) |extent| {
            if (extent.packet.time_ns > extents[0].packet.time_ns) break true;
        } else false) {
            var packet: usize = 0;
            var padding_index: usize = 0;
            var timeline = leading;
            const gap_limit = (budget - retained - temporary) / @sizeOf(Gap);
            while (packet < extents.len) {
                const group = if (padding_index < directory.padding.len and directory.padding[padding_index].first == packet) directory.padding[padding_index] else null;
                const next_packet = if (group) |block| block.end else packet + 1;
                const raw = extents[packet].first;
                const end = if (next_packet < extents.len) extents[next_packet].first else ceiling;
                const gap = media.webm_audio.playbackGap(extents[packet].packet.time_ns, delay, rate, timeline);
                if (gap != 0) {
                    if (gaps.items.len == gap_limit) return error.AudioTooLarge;
                    if (gaps.items.len == gaps.capacity) try gaps.ensureTotalCapacityPrecise(a, @min(gap_limit, @max(1, gaps.capacity *| 2)));
                    gaps.appendAssumeCapacity(.{ .raw = raw, .first = timeline, .count = gap, .inserted_before = inserted });
                    inserted = try std.math.add(usize, inserted, gap);
                    if (inserted +| leading > @as(usize, rate) * 3600) return error.UnsupportedAudioFormat;
                    timeline += gap;
                }
                const trimmed = if (group) |block| @min(end - raw, frames(@abs(block.ns), rate)) else 0;
                if (group != null) padding_index += 1;
                timeline = try std.math.add(usize, timeline, end - raw - trimmed);
                packet = next_packet;
            }
        }
        const samples_with_gaps = try std.math.add(usize, count - removed, inserted);
        const gap_bytes = gaps.items.len * @sizeOf(Gap);
        const owned_gaps = try gaps.toOwnedSlice(a);
        errdefer a.free(owned_gaps);
        const owned_cuts = try a.realloc(cuts, cut_count);
        return .{ .allocator = a, .info = info, .extents = extents, .leading = leading, .gaps = owned_gaps, .sample_count = samples_with_gaps, .cuts = owned_cuts, .tail_trim = tail_trim, .tail_group_start = tail_group_start, .working_bytes = budget - retained - gap_bytes, .frame_directory = frame_directory };
    }

    fn rawAt(self: WebmFlac, playback: usize, comptime start: bool) usize {
        var low: usize = 0;
        var high = self.cuts.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            const coordinate = self.cuts[middle].first - self.cuts[middle].removed_before;
            if (coordinate < playback or (start and coordinate == playback)) low = middle + 1 else high = middle;
        }
        if (low == 0) return playback;
        const cut = self.cuts[low - 1];
        return playback +| (cut.removed_before + (cut.end - cut.first));
    }

    fn afterCut(self: WebmFlac, raw: usize) usize {
        var low: usize = 0;
        var high = self.cuts.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            if (self.cuts[middle].end <= raw) low = middle + 1 else high = middle;
        }
        return low;
    }

    fn playbackAt(self: WebmFlac, raw: usize) usize {
        const next = self.afterCut(raw);
        if (next < self.cuts.len) {
            const cut = self.cuts[next];
            return raw - cut.removed_before - (raw -| cut.first);
        }
        if (next == 0) return raw;
        const cut = self.cuts[next - 1];
        return raw - cut.removed_before - (cut.end - cut.first);
    }

    fn gapAfter(self: WebmFlac, sample: usize) usize {
        var low: usize = 0;
        var high = self.gaps.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            const gap = self.gaps[middle];
            if (gap.first + gap.count <= sample) low = middle + 1 else high = middle;
        }
        return low;
    }

    fn compactAt(self: WebmFlac, sample: usize) usize {
        const next = self.gapAfter(sample);
        if (next < self.gaps.len) {
            const gap = self.gaps[next];
            return sample -| self.leading -| gap.inserted_before -| (sample -| gap.first);
        }
        if (next == 0) return sample -| self.leading;
        const gap = self.gaps[next - 1];
        return sample -| self.leading -| (gap.inserted_before + gap.count);
    }

    fn presentationAt(self: WebmFlac, raw: usize) usize {
        var low: usize = 0;
        var high = self.gaps.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            if (self.gaps[middle].raw <= raw) low = middle + 1 else high = middle;
        }
        const inserted = if (low != 0) self.gaps[low - 1].inserted_before + self.gaps[low - 1].count else 0;
        return self.playbackAt(raw) + self.leading + inserted;
    }

    fn probeStop(self: WebmFlac, sample: usize) usize {
        const raw = self.rawAt(self.compactAt(sample), false);
        if (sample < self.leading) return raw;
        const next_gap = self.gapAfter(sample);
        // A window ending in known silence cannot end at EOF, even when
        // that silence maps to the first raw sample of a fully padded tail.
        if (next_gap < self.gaps.len and sample >= self.gaps[next_gap].first) return raw;
        if (self.tail_trim != 0 and raw >= self.tail_group_start) {
            // At the first sample of a positive final trim, EOF is
            // ambiguous until the group proves it has a surviving sample.
            return raw +| self.tail_trim +| @as(usize, @intFromBool(raw == self.tail_group_start));
        }
        if (self.cuts.len != 0) {
            const cut = self.cuts[self.cuts.len - 1];
            const tail = self.extents[self.extents.len - 1].first;
            if (raw == cut.first and tail < cut.end) return cut.end +| 1;
        }
        return raw;
    }

    fn frames(ns: u64, rate: u32) usize {
        return media.webm_audio.sampleCount(ns, rate);
    }

    fn copy(self: WebmFlac, a: std.mem.Allocator) !WebmFlac {
        var result = self;
        result.allocator = a;
        result.extents = try a.dupe(Extent, self.extents);
        errdefer a.free(result.extents);
        result.cuts = try a.dupe(Cut, self.cuts);
        errdefer a.free(result.cuts);
        result.gaps = try a.dupe(Gap, self.gaps);
        _ = self.frame_directory.refs.fetchAdd(1, .monotonic);
        return result;
    }

    fn deinit(self: *WebmFlac) void {
        self.frame_directory.release();
        self.allocator.free(self.extents);
        self.allocator.free(self.cuts);
        self.allocator.free(self.gaps);
    }

    fn rawSamples(self: WebmFlac) ?usize {
        const count = if (self.info.total_samples != 0) @as(usize, @intCast(self.info.total_samples)) else self.frame_directory.final_raw_samples.load(.acquire);
        return if (count == 0) null else count;
    }

    fn finalSamples(self: WebmFlac) ?usize {
        const raw = self.rawSamples() orelse return null;
        return self.presentationAt(raw - @min(raw -| self.tail_group_start, self.tail_trim));
    }

    fn samples(self: WebmFlac) usize {
        return self.finalSamples() orelse self.sample_count;
    }

    fn estimatedBytes(self: WebmFlac) usize {
        return std.mem.sliceAsBytes(self.extents).len +| std.mem.sliceAsBytes(self.cuts).len +| std.mem.sliceAsBytes(self.gaps).len +| @sizeOf(FrameDirectory);
    }

    fn read(self: WebmFlac, a: std.mem.Allocator, first: usize, last: usize, lookahead: usize, out: []f32, persisted: ?*const FramePlan.Snapshot, capture: ?*providers.FrameCheckpointSession, input: *media.source.Source, control: @import("../execution_control.zig").InferenceExecutionControl) !usize {
        if (capture) |session| if (out.len != 0) session.reset();
        const pcm_bytes = std.mem.sliceAsBytes(out).len;
        if (pcm_bytes > self.working_bytes) return error.AudioTooLarge;
        var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = a, .limit = self.working_bytes - pcm_bytes };
        const scratch = bounded.allocator();
        const last_clipped = if (self.finalSamples()) |final| @max(first, @min(last, final)) else last;
        if (last_clipped == first and lookahead <= first) return 0;
        const silence = @min(last_clipped, self.leading) -| first;
        @memset(out[0..silence], 0);
        if (lookahead <= last and silence == last_clipped - first and (self.tail_trim == 0 or self.rawSamples() != null)) return silence;
        const begin = self.rawAt(self.compactAt(first), true);
        const end = self.rawAt(self.compactAt(last_clipped), false);
        const raw_samples = self.rawSamples();
        const stop_at = if (raw_samples == null) self.probeStop(@max(last_clipped, lookahead)) else end;
        var cut_index = self.afterCut(begin);
        var low: usize = 0;
        var high = self.extents.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            if (self.extents[middle].first <= begin) low = middle + 1 else high = middle;
        }
        var copied = silence;
        for (self.gaps[self.gapAfter(first)..]) |gap| {
            if (gap.first >= last_clipped) break;
            const from = @max(first, gap.first) - first;
            const to = @min(last_clipped, gap.first + gap.count) - first;
            @memset(out[from..to], 0);
            copied += to - from;
        }
        var index = low -| 1;
        const work = control.imageWorkControl();
        while (index < self.extents.len and self.extents[index].first < stop_at) : (index += 1) {
            const extent = self.extents[index];
            const stop = if (index + 1 < self.extents.len) self.extents[index + 1].first else raw_samples;
            if (cut_index < self.cuts.len) {
                while (cut_index < self.cuts.len and self.cuts[cut_index].end <= extent.first) cut_index += 1;
                if (index + 1 < self.extents.len and cut_index < self.cuts.len and self.cuts[cut_index].first <= extent.first and self.cuts[cut_index].end >= stop.?) continue;
            }
            var point: ?FramePoint = null;
            if (extent.first < begin) {
                point = try self.frame_directory.seek(index, begin, control);
                if (persisted) |snapshot| if (snapshot.nearest(begin)) |anchor| {
                    const offset: u64 = @intCast(anchor.offset_ns);
                    if (offset >= extent.packet.offset and offset - extent.packet.offset < extent.packet.size and anchor.ordinal >= extent.first and (point == null or anchor.ordinal > point.?.sample)) {
                        point = .{ .block = index, .sample = @intCast(anchor.ordinal), .offset = @intCast(offset - extent.packet.offset) };
                    }
                };
            }
            var at: usize = if (point) |saved| saved.offset else 0;
            var position = if (point) |saved| saved.sample else extent.first;
            var indexed = position;
            var buffer: CodedFrames = .{};
            defer buffer.bytes.deinit(scratch);
            while (at < extent.packet.size and position < stop_at) {
                try control.check();
                var frame = readFrame(scratch, self.info, input, extent.packet, at, &buffer, .{ .context = work.context, .check_fn = work.check_fn }) catch |err| return if (err == error.OutOfMemory and bounded.denied) error.AudioTooLarge else err;
                defer frame.deinit();
                if (capture) |session| if (out.len != 0 and position < end) {
                    if (std.math.cast(i64, extent.packet.offset + at)) |offset| session.observe(a, .{ .ordinal = position, .offset_ns = offset });
                };
                const next = try std.math.add(usize, position, frame.frame_samples);
                if (stop) |known| if (next > known) return error.UnsupportedAudioFormat;
                var from = @max(position, begin);
                const to = @min(next, end);
                while (from < to) {
                    while (cut_index < self.cuts.len and self.cuts[cut_index].end <= from) cut_index += 1;
                    const cut = if (cut_index < self.cuts.len) self.cuts[cut_index] else Cut{ .first = to, .end = to, .removed_before = 0 };
                    const kept = @min(to, cut.first) -| from;
                    if (kept != 0) {
                        const dest = self.presentationAt(from) - first;
                        if (dest > out.len or kept > out.len - dest) return error.UnsupportedAudioFormat;
                        for (out[dest..][0..kept], from..) |*value, sample| {
                            const index_in_frame = (sample - position) * frame.channels;
                            value.* = mono(frame.samples[index_in_frame..][0..frame.channels]);
                        }
                        copied += kept;
                    }
                    from = @min(to, cut.end);
                }
                position = next;
                at += frame.consumed;
                buffer.consume(frame.consumed);
                if (at < extent.packet.size and (position >= stop_at or position - indexed >= self.info.sample_rate)) {
                    try self.frame_directory.remember(.{ .block = index, .sample = position, .offset = at }, control);
                    indexed = position;
                }
            }
            if (stop) |known| {
                if ((at == extent.packet.size and position != known) or (position == known and at != extent.packet.size)) return error.UnsupportedAudioFormat;
            } else if (at == extent.packet.size) {
                self.frame_directory.final_raw_samples.store(position, .release);
            }
        }
        const required = if (self.finalSamples()) |final| @min(last, final) -| first else last - first;
        if (copied < required) return error.IncompleteAudioStream;
        return required;
    }

    const CodedFrames = struct {
        bytes: std.ArrayList(u8) = .empty,
        start: usize = 0,

        fn prefix(self: *const CodedFrames, limit: usize) []const u8 {
            const bytes = self.bytes.items[self.start..];
            return bytes[0..@min(limit, bytes.len)];
        }

        fn consume(self: *CodedFrames, size: usize) void {
            self.start += size;
            if (self.start == self.bytes.items.len) {
                self.bytes.clearRetainingCapacity();
                self.start = 0;
            }
        }

        fn room(self: *CodedFrames, a: std.mem.Allocator, needed: usize) !void {
            if (needed <= self.bytes.capacity - self.start) return;
            if (self.start != 0) {
                const remaining = self.bytes.items[self.start..];
                std.mem.copyForwards(u8, self.bytes.items[0..remaining.len], remaining);
                self.bytes.items.len = remaining.len;
                self.start = 0;
            }
            try self.bytes.ensureTotalCapacity(a, needed);
        }
    };

    const FrameRefill = struct {
        allocator: std.mem.Allocator,
        input: *media.source.Source,
        offset: u64,
        limit: usize,
        buffer: *CodedFrames,
        control: media.source.Control,

        fn refill(raw: *anyopaque, end: usize) ![]const u8 {
            const self: *FrameRefill = @ptrCast(@alignCast(raw));
            if (end > self.limit) return error.UnsupportedAudioFormat;
            var available = self.buffer.prefix(self.limit).len;
            const wanted = if (self.input.supportsPreferredSparseRead()) @min(self.limit, @max(end, 256, available *| 2)) else end;
            while (available < end) {
                const limits = self.input.remaining();
                const cap = @min(limits.max_read_bytes, limits.max_retained_bytes, limits.max_total_bytes);
                if (cap == 0 or limits.max_reads == 0) return error.ResourceLimitExceeded;
                const required = @min(end - available, cap);
                var preferred = if (limits.max_reads == 1) required else @min(wanted - available, cap);
                self.buffer.room(self.allocator, available + preferred) catch |err| {
                    if (err != error.OutOfMemory or preferred == required) return err;
                    preferred = required;
                    try self.buffer.room(self.allocator, available + required);
                };
                var lease = try self.input.readSparsePreferring(self.offset + available, required, preferred, self.control);
                defer lease.deinit();
                self.buffer.bytes.appendSliceAssumeCapacity(lease.bytes);
                available += lease.bytes.len;
            }
            return self.buffer.prefix(self.limit);
        }
    };

    fn readFrame(a: std.mem.Allocator, info: audio.flac.StreamInfo, input: *media.source.Source, packet: media.webm_audio.Packet, at: usize, buffer: *CodedFrames, control: media.source.Control) !audio.flac.FrameDecoded {
        const limit = @min(packet.size - at, if (info.max_frame_size != 0) info.max_frame_size else 16 * 1024 * 1024);
        var refill = FrameRefill{ .allocator = a, .input = input, .offset = packet.offset + at, .limit = limit, .buffer = buffer, .control = control };
        return audio.flac.decodeFrameRefillAlloc(a, info, buffer.prefix(limit), .{ .context = &refill, .refill = FrameRefill.refill });
    }
};

const FramePlan = @import("media_audio_plan.zig");

pub const Reader = struct {
    index: *const Index,
    source: ?*media.source.Source = null,
    bytes: []const u8 = &.{},
    lane: u1 = 0,
    fork_allocator: ?std.mem.Allocator = null,
    frame_snapshot: ?*const FramePlan.Snapshot = null,
    frame_capture: ?*providers.FrameCheckpointSession = null,
    control: @import("../execution_control.zig").InferenceExecutionControl = .{},
    pub fn provider(self: *Reader) providers.Provider {
        var result = providers.Provider{ .context = self, .samples = self.index.samples(), .sample_rate = self.index.rate(), .read = read, .fork = fork };
        if (self.index.* == .webm_flac) {
            if (self.index.webm_flac.info.total_samples == 0) {
                result.final_samples = finalSamples;
                result.probe_window = probeWindow;
            }
            if (self.source) |input| if (input.storage == .range and input.storage.range.immutable_binding != null) {
                result.acknowledge_frame_start = acknowledgeFrameStart;
                result.restore_frames = restoreFrames;
                result.save_frames = saveFrames;
            };
        }
        return result;
    }

    fn restoreFrames(raw: *anyopaque, a: std.mem.Allocator, store: FramePlan.Store.Context, session: *providers.FrameCheckpointSession) void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        const binding = self.source.?.storage.range.immutable_binding.?;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("webm-flac-frame-seek-v1");
        hash.update(&binding);
        hash.final(&session.identity);
        const directory = self.index.webm_flac.frame_directory;
        session.generation = directory.generation.load(.acquire);
        var context = store;
        context.directory = .flac_frames;
        session.snapshot = FramePlan.Store.load(context, a, session.identity) catch null;
        if (session.snapshot) |*snapshot| self.frame_snapshot = snapshot;
        session.limit = @min(FramePlan.max_points, self.index.webm_flac.working_bytes / (16 * @sizeOf(FramePlan.Point)));
        self.frame_capture = session;
    }

    fn acknowledgeFrameStart(raw: *anyopaque, next_sample: usize) void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        const session = self.frame_capture orelse return;
        const raw_sample = self.index.webm_flac.rawAt(self.index.webm_flac.compactAt(next_sample), true);
        const decoded = FramePlan.Snapshot{ .points = session.decoded.items };
        session.continuation = decoded.nearest(raw_sample);
    }

    fn saveFrames(raw: *anyopaque, a: std.mem.Allocator, store: FramePlan.Store.Context, session: *providers.FrameCheckpointSession) void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        self.frame_snapshot = null;
        self.frame_capture = null;
        defer session.decoded.deinit(a);
        defer if (session.snapshot) |snapshot| snapshot.deinit(a);
        const index = self.index.webm_flac;
        const directory = index.frame_directory;
        if (directory.generation.load(.acquire) == session.generation and session.continuation == null) return;
        self.control.lockWithPoll(&directory.mutex, 100 * std.time.ns_per_us) catch return;
        const points = directory.points;
        const recent = @min(points.len, directory.generation.load(.acquire) -% session.generation);
        const next = directory.next;
        directory.mutex.unlock();

        var builder = FramePlan.Buffer{};
        const count = if (session.snapshot == null) points.len else recent;
        for (0..count) |i| if (points[(next + points.len - count + i) % points.len]) |point| {
            const offset = index.extents[point.block].packet.offset + point.offset;
            const encoded = std.math.cast(i64, offset) orelse continue;
            // Frame snapshots share the bounded journal encoding. Ordinals
            // are raw sample numbers and offsets are absolute coded bytes.
            builder.append(.{ .ordinal = point.sample, .offset_ns = encoded }, true) catch return;
        };
        if (session.continuation) |point| builder.append(point, true) catch return;
        const snapshot = builder.snapshot();
        if (snapshot.points.len == 0) return;
        var context = store;
        context.directory = .flac_frames;
        if (session.snapshot) |previous| {
            var merged = FramePlan.Buffer{};
            merged.merge(previous) catch return;
            merged.merge(snapshot) catch return;
            const base_digest = previous.digest();
            if (std.mem.eql(u8, &base_digest, &merged.snapshot().digest())) return;
            FramePlan.Store.saveUpdate(context, a, session.identity, merged.snapshot(), .{ .base_digest = base_digest, .delta = snapshot }) catch {};
        } else FramePlan.Store.save(context, a, session.identity, snapshot) catch {};
    }

    fn finalSamples(raw: *anyopaque) ?usize {
        const self: *Reader = @ptrCast(@alignCast(raw));
        return self.index.webm_flac.finalSamples();
    }

    fn probeWindow(raw: *anyopaque, a: std.mem.Allocator, first: usize, last: usize, lookahead: usize) !?providers.Lease {
        const self: *Reader = @ptrCast(@alignCast(raw));
        const index = self.index.webm_flac;
        if (index.finalSamples() != null) return null;
        const tail = index.extents[index.extents.len - 1].first;
        const end = @min(lookahead, index.samples());
        if (end <= first or index.probeStop(end) <= tail) return null;
        return try self.readUnknownFlac(a, first, @min(last, end), end);
    }

    fn readUnknownFlac(self: *Reader, a: std.mem.Allocator, first: usize, last: usize, lookahead: usize) !providers.Lease {
        const lane = @import("media_opus_seek.zig").Lane.enter(self.lane);
        defer lane.deinit();
        try self.control.check();
        const check = self.control.imageWorkControl();
        const scope = audio.work_control.Scope.enter(.{ .context = check.context, .check_fn = check.check_fn });
        defer scope.deinit();
        if (last - first > self.index.webm_flac.working_bytes / @sizeOf(f32)) return error.AudioTooLarge;
        const pcm = try a.alloc(f32, last - first);
        errdefer a.free(pcm);
        const count = try self.index.webm_flac.read(a, first, last, lookahead, pcm, self.frame_snapshot, self.frame_capture, self.source orelse return error.UnsupportedAudioFormat, self.control);
        return .{ .pcm = .{ .samples = pcm[0..count], .sample_rate = self.index.rate() }, .owned = pcm };
    }

    fn fork(raw: *anyopaque, a: std.mem.Allocator) !providers.Fork {
        const self: *Reader = @ptrCast(@alignCast(raw));
        const copy = try a.create(Reader);
        copy.* = self.*;
        copy.lane = 1;
        copy.frame_capture = null;
        copy.fork_allocator = a;
        return .{ .provider = copy.provider(), .context = copy, .destroy = destroyFork };
    }
    fn destroyFork(raw: *anyopaque) void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        self.fork_allocator.?.destroy(self);
    }
    fn read(raw: *anyopaque, a: std.mem.Allocator, first: usize, last: usize) !providers.Lease {
        const self: *Reader = @ptrCast(@alignCast(raw));
        if (last < first) return error.InvalidAudioWindow;
        if (self.index.* == .webm_flac and self.index.webm_flac.info.total_samples == 0) {
            const end = @max(first, @min(last, self.index.samples()));
            return self.readUnknownFlac(a, first, end, end);
        }
        const lane = @import("media_opus_seek.zig").Lane.enter(self.lane);
        defer lane.deinit();
        try self.control.check();
        const check = self.control.imageWorkControl();
        const scope = audio.work_control.Scope.enter(.{ .context = check.context, .check_fn = check.check_fn });
        defer scope.deinit();
        if (last > self.index.samples()) return error.InvalidAudioWindow;
        if (self.index.* == .webm_flac and last - first > self.index.webm_flac.working_bytes / @sizeOf(f32)) return error.AudioTooLarge;
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
            .webm_flac => |*index| _ = try index.read(a, first, last, last, samples, self.frame_snapshot, self.frame_capture, self.source orelse return error.UnsupportedAudioFormat, self.control),
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
            if (start >= self.boundary + void_header.len) {
                @memset(out, 0);
                return out.len;
            }
            const count = @min(out.len, self.boundary + void_header.len - start);
            const from_file = @min(count, self.boundary -| start);
            @memcpy(out[0..from_file], self.bytes[@min(start, self.boundary)..][0..from_file]);
            if (count > from_file) @memcpy(out[from_file..count], void_header[start + from_file - self.boundary ..][0 .. count - from_file]);
            @memset(out[count..], 0);
            return out.len;
        }
    };
    var context = Source{ .bytes = webm, .boundary = boundary };
    const upstream = media.source.Range{ .context = &context, .read_at = Source.read, .length = webm.len + void_header.len + 1024 * 1024, .accepts_full_read_size = true };
    var transport = media.remote.ReadAhead{ .allocator = a, .upstream = upstream, .max_bytes = webm.len * 2 };
    defer transport.deinit();
    var source = media.source.Source{ .allocator = a, .identity = "ranged-opus", .storage = .{ .range = transport.range() } };

    var track = try @import("media_chunking_audio_source.zig").Track.init(a, &source, .{}, 64 * 1024, 1024);
    defer track.deinit();
    try std.testing.expect(track.bytes == null);
    try std.testing.expect(source.total_bytes < webm.len / 2);
    try std.testing.expect(transport.transferred_bytes < webm.len);
    try std.testing.expect(transport.reads <= packets.len + 4);
    var baseline = try Index.init(a, webm, 1024);
    defer baseline.deinit();
    try std.testing.expectEqual(baseline.samples(), track.provider().samples);
    var expected_reader = Reader{ .index = &baseline };
    const expected_provider = expected_reader.provider();
    var expected = try expected_provider.read(expected_provider.context, a, 0, 2048);
    defer expected.deinit(a);
    var copied = try track.seek.?.copy(a);
    defer copied.deinit();
    var cold_transport = media.remote.ReadAhead{ .allocator = a, .upstream = upstream, .max_bytes = webm.len };
    defer cold_transport.deinit();
    var second = media.source.Source{ .allocator = a, .identity = source.identity, .storage = .{ .range = cold_transport.range() } };
    var reader = Reader{ .index = &copied, .source = &second };
    const provider = reader.provider();
    var actual = try provider.read(provider.context, a, 0, 2048);
    defer actual.deinit(a);
    try std.testing.expectEqualSlices(f32, expected.pcm.samples, actual.pcm.samples);
    try std.testing.expect(second.total_bytes > 0 and second.total_bytes < webm.len / 2);
    try std.testing.expectEqual(@as(usize, 0), second.retained_bytes);
    var late_transport = media.remote.ReadAhead{ .allocator = a, .upstream = upstream, .max_bytes = webm.len / 2 };
    defer late_transport.deinit();
    var late_source = media.source.Source{ .allocator = a, .identity = source.identity, .storage = .{ .range = late_transport.range() } };
    var late_reader = Reader{ .index = &copied, .source = &late_source };
    const late_provider = late_reader.provider();
    const before = copied.webm.codec.opus.owner.decoded_packets.load(.monotonic);
    const end = late_provider.samples;
    const start = end - 480;
    var late = try late_provider.read(late_provider.context, a, start, end);
    defer late.deinit(a);
    try std.testing.expect(copied.webm.codec.opus.owner.decoded_packets.load(.monotonic) - before < 10);
    var repeated = try late_provider.read(late_provider.context, a, start, end);
    defer repeated.deinit(a);
    try std.testing.expectEqualSlices(f32, late.pcm.samples, repeated.pcm.samples);

    const Cache = @import("media_index_cache.zig").Cache;
    const RuntimeMemory = @import("../runtime/tier/memory.zig");
    const TestAdmission = struct {
        fn acquire(raw: *anyopaque, bytes: usize) !RuntimeMemory.AdmissionLease {
            const owner: *RuntimeMemory.AdmissionController = @ptrCast(@alignCast(raw));
            return owner.tryAcquire(.cpu, .{ .host_limit_bytes = Cache.max_bytes }, .{ .host_scratch_bytes = bytes }, false);
        }
    };
    var controller: RuntimeMemory.AdmissionController = .{};
    const admission: Cache.Admission = .{ .context = &controller, .acquire = TestAdmission.acquire };
    var cache: Cache = .{};
    defer cache.deinit();
    source.storage.range.immutable_binding = @splat(42);
    cache.remember(std.heap.smp_allocator, Cache.audioKey(&source, 1024, 64 * 1024).?, .{ .audio = track.seek.? }, admission);
    {
        const entry = cache.get(Cache.audioKey(&source, 1024, 64 * 1024).?).?;
        try std.testing.expectEqual(@as(usize, 0), Cache.reclaim(&cache, Cache.max_bytes));
        var pinned = @import("media_chunking_audio_source.zig").Track.fromCachedAudio(a, &source, .{}, &cache, entry);
        defer pinned.deinit();
        try std.testing.expectEqual(baseline.samples(), pinned.provider().samples);
    }
    var flights: [256]@import("media_speaker_flights.zig").Registry.Lease = undefined;
    var acquired: usize = 0;
    defer for (flights[0..acquired]) |flight| flight.deinit();
    for (&flights, 0..) |*flight, i| {
        var key: [32]u8 = @splat(0);
        key[0] = @intCast(i);
        flight.* = try cache.acquireBuild(key, .{});
        acquired += 1;
    }
    const reads = source.reads;
    var warm = try @import("media_chunking_audio_source.zig").Track.initCached(a, &source, .{}, 64 * 1024, 1024, &cache, admission);
    defer warm.deinit();
    try std.testing.expect(warm.cached != null);
    try std.testing.expectEqual(reads, source.reads);

    const over_limit = try audio.webm.buildOpusTestFileWithPrerollAlloc(a, ogg.packets[0].bytes, packets, 3 * std.time.ns_per_s);
    defer a.free(over_limit);
    var declared = media.source.Source{ .allocator = a, .identity = "opus-declared-preroll", .storage = .{ .borrowed = over_limit } };
    try std.testing.expectError(error.UnsupportedStreamingAudioProfile, Index.initWebmSource(a, &declared, 1024, 64 * 1024, null));
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
        if (!unknown_total) try std.testing.expectEqual(expected.samples.len, index.samples());
        var reader = Reader{ .index = &index, .source = &source };
        const provider = reader.provider();
        const end = expected.samples.len;
        const first = end - 512;
        const before = source.total_bytes;
        var actual = try provider.read(provider.context, a, first, end);
        defer actual.deinit(a);
        try std.testing.expectEqualSlices(f32, expected.samples[first..end], actual.pcm.samples);
        try std.testing.expectEqual(end, index.samples());
        try std.testing.expect(source.total_bytes - before < encoded.len / 2);
        try std.testing.expectEqual(@as(usize, 0), source.retained_bytes);
    }
}

test "model aware media range FLAC applies signed interior block padding" {
    const a = std.testing.allocator;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/codec-corpus/tone-stereo.flac", a, .limited(1024 * 1024));
    defer a.free(encoded);
    var unpadded = try audio.decode(a, encoded, .{});
    defer unpadded.deinit();
    for ([_]bool{ false, true }) |unknown| {
        const webm = try audio.webm.buildPaddedFlacTestBlocksAlloc(a, encoded, unknown, &.{ -10 * std.time.ns_per_ms, 10 * std.time.ns_per_ms }, null);
        defer a.free(webm);
        var expected = try audio.decode(a, webm, .{});
        defer expected.deinit();
        const drop = unpadded.sample_rate / 100;
        const two_blocks = 2 * @as(usize, (try audio.flac.parseStreamInfo(encoded)).max_block_size);
        const kept = two_blocks - 2 * drop;
        try std.testing.expectEqual(unpadded.samples.len - 2 * drop, expected.samples.len);
        try std.testing.expectEqualSlices(f32, unpadded.samples[drop .. two_blocks - drop], expected.samples[0..kept]);
        try std.testing.expectEqualSlices(f32, unpadded.samples[two_blocks..], expected.samples[kept..]);
        var source = media.source.Source{ .allocator = a, .identity = "signed-flac-padding", .storage = .{ .borrowed = webm } };
        var index = try Index.initWebmSource(a, &source, 1024, 1024 * 1024, null);
        defer index.deinit();
        var reader = Reader{ .index = &index, .source = &source };
        const provider = reader.provider();
        var actual = try provider.read(provider.context, a, 0, expected.samples.len);
        defer actual.deinit(a);
        try std.testing.expectEqualSlices(f32, expected.samples, actual.pcm.samples);
        var middle = try provider.read(provider.context, a, 129, 4096);
        defer middle.deinit(a);
        try std.testing.expectEqualSlices(f32, expected.samples[129..4096], middle.pcm.samples);
    }
}

test "model aware media FLAC gap parity skips discarded packets and defers tail lookahead" {
    const a = std.testing.allocator;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/codec-corpus/tone-stereo.flac", a, .limited(1024 * 1024));
    defer a.free(encoded);
    const info = try audio.flac.parseStreamInfo(encoded);
    const block: usize = info.max_block_size;
    const packet_count = try std.math.divCeil(usize, @intCast(info.total_samples), block);
    const padding = try a.alloc(i64, packet_count);
    defer a.free(padding);
    @memset(padding, 0);
    padding[1] = @intCast(try std.math.divCeil(u128, @as(u128, block) * std.time.ns_per_s, info.sample_rate));
    padding[packet_count - 1] = 5 * std.time.ns_per_ms;
    const webm = try audio.webm.buildPaddedFlacTestBlocksAlloc(a, encoded, true, padding, .{ .packet = 2, .ms = 200 });
    defer a.free(webm);
    var expected = try audio.decode(a, webm, .{});
    defer expected.deinit();

    const Transport = struct {
        bytes: []const u8,
        denied_first: u64 = 0,
        denied_end: u64 = 0,
        fn read(raw: *anyopaque, offset: u64, out: []u8, _: media.source.Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (offset < self.denied_end and offset + out.len > self.denied_first) return error.UnexpectedPaddingRead;
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
            return out.len;
        }
    };
    var transport = Transport{ .bytes = webm };
    var source = media.source.Source{ .allocator = a, .identity = "flac-gap-with-padding", .storage = .{ .range = .{ .context = &transport, .read_at = Transport.read, .length = webm.len } } };
    var index = try Index.initWebmSource(a, &source, 1024, 1024 * 1024, null);
    defer index.deinit();
    const dropped = index.webm_flac.extents[1].packet;
    transport.denied_first = dropped.offset;
    transport.denied_end = dropped.offset + dropped.size;
    var reader = Reader{ .index = &index, .source = &source };
    const provider = reader.provider();
    var early = try provider.read(provider.context, a, 0, block);
    defer early.deinit(a);
    try std.testing.expectEqualSlices(f32, expected.samples[0..block], early.pcm.samples);
    var entire = try provider.read(provider.context, a, 0, expected.samples.len + 512);
    defer entire.deinit(a);
    try std.testing.expectEqualSlices(f32, expected.samples, entire.pcm.samples);
    const gap_start = block + 256;
    var silence = try provider.read(provider.context, a, gap_start, gap_start + 512);
    defer silence.deinit(a);
    try std.testing.expectEqualSlices(f32, expected.samples[gap_start..][0..512], silence.pcm.samples);
}

test "model aware media unpadded FLAC keeps interior timestamp gaps" {
    const a = std.testing.allocator;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/codec-corpus/tone-stereo.flac", a, .limited(1024 * 1024));
    defer a.free(encoded);
    var norm = try audio.decode(a, encoded, .{});
    defer norm.deinit();
    for ([_]bool{ false, true }) |unknown| {
        const webm = try audio.webm.buildPaddedFlacTestBlocksAlloc(a, encoded, unknown, &.{}, .{ .packet = 1, .ms = 200 });
        defer a.free(webm);
        var expected = try audio.decode(a, webm, .{});
        defer expected.deinit();
        const block: usize = (try audio.flac.parseStreamInfo(encoded)).max_block_size;
        const pause_end = @as(usize, norm.sample_rate) / 5;
        try std.testing.expectEqual(norm.samples.len + pause_end - block, expected.samples.len);
        try std.testing.expectEqualSlices(f32, norm.samples[0..block], expected.samples[0..block]);
        for (expected.samples[block..pause_end]) |value| try std.testing.expectEqual(@as(f32, 0), value);
        try std.testing.expectEqualSlices(f32, norm.samples[block..], expected.samples[pause_end..]);
        var source = media.source.Source{ .allocator = a, .identity = "unpadded-flac-gap", .storage = .{ .borrowed = webm } };
        var index = try Index.initWebmSource(a, &source, 1024, 1024 * 1024, null);
        defer index.deinit();
        var reader = Reader{ .index = &index, .source = &source };
        const provider = reader.provider();
        var actual = try provider.read(provider.context, a, block - 1, pause_end + 1);
        defer actual.deinit(a);
        try std.testing.expectEqualSlices(f32, expected.samples[block - 1 .. pause_end + 1], actual.pcm.samples);
    }
}

test "model aware media FLAC reads and indexes individual frames within a WebM block" {
    const a = std.testing.allocator;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/codec-corpus/tone-stereo.flac", a, .limited(1024 * 1024));
    defer a.free(encoded);
    const parsed = try audio.flac.parseStream(encoded);
    // Keep STREAMINFO and drop optional metadata so the read cap isolates
    // the packet extent constraint.
    const compact = try std.mem.concat(a, u8, &.{ encoded[0..42], encoded[parsed.data_offset..] });
    defer a.free(compact);
    compact[4] |= 0x80;
    const webm = try audio.webm.buildFlacTestBlocksAlloc(a, compact, false, std.math.maxInt(usize));
    defer a.free(webm);
    var reference = try audio.decode(a, encoded, .{});
    defer reference.deinit();
    const Transport = struct {
        bytes: []const u8,
        floor: u64 = 0,
        fn read(raw: *anyopaque, offset: u64, out: []u8, _: media.source.Control) !usize {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (offset < self.floor) return error.RemoteReadBudgetExceeded;
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
            return out.len;
        }
    };
    var transport = Transport{ .bytes = webm };
    const read_limit = parsed.stream_info.max_frame_size;
    var source = media.source.Source{
        .allocator = a,
        .identity = "multiframe-flac",
        .storage = .{ .range = .{ .context = &transport, .read_at = Transport.read, .length = webm.len, .immutable_binding = @splat(42) } },
        .limits = .{ .max_read_bytes = read_limit, .max_retained_bytes = read_limit },
    };
    var index = try Index.initWebmSource(a, &source, 1024, 1024 * 1024, null);
    defer index.deinit();
    try std.testing.expect(index.webm_flac.extents[0].packet.size > read_limit);
    const boundary: usize = index.webm_flac.info.max_block_size;
    const page_end = boundary + 256;
    const overlapped = boundary + 128;
    var reader = Reader{ .index = &index, .source = &source };
    const provider = reader.provider();
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{directory.sub_path});
    defer a.free(root);
    var store = FramePlan.Store{};
    const context = FramePlan.Store.Context{ .store = &store, .io = std.testing.io, .models_dir = root };
    var session = providers.FrameCheckpointSession{};
    provider.restore_frames.?(provider.context, a, context, &session);
    const before = source.total_bytes;
    var first = try provider.read(provider.context, a, 0, page_end);
    defer first.deinit(a);
    try std.testing.expectEqualSlices(f32, reference.samples[0..page_end], first.pcm.samples);
    provider.acknowledge_frame_start.?(provider.context, overlapped);
    const point = session.continuation.?;
    try std.testing.expectEqual(boundary, point.ordinal);
    try std.testing.expect(source.total_bytes - before < encoded.len / 2);
    var copy = try index.copy(a);
    defer copy.deinit();
    try std.testing.expect((try copy.webm_flac.frame_directory.seek(0, overlapped, .{})) == null);
    reader.index = &copy;
    var second = try provider.read(provider.context, a, boundary, @as(usize, boundary) + 512);
    defer second.deinit(a);
    try std.testing.expectEqualSlices(f32, reference.samples[boundary..][0..512], second.pcm.samples);
    provider.save_frames.?(provider.context, a, context, &session);

    var cold = try Index.initWebmSource(a, &source, 1024, 1024 * 1024, null);
    defer cold.deinit();
    reader.index = &cold;
    transport.floor = @intCast(point.offset_ns);
    try std.testing.expectError(error.RemoteReadBudgetExceeded, provider.read(provider.context, a, overlapped, overlapped + 512));
    var restored = providers.FrameCheckpointSession{};
    provider.restore_frames.?(provider.context, a, context, &restored);
    defer provider.save_frames.?(provider.context, a, context, &restored);
    var resumed = try provider.read(provider.context, a, overlapped, overlapped + 512);
    defer resumed.deinit(a);
    try std.testing.expectEqualSlices(f32, reference.samples[overlapped..][0..512], resumed.pcm.samples);
}

test "model aware media unknown FLAC duration does not read the tail before the first window" {
    const a = std.testing.allocator;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/codec-corpus/tone-stereo.flac", a, .limited(1024 * 1024));
    defer a.free(encoded);
    const webm = try audio.webm.buildFlacTestBlocksAlloc(a, encoded, true, std.math.maxInt(usize));
    defer a.free(webm);
    var borrowed = media.source.Source{ .allocator = a, .identity = "flac-tail", .storage = .{ .borrowed = webm } };
    const directory = try media.webm_audio.Directory.init(a, &borrowed, .{});
    defer directory.deinit();
    const packet = directory.packets[directory.packets.len - 1];
    const info = try audio.flac.parseStreamInfo(encoded);
    const Transport = struct {
        bytes: []const u8,
        tail: u64,
        limit: usize,
        remaining: usize,
        fn read(raw: *anyopaque, offset: u64, out: []u8, _: media.source.Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (offset >= self.tail) {
                if (out.len > self.limit) return error.UnboundedTailRead;
                if (out.len > self.remaining) return error.RemoteReadBudgetExceeded;
                self.remaining -= out.len;
            }
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
            return out.len;
        }
    };
    try std.testing.expect(info.max_frame_size != 0 and info.max_frame_size < packet.size);
    var transport = Transport{ .bytes = webm, .tail = packet.offset, .limit = info.max_frame_size, .remaining = @as(usize, info.max_frame_size) + 64 };
    var input = media.source.Source{ .allocator = a, .identity = "flac-tail", .storage = .{ .range = .{ .context = &transport, .read_at = Transport.read, .length = webm.len } } };
    var indexed = try Index.initWebmSource(a, &input, 1024, 1024 * 1024, null);
    defer indexed.deinit();
    var reader = Reader{ .index = &indexed, .source = &input };
    const provider = reader.provider();
    try std.testing.expect(provider.final_samples.?(provider.context) == null);
    var first = (try provider.probe_window.?(provider.context, a, 0, 512, 512)).?;
    defer first.deinit(a);
    try std.testing.expectEqual(@as(usize, 512), first.pcm.samples.len);
    try std.testing.expect(provider.final_samples.?(provider.context) == null);

    transport.remaining = packet.size;
    const end: usize = @intCast(info.total_samples);
    var last = (try provider.probe_window.?(provider.context, a, end - 512, end - 256, end + 512)).?;
    defer last.deinit(a);
    try std.testing.expectEqual(@as(usize, 256), last.pcm.samples.len);
    try std.testing.expectEqual(@as(?usize, end), provider.final_samples.?(provider.context));
}

test "model aware media FLAC refill preserves the final footer under source and wire quota" {
    const a = std.testing.allocator;
    const Transport = struct {
        fn read(_: *anyopaque, _: u64, out: []u8, _: media.source.Control) !usize {
            if (out.len > 2) return error.RemoteReadBudgetExceeded;
            @memcpy(out, &[_]u8{ 0x12, 0x34 });
            return out.len;
        }
    };
    for ([_]bool{ false, true }) |wire_quota| {
        var context: u8 = 0;
        var source = media.source.Source{ .allocator = a, .identity = "flac-footer", .storage = .{ .range = .{ .context = &context, .read_at = Transport.read, .length = 512 } } };
        if (!wire_quota) source.limits.max_total_bytes = 2;
        var buffer: WebmFlac.CodedFrames = .{};
        defer buffer.bytes.deinit(a);
        try buffer.bytes.appendNTimes(a, 0, 250);
        var input = WebmFlac.FrameRefill{ .allocator = a, .input = &source, .offset = 0, .limit = 512, .buffer = &buffer, .control = .{} };
        const prefix = try WebmFlac.FrameRefill.refill(&input, 252);
        try std.testing.expectEqualSlices(u8, &.{ 0x12, 0x34 }, prefix[250..]);
        try std.testing.expectEqual(@as(u64, 2), source.total_bytes);
    }
}
