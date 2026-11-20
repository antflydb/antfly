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

//! One immutable MP4 index and bounded coded-packet leases across model windows.
const std = @import("std");
const media = @import("antfly_media");
const video = @import("antfly_video");
const model_video = @import("embedding_gemma2_video.zig");
/// Packet-interval index for fields/partitions. Queries visit intersecting
/// pictures rather than rescanning the clip; subset order stays decode-stable.
const Dependencies = struct {
    allocator: std.mem.Allocator,
    ids: []usize,
    maxima: []usize,
    base: usize,
    fn init(a: std.mem.Allocator, index: video.h264_pictures.Index) !Dependencies {
        const ids = try a.alloc(usize, index.pictures.len);
        errdefer a.free(ids);
        for (ids, 0..) |*id, i| id.* = i;
        std.mem.sort(usize, ids, index, struct {
            fn less(pictures: video.h264_pictures.Index, x: usize, y: usize) bool {
                const left = pictures.pictures[x].first_packet;
                const right = pictures.pictures[y].first_packet;
                return left < right or (left == right and x < y);
            }
        }.less);
        const base = try std.math.ceilPowerOfTwo(usize, @max(@as(usize, 1), ids.len));
        const maxima = try a.alloc(usize, try std.math.mul(usize, base, 2));
        errdefer a.free(maxima);
        @memset(maxima, 0);
        for (ids, 0..) |id, i| maxima[base + i] = index.pictures[id].last_packet orelse return error.IncompleteVideoPicture;
        var node = base;
        while (node > 1) {
            node -= 1;
            maxima[node] = @max(maxima[node * 2], maxima[node * 2 + 1]);
        }
        return .{ .allocator = a, .ids = ids, .maxima = maxima, .base = base };
    }
    fn deinit(self: *Dependencies) void {
        self.allocator.free(self.ids);
        self.allocator.free(self.maxima);
    }
    fn lower(self: *const Dependencies, index: video.h264_pictures.Index, packet: usize) usize {
        var lo: usize = 0;
        var hi = self.ids.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (index.pictures[self.ids[mid]].first_packet < packet) lo = mid + 1 else hi = mid;
        }
        return lo;
    }
    fn extend(self: *const Dependencies, index: video.h264_pictures.Index, node: usize, lo: usize, hi: usize, first: *usize, stop: *usize, end: i128, origin: i64, timescale: u32) void {
        if (lo >= self.ids.len or self.maxima[node] < first.* or index.pictures[self.ids[lo]].first_packet >= stop.*) return;
        if (hi - lo == 1) {
            const picture = index.pictures[self.ids[lo]];
            const last = picture.last_packet.?;
            if (picture.first_packet < first.* and last >= first.* and last < stop.*) first.* = picture.first_packet;
            if (picture.first_packet >= first.* and picture.first_packet < stop.* and last >= stop.* and (@as(i128, picture.pts) - origin) * std.time.ns_per_s < end * timescale) stop.* = last + 1;
            return;
        }
        const mid = lo + (hi - lo) / 2;
        self.extend(index, node * 2, lo, mid, first, stop, end, origin, timescale);
        self.extend(index, node * 2 + 1, mid, hi, first, stop, end, origin, timescale);
    }
};
/// Pure immutable planning metadata. No source, decoder, admission token or
/// mutable seek cache survives a request. The cache's arena owns every slice.
pub const Snapshot = struct {
    transport: ?video.transport_seek.Mapping = null,
    registry: ?video.h264_dynamic.Snapshot = null,
    hevc_predecessors: []usize,
    timeline: video.sampling.Timeline,
    logical: ?video.h264_pictures.Index,
    dependencies: ?Dependencies,
    origin: i64,
    duration_ns: i64,
    pub fn storageBytes(owner: *const Owner) usize {
        var bytes = std.mem.sliceAsBytes(owner.hevc_predecessors).len +| std.mem.sliceAsBytes(owner.timeline.entries).len +| std.mem.sliceAsBytes(owner.timeline.maxima).len;
        if (owner.transport) |map| bytes +|= std.mem.sliceAsBytes(map.nals).len +| std.mem.sliceAsBytes(map.runs).len;
        if (owner.registry) |registry| bytes +|= video.h264_dynamic.Snapshot.storageBytes(registry);
        if (owner.logical) |index| bytes +|= std.mem.sliceAsBytes(index.pictures).len;
        if (owner.dependencies) |deps| bytes +|= std.mem.sliceAsBytes(deps.ids).len +| std.mem.sliceAsBytes(deps.maxima).len;
        return bytes;
    }
    pub fn copy(a: std.mem.Allocator, owner: *const Owner) !Snapshot {
        var result: Snapshot = .{ .hevc_predecessors = try a.dupe(usize, owner.hevc_predecessors), .timeline = owner.timeline, .logical = owner.logical, .dependencies = owner.dependencies, .origin = owner.origin, .duration_ns = try owner.durationNs() };
        if (owner.transport) |map| result.transport = try map.copy(a);
        if (owner.registry) |registry| result.registry = try video.h264_dynamic.Snapshot.copy(a, registry);
        result.timeline.entries = try a.dupe(@typeInfo(@TypeOf(owner.timeline.entries)).pointer.child, owner.timeline.entries);
        result.timeline.maxima = try a.dupe(i128, owner.timeline.maxima);
        result.timeline.allocator = a;
        if (result.logical) |*index| {
            index.allocator = a;
            index.pictures = try a.dupe(@typeInfo(@TypeOf(index.pictures)).pointer.child, index.pictures);
            index.storage = null;
            index.reservation = .{};
            index.borrowed = true;
        }
        if (result.dependencies) |*deps| {
            deps.allocator = a;
            deps.ids = try a.dupe(usize, deps.ids);
            deps.maxima = try a.dupe(usize, deps.maxima);
        }
        return result;
    }
};

pub const Owner = struct {
    transport: ?video.transport_seek.Mapping = null,
    hevc_predecessors: []usize = &.{},
    borrowed_planning: bool = false,
    duration_ns: ?i64 = null,
    continuation: model_video.DecoderContinuation,
    source_binding: [32]u8,
    allocator: std.mem.Allocator,
    reader: *media.mp4.Reader,
    timeline: video.sampling.Timeline,
    metadata_reservation: media.admission.Token,
    seek_cache: ?video.decode_plan.SeekCache = null,
    logical: ?video.h264_pictures.Index = null,
    dependencies: ?Dependencies = null,
    registry: ?*video.h264_dynamic.Session = null,
    origin: i64,
    pool: *media.admission.Pool,
    limits: @import("media_chunk_limits.zig").Limits,
    pub fn init(a: std.mem.Allocator, reader: *media.mp4.Reader, pool: *media.admission.Pool, limits: @import("media_chunk_limits.zig").Limits) !Owner {
        return initPlanned(a, reader, pool, limits, null);
    }
    pub fn initPlanned(a: std.mem.Allocator, reader: *media.mp4.Reader, pool: *media.admission.Pool, limits: @import("media_chunk_limits.zig").Limits, headers: ?[]const []u8) !Owner {
        if (reader.packets.len == 0) return error.EmptyVideo;
        if (reader.packets.len > limits.max_index_packets) return error.ResourceLimitExceeded;
        if (reader.input.identity.len == 0) return error.InvalidVideoReaderSelection;
        var planning = video.h264_pictures.Options.forTrack(reader.packets.len);
        planning.decode.max_dependency_packets = limits.max_dependency_packets;
        const registry = if (headers) |source| try video.h264_dynamic.Session.initWithHeaders(a, reader, planning.decode, source) else if (reader.track.requiresAvcRegistry()) try video.h264_dynamic.Session.initPlanning(a, reader, planning.decode) else null;
        errdefer if (registry) |owner| owner.deinit();
        var logical: ?video.h264_pictures.Index = null;
        errdefer if (logical) |*index| index.deinit();
        if (try video.packet_sampling.needsLogicalIndex(a, reader.track))
            logical = try video.h264_pictures.planning(a, reader, planning, registry);
        const count = if (logical) |index| index.pictures.len else reader.packets.len;
        var metadata_bytes = try video.sampling.Timeline.storageBytes(count);
        if ((reader.track.codec == .avc and registry == null) or reader.track.codec == .hevc) metadata_bytes = try std.math.add(usize, metadata_bytes, try std.math.mul(usize, reader.packets.len, @sizeOf(usize)));
        if (logical != null) {
            const base = try std.math.ceilPowerOfTwo(usize, @max(@as(usize, 1), count));
            metadata_bytes = try std.math.add(usize, metadata_bytes, try std.math.mul(usize, try std.math.add(usize, count, try std.math.mul(usize, base, 2)), @sizeOf(usize)));
        }
        var metadata_reservation = try pool.acquire(.{ .host_bytes = metadata_bytes });
        errdefer metadata_reservation.deinit();
        var timeline = build: {
            var scratch = try pool.acquire(.{ .host_bytes = try std.math.mul(usize, count, @sizeOf(video.sampling.TimedPicture)) });
            defer scratch.deinit();
            const timed = try a.alloc(video.sampling.TimedPicture, count);
            defer a.free(timed);
            if (logical) |index| {
                for (timed, index.pictures) |*dest, picture| {
                    dest.* = .{ .pts = picture.pts, .duration = picture.duration };
                }
            } else {
                for (timed, reader.packets) |*dest, packet| {
                    dest.* = .{ .pts = packet.pts, .duration = packet.duration };
                }
            }
            break :build try video.sampling.Timeline.initEdited(a, timed, reader.input.control, reader.track.edit);
        };
        errdefer timeline.deinit();
        const hevc_predecessors = try a.alloc(usize, if (reader.track.codec == .hevc) reader.packets.len else 0);
        errdefer a.free(hevc_predecessors);
        if (hevc_predecessors.len != 0) try video.hevc_restart.fillPredecessors(reader, hevc_predecessors);
        var seek_cache: ?video.decode_plan.SeekCache = if (registry) |session| registrySeekCache(a, reader, session) else if (reader.track.codec == .avc) try video.decode_plan.SeekCache.init(a, reader, true) else null;
        errdefer if (seek_cache) |*cache| cache.deinit();
        var dependencies: ?Dependencies = if (logical) |index| try Dependencies.init(a, index) else null;
        errdefer if (dependencies) |*index| index.deinit();
        return .{ .hevc_predecessors = hevc_predecessors, .metadata_reservation = metadata_reservation, .dependencies = dependencies, .timeline = timeline, .seek_cache = seek_cache, .source_binding = model_video.immutableSourceBinding(reader), .continuation = .{ .allocator = a, .reader = reader, .parameter_sets = registry, .max_packets = limits.max_dependency_packets }, .allocator = a, .reader = reader, .logical = logical, .registry = registry, .origin = timeline.first, .pool = pool, .limits = limits };
    }
    fn registrySeekCache(a: std.mem.Allocator, reader: *const media.mp4.Reader, registry: *const video.h264_dynamic.Session) video.decode_plan.SeekCache {
        var cache = video.decode_plan.SeekCache.forPage(a, reader, registry.progressive_idrs.items);
        for (registry.entries.items) |entry| {
            const cfg = entry.config;
            if (!cfg.frame_only or cfg.separate_planes or cfg.profile == 88) return cache;
        }
        cache.complete = true;
        return cache;
    }
    pub fn fromSnapshot(a: std.mem.Allocator, reader: *media.mp4.Reader, pool: *media.admission.Pool, limits: @import("media_chunk_limits.zig").Limits, snapshot: *const Snapshot) !Owner {
        try reader.input.control.check();
        var logical = snapshot.logical;
        if (logical) |*index| index.allocator = a;
        var options = video.h264_pictures.Options.forTrack(reader.packets.len);
        options.decode.max_dependency_packets = limits.max_dependency_packets;
        const registry = if (snapshot.registry) |*shared| try shared.bind(a, reader, options.decode) else null;
        errdefer if (registry) |session| session.deinit();
        const seek_cache = if (registry) |session| registrySeekCache(a, reader, session) else if (reader.track.codec == .avc) video.decode_plan.SeekCache.forPage(a, reader, &.{}) else null;
        return .{ .hevc_predecessors = snapshot.hevc_predecessors, .borrowed_planning = true, .duration_ns = snapshot.duration_ns, .metadata_reservation = .{}, .dependencies = snapshot.dependencies, .timeline = snapshot.timeline, .seek_cache = seek_cache, .source_binding = model_video.immutableSourceBinding(reader), .continuation = .{ .allocator = a, .reader = reader, .parameter_sets = registry, .max_packets = limits.max_dependency_packets }, .allocator = a, .reader = reader, .logical = logical, .registry = registry, .origin = snapshot.origin, .pool = pool, .limits = limits };
    }
    pub fn deinit(self: *Owner) void {
        self.continuation.deinit();
        if (!self.borrowed_planning) self.timeline.deinit();
        if (self.seek_cache) |*cache| cache.deinit();
        if (self.registry) |registry| registry.deinit();
        if (!self.borrowed_planning) {
            if (self.dependencies) |*index| index.deinit();
            if (self.logical) |*index| index.deinit();
            self.allocator.free(self.hevc_predecessors);
        }
        self.metadata_reservation.deinit();
    }
    pub fn durationNs(self: *const Owner) !i64 {
        if (self.duration_ns) |value| return value;
        const stop = self.timeline.last;
        return std.math.cast(i64, @divFloor((stop - self.origin) * std.time.ns_per_s + self.reader.track.timescale - 1, self.reader.track.timescale)) orelse error.TimestampOverflow;
    }
    pub fn prepare(self: *Owner, policy: video.sampling.TimestampPolicy) !Window {
        const a = self.allocator;
        const selected = try self.timeline.select(a, self.reader.track.timescale, policy, self.reader.input.control, true);
        defer a.free(selected);
        var selected_packets: [32]usize = undefined;
        const packets = selected_packets[0..selected.len];
        var storage: [32]video.decode_plan.Run = undefined;
        var count: usize = 0;
        for (selected, packets) |id, *packet| {
            packet.* = if (self.logical) |index| index.pictures[id].first_packet else id;
            if (self.reader.track.codec == .avc) continue;
            const start = if (self.reader.track.codec == .hevc)
                try video.hevc_restart.startFor(self.reader.packets, self.hevc_predecessors, packet.*)
            else
                packet.*;
            storage[count] = .{ .first = start, .last = packet.* };
            count += 1;
        }
        if (self.reader.track.codec == .avc) {
            // Overlapping display intervals can select A, B, A. Preserve those
            // output slots while planning each coded dependency only once.
            var unique: [32]usize = undefined;
            var n: usize = 0;
            for (packets) |packet| {
                if (std.mem.indexOfScalar(usize, unique[0..n], packet) != null) continue;
                unique[n] = packet;
                n += 1;
            }
            var plan = try video.decode_plan.create(a, self.reader, unique[0..n], .{ .seek_cache = if (self.seek_cache) |*cache| cache else null, .portable_config = true, .headers_only = true, .max_decode_packets = self.limits.max_dependency_packets, .max_run_packets = self.limits.max_dependency_packets });
            defer plan.deinit();
            count = 0;
            for (plan.runs) |run| {
                var begin = run.first;
                var end = run.last + 1;
                if (self.logical) |index| {
                    // Complete each selected picture, including members whose
                    // PTS falls beyond the last sampling target.
                    for (selected, packets) |id, packet| {
                        if (packet >= run.first and packet <= run.last) end = @max(end, index.pictures[id].last_packet.? + 1);
                    }
                    while (true) {
                        const previous_first = begin;
                        const previous_stop = end;
                        self.dependencies.?.extend(index, 1, 0, self.dependencies.?.base, &begin, &end, policy.end_ns orelse return error.InvalidSamplingPolicy, self.origin, self.reader.track.timescale);
                        if (end - begin > self.limits.max_dependency_packets) return error.ChunkDependencyBudgetExceeded;
                        if (begin == previous_first and end == previous_stop) break;
                    }
                }
                storage[count] = .{ .first = begin, .last = end - 1 };
                count += 1;
            }
        }
        // Presentation order can differ from decode order, including HEVC
        // leading pictures whose restart precedes the previous target's IRAP.
        std.mem.sort(video.decode_plan.Run, storage[0..count], {}, struct {
            fn less(_: void, left: video.decode_plan.Run, right: video.decode_plan.Run) bool {
                return left.first < right.first;
            }
        }.less);
        var merged: usize = 0;
        for (storage[0..count]) |run| {
            if (merged != 0 and run.first <= storage[merged - 1].last + 1) {
                storage[merged - 1].last = @max(storage[merged - 1].last, run.last);
            } else {
                storage[merged] = run;
                merged += 1;
            }
        }
        count = merged;
        const runs = storage[0..count];
        var total_packets: usize = 0;
        var coded_bytes: u64 = 0;
        var prefix_storage: [32]video.live_windows.PacketPrefix = undefined;
        var scratch = media.admission.TrackedAllocator{ .backing = a, .limit = self.limits.max_coded_window_bytes, .reservation = try self.pool.acquire(.{}) };
        defer scratch.reservation.deinit();
        const staging = scratch.allocator();
        var prefix_count: usize = 0;
        defer for (prefix_storage[0..prefix_count]) |prefix| staging.free(prefix.bytes);
        for (runs, 0..) |run, i| {
            const local = total_packets;
            total_packets += run.last - run.first + 1;
            if (total_packets > self.limits.max_dependency_packets) return error.ChunkDependencyBudgetExceeded;
            for (self.reader.packets[run.first .. run.last + 1]) |packet| coded_bytes += packet.size;
            if (self.registry) |registry| {
                const prefix = registry.bootstrap(staging, run.first, i != 0 and self.reader.track.inband_parameter_sets) catch |err| return if (err == error.OutOfMemory) scratch.denied orelse err else err;
                prefix_storage[prefix_count] = .{ .index = local, .bytes = prefix };
                prefix_count += 1;
                coded_bytes = try std.math.add(u64, coded_bytes, prefix.len);
            }
            if (coded_bytes > self.limits.max_coded_window_bytes) return error.ChunkCodedWindowBudgetExceeded;
        }
        const options = video.live_windows.Options{ .max_packets = self.limits.max_dependency_packets, .stream = .{ .max_bytes = self.limits.max_coded_window_bytes, .admission_pool = self.pool, .control = self.reader.input.control } };
        var window = try video.live_windows.borrowRuns(a, self.reader, runs, options);
        errdefer window.deinit();
        try video.live_windows.attachPrefixes(&window, prefix_storage[0..prefix_count], options);
        for (window.reader.packets) |*packet| packet.pts = try std.math.sub(i64, packet.pts, self.origin);
        var result = Window{ .coded = window, .runs = storage, .run_count = count, .packet_base = runs[0].first, .source_binding = self.source_binding };
        result.packet_count = packets.len;
        for (packets, result.packet_ids[0..packets.len]) |packet, *id| id.* = try video.live_windows.localPacket(runs, packet);
        return result;
    }
};
pub const Window = struct {
    coded: video.live_windows.Window,
    runs: [32]video.decode_plan.Run,
    run_count: usize,
    source_binding: [32]u8,
    packet_ids: [32]usize = undefined,
    packet_count: usize = 0,
    selection: ?model_video.ReaderSelection = null,
    packet_base: usize,
    /// Bind only after the Window reaches its final address; workers borrow it.
    pub fn bind(self: *Window, policy: video.sampling.TimestampPolicy, pool: *media.admission.Pool) !void {
        self.selection = try model_video.ReaderSelection.fromMappedPacketIds(self.coded.reader.allocator, &self.coded.reader, .{ .source_timestamps = policy }, self.packet_ids[0..self.packet_count], pool, self.coded.reader.input.control, self.source_binding, self.packet_base, self.runs[0..self.run_count]);
    }
    pub fn deinit(self: *Window) void {
        if (self.selection) |*selection| selection.deinit();
        self.coded.deinit();
    }
};

test "model aware media packet interval index matches closure for scattered fields and partitions" {
    const a = std.testing.allocator;
    var rng = std.Random.DefaultPrng.init(3819);
    const random = rng.random();
    var pictures: [80]video.h264.Picture = undefined;
    for (0..50) |_| {
        for (&pictures) |*picture| {
            const first = random.intRangeLessThan(usize, 0, 200);
            picture.* = .{ .first_packet = first, .last_packet = first + random.intRangeLessThan(usize, 0, 30), .pts = random.intRangeLessThan(i64, -10, 200), .duration = 1, .width = 16, .height = 16, .bit_depth = 8, .chroma_format = 1 };
        }
        const index = video.h264_pictures.Index{ .allocator = a, .pictures = &pictures, .timescale = 1 };
        var deps = try Dependencies.init(a, index);
        defer deps.deinit();
        var expected_first: usize = 75;
        var expected_stop: usize = 90;
        var actual_first = expected_first;
        var actual_stop = expected_stop;
        while (true) {
            const before_first = expected_first;
            const before_stop = expected_stop;
            for (pictures) |picture| {
                const last = picture.last_packet.?;
                if (picture.first_packet < expected_first and last >= expected_first and last < expected_stop) expected_first = picture.first_packet;
                if (picture.first_packet >= expected_first and picture.first_packet < expected_stop and last >= expected_stop and picture.pts < 100) expected_stop = last + 1;
            }
            if (before_first == expected_first and before_stop == expected_stop) break;
        }
        while (true) {
            const before_first = actual_first;
            const before_stop = actual_stop;
            deps.extend(index, 1, 0, deps.base, &actual_first, &actual_stop, 100 * std.time.ns_per_s, 0, 1);
            if (before_first == actual_first and before_stop == actual_stop) break;
        }
        try std.testing.expectEqual(expected_first, actual_first);
        try std.testing.expectEqual(expected_stop, actual_stop);
        const selected = deps.ids[deps.lower(index, actual_first)..deps.lower(index, actual_stop)];
        for (selected) |id| try std.testing.expect(pictures[id].first_packet >= actual_first and pictures[id].first_packet < actual_stop);
    }
}
