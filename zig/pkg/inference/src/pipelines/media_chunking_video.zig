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
        if (reader.packets.len == 0) return error.EmptyVideo;
        if (reader.packets.len > limits.max_index_packets) return error.ResourceLimitExceeded;
        if (reader.input.identity.len == 0) return error.InvalidVideoReaderSelection;
        var planning = video.h264_pictures.Options.forTrack(reader.packets.len);
        planning.decode.max_dependency_packets = limits.max_dependency_packets;
        const registry = if (reader.track.requiresAvcRegistry()) try video.h264_dynamic.Session.initPlanning(a, reader, planning.decode) else null;
        errdefer if (registry) |owner| owner.deinit();
        var logical: ?video.h264_pictures.Index = null;
        errdefer if (logical) |*index| index.deinit();
        if (try video.packet_sampling.needsLogicalIndex(a, reader.track))
            logical = try video.h264_pictures.planning(a, reader, planning, registry);
        const count = if (logical) |index| index.pictures.len else reader.packets.len;
        var metadata_bytes = try video.sampling.Timeline.storageBytes(count);
        if (reader.track.codec == .avc or reader.track.codec == .hevc) metadata_bytes = try std.math.add(usize, metadata_bytes, try std.math.mul(usize, reader.packets.len, @sizeOf(usize)));
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
        var seek_cache: ?video.decode_plan.SeekCache = if (reader.track.codec == .avc) try video.decode_plan.SeekCache.init(a, reader, true) else null;
        errdefer if (seek_cache) |*cache| cache.deinit();
        if (registry) |owner| {
            for (owner.progressive_idrs.items) |packet| seek_cache.?.nearest[packet] = packet;
        }
        var dependencies: ?Dependencies = if (logical) |index| try Dependencies.init(a, index) else null;
        errdefer if (dependencies) |*index| index.deinit();
        return .{ .hevc_predecessors = hevc_predecessors, .metadata_reservation = metadata_reservation, .dependencies = dependencies, .timeline = timeline, .seek_cache = seek_cache, .source_binding = model_video.immutableSourceBinding(reader), .continuation = .{ .allocator = a, .reader = reader, .parameter_sets = registry, .max_packets = limits.max_dependency_packets }, .allocator = a, .reader = reader, .logical = logical, .registry = registry, .origin = timeline.first, .pool = pool, .limits = limits };
    }
    pub fn fromSnapshot(a: std.mem.Allocator, reader: *media.mp4.Reader, pool: *media.admission.Pool, limits: @import("media_chunk_limits.zig").Limits, snapshot: *const Snapshot) !Owner {
        try reader.input.control.check();
        var logical = snapshot.logical;
        if (logical) |*index| index.allocator = a;
        var options = video.h264_pictures.Options.forTrack(reader.packets.len);
        options.decode.max_dependency_packets = limits.max_dependency_packets;
        const registry = if (snapshot.registry) |*shared| try shared.bind(a, reader, options.decode) else null;
        errdefer if (registry) |session| session.deinit();
        const seek_cache = if (reader.track.codec == .avc) video.decode_plan.SeekCache.forPage(a, reader, if (registry) |session| session.progressive_idrs.items else &.{}) else null;
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
        var first: usize = std.math.maxInt(usize);
        var stop: usize = 0;
        for (selected, packets) |id, *packet| {
            packet.* = if (self.logical) |index| index.pictures[id].first_packet else id;
            const dependency = if (self.reader.track.codec == .hevc)
                try video.hevc_restart.startFor(self.reader.packets, self.hevc_predecessors, packet.*)
            else
                packet.*;
            first = @min(first, dependency);
            const last = if (self.logical) |index| index.pictures[id].last_packet orelse return error.IncompleteVideoPicture else id;
            stop = @max(stop, last + 1);
        }
        if (self.reader.track.codec == .avc) {
            var plan = try video.decode_plan.create(a, self.reader, packets, .{ .seek_cache = if (self.seek_cache) |*cache| cache else null, .portable_config = true, .max_decode_packets = self.limits.max_dependency_packets, .max_run_packets = self.limits.max_dependency_packets });
            defer plan.deinit();
            for (plan.runs) |run| first = @min(first, run.first);
            // Reorder drain is bounded. Logical completion below extends this
            // prefix when a field/partition assembly crosses the nominal tail.
            stop = @min(self.reader.packets.len, stop +| 32);
        }
        if (self.logical) |index| {
            var changed = true;
            while (changed) {
                changed = false;
                const before_first = first;
                const before_stop = stop;
                self.dependencies.?.extend(index, 1, 0, self.dependencies.?.base, &first, &stop, policy.end_ns orelse return error.InvalidSamplingPolicy, self.origin, self.reader.track.timescale);
                changed = first != before_first or stop != before_stop;
                if (stop - first > self.limits.max_dependency_packets) return error.ChunkDependencyBudgetExceeded;
            }
        }
        if (stop <= first or stop - first > self.limits.max_dependency_packets) return error.ChunkDependencyBudgetExceeded;
        var coded_bytes: u64 = 0;
        for (self.reader.packets[first..stop]) |packet| coded_bytes = try std.math.add(u64, coded_bytes, packet.size);
        if (coded_bytes > self.limits.max_coded_window_bytes) return error.ChunkCodedWindowBudgetExceeded;
        var view = self.reader.*;
        view.packets = self.reader.packets[first..stop];
        const bootstrap = if (self.registry) |registry| try registry.bootstrapBefore(a, first) else null;
        defer if (bootstrap) |bytes| a.free(bytes);
        if (try std.math.add(u64, coded_bytes, if (bootstrap) |value| value.len else 0) > self.limits.max_coded_window_bytes) return error.ChunkCodedWindowBudgetExceeded;
        var window = try video.live_windows.borrowWithParameters(a, &view, bootstrap orelse &.{}, .{ .max_packets = self.limits.max_dependency_packets, .stream = .{ .max_bytes = self.limits.max_coded_window_bytes, .admission_pool = self.pool, .control = self.reader.input.control } });
        errdefer window.deinit();
        for (window.reader.packets) |*packet| packet.pts = try std.math.sub(i64, packet.pts, self.origin);
        var result = Window{ .coded = window, .packet_base = first, .source_binding = self.source_binding };
        result.packet_count = packets.len;
        for (packets, result.packet_ids[0..packets.len]) |packet, *id| id.* = packet - first;
        return result;
    }
};
pub const Window = struct {
    coded: video.live_windows.Window,
    source_binding: [32]u8,
    packet_ids: [32]usize = undefined,
    packet_count: usize = 0,
    selection: ?model_video.ReaderSelection = null,
    packet_base: usize,
    /// Bind only after the Window reaches its final address; workers borrow it.
    pub fn bind(self: *Window, policy: video.sampling.TimestampPolicy, pool: *media.admission.Pool) !void {
        self.selection = try model_video.ReaderSelection.fromPacketIds(self.coded.reader.allocator, &self.coded.reader, .{ .source_timestamps = policy }, self.packet_ids[0..self.packet_count], pool, self.coded.reader.input.control, self.source_binding, self.packet_base);
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
