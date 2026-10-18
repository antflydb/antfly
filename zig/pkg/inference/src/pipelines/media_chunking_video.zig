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
pub const Owner = struct {
    continuation: model_video.DecoderContinuation,
    source_binding: [32]u8,
    allocator: std.mem.Allocator,
    reader: *media.mp4.Reader,
    timed: []video.sampling.TimedPicture,
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
        if (reader.input.identity.len == 0) return error.InvalidVideoReaderSelection;
        const registry = if (reader.track.inband_parameter_sets) try video.h264_dynamic.Session.initPlanning(a, reader, .{ .max_parameter_packets = reader.packets.len }) else null;
        errdefer if (registry) |owner| owner.deinit();
        var logical: ?video.h264_pictures.Index = null;
        errdefer if (logical) |*index| index.deinit();
        if (reader.track.codec == .avc) {
            const cfg = try video.h264.configParse(a, reader.track.avcc);
            defer cfg.groups.deinit(a);
            if (reader.track.inband_parameter_sets or !cfg.frame_only or cfg.profile == 88 or cfg.separate_planes or cfg.groups.count > 1 or cfg.redundant) logical = try video.h264_pictures.planning(a, reader, .{ .max_packets = reader.packets.len, .max_pictures = reader.packets.len }, registry);
        }
        const count = if (logical) |index| index.pictures.len else reader.packets.len;
        var metadata_bytes = try std.math.add(usize, try video.sampling.Timeline.storageBytes(count), try std.math.mul(usize, count, @sizeOf(video.sampling.TimedPicture)));
        if (reader.track.codec == .avc) metadata_bytes = try std.math.add(usize, metadata_bytes, try std.math.mul(usize, reader.packets.len, @sizeOf(usize)));
        if (logical != null) {
            const base = try std.math.ceilPowerOfTwo(usize, @max(@as(usize, 1), count));
            metadata_bytes = try std.math.add(usize, metadata_bytes, try std.math.mul(usize, try std.math.add(usize, count, try std.math.mul(usize, base, 2)), @sizeOf(usize)));
        }
        var metadata_reservation = try pool.acquire(.{ .host_bytes = metadata_bytes });
        errdefer metadata_reservation.deinit();
        const timed = try a.alloc(video.sampling.TimedPicture, count);
        errdefer a.free(timed);
        var origin: i64 = std.math.maxInt(i64);
        if (logical) |index| {
            for (timed, index.pictures) |*dest, picture| {
                dest.* = .{ .pts = picture.pts, .duration = picture.duration };
                origin = @min(origin, picture.pts);
            }
        } else {
            for (timed, reader.packets) |*dest, packet| {
                dest.* = .{ .pts = packet.pts, .duration = packet.duration };
                origin = @min(origin, packet.pts);
            }
        }
        var timeline = try video.sampling.Timeline.init(a, timed, reader.input.control);
        errdefer timeline.deinit();
        var seek_cache: ?video.decode_plan.SeekCache = if (reader.track.codec == .avc) try video.decode_plan.SeekCache.init(a, reader, true) else null;
        errdefer if (seek_cache) |*cache| cache.deinit();
        var dependencies: ?Dependencies = if (logical) |index| try Dependencies.init(a, index) else null;
        errdefer if (dependencies) |*index| index.deinit();
        return .{ .metadata_reservation = metadata_reservation, .dependencies = dependencies, .timeline = timeline, .seek_cache = seek_cache, .source_binding = model_video.immutableSourceBinding(reader), .continuation = .{ .allocator = a, .reader = reader, .parameter_sets = registry, .max_packets = limits.max_dependency_packets }, .allocator = a, .reader = reader, .timed = timed, .logical = logical, .registry = registry, .origin = origin, .pool = pool, .limits = limits };
    }
    pub fn deinit(self: *Owner) void {
        self.continuation.deinit();
        self.timeline.deinit();
        if (self.seek_cache) |*cache| cache.deinit();
        if (self.registry) |registry| registry.deinit();
        if (self.dependencies) |*index| index.deinit();
        if (self.logical) |*index| index.deinit();
        self.allocator.free(self.timed);
        self.metadata_reservation.deinit();
    }
    pub fn durationNs(self: *const Owner) !i64 {
        var stop: i128 = self.origin;
        for (self.timed) |picture| stop = @max(stop, @as(i128, picture.pts) + (picture.duration orelse return error.UnknownVideoDuration));
        return std.math.cast(i64, @divFloor((stop - self.origin) * std.time.ns_per_s + self.reader.track.timescale - 1, self.reader.track.timescale)) orelse error.TimestampOverflow;
    }
    pub fn prepare(self: *Owner, policy: video.sampling.TimestampPolicy) !Window {
        const a = self.allocator;
        const selected = try self.timeline.select(a, self.reader.track.timescale, policy, self.reader.input.control, true);
        defer a.free(selected);
        const packets = try a.alloc(usize, selected.len);
        defer a.free(packets);
        var first: usize = std.math.maxInt(usize);
        var stop: usize = 0;
        for (selected, packets) |id, *packet| {
            packet.* = if (self.logical) |index| index.pictures[id].first_packet else id;
            first = @min(first, packet.*);
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
        var subset: ?video.h264_pictures.Index = null;
        errdefer if (subset) |*index| index.deinit();
        if (self.logical) |index| {
            var pictures = std.ArrayList(video.h264.Picture).empty;
            errdefer pictures.deinit(a);
            var pending_pts: ?i64 = null;
            var pending_packet: ?usize = null;
            const deps = &self.dependencies.?;
            const begin = deps.lower(index, first);
            const finish = deps.lower(index, stop);
            const local_ids = try a.dupe(usize, deps.ids[begin..finish]);
            defer a.free(local_ids);
            std.mem.sort(usize, local_ids, {}, std.sort.asc(usize));
            for (local_ids) |id| {
                const picture = index.pictures[id];
                const last = picture.last_packet.?;
                if (last >= stop) {
                    const pts = try std.math.sub(i64, picture.pts, self.origin);
                    pending_pts = if (pending_pts) |old| @min(old, pts) else pts;
                    pending_packet = if (pending_packet) |old| @min(old, picture.first_packet - first) else picture.first_packet - first;
                    continue;
                }
                var copy = picture;
                copy.first_packet -= first;
                copy.last_packet = last - first;
                copy.pts = try std.math.sub(i64, picture.pts, self.origin);
                try pictures.append(a, copy);
            }
            subset = .{ .allocator = a, .pictures = try pictures.toOwnedSlice(a), .timescale = self.reader.track.timescale, .packet_count = window.reader.packets.len, .pending_pts = pending_pts, .pending_first_packet = pending_packet };
        }
        if (subset == null) {
            const cfg = if (self.reader.track.codec == .avc) try video.h264.configParse(a, self.reader.track.avcc) else null;
            defer if (cfg) |configuration| configuration.groups.deinit(a);
            const pictures = try a.alloc(video.h264.Picture, window.reader.packets.len);
            for (pictures, window.reader.packets, 0..) |*picture, packet, id| picture.* = .{ .first_packet = id, .last_packet = id, .pts = packet.pts, .duration = packet.duration, .width = if (cfg) |configuration| @intCast(configuration.width) else self.reader.track.width, .height = if (cfg) |configuration| @intCast(configuration.height) else self.reader.track.height, .bit_depth = if (cfg) |configuration| configuration.bit_depth else 8, .chroma_format = if (cfg) |configuration| configuration.chroma_format else 1 };
            subset = .{ .allocator = a, .pictures = pictures, .timescale = self.reader.track.timescale, .packet_count = pictures.len };
        }
        return .{ .coded = window, .logical = subset, .packet_base = first, .source_binding = self.source_binding };
    }
};
pub const Window = struct {
    coded: video.live_windows.Window,
    source_binding: [32]u8,
    logical: ?video.h264_pictures.Index = null,
    selection: ?model_video.ReaderSelection = null,
    packet_base: usize,
    /// Bind only after the Window reaches its final address; workers borrow it.
    pub fn bind(self: *Window, policy: video.sampling.TimestampPolicy, pool: *media.admission.Pool) !void {
        if (self.logical) |*index| self.selection = try model_video.ReaderSelection.initImmutable(self.coded.reader.allocator, &self.coded.reader, .{ .source_timestamps = policy }, index, pool, self.coded.reader.input.control, self.source_binding, self.packet_base);
    }
    pub fn deinit(self: *Window) void {
        if (self.selection) |*selection| selection.deinit();
        if (self.logical) |*index| index.deinit();
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
