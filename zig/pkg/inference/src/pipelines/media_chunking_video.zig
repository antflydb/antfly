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
pub const Owner = struct {
    continuation: model_video.DecoderContinuation,
    allocator: std.mem.Allocator,
    reader: *media.mp4.Reader,
    timed: []video.sampling.TimedPicture,
    logical: ?video.h264_pictures.Index = null,
    registry: ?*video.h264_dynamic.Session = null,
    origin: i64,
    pool: *media.admission.Pool,
    limits: @import("media_chunk_limits.zig").Limits,
    pub fn init(a: std.mem.Allocator, reader: *media.mp4.Reader, pool: *media.admission.Pool, limits: @import("media_chunk_limits.zig").Limits) !Owner {
        if (reader.packets.len == 0) return error.EmptyVideo;
        var logical: ?video.h264_pictures.Index = null;
        errdefer if (logical) |*index| index.deinit();
        if (reader.track.codec == .avc) {
            const cfg = try video.h264.configParse(a, reader.track.avcc);
            defer cfg.groups.deinit(a);
            if (reader.track.inband_parameter_sets or !cfg.frame_only or cfg.profile == 88 or cfg.separate_planes or cfg.groups.count > 1 or cfg.redundant) logical = try video.h264_pictures.index(a, reader, .{});
        }
        const timed = try a.alloc(video.sampling.TimedPicture, if (logical) |index| index.pictures.len else reader.packets.len);
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
        const registry = if (reader.track.inband_parameter_sets) try video.h264_dynamic.Session.init(a, reader, .{}) else null;
        return .{ .continuation = .{ .allocator = a, .reader = reader, .parameter_sets = registry, .max_packets = limits.max_dependency_packets }, .allocator = a, .reader = reader, .timed = timed, .logical = logical, .registry = registry, .origin = origin, .pool = pool, .limits = limits };
    }
    pub fn deinit(self: *Owner) void {
        self.continuation.deinit();
        if (self.registry) |registry| registry.deinit();
        if (self.logical) |*index| index.deinit();
        self.allocator.free(self.timed);
    }
    pub fn durationNs(self: *const Owner) !i64 {
        var stop: i128 = self.origin;
        for (self.timed) |picture| stop = @max(stop, @as(i128, picture.pts) + (picture.duration orelse return error.UnknownVideoDuration));
        return std.math.cast(i64, @divFloor((stop - self.origin) * std.time.ns_per_s + self.reader.track.timescale - 1, self.reader.track.timescale)) orelse error.TimestampOverflow;
    }
    pub fn prepare(self: *Owner, policy: video.sampling.TimestampPolicy) !Window {
        const a = self.allocator;
        const selected = try video.sampling.cappedTimestamps(a, self.timed, self.reader.track.timescale, policy, self.reader.input.control);
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
            var plan = try video.decode_plan.create(a, self.reader, packets, .{ .portable_config = true, .max_decode_packets = self.limits.max_dependency_packets, .max_run_packets = self.limits.max_dependency_packets });
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
                for (index.pictures) |picture| {
                    const last = picture.last_packet orelse return error.IncompleteVideoPicture;
                    if (picture.first_packet < first and last >= first and last < stop) {
                        first = picture.first_packet;
                        changed = true;
                    }
                    const before_end = (@as(i128, picture.pts) - self.origin) * std.time.ns_per_s < @as(i128, policy.end_ns orelse return error.InvalidSamplingPolicy) * self.reader.track.timescale;
                    if (picture.first_packet >= first and picture.first_packet < stop and last >= stop and before_end) {
                        stop = last + 1;
                        changed = true;
                    }
                }
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
        var window = try video.live_windows.copyWithParameters(a, self.reader.input.identity, &view, 0, bootstrap orelse &.{}, .{ .max_packets = self.limits.max_dependency_packets, .stream = .{ .max_bytes = self.limits.max_coded_window_bytes, .admission_pool = self.pool, .control = self.reader.input.control } });
        errdefer window.deinit();
        for (window.reader.packets) |*packet| packet.pts = try std.math.sub(i64, packet.pts, self.origin);
        var subset: ?video.h264_pictures.Index = null;
        errdefer if (subset) |*index| index.deinit();
        if (self.logical) |index| {
            var pictures = std.ArrayList(video.h264.Picture).empty;
            errdefer pictures.deinit(a);
            var pending_pts: ?i64 = null;
            var pending_packet: ?usize = null;
            for (index.pictures) |picture| {
                if (picture.first_packet < first or picture.first_packet >= stop) continue;
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
        return .{ .coded = window, .logical = subset, .packet_base = first };
    }
};
pub const Window = struct {
    coded: video.live_windows.Window,
    logical: ?video.h264_pictures.Index = null,
    selection: ?model_video.ReaderSelection = null,
    packet_base: usize,
    /// Bind only after the Window reaches its final address; workers borrow it.
    pub fn bind(self: *Window, policy: video.sampling.TimestampPolicy, pool: *media.admission.Pool) !void {
        if (self.logical) |*index| self.selection = try model_video.ReaderSelection.init(self.coded.reader.allocator, &self.coded.reader, .{ .source_timestamps = policy }, index, pool, self.coded.reader.input.control);
    }
    pub fn deinit(self: *Window) void {
        if (self.selection) |*selection| selection.deinit();
        if (self.logical) |*index| index.deinit();
        self.coded.deinit();
    }
};
