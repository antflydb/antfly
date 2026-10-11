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

//! Logical displayed pictures for WebM model sampling. Indexing retains metadata
//! only; replay reconstructs dependencies once and publishes selected pictures.
const std = @import("std");
const media = @import("antfly_media");
const codecs = @import("codecs/decoder.zig");
const frames = @import("codecs/frame.zig");
const vp8 = @import("vp8.zig");
const sampling = @import("sampling.zig");
pub const Picture = struct { pts: i64, duration: ?u64, width: u32, height: u32 };
pub const Options = struct { max_packets: usize = 8192, max_pictures: usize = 65536, decoder: codecs.Options = .{} };
pub const Index = struct {
    allocator: std.mem.Allocator,
    pictures: []Picture,
    duration_seconds: ?f64 = null,
    pub fn deinit(self: *Index) void {
        self.allocator.free(self.pictures);
        self.* = undefined;
    }
    pub fn select(self: Index) ![]usize {
        if (self.pictures.len == 0) return error.EmptyVideo;
        const order = try self.allocator.alloc(usize, self.pictures.len);
        defer self.allocator.free(order);
        for (order, 0..) |*id, i| id.* = i;
        const Less = struct {
            fn less(pictures: []const Picture, l: usize, r: usize) bool {
                return pictures[l].pts < pictures[r].pts or (pictures[l].pts == pictures[r].pts and l < r);
            }
        };
        std.mem.sort(usize, order, @as([]const Picture, self.pictures), Less.less);
        const first = self.pictures[order[0]].pts;
        const last = self.pictures[order[order.len - 1]];
        // Without an explicit last duration, infer the final positive PTS step.
        // If no rate can be established, upstream falls back to capped indices.
        var tail = last.duration;
        if (tail == null) {
            var i = order.len - 1;
            while (i > 0) : (i -= 1) {
                const delta = @as(i128, last.pts) - self.pictures[order[i - 1]].pts;
                if (delta > 0) {
                    tail = std.math.cast(u64, delta) orelse return error.TimestampOverflow;
                    break;
                }
            }
        }
        const span = @as(i128, last.pts) - first + (tail orelse 0);
        const seconds: ?f64 = self.duration_seconds orelse if (span > 0) @as(f64, @floatFromInt(span)) / 1e9 else null;
        const selected = try sampling.embeddingGemma2(self.allocator, .{ .total_frames = order.len, .duration_seconds = seconds, .fps = if (seconds) |s| @as(f64, @floatFromInt(order.len)) / s else null }, .{});
        for (selected) |*id| id.* = order[id.*];
        return selected;
    }
};
const Collect = struct {
    allocator: std.mem.Allocator,
    pictures: std.ArrayList(Picture) = .empty,
    limit: usize,
    fn accept(raw: *anyopaque, _: usize, frame: *const codecs.Frame) !void {
        const self: *Collect = @ptrCast(@alignCast(raw));
        if (self.pictures.items.len >= self.limit) return error.ResourceLimitExceeded;
        try self.pictures.append(self.allocator, .{ .pts = frame.pts_ns, .duration = frame.duration_ns, .width = frame.width, .height = frame.height });
    }
};
pub fn index(a: std.mem.Allocator, reader: *media.webm.Reader, options: Options) !Index {
    var collector = Collect{ .allocator = a, .limit = options.max_pictures };
    defer collector.pictures.deinit(a);
    try walk(a, reader, options, null, &collector, Collect.accept);
    return .{ .allocator = a, .pictures = try collector.pictures.toOwnedSlice(a), .duration_seconds = reader.duration_seconds };
}
/// IDs are visible decoder-output ordinals, never container packet ordinals.
/// The callback borrows the frame until it returns. Repeated selections publish
/// once; the model reuses that projection in each requested presentation slot.
pub fn walk(a: std.mem.Allocator, reader: *media.webm.Reader, options: Options, selected: ?[]const usize, context: *anyopaque, accept: *const fn (*anyopaque, usize, *const codecs.Frame) anyerror!void) !void {
    if (reader.packets.len > options.max_packets) return error.ResourceLimitExceeded;
    if (selected) |ids| if (ids.len == 0) return;
    var last: usize = 0;
    if (selected) |ids| for (ids) |id| {
        last = @max(last, id);
    };
    var logical: usize = 0;
    if (reader.track.codec == .vp8) {
        const session = try vp8.Session.init(a, reader, .{ .max_packet_bytes = options.decoder.max_packet_bytes, .max_pixels = options.decoder.max_pixels, .max_decode_bytes = options.decoder.max_decode_bytes, .admission_pool = options.decoder.admission_pool });
        defer session.deinit();
        while (session.next_index < reader.packets.len) {
            try options.decoder.control.check();
            var decoded = (try session.next()) orelse continue;
            defer decoded.deinit();
            if (logical >= options.max_pictures) return error.ResourceLimitExceeded;
            if (selected == null or std.mem.indexOfScalar(usize, selected.?, logical) != null) {
                const p = decoded.planes;
                var frame = try frames.copy(a, options.decoder.admission_pool, @intCast(p.width), @intCast(p.height), 8, 1, .{ .matrix = 6 }, .{ p.y.ptr, p.u.ptr, p.v.ptr }, .{ p.y_stride, p.uv_stride, p.uv_stride }, options.decoder.max_output_bytes);
                defer frame.deinit();
                frame.pts_ns = decoded.pts_ns;
                frame.duration_ns = decoded.duration_ns;
                try accept(context, logical, &frame);
            }
            logical += 1;
            if (selected != null and logical > last) return;
        }
    } else {
        const session = try codecs.Session.create(a, if (reader.track.codec == .vp9) .vp9 else .av1, options.decoder);
        defer session.destroy();
        for (reader.packets, 0..) |packet, packet_id| {
            var batch = try session.decodeWebmPacket(reader, packet_id);
            defer batch.deinit();
            // Container-invisible packets still update codec references.
            if (packet.invisible) continue;
            for (batch.frames) |*frame| {
                if (logical >= options.max_pictures) return error.ResourceLimitExceeded;
                if (selected == null or std.mem.indexOfScalar(usize, selected.?, logical) != null) try accept(context, logical, frame);
                logical += 1;
                if (selected != null and logical > last) return;
            }
        }
    }
    if (selected != null and logical <= last) return error.InvalidPacketIndex;
}

test "WebM logical pictures sample visible VP8 VP9 AV1 and replay selected pictures" {
    const a = std.testing.allocator;
    inline for (.{ "../testdata/vp8-altref.webm", "../testdata/vpx-av1/vp9.webm", "../testdata/vpx-av1/av1.webm" }) |path| {
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile(path) }, .identity = path };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        var pictures = try index(a, &reader, .{});
        defer pictures.deinit();
        try std.testing.expect(pictures.pictures.len > 0);
        const selected = try pictures.select();
        defer a.free(selected);
        var collector = Collect{ .allocator = a, .limit = 32 };
        defer collector.pictures.deinit(a);
        try walk(a, &reader, .{}, selected, &collector, Collect.accept);
        var unique: usize = 0;
        for (selected, 0..) |id, i| if (std.mem.indexOfScalar(usize, selected[0..i], id) == null) {
            unique += 1;
        };
        try std.testing.expectEqual(unique, collector.pictures.items.len);
        try std.testing.expectError(error.ResourceLimitExceeded, index(a, &reader, .{ .max_packets = 0 }));
    }
}

test "WebM picture sampling orders timestamps and falls back when duration is absent" {
    const a = std.testing.allocator;
    var index_value = Index{ .allocator = a, .pictures = try a.dupe(Picture, &.{
        .{ .pts = 1_000_000_000, .duration = null, .width = 48, .height = 48 },
        .{ .pts = 0, .duration = null, .width = 48, .height = 48 },
        .{ .pts = 500_000_000, .duration = null, .width = 48, .height = 48 },
    }) };
    defer index_value.deinit();
    const selected = try index_value.select();
    defer a.free(selected);
    try std.testing.expectEqualSlices(usize, &.{1}, selected);
    for (index_value.pictures) |*picture| picture.pts = 0;
    const no_rate = try index_value.select();
    defer a.free(no_rate);
    try std.testing.expectEqualSlices(usize, &.{ 0, 1, 2 }, no_rate);
}

test "WebM replay callback failures release sessions and permit a fresh replay" {
    const a = std.testing.allocator;
    inline for (.{ "../testdata/vp8-inter.webm", "../testdata/vpx-av1/vp9.webm", "../testdata/vpx-av1/av1.webm" }) |path| {
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile(path) }, .identity = path };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        const retained_metadata = source.retained_bytes;
        const Fail = struct {
            fn accept(_: *anyopaque, _: usize, _: *const codecs.Frame) !void {
                return error.InjectedConsumerFailure;
            }
        };
        var collector = Collect{ .allocator = a, .limit = 32 };
        defer collector.pictures.deinit(a);
        try std.testing.expectError(error.InjectedConsumerFailure, walk(a, &reader, .{}, &.{0}, &collector, Fail.accept));
        try walk(a, &reader, .{}, &.{0}, &collector, Collect.accept);
        try std.testing.expectEqual(@as(usize, 1), collector.pictures.items.len);
        try std.testing.expectError(error.ResourceLimitExceeded, index(a, &reader, .{ .max_pictures = 0 }));
        try std.testing.expectEqual(retained_metadata, source.retained_bytes);
    }
}

test "WebM declared duration preserves two-second FPS sampling at rounded PTS boundaries" {
    const a = std.testing.allocator;
    inline for (.{ "../testdata/vpx-av1/vp9.webm", "../testdata/vpx-av1/av1.webm" }) |path| {
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile(path) }, .identity = path };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        try std.testing.expectEqual(@as(?f64, 2), reader.duration_seconds);
        var pictures = try index(a, &reader, .{});
        defer pictures.deinit();
        const selected = try pictures.select();
        defer a.free(selected);
        try std.testing.expectEqualSlices(usize, &.{ 0, 6 }, selected);
    }
}
