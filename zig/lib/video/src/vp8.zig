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

//! Pure Zig VP8 displayed-keyframe decoding. Interframes remain unsupported.
const std = @import("std");
const media = @import("antfly_media");
const image = @import("antfly_image");
const Budget = @import("decode_budget.zig").Budget;
pub const Options = struct {
    max_packet_bytes: usize = 16 * 1024 * 1024,
    max_pixels: usize = 16 * 1024 * 1024,
    max_decode_bytes: usize = 128 * 1024 * 1024,
    admission_pool: ?*media.admission.Pool = null,
};
pub const Frame = struct {
    allocator: std.mem.Allocator,
    planes: image.webp.Vp8FramePlanes,
    decode_index: usize,
    pts_ns: i64,
    duration_ns: ?u64,
    decode_high_water: usize,
    reservation: media.admission.Token = .{},
    pub fn deinit(self: *Frame) void {
        self.planes.deinit(self.allocator);
        self.reservation.deinit();
        self.* = undefined;
    }
};
/// Checks the coded frame tag independently of the container sync hint.
/// Returns native padded 8-bit 4:2:0 planes without an RGB round trip.
pub fn decodeFrame(a: std.mem.Allocator, reader: *media.webm.Reader, index: usize, options: Options) !Frame {
    try reader.input.control.check();
    if (reader.track.codec != .vp8) return error.UnsupportedVideoCodec;
    if (index >= reader.packets.len) return error.InvalidPacketIndex;
    const packet = reader.packets[index];
    if (packet.invisible) return error.UnsupportedVideoProfile;
    if (packet.size > options.max_packet_bytes) return error.ResourceLimitExceeded;
    var lease = try reader.readPacket(index);
    defer lease.deinit();
    var reservation = if (options.admission_pool) |pool| try pool.acquire(.{ .host_bytes = options.max_decode_bytes }) else media.admission.Token{};
    errdefer reservation.deinit();
    var budget = Budget{ .backing = a, .limit = options.max_decode_bytes };
    var scope = image.work_control.Scope.enter(.{ .context = reader.input.control.context, .check_fn = reader.input.control.check_fn });
    defer scope.deinit();
    var planes = image.webp.decodeVp8KeyframePlanesLimited(budget.allocator(), lease.bytes, .{ .max_pixels = options.max_pixels, .max_rgba_bytes = options.max_decode_bytes }) catch |err| {
        return if (err == error.ImageTooLarge or (err == error.OutOfMemory and budget.denied)) error.ResourceLimitExceeded else err;
    };
    errdefer planes.deinit(a);
    if (planes.width != reader.track.width or planes.height != reader.track.height) return error.UnsupportedDynamicGeometry;
    try reader.input.control.check();
    const retained = planes.y.len + planes.u.len + planes.v.len;
    std.debug.assert(budget.live == retained);
    try reservation.resize(.{ .host_bytes = retained });
    return .{ .allocator = a, .planes = planes, .decode_index = index, .pts_ns = packet.pts, .duration_ns = packet.duration_ns, .decode_high_water = budget.peak, .reservation = reservation };
}

test "VP8 WebM keyframe planes match independent FFmpeg decode and enforce limits" {
    const a = std.testing.allocator;
    var input = media.source.Source{ .allocator = a, .identity = "vp8-keyframes", .storage = .{ .borrowed = @embedFile("../testdata/vp8-keyframes.webm") } };
    var reader = try media.webm.Reader.init(a, &input, .{});
    defer reader.deinit();
    const expected = @embedFile("../testdata/vp8-keyframes.yuv");
    try std.testing.expectEqual(@as(usize, 3), reader.packets.len);
    var cursor: usize = 0;
    for (0..reader.packets.len) |index| {
        var frame = try decodeFrame(a, &reader, index, .{});
        defer frame.deinit();
        const p = frame.planes;
        for ([_][]const u8{ p.y, p.u, p.v }, [_]usize{ p.y_stride, p.uv_stride, p.uv_stride }, [_]usize{ p.width, (p.width + 1) / 2, (p.width + 1) / 2 }, [_]usize{ p.height, (p.height + 1) / 2, (p.height + 1) / 2 }) |plane, stride, width, height| {
            for (0..height) |y| {
                try std.testing.expectEqualSlices(u8, expected[cursor..][0..width], plane[y * stride ..][0..width]);
                cursor += width;
            }
        }
    }
    try std.testing.expectEqual(expected.len, cursor);
    try std.testing.expectError(error.ResourceLimitExceeded, decodeFrame(a, &reader, 0, .{ .max_decode_bytes = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, decodeFrame(a, &reader, 0, .{ .max_packet_bytes = 1 }));
    try std.testing.expectEqual(@as(usize, 0), input.retained_bytes - reader.metadata.bytes.len);
    try std.testing.expectError(error.UnsupportedVp8Interframe, image.webp.decodeVp8KeyframePlanesLimited(a, &.{ 1, 0, 0 }, image.DecodeLimits.inference_default));
}

test "VP8 decoder allocation failures and shared admission retain no abandoned planes" {
    const a = std.testing.allocator;
    var input = media.source.Source{ .allocator = a, .identity = "vp8-keyframes", .storage = .{ .borrowed = @embedFile("../testdata/vp8-keyframes.webm") } };
    var reader = try media.webm.Reader.init(a, &input, .{});
    defer reader.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
    const options = Options{ .max_decode_bytes = 1024 * 1024, .admission_pool = &pool };
    var baseline = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    var kept = try decodeFrame(baseline.allocator(), &reader, 0, options);
    const bytes = kept.planes.y.len + kept.planes.u.len + kept.planes.v.len;
    try std.testing.expectEqual(bytes, pool.snapshot().host_bytes);
    try std.testing.expectError(error.SharedAdmissionExceeded, decodeFrame(a, &reader, 1, options));
    kept.deinit();
    for (0..baseline.alloc_index) |index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = index, .resize_fail_index = 0 });
        try std.testing.expectError(error.OutOfMemory, decodeFrame(failing.allocator(), &reader, 0, options));
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        try std.testing.expectEqual(@as(u64, 0), pool.snapshot().host_bytes);
    }
}

test "VP8 cancellation during reconstruction releases workspace and permits retry" {
    const a = std.testing.allocator;
    var input = media.source.Source{ .allocator = a, .identity = "vp8-keyframes", .storage = .{ .borrowed = @embedFile("../testdata/vp8-keyframes.webm") } };
    var reader = try media.webm.Reader.init(a, &input, .{});
    defer reader.deinit();
    const retained = input.retained_bytes;
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.calls += 1;
            if (self.calls >= 5) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    input.control = .{ .context = &cancel, .check_fn = Cancel.check };
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
    const options = Options{ .max_decode_bytes = 1024 * 1024, .admission_pool = &pool };
    try std.testing.expectError(error.Cancelled, decodeFrame(a, &reader, 0, options));
    try std.testing.expectEqual(retained, input.retained_bytes);
    try std.testing.expectEqual(@as(u64, 0), pool.snapshot().host_bytes);
    input.control = .{};
    var frame = try decodeFrame(a, &reader, 0, options);
    frame.deinit();
    try std.testing.expectEqual(@as(u64, 0), pool.snapshot().host_bytes);
}
