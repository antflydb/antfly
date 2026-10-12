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

//! Pure Zig VP8 keyframes and stateful interframe decoding.
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

/// Sequential coded-packet decoder, including invisible reference pictures.
/// Output frames are independently owned and can outlive this session. Decode
/// order is enforced; errors leave the same packet available for retry.
pub const Session = struct {
    allocator: std.mem.Allocator,
    reader: *media.webm.Reader,
    options: Options,
    budget: Budget,
    decoder: image.webp.Vp8Decoder = .{},
    next_index: usize = 0,
    reservation: media.admission.Token,

    pub fn init(a: std.mem.Allocator, reader: *media.webm.Reader, options: Options) !*Session {
        if (reader.track.codec != .vp8) return error.UnsupportedVideoCodec;
        if (reader.track.width == 0 or reader.track.height == 0 or reader.track.width > 16384 or reader.track.height > 16384) return error.InvalidVideoGeometry;
        if (@as(u64, reader.track.width) * reader.track.height > options.max_pixels) return error.ResourceLimitExceeded;
        const self = try a.create(Session);
        errdefer a.destroy(self);
        const reservation = if (options.admission_pool) |pool| try pool.acquire(.{ .host_bytes = options.max_decode_bytes }) else media.admission.Token{};
        self.* = .{ .allocator = a, .reader = reader, .options = options, .budget = .{ .backing = a, .limit = options.max_decode_bytes }, .reservation = reservation };
        return self;
    }
    pub fn deinit(self: *Session) void {
        self.decoder.deinit(self.budget.allocator());
        std.debug.assert(self.budget.live == 0);
        self.reservation.deinit();
        self.allocator.destroy(self);
    }
    /// A null result denotes an invisible decoded reference picture; it still
    /// consumes one coded packet. A null result at EOF consumes nothing.
    pub fn next(self: *Session) !?Frame {
        try self.reader.input.control.check();
        if (self.next_index == self.reader.packets.len) return null;
        const index = self.next_index;
        const packet = self.reader.packets[index];
        if (packet.size > self.options.max_packet_bytes) return error.ResourceLimitExceeded;
        var lease = try self.reader.readPacket(index);
        defer lease.deinit();
        return self.decodePacketBytes(lease.bytes, self.reader.input.control);
    }
    /// Decode the next indexed packet from an already acquired lease. Keeping
    /// source errors outside this call permits callers to distinguish corrupt
    /// coded bytes from transport failures. On decode failure, destroy/restart
    /// the session; partially updated references must not be reused.
    pub fn decodePacketBytes(self: *Session, bytes: []const u8, control: media.source.Control) !?Frame {
        try control.check();
        if (self.next_index >= self.reader.packets.len) return error.InvalidPacketIndex;
        if (bytes.len > self.options.max_packet_bytes) return error.ResourceLimitExceeded;
        const index = self.next_index;
        const packet = self.reader.packets[index];
        if (bytes.len < 3) return error.WebpDecodeFailed;
        if (bytes[0] & 1 == 0) {
            if (bytes.len < 10) return error.WebpDecodeFailed;
            const width = std.mem.readInt(u16, bytes[6..8], .little) & 0x3fff;
            const height = std.mem.readInt(u16, bytes[8..10], .little) & 0x3fff;
            if (width != self.reader.track.width or height != self.reader.track.height) return error.UnsupportedDynamicGeometry;
        }
        const visible = bytes[0] & 16 != 0 and !packet.invisible;
        const w: usize = (self.reader.track.width + 15) / 16 * 16;
        const h: usize = (self.reader.track.height + 15) / 16 * 16;
        const output_bytes = w * h * 3 / 2;
        var reservation = if (visible and self.options.admission_pool != null) try self.options.admission_pool.?.acquire(.{ .host_bytes = output_bytes }) else media.admission.Token{};
        errdefer reservation.deinit();
        var scope = image.work_control.Scope.enter(.{ .context = control.context, .check_fn = control.check_fn });
        defer scope.deinit();
        self.budget.denied = false;
        var p = self.decoder.decode(self.budget.allocator(), bytes, .{ .max_pixels = self.options.max_pixels, .max_rgba_bytes = self.options.max_decode_bytes }) catch |err| {
            return if (err == error.ImageTooLarge or (err == error.OutOfMemory and self.budget.denied)) error.ResourceLimitExceeded else err;
        };
        self.next_index += 1;
        if (!visible) {
            p.deinit(self.budget.allocator());
            return null;
        }
        const retained = p.y.len + p.u.len + p.v.len;
        std.debug.assert(retained == output_bytes);
        // Transfer the output to the backing allocator and its own admission
        // token. Only reference pictures remain charged to the session budget.
        self.budget.live -= retained;
        return .{ .allocator = self.allocator, .planes = p, .decode_index = index, .pts_ns = packet.pts, .duration_ns = packet.duration_ns, .decode_high_water = self.budget.peak, .reservation = reservation };
    }
};

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

fn differential(bytes: []const u8, expected: []const u8, count: usize) ![5]usize {
    const a = std.testing.allocator;
    var input = media.source.Source{ .allocator = a, .identity = "vp8-inter", .storage = .{ .borrowed = bytes } };
    var reader = try media.webm.Reader.init(a, &input, .{});
    defer reader.deinit();
    var decoder = image.webp.Vp8Decoder{};
    defer decoder.deinit(a);
    var cursor: usize = 0;
    try std.testing.expect(reader.packets.len >= count);
    for (0..reader.packets.len) |index| {
        var lease = try reader.readPacket(index);
        defer lease.deinit();
        var p = try decoder.decode(a, lease.bytes, image.DecodeLimits.inference_default);
        defer p.deinit(a);
        if (lease.bytes[0] & 16 == 0) continue;
        for ([_][]const u8{ p.y, p.u, p.v }, [_]usize{ p.y_stride, p.uv_stride, p.uv_stride }, [_]usize{ p.width, (p.width + 1) / 2, (p.width + 1) / 2 }, [_]usize{ p.height, (p.height + 1) / 2, (p.height + 1) / 2 }) |plane, stride, width, height| {
            for (0..height) |y| {
                if (!std.mem.eql(u8, expected[cursor..][0..width], plane[y * stride ..][0..width])) std.debug.print("VP8 differential failure frame={d} row={d} cursor={d} modes={any} split={any}\n", .{ index, y, cursor, decoder.modes, decoder.split_shapes });
                try std.testing.expectEqualSlices(u8, expected[cursor..][0..width], plane[y * stride ..][0..width]);
                cursor += width;
            }
        }
    }
    try std.testing.expectEqual(expected.len, cursor);
    try std.testing.expect(decoder.modes[0] > 0);
    try std.testing.expect(decoder.modes[3] > 0);
    return decoder.modes;
}

test "VP8 interframes match independent FFmpeg planar pixels" {
    const modes = try differential(@embedFile("../testdata/vp8-inter.webm"), @embedFile("../testdata/vp8-inter.yuv"), 24);
    try std.testing.expect(modes[4] > 0);
    _ = try differential(@embedFile("../testdata/vp8-altref.webm"), @embedFile("../testdata/vp8-altref.yuv"), 48);
    _ = try differential(@embedFile("../testdata/vp8-bilinear.webm"), @embedFile("../testdata/vp8-bilinear.yuv"), 8);
    _ = try differential(@embedFile("../testdata/vp8-version2.webm"), @embedFile("../testdata/vp8-version2.yuv"), 8);
    _ = try differential(@embedFile("../testdata/vp8-version3.webm"), @embedFile("../testdata/vp8-version3.yuv"), 8);
    _ = try differential(@embedFile("../testdata/vp8-partitions.webm"), @embedFile("../testdata/vp8-partitions.yuv"), 30);
}

test "VP8 interframe failures retain reference state and permit retry" {
    const a = std.testing.allocator;
    var input = media.source.Source{ .allocator = a, .identity = "vp8-inter", .storage = .{ .borrowed = @embedFile("../testdata/vp8-inter.webm") } };
    var reader = try media.webm.Reader.init(a, &input, .{});
    defer reader.deinit();
    var key = try reader.readPacket(0);
    defer key.deinit();
    var inter = try reader.readPacket(1);
    defer inter.deinit();
    var baseline = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    var state = image.webp.Vp8Decoder{};
    defer state.deinit(baseline.allocator());
    var first = try state.decode(baseline.allocator(), key.bytes, image.DecodeLimits.inference_default);
    first.deinit(baseline.allocator());
    const key_allocs = baseline.alloc_index;
    var second = try state.decode(baseline.allocator(), inter.bytes, image.DecodeLimits.inference_default);
    second.deinit(baseline.allocator());
    const inter_allocs = baseline.alloc_index - key_allocs;
    for (0..inter_allocs) |i| {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = key_allocs + i, .resize_fail_index = 0 });
        var decoder = image.webp.Vp8Decoder{};
        defer decoder.deinit(failing.allocator());
        var initial = try decoder.decode(failing.allocator(), key.bytes, image.DecodeLimits.inference_default);
        initial.deinit(failing.allocator());
        const old = decoder.references[0].?.y.ptr;
        const retained = decoder.retainedBytes();
        try std.testing.expectError(error.OutOfMemory, decoder.decode(failing.allocator(), inter.bytes, image.DecodeLimits.inference_default));
        try std.testing.expectEqual(@as(usize, 1), decoder.decoded);
        try std.testing.expectEqual(old, decoder.references[0].?.y.ptr);
        try std.testing.expectEqual(retained, decoder.retainedBytes());
        failing.fail_index = std.math.maxInt(usize);
        var retried = try decoder.decode(failing.allocator(), inter.bytes, image.DecodeLimits.inference_default);
        defer retried.deinit(failing.allocator());
        try std.testing.expectEqualSlices(u8, state.references[0].?.y, retried.y);
    }
    try std.testing.expectError(error.WebpDecodeFailed, state.decode(baseline.allocator(), inter.bytes[0..3], image.DecodeLimits.inference_default));
    try std.testing.expectEqual(@as(usize, 2), state.decoded);
    var missing = image.webp.Vp8Decoder{};
    try std.testing.expectError(error.MissingVp8Reference, missing.decode(a, inter.bytes, image.DecodeLimits.inference_default));
}

test "VP8 sequential session bounds memory and returned frames outlive it" {
    const a = std.testing.allocator;
    var input = media.source.Source{ .allocator = a, .identity = "vp8-inter", .storage = .{ .borrowed = @embedFile("../testdata/vp8-inter.webm") } };
    var reader = try media.webm.Reader.init(a, &input, .{});
    defer reader.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 } };
    const session = try Session.init(a, &reader, .{ .max_decode_bytes = 128 * 1024, .admission_pool = &pool });
    var kept = (try session.next()).?;
    defer kept.deinit();
    while (session.next_index < reader.packets.len) {
        if (try session.next()) |value| {
            var frame = value;
            frame.deinit();
        }
    }
    try std.testing.expect((try session.next()) == null);
    session.deinit();
    try std.testing.expectEqual(kept.planes.y.len + kept.planes.u.len + kept.planes.v.len, pool.snapshot().host_bytes);
    const tiny = try Session.init(a, &reader, .{ .max_decode_bytes = 1 });
    defer tiny.deinit();
    try std.testing.expectError(error.ResourceLimitExceeded, tiny.next());
    try std.testing.expectEqual(@as(usize, 0), tiny.next_index);
    try std.testing.expectEqual(@as(usize, 0), tiny.budget.live);
}

test "VP8 real Sintel interframes match independent FFmpeg frame hashes" {
    const a = std.testing.allocator;
    var input = media.source.Source{ .allocator = a, .identity = "vp8-sintel", .storage = .{ .borrowed = @embedFile("../testdata/vp8-sintel.webm") } };
    var reader = try media.webm.Reader.init(a, &input, .{});
    defer reader.deinit();
    const session = try Session.init(a, &reader, .{});
    defer session.deinit();
    const hashes = @embedFile("../testdata/vp8-sintel.sha256");
    var displayed: usize = 0;
    while (session.next_index < reader.packets.len) {
        var frame = (try session.next()) orelse continue;
        defer frame.deinit();
        var digest = std.crypto.hash.sha2.Sha256.init(.{});
        const p = frame.planes;
        for ([_][]const u8{ p.y, p.u, p.v }, [_]usize{ p.y_stride, p.uv_stride, p.uv_stride }, [_]usize{ p.width, (p.width + 1) / 2, (p.width + 1) / 2 }, [_]usize{ p.height, (p.height + 1) / 2, (p.height + 1) / 2 }) |plane, stride, width, height| {
            for (0..height) |y| digest.update(plane[y * stride ..][0..width]);
        }
        const actual = std.fmt.bytesToHex(digest.finalResult(), .lower);
        try std.testing.expectEqualSlices(u8, hashes[displayed * 65 ..][0..64], &actual);
        displayed += 1;
    }
    try std.testing.expectEqual(@as(usize, 48), displayed);
    try std.testing.expectEqual(displayed * 65, hashes.len);
}

test "VP8 interframe cancellation preserves packet position and retained references" {
    var input = media.source.Source{ .allocator = std.testing.allocator, .identity = "vp8-inter", .storage = .{ .borrowed = @embedFile("../testdata/vp8-inter.webm") } };
    var reader = try media.webm.Reader.init(std.testing.allocator, &input, .{});
    defer reader.deinit();
    const session = try Session.init(std.testing.allocator, &reader, .{});
    defer session.deinit();
    var key = (try session.next()).?;
    defer key.deinit();
    const old = session.decoder.references[0].?.y.ptr;
    const live = session.budget.live;
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
    try std.testing.expectError(error.Cancelled, session.next());
    try std.testing.expectEqual(@as(usize, 1), session.next_index);
    try std.testing.expectEqual(old, session.decoder.references[0].?.y.ptr);
    try std.testing.expectEqual(live, session.budget.live);
    input.control = .{};
    var retried = (try session.next()).?;
    defer retried.deinit();
    try std.testing.expectEqual(@as(usize, 2), session.next_index);
}

test "VP8 shared keyframe references count one owner and preserve mutable output independence" {
    const a = std.testing.allocator;
    var input = media.source.Source{ .allocator = a, .identity = "vp8-reference-sharing", .storage = .{ .borrowed = @embedFile("../testdata/vp8-inter.webm") } };
    var reader = try media.webm.Reader.init(a, &input, .{});
    defer reader.deinit();
    const session = try Session.init(a, &reader, .{});
    defer session.deinit();
    var frame = (try session.next()).?;
    const reference = session.decoder.references[0].?;
    try std.testing.expectEqual(reference.y.ptr, session.decoder.references[1].?.y.ptr);
    try std.testing.expectEqual(reference.y.ptr, session.decoder.references[2].?.y.ptr);
    try std.testing.expect(frame.planes.y.ptr != reference.y.ptr);
    const bytes = reference.y.len + reference.u.len + reference.v.len;
    try std.testing.expectEqual(bytes + session.decoder.segments.len, session.decoder.retainedBytes());
    try std.testing.expectEqual(session.decoder.retainedBytes(), session.budget.live);
    try std.testing.expectEqual(@as(u64, bytes), session.decoder.reference_copy_bytes);
    const first = reference.y[0];
    frame.planes.y[0] ^= 255;
    try std.testing.expectEqual(first, reference.y[0]);
    frame.deinit();
    while (session.next_index < reader.packets.len) {
        var next = (try session.next()) orelse continue;
        next.deinit();
        try std.testing.expectEqual(session.decoder.retainedBytes(), session.budget.live);
    }
}

test "VP8 shared reference commit cancellation reclaims candidate and preserves aliases" {
    const a = std.testing.allocator;
    var input = media.source.Source{ .allocator = a, .identity = "vp8-cancel-commit", .storage = .{ .borrowed = @embedFile("../testdata/vp8-inter.webm") } };
    var reader = try media.webm.Reader.init(a, &input, .{});
    defer reader.deinit();
    var key = try reader.readPacket(0);
    defer key.deinit();
    var inter = try reader.readPacket(1);
    defer inter.deinit();
    const Guard = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(raw: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.checks += 1;
            if (self.checks == self.fail_at) return error.Cancelled;
        }
    };
    var count = Guard{};
    var decoder = image.webp.Vp8Decoder{};
    defer decoder.deinit(a);
    var first = try decoder.decode(a, key.bytes, image.DecodeLimits.inference_default);
    first.deinit(a);
    {
        const scope = image.work_control.Scope.enter(.{ .context = &count, .check_fn = Guard.check });
        defer scope.deinit();
        var second = try decoder.decode(a, inter.bytes, image.DecodeLimits.inference_default);
        second.deinit(a);
    }
    // The final checkpoint occurs after candidate-reference allocation.
    var retry = image.webp.Vp8Decoder{};
    defer retry.deinit(a);
    var initial = try retry.decode(a, key.bytes, image.DecodeLimits.inference_default);
    initial.deinit(a);
    const old = retry.references;
    const copied = retry.reference_copy_bytes;
    const retained = retry.retainedBytes();
    var cancel = Guard{ .fail_at = count.checks };
    const scope = image.work_control.Scope.enter(.{ .context = &cancel, .check_fn = Guard.check });
    defer scope.deinit();
    try std.testing.expectError(error.Cancelled, retry.decode(a, inter.bytes, image.DecodeLimits.inference_default));
    try std.testing.expectEqual(@as(usize, 1), retry.decoded);
    try std.testing.expectEqual(copied, retry.reference_copy_bytes);
    try std.testing.expectEqual(retained, retry.retainedBytes());
    for (old, retry.references) |before, after| try std.testing.expectEqual(before.?.y.ptr, after.?.y.ptr);
    cancel.fail_at = std.math.maxInt(usize);
    var completed = try retry.decode(a, inter.bytes, image.DecodeLimits.inference_default);
    defer completed.deinit(a);
    try std.testing.expectEqualSlices(u8, decoder.references[0].?.y, completed.y);
}

test "VP8 real true motion edge regression matches independent decoded pictures" {
    const a = std.testing.allocator;
    var input = media.source.Source{ .allocator = a, .identity = "vp8-true-motion-border", .storage = .{ .borrowed = @embedFile("../testdata/vp8-true-motion-border.webm") } };
    var reader = try media.webm.Reader.init(a, &input, .{});
    defer reader.deinit();
    const manifest = try std.json.parseFromSlice(struct { picture_sha256: []const []const u8 }, a, @embedFile("../testdata/vp8-true-motion-border.json"), .{ .ignore_unknown_fields = true });
    defer manifest.deinit();
    const session = try Session.init(a, &reader, .{});
    defer session.deinit();
    var count: usize = 0;
    while (session.next_index < reader.packets.len) {
        var decoded = (try session.next()) orelse continue;
        defer decoded.deinit();
        const p = decoded.planes;
        var native_frame = try @import("codecs/frame.zig").copy(a, null, p.width, p.height, 8, 1, .{ .matrix = 6 }, .{ p.y.ptr, p.u.ptr, p.v.ptr }, .{ p.y_stride, p.uv_stride, p.uv_stride }, 128 * 1024 * 1024);
        defer native_frame.deinit();
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(native_frame.bytes, &digest, .{});
        const hex = std.fmt.bytesToHex(digest, .lower);
        try std.testing.expect(count < manifest.value.picture_sha256.len);
        try std.testing.expectEqualStrings(manifest.value.picture_sha256[count], &hex);
        count += 1;
    }
    try std.testing.expectEqual(manifest.value.picture_sha256.len, count);
}
