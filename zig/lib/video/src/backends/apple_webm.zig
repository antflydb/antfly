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

//! VideoToolbox replay uses the same header-indexed logical picture identities
//! as portable replay. Multiple visible outputs per packet and changing formats
//! are deliberately routed to Zig before any consumer is called.
const std = @import("std");
const media = @import("antfly_media");
const apple = @import("apple.zig");
const pictures = @import("../webm_pictures.zig");
extern fn av_test_device_create() ?*anyopaque;
extern fn av_test_device_destroy(*anyopaque) void;
pub const Options = struct {
    decode: apple.Options = .{},
    pool: ?*media.admission.Pool = null,
    device_surfaces: bool = false,
};
pub const Receipt = struct { hardware: bool, submitted_packets: usize };
pub const Sink = struct {
    context: *anyopaque,
    accept: *const fn (*anyopaque, usize, *const apple.Surface, pictures.Picture) anyerror!void,
};
pub const Configuration = struct { bytes: [12]u8 = @splat(0), len: usize, format: apple.Format };
pub fn configuration(codec: media.webm.Codec, picture: pictures.Picture) !Configuration {
    if (picture.chroma_format != 1 or (picture.bit_depth != 8 and picture.bit_depth != 10)) return error.UnsupportedVideoProfile;
    var value = Configuration{ .len = 0, .format = if (picture.bit_depth == 10)
        (if (picture.color.full_range) .p010_full else .p010_video)
    else
        (if (picture.color.full_range) .nv12_full else .nv12_video) };
    switch (codec) {
        .vp8 => return error.UnsupportedVideoProfile,
        .av1 => {
            const config = picture.av1_config orelse return error.UnsupportedVideoProfile;
            @memcpy(value.bytes[0..4], &config);
            value.len = 4;
        },
        .vp9 => {
            // ISO VP Codec Configuration Box full-box payload; 4:2:0 co-located
            // chroma, no codec initialization payload. Level zero means
            // undefined (WebM does not carry a VP9 level declaration).
            // https://www.webmproject.org/vp9/mp4/
            value.bytes = .{ 1, 0, 0, 0, picture.profile, 0, (picture.bit_depth << 4) | 2 | @as(u8, @intFromBool(picture.color.full_range)), picture.color.primaries, picture.color.transfer, picture.color.matrix, 0, 0 };
            value.len = 12;
        },
    }
    return value;
}
fn sameFormat(left: pictures.Picture, right: pictures.Picture) bool {
    return left.width == right.width and left.height == right.height and left.bit_depth == right.bit_depth and
        left.chroma_format == right.chroma_format and left.profile == right.profile and
        std.meta.eql(left.color, right.color) and std.meta.eql(left.av1_config, right.av1_config);
}
const Capture = struct {
    indexed: pictures.Index,
    selected: []const usize,
    slots: []?apple.Surface,
    seen: []bool,
    format: apple.Format,
    control: media.source.Control,
    failure: ?anyerror = null,
    retained: u64 = 0,
    limit: u64,
    mutex: std.atomic.Mutex = .unlocked,
    fn callback(raw: *anyopaque, packet_id: usize, status: i32, image: ?*anyopaque) callconv(.c) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.failure != null) return;
        self.control.check() catch |err| {
            self.failure = err;
            return;
        };
        if (status != 0) {
            self.failure = error.VideoDecodeFailed;
            return;
        }
        for (self.selected, 0..) |id, slot| {
            const picture = self.indexed.pictures[id];
            if (picture.packet_index.? != packet_id) continue;
            if (self.seen[slot]) {
                self.failure = error.DuplicateDecodedFrame;
                return;
            }
            if (image == null) {
                self.failure = error.MissingDecodedFrame;
                return;
            }
            var surface = apple.Surface.fromBorrowed(image.?) catch |err| {
                self.failure = err;
                return;
            };
            const size = surface.storageBytes();
            if (surface.width != picture.width or surface.height != picture.height or surface.format != self.format) {
                surface.deinit();
                self.failure = error.UnsupportedSurfaceFormat;
                return;
            }
            if (size == 0 or size > self.limit -| self.retained) {
                surface.deinit();
                self.failure = error.ResourceLimitExceeded;
                return;
            }
            self.retained += size;
            self.slots[slot] = surface;
            self.seen[slot] = true;
            return;
        }
    }
    fn flush(self: *@This(), sink: Sink) !void {
        if (self.failure) |err| return err;
        for (self.slots, self.selected) |*slot, id| if (slot.*) |*surface| {
            try self.control.check();
            try sink.accept(sink.context, id, surface, self.indexed.pictures[id]);
            self.retained -= surface.storageBytes();
            surface.deinit();
            slot.* = null;
        };
    }
};
pub fn decodeTo(a: std.mem.Allocator, reader: *media.webm.Reader, indexed: pictures.Index, selected: []const usize, options: Options, sink: Sink) !Receipt {
    if (@import("builtin").os.tag != .macos) return error.UnsupportedVideoBackend;
    try reader.input.control.check();
    if (indexed.reader != reader or indexed.operating_point != 0) return error.InvalidVideoIndex;
    if (selected.len == 0 or selected.len > options.decode.max_frames or selected.len > 64) return error.ResourceLimitExceeded;
    if (reader.packets.len > options.decode.max_decode_packets) return error.ResourceLimitExceeded;
    if (indexed.pictures.len == 0) return error.EmptyVideo;
    var first = selected[0];
    var last = first;
    for (selected, 0..) |id, slot| {
        if (id >= indexed.pictures.len or std.mem.indexOfScalar(usize, selected[0..slot], id) != null) return error.InvalidPacketIndex;
        first = @min(first, id);
        last = @max(last, id);
    }
    if (!indexed.apple_packet_layout) return error.UnsupportedVideoProfile;
    const shape = indexed.pictures[0];
    const config = try configuration(reader.track.codec, shape);
    var previous: ?usize = null;
    // Validate the whole indexed format before entering native decode. A packet
    // with multiple visible pictures cannot be identified by sourceFrameRefCon.
    for (indexed.pictures) |picture| {
        try reader.input.control.check();
        const packet_id = picture.packet_index orelse return error.UnsupportedVideoProfile;
        if (packet_id >= reader.packets.len) return error.InvalidVideoIndex;
        if (previous) |p| if (packet_id <= p) return error.UnsupportedVideoProfile;
        previous = packet_id;
        if (!picture.apple_compatible or !sameFormat(shape, picture)) return error.UnsupportedVideoProfile;
    }
    var restart: pictures.Restart = .{};
    for (indexed.restarts) |point| {
        if (point.logical > first) break;
        restart = point;
    }
    const end = indexed.pictures[last].packet_index.?;
    if (restart.packet > end) return error.InvalidVideoIndex;
    const bytes = try apple.surfaceAdmissionBytesForDepth(shape.width, shape.height, selected.len, shape.bit_depth);
    if (bytes > options.decode.max_surface_bytes) return error.ResourceLimitExceeded;
    const metadata = try std.math.mul(usize, selected.len, @sizeOf(?apple.Surface) + @sizeOf(bool));
    const charge = if (options.device_surfaces) media.admission.Resources{ .device_bytes = bytes, .host_bytes = metadata } else media.admission.Resources{ .host_bytes = try std.math.add(u64, bytes, metadata) };
    var reservation = if (options.pool) |pool| try pool.acquire(charge) else media.admission.Token{};
    defer reservation.deinit();
    const slots = try a.alloc(?apple.Surface, selected.len);
    defer a.free(slots);
    @memset(slots, null);
    defer for (slots) |*slot| if (slot.*) |*surface| surface.deinit();
    const seen = try a.alloc(bool, selected.len);
    defer a.free(seen);
    @memset(seen, false);
    var capture = Capture{ .indexed = indexed, .selected = selected, .slots = slots, .seen = seen, .format = config.format, .control = reader.input.control, .limit = options.decode.max_retained_surface_bytes };
    var session = try apple.CodedSession.init(if (reader.track.codec == .vp9) .vp9 else .av1, config.bytes[0..config.len], shape.width, shape.height, config.format, options.decode.require_hardware, Capture.callback, &capture);
    defer session.deinit();
    for (restart.packet..end + 1) |packet_id| {
        try reader.input.control.check();
        const packet = reader.packets[packet_id];
        if (packet.size > options.decode.max_packet_bytes) return error.ResourceLimitExceeded;
        var lease = try reader.readPacket(packet_id);
        defer lease.deinit();
        var combined: ?[]u8 = null;
        defer if (combined) |value| a.free(value);
        // Strip a single-frame VP9 superframe index before platform submission.
        var payload = if (reader.track.codec == .vp9) (try @import("../codecs/vp9_header.zig").split(lease.bytes)).packets[0] else lease.bytes;
        if (packet_id == restart.packet) if (restart.sequence) |span| {
            if (span.packet >= reader.packets.len or span.offset > reader.packets[span.packet].size or span.size > reader.packets[span.packet].size - span.offset) return error.InvalidVideoIndex;
            var seed = try reader.input.read(try std.math.add(u64, reader.packets[span.packet].offset, span.offset), span.size);
            defer seed.deinit();
            const length = try std.math.add(usize, span.size, payload.len);
            if (length > options.decode.max_packet_bytes) return error.ResourceLimitExceeded;
            combined = try a.alloc(u8, length);
            @memcpy(combined.?[0..span.size], seed.bytes);
            @memcpy(combined.?[span.size..], payload);
            payload = combined.?;
        };
        // SourceFrameRefCon identifies packets; logical IDs come from the index.
        try session.submit(payload, packet_id, packet.pts, packet.duration_ns, 1_000_000_000);
        try capture.flush(sink);
    }
    try session.drain();
    try capture.flush(sink);
    for (seen) |value| if (!value) return error.MissingDecodedFrame;
    try reader.input.control.check();
    return .{ .hardware = session.hardware(), .submitted_packets = end - restart.packet + 1 };
}

test "Apple WebM configuration preserves depth range and excludes unsupported layouts" {
    const base = pictures.Picture{ .pts = 0, .duration = null, .width = 64, .height = 48 };
    const cfg = try configuration(.vp9, base);
    try std.testing.expectEqualSlices(u8, &.{ 1, 0, 0, 0 }, cfg.bytes[0..4]);
    try std.testing.expectEqual(apple.Format.nv12_video, cfg.format);
    var high = base;
    high.bit_depth = 10;
    high.profile = 2;
    high.color.full_range = true;
    try std.testing.expectEqual(apple.Format.p010_full, (try configuration(.vp9, high)).format);
    high.chroma_format = 3;
    try std.testing.expectError(error.UnsupportedVideoProfile, configuration(.vp9, high));
    try std.testing.expectError(error.UnsupportedVideoProfile, configuration(.vp8, base));
    try std.testing.expectError(error.UnsupportedVideoProfile, configuration(.av1, base));
}

const Parity = struct {
    reader: *media.webm.Reader,
    indexed: pictures.Index,
    checked: usize = 0,
    check_metal: bool = false,
    fn accept(raw: *anyopaque, id: usize, surface: *const apple.Surface, picture: pictures.Picture) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const Portable = struct {
            surface: *const apple.Surface,
            fn accept(context: *anyopaque, _: usize, decoded: *const @import("../codecs/decoder.zig").Frame) !void {
                const owned: *@This() = @ptrCast(@alignCast(context));
                var mapped = try owned.surface.map();
                defer mapped.deinit();
                const host = decoded.host();
                const bytes: usize = if (decoded.bit_depth == 8) 1 else 2;
                for (host.planes, mapped.planes, 0..) |expected, actual, plane| {
                    const columns = expected.width * (if (plane == 0) @as(usize, 1) else 2);
                    for (0..expected.height) |row| for (0..columns) |col| {
                        const e = expected.bytes[row * expected.stride + col * bytes ..][0..bytes];
                        const a = actual.bytes[row * actual.stride + col * bytes ..][0..bytes];
                        if (bytes == 1) try std.testing.expectEqual(e[0], a[0]) else {
                            const left = std.mem.readInt(u16, e[0..2], .little);
                            const right = std.mem.readInt(u16, a[0..2], .little) >> owned.surface.format.sampleShift();
                            try std.testing.expectEqual(left, right);
                        }
                    };
                }
            }
        };
        try std.testing.expectEqual(picture.width, surface.width);
        var compare = Portable{ .surface = surface };
        try pictures.walkIndexed(std.testing.allocator, self.reader, .{ .decoder = .{ .av1_all_layers = false } }, self.indexed, &.{id}, &compare, Portable.accept);
        if (self.check_metal) {
            const prep = @import("../preparation.zig");
            const a = std.testing.allocator;
            const device = av_test_device_create() orelse return error.SkipZigTest;
            defer av_test_device_destroy(device);
            var metal = try prep.Metal.init(device);
            defer metal.deinit();
            const options = prep.Options{ .width = 48, .height = 48, .matrix = .bt709 };
            const expected = try prep.reference(a, surface, options, .{});
            defer a.free(expected);
            var prepared = try metal.submit(a, surface, options, .{});
            defer prepared.deinit();
            try prepared.wait(std.testing.io, .{});
            const actual = try prepared.readback(a);
            defer a.free(actual);
            for (actual, expected) |got, want| try std.testing.expectApproxEqAbs(want, got, 2.0 / 255.0 + 1e-6);
        }
        self.checked += 1;
    }
};

fn qualify(bytes: []const u8, check_metal: bool) !void {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "Apple WebM parity", .admission_pool = &pool };
    var reader = try media.webm.Reader.init(a, &source, .{});
    defer reader.deinit();
    var indexed = try pictures.index(a, &reader, .{ .decoder = .{ .av1_all_layers = false } });
    defer indexed.deinit();
    const baseline = pool.snapshot();
    var parity = Parity{ .reader = &reader, .indexed = indexed, .check_metal = check_metal };
    const ids = [_]usize{ 0, indexed.pictures.len - 1 };
    const unique = ids[0..if (ids[1] == 0) @as(usize, 1) else 2];
    const receipt = decodeTo(a, &reader, indexed, unique, .{ .decode = .{ .require_hardware = false }, .pool = &pool }, .{ .context = &parity, .accept = Parity.accept }) catch |err| {
        try std.testing.expectEqual(baseline, pool.snapshot());
        if (err == error.VideoDecoderUnavailable) return error.SkipZigTest;
        return err;
    };
    try std.testing.expectEqual(unique.len, parity.checked);
    std.debug.print("Apple WebM receipt: codec={s} hardware={} selected={d} submitted={d}\n", .{ @tagName(reader.track.codec), receipt.hardware, parity.checked, receipt.submitted_packets });
    try std.testing.expect(receipt.submitted_packets != 0);
    try std.testing.expectEqual(baseline, pool.snapshot());
}
test "Apple WebM VP9 selected surfaces match portable pixels" {
    try qualify(@embedFile("../../testdata/vpx-av1/vp9.webm"), false);
}
test "Apple WebM AV1 selected surfaces match portable pixels" {
    try qualify(@embedFile("../../testdata/vpx-av1/av1.webm"), false);
}
test "Apple WebM VP9 interframes match portable pixels" {
    try qualify(@embedFile("../../testdata/vpx-av1/vp9-sintel.webm"), false);
}
test "Apple WebM AV1 interframes match portable pixels" {
    try qualify(@embedFile("../../testdata/vpx-av1/av1-sintel.webm"), false);
}

test "Apple WebM failure and cancellation drain surfaces before releasing reservations" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../../testdata/vpx-av1/vp9.webm") }, .identity = "failure" };
    var reader = try media.webm.Reader.init(a, &source, .{});
    defer reader.deinit();
    var indexed = try pictures.index(a, &reader, .{});
    defer indexed.deinit();
    const Failure = struct {
        fn accept(_: *anyopaque, _: usize, _: *const apple.Surface, _: pictures.Picture) !void {
            return error.ConsumerFailure;
        }
        fn cancelled(_: ?*const anyopaque) !void {
            return error.Cancelled;
        }
    };
    var dummy: u8 = 0;
    const sink = Sink{ .context = &dummy, .accept = Failure.accept };
    const result = decodeTo(a, &reader, indexed, &.{0}, .{ .decode = .{ .require_hardware = false }, .pool = &pool }, sink);
    if (result) |_| return error.TestUnexpectedResult else |err| {
        try std.testing.expect(err == error.ConsumerFailure or err == error.VideoDecoderUnavailable);
    }
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    source.control = .{ .check_fn = Failure.cancelled };
    try std.testing.expectError(error.Cancelled, decodeTo(a, &reader, indexed, &.{0}, .{ .pool = &pool }, sink));
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}

fn qualifyIvf(bytes: []const u8, codec: media.webm.Codec, check_metal: bool) !void {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    // Reuse the independently generated coded fixture without storing another
    // container/payload copy. The reader's packet boundary is the public API.
    const size = std.mem.readInt(u32, bytes[32..36], .little);
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "native-depth IVF packet" };
    var packets = [_]media.webm.Packet{.{ .cluster_offset = 0, .offset = 44, .size = size, .pts = 0, .duration_ns = 41666667, .sync = true, .invisible = false, .discardable = false }};
    var reader = media.webm.Reader{ .allocator = a, .input = &source, .metadata = .{ .bytes = &.{}, .source = null }, .track = .{ .number = 1, .codec = codec, .width = std.mem.readInt(u16, bytes[12..14], .little), .height = std.mem.readInt(u16, bytes[14..16], .little) }, .packets = &packets, .cues = &.{}, .timestamp_scale_ns = 1_000_000 };
    var indexed = try pictures.index(a, &reader, .{ .decoder = .{ .av1_all_layers = false } });
    defer indexed.deinit();
    try std.testing.expectEqual(@as(u8, 10), indexed.pictures[0].bit_depth);
    var parity = Parity{ .reader = &reader, .indexed = indexed, .check_metal = check_metal };
    const receipt = decodeTo(a, &reader, indexed, &.{0}, .{ .decode = .{ .require_hardware = false } }, .{ .context = &parity, .accept = Parity.accept }) catch |err| {
        if (err == error.VideoDecoderUnavailable) return error.SkipZigTest;
        return err;
    };
    try std.testing.expectEqual(@as(usize, 1), parity.checked);
    std.debug.print("Apple WebM native-depth receipt: codec={s} hardware={} depth=10\n", .{ @tagName(codec), receipt.hardware });
}
test "Apple WebM VP9 ten-bit P010 decode preserves samples" {
    try qualifyIvf(@embedFile("../../testdata/vpx-av1/vp9-yuv420p10le.ivf"), .vp9, false);
}
test "Apple WebM AV1 ten-bit P010 decode preserves samples" {
    try qualifyIvf(@embedFile("../../testdata/vpx-av1/av1-10.ivf"), .av1, false);
}

test "Apple WebM AV1 seek restores sequence OBU and ignores container sync hints" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../../testdata/vpx-av1/av1.webm") }, .identity = "Apple AV1 seek" };
    var reader = try media.webm.Reader.init(a, &source, .{});
    defer reader.deinit();
    const key = reader.packets[0];
    const stop = blk: {
        var lease = try reader.readPacket(0);
        defer lease.deinit();
        const headers = try @import("../codecs/av1_headers.zig").Session.create(a, .{ .max_pixels = 1024 * 1024 });
        defer headers.deinit();
        const Ignore = struct {
            fn accept(_: *anyopaque, _: @import("../codecs/vp9_header.zig").Geometry) !void {}
        };
        var dummy: u8 = 0;
        try headers.walk(lease.bytes, &dummy, Ignore.accept);
        const span = headers.packet_sequence orelse return error.TestUnexpectedResult;
        break :blk span.offset + span.size;
    };
    const packets = try a.alloc(media.webm.Packet, 4);
    for (packets, 0..) |*packet, i| {
        packet.* = key;
        packet.sync = false;
        packet.pts = @intCast(i * 1_000_000_000);
        if (i != 0) {
            packet.offset += stop;
            packet.size -= stop;
        }
    }
    a.free(reader.packets);
    reader.packets = packets;
    reader.track.codec_private = &.{};
    var indexed = try pictures.index(a, &reader, .{ .decoder = .{ .av1_all_layers = false } });
    defer indexed.deinit();
    var parity = Parity{ .reader = &reader, .indexed = indexed };
    const receipt = decodeTo(a, &reader, indexed, &.{3}, .{ .decode = .{ .require_hardware = false } }, .{ .context = &parity, .accept = Parity.accept }) catch |err| {
        if (err == error.VideoDecoderUnavailable) return error.SkipZigTest;
        return err;
    };
    try std.testing.expectEqual(@as(usize, 1), parity.checked);
    try std.testing.expectEqual(@as(usize, 1), receipt.submitted_packets);
    try std.testing.expectEqual(@as(i64, 3_000_000_000), indexed.pictures[3].pts);
}

test "Apple WebM Metal imports preserve decoded NV12 and P010 preparation" {
    try qualify(@embedFile("../../testdata/vpx-av1/vp9.webm"), true);
    try qualifyIvf(@embedFile("../../testdata/vpx-av1/vp9-yuv420p10le.ivf"), .vp9, true);
    try qualifyIvf(@embedFile("../../testdata/vpx-av1/av1-10.ivf"), .av1, true);
}
