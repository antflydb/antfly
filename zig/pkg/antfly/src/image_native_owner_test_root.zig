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

const std = @import("std");
const image = @import("image");
const fixtures = if (std.mem.eql(u8, mode, "source")) image.test_support else @import("fixtures");
const c = @import("image_native_contract");
const media = @import("media");
const mode = @import("test_options").mode;
const a = std.testing.allocator;
fn fixture(path: []const u8) ![]u8 {
    return fixtures.readFixtureAlloc(a, std.testing.io, path);
}

test "compiled image native PNG encode decode preserves stride limits and allocator ownership" {
    const pixels = [_]u8{ 255, 0, 0, 255, 0, 255, 0, 255, 99, 99, 99, 99, 0, 0, 255, 255, 255, 255, 255, 255 };
    const encoded = try image.png.encodeRgbaViewWithOptions(a, a, .{ .data = &pixels, .width = 2, .height = 2, .format = .rgba8, .row_stride_bytes = 12 }, .{});
    defer a.free(encoded);
    const info = try image.inspectEncoded(encoded);
    try std.testing.expectEqual(@as(u32, 2), info.width);
    const decoded = try image.png.decodeRgba(a, encoded);
    defer a.free(decoded.rgba);
    try std.testing.expectEqualSlices(u8, pixels[0..8], decoded.rgba[0..8]);
    try std.testing.expectEqualSlices(u8, pixels[12..20], decoded.rgba[8..16]);
    try std.testing.expectError(error.ImageTooLarge, image.png.encodeRgbaWithOptions(a, a, 2, 2, decoded.rgba, .{ .max_output_bytes = 8 }));
    try std.testing.expectError(error.InvalidRgbaSize, image.png.encodeRgbaViewWithOptions(a, a, .{ .data = &.{1}, .width = 2, .height = 2, .format = .rgba8 }, .{}));
    var endpoint: c.protocol.Endpoint = .{ .version = 0, .contract = c.protocol.signature("inspect"), .invoke = undefined };
    try std.testing.expectError(error.InvalidArgument, c.protocol.validate("inspect", &endpoint));
}

test "compiled image native endpoint rejects old versions counts and nested layout mismatches" {
    var endpoint: c.protocol.Endpoint = .{ .contract = c.protocol.signature("inspect"), .invoke = undefined };
    try c.protocol.validate("inspect", &endpoint);
    endpoint.version = 1;
    // Both rejected cases carry no readable layout table. Validation must stop
    // before following this pointer or invoking the undefined callback.
    endpoint.layout = @ptrFromInt(@alignOf(c.transport.native.TypeContract));
    try std.testing.expectError(error.InvalidArgument, c.protocol.validate("inspect", &endpoint));
    endpoint.version = 2;
    endpoint.layout_count = c.protocol.layouts.len - 1;
    try std.testing.expectError(error.InvalidArgument, c.protocol.validate("inspect", &endpoint));
    endpoint.layout_count = c.protocol.layouts.len + 1;
    try std.testing.expectError(error.InvalidArgument, c.protocol.validate("inspect", &endpoint));
    endpoint.layout_count = c.protocol.layouts.len;
    var layouts = c.protocol.layouts;
    endpoint.layout = &layouts;
    const actual = c.transport.native.TypeContract.of(c.values.EncodedImageInfo);
    const ReorderedInfo = struct { height: u32, width: u32 };
    try std.testing.expectEqual(@sizeOf(c.values.EncodedImageInfo), @sizeOf(ReorderedInfo));
    var found = false;
    for (&layouts) |*layout| {
        if (!layout.matches(actual)) continue;
        found = true;
        layout.layout_id = c.transport.native.TypeContract.of(ReorderedInfo).layout_id;
    }
    try std.testing.expect(found);
    try std.testing.expectError(error.InvalidArgument, c.protocol.validate("inspect", &endpoint));
}

test "compiled image native source-free decoders match manifest pixel hashes" {
    const manifest = try fixtures.loadManifest(a, std.testing.io);
    defer fixtures.freeManifest(a, manifest);
    var tested: usize = 0;
    for (manifest.fixtures) |entry| {
        if (!std.mem.eql(u8, entry.result, "success") or entry.frames != 1 or entry.pixel_hashes.len != 1) continue;
        if (!std.mem.eql(u8, entry.pixel_format orelse "", "rgba8")) continue;
        if (!std.mem.eql(u8, entry.format, "png") and !std.mem.eql(u8, entry.format, "jpeg") and !std.mem.eql(u8, entry.format, "gif") and !std.mem.eql(u8, entry.format, "bmp") and !std.mem.eql(u8, entry.format, "webp")) continue;
        const bytes = try fixture(entry.path);
        defer a.free(bytes);
        const decoded: c.values.DecodedRgba = switch (image.detectFormat(bytes)) {
            .png => try image.png.decodeRgba(a, bytes),
            .jpeg => try image.jpeg.decodeRgba(a, bytes),
            .bmp => try image.bmp.decodeRgbaLimited(a, bytes, image.DecodeLimits.inference_default),
            .webp => try image.webp.decodeRgbaLimited(a, bytes, image.DecodeLimits.inference_default),
            .gif => blk: {
                const frame = try image.gif.decodeFirstFrameAlloc(a, bytes);
                break :blk .{ .rgba = frame.rgba, .width = frame.width, .height = frame.height };
            },
            else => return error.TestUnexpectedResult,
        };
        defer a.free(decoded.rgba);
        try std.testing.expectEqual(entry.width.?, decoded.width);
        try std.testing.expectEqual(entry.height.?, decoded.height);
        const hash = try fixtures.sha256HexAlloc(a, decoded.rgba);
        defer a.free(hash);
        std.debug.print("IMAGE_FIXTURE mode={s} path={s} hash={s}\n", .{ mode, entry.path, hash });
        tested += 1;
    }
    try std.testing.expect(tested >= 30);
}

test "compiled image native GIF visitor preserves partial progress early stop and caller errors" {
    const bytes = try fixture("gif/conformance/disposal-previous-3x3.gif");
    defer a.free(bytes);
    const Capture = struct {
        calls: usize = 0,
        fail: bool = false,
        fn accept(raw: *anyopaque, index: usize, frame: image.gif.BorrowedFrame) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.calls, index);
            try std.testing.expect(frame.rgba.len >= frame.width * frame.height * 4);
            if (self.fail) return error.ImageCallerOnlyVisitorStopped;
            self.calls += 1;
            return self.calls < 2;
        }
    };
    var capture: Capture = .{};
    var result: image.gif.VisitResult = .{};
    try image.gif.visitFramesAllocBounded(a, bytes, 1024 * 1024, .{ .context = &capture, .accept = Capture.accept, .result = &result });
    try std.testing.expectEqual(@as(usize, 2), capture.calls);
    try std.testing.expect(!result.complete);
    capture = .{ .fail = true };
    try std.testing.expectError(error.ImageCallerOnlyVisitorStopped, image.gif.visitFramesAllocBounded(a, bytes, 1024 * 1024, .{ .context = &capture, .accept = Capture.accept, .result = &result }));
    const frames = try image.gif.decodeFramesAllocBounded(a, bytes, 1, 1024 * 1024);
    defer {
        for (frames) |frame| a.free(frame.rgba);
        a.free(frames);
    }
    try std.testing.expectEqual(@as(usize, 1), frames.len);
}

test "compiled image native JPEG fused output and control errors preserve caller domain" {
    const bytes = try fixture("jpeg/baseline/white-2x1.jpg");
    defer a.free(bytes);
    const info = try image.jpeg.inspectMjpeg(bytes);
    try std.testing.expect(info.supported_profile and info.complete_packet);
    const decoded = try image.jpeg.decodeRgba(a, bytes);
    defer a.free(decoded.rgba);
    const mean: [3]f32 = .{ 0.5, 0.25, 0.125 };
    const deviation: [3]f32 = .{ 0.25, 0.5, 1 };
    const expected = try image.processing.preprocessDecoded(a, .{ .data = decoded.rgba, .width = decoded.width, .height = decoded.height, .format = .rgba8 }, 4, mean, deviation);
    defer a.free(expected);
    var out: [48]f32 = undefined;
    for ([_]bool{ false, true }) |scaled| {
        if (scaled) try image.jpeg.preprocessClipChwDctScaledInto(a, bytes, &out, 4, mean, deviation) else try image.jpeg.preprocessClipChwInto(a, bytes, &out, 4, mean, deviation);
        for (expected, out) |reference, actual| try std.testing.expectApproxEqAbs(reference, actual, 0.02);
    }
    const Control = struct {
        fn check(_: ?*const anyopaque) anyerror!void {
            return error.ImageCallerOnlyDecoderStopped;
        }
    };
    {
        const scope = image.work_control.Scope.enter(.{ .check_fn = Control.check });
        defer scope.deinit();
        try std.testing.expectError(error.ImageCallerOnlyDecoderStopped, image.jpeg.decodeRgba(a, bytes));
    }
    const recovered = try image.jpeg.decodeRgba(a, bytes);
    a.free(recovered.rgba);
}

test "compiled image native retained VP8 empty state and allocation failures" {
    var decoder = try image.webp.Vp8Decoder.init(a);
    defer decoder.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), decoder.retainedBytes());
    try std.testing.expectError(error.WebpDecodeFailed, decoder.decode(a, &.{}, image.DecodeLimits.inference_default));
    try std.testing.expectEqual(@as(usize, 0), decoder.retainedBytes());
    const bytes = try fixture("jpeg/baseline/white-2x1.jpg");
    defer a.free(bytes);
    const Exercise = struct {
        fn run(alloc: std.mem.Allocator, input: []const u8) !void {
            const decoded = try image.jpeg.decodeRgba(alloc, input);
            defer alloc.free(decoded.rgba);
            const encoded = try image.png.encodeRgba(alloc, decoded.width, decoded.height, decoded.rgba);
            defer alloc.free(encoded);
            var retained = try image.webp.Vp8Decoder.init(alloc);
            defer retained.deinit(alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Exercise.run, .{bytes});
}

test "compiled image native VP8 references preserve YUV fixtures and retry after failure" {
    for ([_][]const u8{ "vp8-inter", "vp8-version2", "vp8-version3", "vp8-partitions" }) |name| {
        const source_path = try std.fmt.allocPrint(a, "lib/video/testdata/{s}.webm", .{name});
        defer a.free(source_path);
        const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, source_path, a, .limited(32 * 1024 * 1024));
        defer a.free(bytes);
        const expected_path = try std.fmt.allocPrint(a, "lib/video/testdata/{s}.yuv", .{name});
        defer a.free(expected_path);
        const expected = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, expected_path, a, .limited(32 * 1024 * 1024));
        defer a.free(expected);
        var input = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = bytes } };
        var reader = try media.webm.Reader.init(a, &input, .{});
        defer reader.deinit();
        var decoder = try image.webp.Vp8Decoder.init(a);
        defer decoder.deinit(a);
        var cursor: usize = 0;
        for (0..reader.packets.len) |index| {
            var packet = try reader.readPacket(index);
            defer packet.deinit();
            var planes = try decoder.decode(a, packet.bytes, image.DecodeLimits.inference_default);
            defer planes.deinit(a);
            for ([_][]const u8{ planes.y, planes.u, planes.v }, 0..) |plane, channel| {
                const width: usize = if (channel == 0) planes.width else (planes.width + 1) / 2;
                const height: usize = if (channel == 0) planes.height else (planes.height + 1) / 2;
                const stride = if (channel == 0) planes.y_stride else planes.uv_stride;
                for (0..height) |row| {
                    try std.testing.expect(cursor + width <= expected.len);
                    try std.testing.expectEqualSlices(u8, expected[cursor..][0..width], plane[row * stride ..][0..width]);
                    cursor += width;
                }
            }
            const retained = decoder.retainedBytes();
            try std.testing.expect(retained > 0);
            try std.testing.expectError(error.WebpDecodeFailed, decoder.decode(a, &.{}, image.DecodeLimits.inference_default));
            try std.testing.expectEqual(retained, decoder.retainedBytes());
        }
        try std.testing.expectEqual(expected.len, cursor);
        std.debug.print("IMAGE_VP8 mode={s} fixture={s} packets={d} bytes={d}\n", .{ mode, name, reader.packets.len, cursor });
    }
}
