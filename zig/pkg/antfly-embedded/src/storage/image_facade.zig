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

//! Reader-free image client. Pixel math remains local; codecs execute in media.
const std = @import("std");
const c = @import("image_native_contract");
const v = c.values;
const A = std.mem.Allocator;
pub const processing = @import("image_processing");
pub const work_control = c.transport.work;
pub const Format = v.Format;
pub const DecodeLimits = v.DecodeLimits;
pub const RasterFormat = v.RasterFormat;
pub const BorrowedRasterAttachment = v.BorrowedRasterAttachment;
pub const rgba8_raster_mime_type = v.rgba8_raster_mime_type;
pub const EncodedImageInfo = v.EncodedImageInfo;
pub const inferenceFormatForMimeEssence = v.inferenceFormatForMimeEssence;
pub const inferenceMimeEssenceForFormat = v.inferenceMimeEssenceForFormat;
pub const supportsInferenceMimeEssence = v.supportsInferenceMimeEssence;
pub fn detectFormat(bytes: []const u8) Format {
    if (png.hasSignature(bytes)) return .png;
    if (bytes.len >= 12 and std.mem.eql(u8, bytes[0..12], &.{ 0, 0, 0, 12, 'j', 'P', ' ', ' ', 13, 10, 135, 10 })) return .jpeg2000_jp2;
    if (bytes.len >= 2 and bytes[0] == 0xff and bytes[1] == 0x4f) return .jpeg2000_j2k;
    if (jpeg.hasSignature(bytes)) return .jpeg;
    if (gif.hasSignature(bytes)) return .gif;
    if (bmp.hasSignature(bytes)) return .bmp;
    if (webp.hasSignature(bytes)) return .webp;
    return .unknown;
}
pub fn inspectEncoded(bytes: []const u8) !EncodedImageInfo {
    return c.call("inspect", .{bytes});
}
pub const png = struct {
    pub const DecodedImage = v.DecodedRgba;
    pub const CancellationProbe = v.PngCancellationProbe;
    pub const EncodeOptions = v.PngEncodeOptions;
    pub fn hasSignature(bytes: []const u8) bool {
        return bytes.len >= 8 and std.mem.eql(u8, bytes[0..8], &.{ 137, 'P', 'N', 'G', 13, 10, 26, 10 });
    }
    pub fn decodeRgba(a: A, bytes: []const u8) !DecodedImage {
        return c.call("decode", .{ a, Format.png, bytes, DecodeLimits.inference_default });
    }
    pub fn encodeRgba(a: A, width: u32, height: u32, rgba: []const u8) ![]u8 {
        return encodeRgbaWithOptions(a, a, width, height, rgba, .{});
    }
    pub fn encodeRgbaWithCancellation(scratch: A, output: A, width: u32, height: u32, rgba: []const u8, probe: CancellationProbe) ![]u8 {
        return encodeRgbaWithOptions(scratch, output, width, height, rgba, .{ .cancellation = probe });
    }
    pub fn encodeRgbaWithOptions(scratch: A, output: A, width: u32, height: u32, rgba: []const u8, options: EncodeOptions) ![]u8 {
        return encodeRgbaViewWithOptions(scratch, output, .{ .data = rgba, .width = width, .height = height, .format = .rgba8 }, options);
    }
    pub fn encodeRgbaViewWithOptions(scratch: A, output: A, view: v.ImageU8, options: EncodeOptions) ![]u8 {
        return c.call("pngEncode", .{ scratch, output, view, options });
    }
};
pub const jpeg = struct {
    pub const DecodedImage = v.DecodedRgba;
    pub fn hasSignature(bytes: []const u8) bool {
        return bytes.len >= 3 and bytes[0] == 255 and bytes[1] == 216 and bytes[2] == 255;
    }
    pub fn decodeRgba(a: A, bytes: []const u8) !DecodedImage {
        return c.call("decode", .{ a, Format.jpeg, bytes, DecodeLimits.inference_default });
    }
    pub fn preprocessClipChwInto(a: A, bytes: []const u8, out: []f32, target: u32, mean: [3]f32, deviation: [3]f32) !void {
        return c.call("jpegClip", .{ a, bytes, out, target, mean, deviation, false });
    }
    pub fn preprocessClipChwDctScaledInto(a: A, bytes: []const u8, out: []f32, target: u32, mean: [3]f32, deviation: [3]f32) !void {
        return c.call("jpegClip", .{ a, bytes, out, target, mean, deviation, true });
    }
    pub fn inspectMjpeg(bytes: []const u8) !v.MjpegInfo {
        return c.call("mjpegInfo", .{bytes});
    }
};
pub const bmp = struct {
    pub const DecodedImage = v.DecodedRgba;
    pub fn hasSignature(bytes: []const u8) bool {
        return std.mem.startsWith(u8, bytes, "BM");
    }
    pub fn decodeRgbaLimited(a: A, bytes: []const u8, limits: DecodeLimits) !DecodedImage {
        return c.call("decode", .{ a, Format.bmp, bytes, limits });
    }
};
pub const gif = struct {
    pub const Frame = v.GifFrame;
    pub const BorrowedFrame = v.GifBorrowedFrame;
    pub const VisitResult = v.GifVisitResult;
    pub const Visitor = v.GifVisitor;
    pub fn hasSignature(bytes: []const u8) bool {
        return std.mem.startsWith(u8, bytes, "GIF87a") or std.mem.startsWith(u8, bytes, "GIF89a");
    }
    pub fn decodeFirstFrameAlloc(a: A, bytes: []const u8) !Frame {
        return c.call("gifFirst", .{ a, bytes });
    }
    pub fn decodeFramesAllocBounded(a: A, bytes: []const u8, maximum: usize, retained: usize) ![]Frame {
        return c.call("gifFrames", .{ a, bytes, maximum, retained });
    }
    const Lease = struct {
        visitor: Visitor,
        fn accept(raw: *anyopaque, index: usize, frame: BorrowedFrame) c.transport.Outcome(bool) {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .value = self.visitor.accept(self.visitor.context, index, frame) catch |err| return .{ .failure = .fromError(err) } };
        }
    };
    pub fn visitFramesAllocBounded(a: A, bytes: []const u8, maximum: usize, visitor: Visitor) !void {
        var lease: Lease = .{ .visitor = visitor };
        return c.call("gifVisit", .{ a, bytes, maximum, c.Visitor{ .context = &lease, .accept = Lease.accept, .result = visitor.result } });
    }
};
pub const webp = struct {
    pub const DecodedImage = v.DecodedRgba;
    pub const Vp8FramePlanes = v.Vp8FramePlanes;
    pub fn hasSignature(bytes: []const u8) bool {
        return bytes.len >= 12 and std.mem.eql(u8, bytes[0..4], "RIFF") and std.mem.eql(u8, bytes[8..12], "WEBP");
    }
    pub fn decodeRgbaLimited(a: A, bytes: []const u8, limits: DecodeLimits) !DecodedImage {
        return c.call("decode", .{ a, Format.webp, bytes, limits });
    }
    pub fn decodeVp8KeyframePlanesLimited(a: A, bytes: []const u8, limits: DecodeLimits) !Vp8FramePlanes {
        return c.call("vp8Key", .{ a, bytes, limits });
    }
    pub const Vp8Decoder = struct {
        handle: ?*c.Vp8 = null,
        decoded: usize = 0,
        reference_copy_bytes: u64 = 0,
        modes: [5]usize = @splat(0),
        split_shapes: [4]usize = @splat(0),
        pub fn init(metadata_allocator: A) !Vp8Decoder {
            return .{ .handle = try c.call("vp8Open", .{metadata_allocator}) };
        }
        pub fn deinit(self: *Vp8Decoder, frame_allocator: A) void {
            if (self.handle) |handle| c.call("vp8Close", .{ handle, frame_allocator }) catch unreachable;
            self.* = .{};
        }
        fn update(self: *Vp8Decoder, value: v.Vp8Stats) void {
            self.decoded = value.decoded;
            self.reference_copy_bytes = value.reference_copy_bytes;
            self.modes = value.modes;
            self.split_shapes = value.split_shapes;
        }
        pub fn retainedBytes(self: *const Vp8Decoder) usize {
            const handle = self.handle orelse return 0;
            return (c.call("vp8Stats", .{handle}) catch unreachable).retained_bytes;
        }
        pub fn decode(self: *Vp8Decoder, a: A, bytes: []const u8, limits: DecodeLimits) !Vp8FramePlanes {
            const handle = self.handle orelse return error.InvalidArgument;
            const out = c.call("vp8Decode", .{ handle, a, bytes, limits }) catch |err| {
                self.update(c.call("vp8Stats", .{handle}) catch unreachable);
                return err;
            };
            self.update(out.stats);
            return out.planes;
        }
    };
};
