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

//! Raw image implementations live only in the media owner.
const std = @import("std");
const c = @import("image_native_contract");
const image = @import("antfly_image");
const A = std.mem.Allocator;
const State = struct { decoder: image.webp.Vp8Decoder = .{}, metadata_allocator: A };
fn state(handle: *c.Vp8) *State {
    return @ptrCast(@alignCast(handle));
}
fn stats(value: *const image.webp.Vp8Decoder) c.values.Vp8Stats {
    return .{ .retained_bytes = value.retainedBytes(), .decoded = value.decoded, .reference_copy_bytes = value.reference_copy_bytes, .modes = value.modes, .split_shapes = value.split_shapes };
}
const VisitorLease = struct {
    host: c.Visitor,
    fn accept(raw: *anyopaque, index: usize, frame: image.gif.BorrowedFrame) anyerror!bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const result = self.host.accept(self.host.context, index, frame);
        if (result.failure.ptr != null) {
            c.transport.work.recordFailure(error.Canceled, result.failure);
            return error.Canceled;
        }
        return result.value;
    }
};
pub const Ops = struct {
    pub fn inspect(bytes: []const u8) !c.values.EncodedImageInfo {
        return image.inspectEncoded(bytes);
    }
    pub fn decode(a: A, format: c.values.Format, bytes: []const u8, limits: c.values.DecodeLimits) !c.values.DecodedRgba {
        return switch (format) {
            .png => image.png.decodeRgba(a, bytes),
            .jpeg => image.jpeg.decodeRgba(a, bytes),
            .bmp => image.bmp.decodeRgbaLimited(a, bytes, limits),
            .webp => image.webp.decodeRgbaLimited(a, bytes, limits),
            else => error.UnsupportedImageEncoding,
        };
    }
    pub fn gifFirst(a: A, bytes: []const u8) !c.values.GifFrame {
        return image.gif.decodeFirstFrameAlloc(a, bytes);
    }
    pub fn gifFrames(a: A, bytes: []const u8, frames: usize, retained: usize) ![]c.values.GifFrame {
        return image.gif.decodeFramesAllocBounded(a, bytes, frames, retained);
    }
    pub fn gifVisit(a: A, bytes: []const u8, maximum: usize, visitor: c.Visitor) !void {
        var lease: VisitorLease = .{ .host = visitor };
        return image.gif.visitFramesAllocBounded(a, bytes, maximum, .{ .context = &lease, .accept = VisitorLease.accept, .result = visitor.result });
    }
    pub fn pngEncode(scratch: A, output: A, view: c.values.ImageU8, options: c.values.PngEncodeOptions) ![]u8 {
        return image.png.encodeRgbaViewWithOptions(scratch, output, view, options);
    }
    pub fn jpegClip(a: A, bytes: []const u8, out: []f32, target: u32, mean: [3]f32, deviation: [3]f32, scaled: bool) !void {
        if (scaled) return image.jpeg.preprocessClipChwDctScaledInto(a, bytes, out, target, mean, deviation);
        return image.jpeg.preprocessClipChwInto(a, bytes, out, target, mean, deviation);
    }
    pub fn mjpegInfo(bytes: []const u8) !c.values.MjpegInfo {
        return image.jpeg.inspectMjpeg(bytes);
    }
    pub fn vp8Open(a: A) !*c.Vp8 {
        const value = try a.create(State);
        value.* = .{ .metadata_allocator = a };
        return @ptrCast(value);
    }
    pub fn vp8Close(handle: *c.Vp8, a: A) !void {
        const value = state(handle);
        value.decoder.deinit(a);
        value.metadata_allocator.destroy(value);
    }
    pub fn vp8Decode(handle: *c.Vp8, a: A, bytes: []const u8, limits: c.values.DecodeLimits) !c.values.Vp8Output {
        const value = state(handle);
        return .{ .planes = try value.decoder.decode(a, bytes, limits), .stats = stats(&value.decoder) };
    }
    pub fn vp8Stats(handle: *c.Vp8) !c.values.Vp8Stats {
        return stats(&state(handle).decoder);
    }
    pub fn vp8Key(a: A, bytes: []const u8, limits: c.values.DecodeLimits) !c.values.Vp8FramePlanes {
        return image.webp.decodeVp8KeyframePlanesLimited(a, bytes, limits);
    }
};
pub fn exportOperations() void {
    c.protocol.exportOperations(Ops);
}
