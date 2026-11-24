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

//! Codec-free image values shared by source implementations and native clients.
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const DecodeLimits = struct {
    max_pixels: usize,
    max_rgba_bytes: usize,

    pub const inference_default = DecodeLimits{
        .max_pixels = 32 * 1024 * 1024,
        .max_rgba_bytes = 128 * 1024 * 1024,
    };

    pub fn validate(self: DecodeLimits, width: u32, height: u32) !void {
        const pixels_u64 = @as(u64, width) * @as(u64, height);
        if (pixels_u64 > std.math.maxInt(usize)) return error.ImageTooLarge;
        const pixels: usize = @intCast(pixels_u64);
        if (pixels > self.max_pixels) return error.ImageTooLarge;
        if (pixels > std.math.maxInt(usize) / 4) return error.ImageTooLarge;
        if (pixels * 4 > self.max_rgba_bytes) return error.ImageTooLarge;
    }
};

pub const Resample = enum(u32) {
    nearest = 0,
    lanczos = 1,
    bilinear = 2,
    bicubic = 3,
    /// Pillow's antialiased, quantized two-pass BICUBIC implementation. This
    /// is distinct from the legacy bicubic sampler used by existing model
    /// configurations so parity-sensitive callers must opt in explicitly.
    pillow_bicubic = 4,
    /// Torchvision uint8 antialiased bicubic, with adaptive int16 coefficients.
    torchvision_bicubic = 5,
    /// Pillow antialiased, quantized two-pass BILINEAR.
    pillow_bilinear = 6,
};

pub const PixelFormat = enum(u8) {
    rgb8 = 3,
    rgba8 = 4,
};

pub const ImageU8 = struct {
    data: []const u8,
    width: u32,
    height: u32,
    format: PixelFormat,
    /// Zero selects the tightly packed width*channels layout. A non-zero
    /// stride permits borrowing renderer-owned rows without repacking them.
    row_stride_bytes: usize = 0,

    pub fn channels(self: ImageU8) usize {
        return @backingInt(self.format);
    }

    pub fn rowStride(self: ImageU8) !usize {
        const packed_row = std.math.mul(usize, self.width, self.channels()) catch
            return error.InvalidImageBuffer;
        const stride = if (self.row_stride_bytes == 0) packed_row else self.row_stride_bytes;
        if (stride < packed_row) return error.InvalidImageBuffer;
        return stride;
    }

    pub fn validate(self: ImageU8) !void {
        if (self.width == 0 or self.height == 0) return error.InvalidImageBuffer;
        const stride = try self.rowStride();
        const last_row_offset = std.math.mul(usize, self.height - 1, stride) catch
            return error.InvalidImageBuffer;
        const packed_row = std.math.mul(usize, self.width, self.channels()) catch
            return error.InvalidImageBuffer;
        const required = std.math.add(usize, last_row_offset, packed_row) catch
            return error.InvalidImageBuffer;
        if (self.data.len < required) return error.InvalidImageBuffer;
    }
};

pub const Format = enum { png, jpeg, jpeg2000_jp2, jpeg2000_j2k, gif, bmp, webp, unknown };

/// Canonical MIME for the linked-process raw raster ABI. This is deliberately
/// not an encoded-image MIME and must never be sent over HTTP or advertised as
/// a model input codec. `RasterFormat` remains separately explicit so a future
/// ABI extension cannot reinterpret existing bytes based on a MIME string.
pub const rgba8_raster_mime_type = "image/x-antfly-rgba8";

pub const RasterFormat = enum {
    rgba8,

    pub fn channels(self: RasterFormat) usize {
        return switch (self) {
            .rgba8 => 4,
        };
    }

    pub fn mimeType(self: RasterFormat) []const u8 {
        return switch (self) {
            .rgba8 => rgba8_raster_mime_type,
        };
    }
};

/// A synchronous borrowed view of a decoded raster plus stable work identity.
/// The producer owns `bytes` and every identity slice for the complete
/// invocation; consumers must not retain any pointer after returning. Rows may
/// contain padding. A buffer ends either after the last pixel or its complete
/// padded row, allowing borrowed crop views without copying unrelated pixels.
///
/// This physical ABI is task-neutral. Reader, generator, embedder, reranker,
/// and extractor adapters may use it only after their resolved local executor
/// advertises a concrete raw-raster path.
pub const BorrowedRasterAttachment = struct {
    bytes: []const u8,
    width: u32,
    height: u32,
    stride_bytes: usize,
    format: RasterFormat = .rgba8,
    mime_type: []const u8 = rgba8_raster_mime_type,
    item_id: []const u8 = "",
    source_fingerprint: ?[]const u8 = null,
    page_number: ?u32 = null,

    pub fn validate(self: BorrowedRasterAttachment) !void {
        if (self.width == 0 or self.height == 0) return error.InvalidRasterDimensions;
        if (!std.mem.eql(u8, self.mime_type, self.format.mimeType()))
            return error.InvalidRasterMimeType;
        const row_bytes = std.math.mul(
            usize,
            @as(usize, self.width),
            self.format.channels(),
        ) catch return error.InvalidRasterDimensions;
        if (self.stride_bytes < row_bytes) return error.InvalidRasterStride;
        const required = std.math.mul(
            usize,
            self.stride_bytes,
            @as(usize, self.height),
        ) catch return error.InvalidRasterDimensions;
        const minimum = std.math.add(usize, required - self.stride_bytes, row_bytes) catch
            return error.InvalidRasterDimensions;
        if (self.bytes.len != required and self.bytes.len != minimum) return error.InvalidRasterBuffer;
    }

    pub fn pixels(self: BorrowedRasterAttachment) !u64 {
        return std.math.mul(u64, self.width, self.height) catch
            error.InvalidRasterDimensions;
    }

    pub fn imageView(self: BorrowedRasterAttachment) !ImageU8 {
        try self.validate();
        return .{
            .data = self.bytes,
            .width = self.width,
            .height = self.height,
            .format = switch (self.format) {
                .rgba8 => .rgba8,
            },
            .row_stride_bytes = self.stride_bytes,
        };
    }
};

/// Resolve a canonical MIME essence to a format supported by the shared
/// inference image decoder. Keeping this registry beside `detectFormat` makes
/// capability advertisement, header inspection, and decoding use one codec
/// contract instead of three independently maintained allowlists.
pub fn inferenceFormatForMimeEssence(essence: []const u8) ?Format {
    if (std.ascii.eqlIgnoreCase(essence, "image/png")) return .png;
    if (std.ascii.eqlIgnoreCase(essence, "image/jpeg") or
        std.ascii.eqlIgnoreCase(essence, "image/jpg")) return .jpeg;
    if (std.ascii.eqlIgnoreCase(essence, "image/gif")) return .gif;
    if (std.ascii.eqlIgnoreCase(essence, "image/bmp")) return .bmp;
    if (std.ascii.eqlIgnoreCase(essence, "image/webp")) return .webp;
    return null;
}

pub fn inferenceMimeEssenceForFormat(format: Format) ?[]const u8 {
    return switch (format) {
        .png => "image/png",
        .jpeg => "image/jpeg",
        .gif => "image/gif",
        .bmp => "image/bmp",
        .webp => "image/webp",
        else => null,
    };
}

pub fn supportsInferenceMimeEssence(essence: []const u8) bool {
    return inferenceFormatForMimeEssence(essence) != null;
}

pub const EncodedImageInfo = struct {
    width: u32,
    height: u32,

    pub fn pixels(self: EncodedImageInfo) !u64 {
        return std.math.mul(u64, self.width, self.height) catch error.ImageDimensionsOverflow;
    }
};

/// Inspect dimensions from a bounded container prefix before admitting decode work.
pub fn inspectWebpPrefix(bytes: []const u8) !EncodedImageInfo {
    if (bytes.len < 12 or !std.mem.eql(u8, bytes[0..4], "RIFF") or !std.mem.eql(u8, bytes[8..12], "WEBP"))
        return error.WebpDecodeFailed;
    const riff_end = @as(u64, std.mem.readInt(u32, bytes[4..8], .little)) + 8;
    const end: usize = @intCast(@min(riff_end, bytes.len));
    var cursor: usize = 12;
    while (cursor < end) {
        if (end - cursor < 8) return error.WebpDecodeFailed;
        const kind = bytes[cursor..][0..4];
        const size = std.mem.readInt(u32, bytes[cursor + 4 ..][0..4], .little);
        const start = cursor + 8;
        if (size > riff_end - start) return error.WebpDecodeFailed;
        const payload = bytes[start..][0..@min(size, end - start)];
        if (std.mem.eql(u8, kind, "VP8X")) {
            if (payload.len != 10 or size != 10) return error.WebpDecodeFailed;
            if ((payload[0] & 0xc1) != 0 or !std.mem.allEqual(u8, payload[1..4], 0)) return error.UnsupportedWebpFormat;
            return .{
                .width = 1 + @as(u32, std.mem.readInt(u24, payload[4..7], .little)),
                .height = 1 + @as(u32, std.mem.readInt(u24, payload[7..10], .little)),
            };
        }
        if (std.mem.eql(u8, kind, "VP8 ")) {
            if (payload.len < 10 or (payload[0] & 1) != 0 or !std.mem.eql(u8, payload[3..6], &.{ 0x9d, 1, 0x2a }))
                return error.WebpDecodeFailed;
            const width = std.mem.readInt(u16, payload[6..8], .little) & 0x3fff;
            const height = std.mem.readInt(u16, payload[8..10], .little) & 0x3fff;
            if (width == 0 or height == 0) return error.WebpDecodeFailed;
            return .{ .width = width, .height = height };
        }
        if (std.mem.eql(u8, kind, "VP8L")) {
            if (payload.len < 5 or payload[0] != 0x2f) return error.WebpDecodeFailed;
            const header = std.mem.readInt(u32, payload[1..5], .little);
            if (header >> 29 != 0) return error.UnsupportedWebpFormat;
            return .{ .width = 1 + (header & 0x3fff), .height = 1 + ((header >> 14) & 0x3fff) };
        }
        if (size > end - start) return error.WebpDecodeFailed;
        cursor = start + size;
        if (cursor < end and size % 2 != 0) cursor += 1;
    }
    return error.WebpDecodeFailed;
}

test "WebP prefix reports dimensions without reading image payload" {
    const bytes = "RIFF\x64\x00\x00\x00WEBPVP8L\x58\x00\x00\x00\x2f\x01\x80\x00\x00";
    try std.testing.expectEqual(EncodedImageInfo{ .width = 2, .height = 3 }, try inspectWebpPrefix(bytes));
    try std.testing.expectError(error.WebpDecodeFailed, inspectWebpPrefix(bytes[0 .. bytes.len - 1]));
}

pub const DecodedRgba = struct {
    rgba: []u8,
    width: u32,
    height: u32,
};

/// Borrowed, allocation-free cancellation source for bounded PNG work.
pub const PngCancellationProbe = struct {
    context: ?*const anyopaque = null,
    is_cancelled_fn: ?*const fn (?*const anyopaque) bool = null,

    pub fn check(self: PngCancellationProbe) !void {
        if (self.is_cancelled_fn) |is_cancelled| if (is_cancelled(self.context)) return error.Canceled;
    }
};

pub const PngEncodeOptions = struct {
    cancellation: PngCancellationProbe = .{},
    /// Includes signature, chunks and zlib data. Exceeding this ceiling returns
    /// ImageTooLarge before allocating the final PNG or growing compression
    /// storage beyond the ceiling plus Deflate's fixed lookahead.
    max_output_bytes: usize = std.math.maxInt(usize),
};

pub const GifFrame = struct {
    rgba: []u8,
    width: u32,
    height: u32,
    delay_ms: u32,
};

pub const GifVisitResult = struct { frames: usize = 0, duration_ms: u64 = 0, complete: bool = false };
pub const GifBorrowedFrame = struct { rgba: []const u8, width: u32, height: u32, delay_ms: u32 };
pub const GifVisitor = struct {
    context: *anyopaque,
    /// Borrowed composited canvas; false stops before the next picture.
    accept: *const fn (*anyopaque, usize, GifBorrowedFrame) anyerror!bool,
    result: *GifVisitResult,
};

pub const Vp8FramePlanes = struct {
    y: []u8,
    u: []u8,
    v: []u8,
    width: u32,
    height: u32,
    y_stride: usize,
    uv_stride: usize,
    y_padded_height: usize,
    uv_padded_height: usize,

    pub fn deinit(self: *Vp8FramePlanes, alloc: Allocator) void {
        alloc.free(self.y);
        alloc.free(self.u);
        alloc.free(self.v);
        self.* = undefined;
    }
};

pub const MjpegInfo = struct { width: u32, height: u32, supported_profile: bool, complete_packet: bool };
pub const Vp8Stats = struct {
    retained_bytes: usize = 0,
    decoded: usize = 0,
    reference_copy_bytes: u64 = 0,
    modes: [5]usize = @splat(0),
    split_shapes: [4]usize = @splat(0),
};
pub const Vp8Output = struct { planes: Vp8FramePlanes, stats: Vp8Stats };
