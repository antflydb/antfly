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

//! Complete encoded-image and retained CPU decoder operations.
const std = @import("std");
pub const transport = @import("media_native_abi");
pub const values = @import("image_values");
pub const A = std.mem.Allocator;
pub const Vp8 = opaque {};
pub const Visitor = struct {
    context: *anyopaque,
    accept: *const fn (*anyopaque, usize, values.GifBorrowedFrame) transport.Outcome(bool),
    result: *values.GifVisitResult,
};
pub const Methods = struct {
    inspect: *const fn ([]const u8) anyerror!values.EncodedImageInfo,
    decode: *const fn (A, values.Format, []const u8, values.DecodeLimits) anyerror!values.DecodedRgba,
    gifFirst: *const fn (A, []const u8) anyerror!values.GifFrame,
    gifFrames: *const fn (A, []const u8, usize, usize) anyerror![]values.GifFrame,
    gifVisit: *const fn (A, []const u8, usize, Visitor) anyerror!void,
    pngEncode: *const fn (A, A, values.ImageU8, values.PngEncodeOptions) anyerror![]u8,
    jpegClip: *const fn (A, []const u8, []f32, u32, [3]f32, [3]f32, bool) anyerror!void,
    mjpegInfo: *const fn ([]const u8) anyerror!values.MjpegInfo,
    vp8Open: *const fn (A) anyerror!*Vp8,
    vp8Close: *const fn (*Vp8, A) anyerror!void,
    vp8Decode: *const fn (*Vp8, A, []const u8, values.DecodeLimits) anyerror!values.Vp8Output,
    vp8Stats: *const fn (*Vp8) anyerror!values.Vp8Stats,
    vp8Key: *const fn (A, []const u8, values.DecodeLimits) anyerror!values.Vp8FramePlanes,
};
pub const protocol = transport.Protocol("antfly_image_native_", Methods, .{ values.Format, values.EncodedImageInfo, values.DecodeLimits, values.DecodedRgba, values.GifFrame, values.GifBorrowedFrame, values.GifVisitResult, Visitor, values.ImageU8, values.PixelFormat, values.PngEncodeOptions, values.PngCancellationProbe, values.MjpegInfo, values.Vp8FramePlanes, values.Vp8Stats, values.Vp8Output });
pub const call = protocol.call;
