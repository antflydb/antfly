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

//! Gemma 4 video input. CUDA decodes on the device; every other backend reuses
//! the qualified lib/video routes and projects frames as ordinary images.
const std = @import("std");
const media = @import("antfly_media");
const video = @import("antfly_video");
const projector_mod = @import("../architectures/gemma4_projector.zig");
const embedding_video = @import("embedding_gemma2_video.zig");
const embedding_gemma2 = @import("embedding_gemma2.zig");
const ComputeBackend = @import("../ops/ops.zig").ComputeBackend;
const Control = @import("../execution_control.zig").InferenceExecutionControl;

const sampling: embedding_video.Sampling = .{ .timestamps = .{} };

/// Decoder and indexing memory, shared through one pool per request.
const portable_pool_bytes: u64 = 512 * 1024 * 1024;
/// Prepared model-sized frames, plus one displayed frame at the source pixel limit.
const portable_frame_bytes: usize = 96 * 1024 * 1024 + 48 * 1024 * 1024;
/// Reserved through generation admission before any portable video work starts.
/// CUDA holds it too, because NVDEC can hand a clip to the portable route late.
pub const portable_host_bytes: usize = portable_pool_bytes + portable_frame_bytes;

pub fn encode(cb: *const ComputeBackend, a: std.mem.Allocator, projector: *projector_mod.ProjectorStore, bytes: []const u8) !projector_mod.ProjectedVideo {
    if (cb.kind() == .cuda) {
        // NVDEC takes a narrower set of clips than the catalog advertises, and
        // some rejections only surface mid-bitstream. Portable decoding decides
        // what's really invalid; either route projects on the device.
        if (projector_mod.encodeProjectedVideoResident(cb, a, projector, bytes)) |projected| {
            return projected;
        } else |err| {
            if (err != error.NvdecUnavailable and inputError(err) != error.InvalidVideoInput) return err;
            std.log.info("gemma4 video falling back to portable decoding: {s}", .{@errorName(err)});
        }
    }
    return encodePortable(cb, a, projector, bytes);
}

const Observer = struct {
    a: std.mem.Allocator,
    cb: *const ComputeBackend,
    frames: *projector_mod.PortableVideoFrames,
    fn accept(raw: *anyopaque, slot: usize, frame: video.preparation.BorrowedFrame, options: video.preparation.Options, control: media.source.Control) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.cb.checkExecutionControl();
        var displayed = try frame.display(self.a, options, control);
        defer displayed.deinit(self.a);
        try self.frames.prepare(slot, displayed.view(), options.presentation_width, options.presentation_height);
    }
};

fn encodePortable(cb: *const ComputeBackend, a: std.mem.Allocator, projector: *projector_mod.ProjectorStore, bytes: []const u8) !projector_mod.ProjectedVideo {
    // Indexing and decoding run under the model execution lock, so they must honor
    // the request's cancellation and deadline between selected frames too.
    const control = cb.execution_control orelse Control{};
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes } };
    const timestamps = embedding_video.selectedPresentationUs(a, bytes, control, sampling, &pool) catch |err| return inputError(err);
    defer a.free(timestamps);
    var frames = try projector_mod.PortableVideoFrames.init(cb, a, projector, timestamps.len);
    defer frames.deinit();
    var observer: Observer = .{ .a = a, .cb = cb, .frames = &frames };
    embedding_video.observeConfigured(a, bytes, null, control, .{
        .observer = .{ .context = &observer, .accept = Observer.accept },
        .sampling = sampling,
        .pool = &pool,
    }) catch |err| return inputError(err);
    return frames.project(timestamps);
}

/// Client media failures become one input error. Cancellation, deadlines and
/// capacity errors keep their identity through the shared embedding normalizer.
fn inputError(err: anyerror) anyerror {
    return embedding_gemma2.videoInputError(err);
}

test "gemma4 video timestamps follow presentation order on every portable container" {
    const a = std.testing.allocator;
    inline for (.{ "mjpeg.mov", "bframes.mp4", "closed.mp4", "vp9.webm", "av1.webm" }) |name| {
        const times = try embedding_video.selectedPresentationUs(a, @embedFile("../testdata/video/" ++ name), .{}, sampling, null);
        defer a.free(times);
        try std.testing.expect(times.len > 0 and times.len <= 32);
        try std.testing.expectEqual(@as(i64, 0), times[0]);
        for (times[1..], times[0 .. times.len - 1]) |next, previous| try std.testing.expect(next > previous);
    }
}

test "gemma4 video timestamps match the frames delivered to the observer" {
    const a = std.testing.allocator;
    inline for (.{ "mjpeg.mov", "bframes.mp4", "vp9.webm" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name);
        const times = try embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, null);
        defer a.free(times);
        const Count = struct {
            seen: []bool,
            fn accept(raw: *anyopaque, slot: usize, frame: video.preparation.BorrowedFrame, options: video.preparation.Options, control: media.source.Control) anyerror!void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                var displayed = try frame.display(std.testing.allocator, options, control);
                defer displayed.deinit(std.testing.allocator);
                try std.testing.expect(displayed.width > 0);
                try std.testing.expect(!self.seen[slot]);
                self.seen[slot] = true;
            }
        };
        const seen = try a.alloc(bool, times.len);
        defer a.free(seen);
        @memset(seen, false);
        var counter: Count = .{ .seen = seen };
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        try embedding_video.observeConfigured(a, bytes, null, .{}, .{ .observer = .{ .context = &counter, .accept = Count.accept }, .sampling = sampling, .pool = &pool });
        for (seen) |value| try std.testing.expect(value);
    }
}

test "gemma4 video timestamps honor request cancellation before indexing and decoding" {
    const a = std.testing.allocator;
    const Cancelled = struct {
        fn cancelled(_: ?*anyopaque) bool {
            return true;
        }
    };
    var unused: u8 = 0;
    const control: Control = .{ .cancellation = .{ .is_cancelled_fn = Cancelled.cancelled } };
    inline for (.{ "bframes.mp4", "vp9.webm" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name);
        try std.testing.expectError(error.Cancelled, embedding_video.selectedPresentationUs(a, bytes, control, sampling, null));
        const Refuse = struct {
            fn accept(_: *anyopaque, _: usize, _: video.preparation.BorrowedFrame, _: video.preparation.Options, _: media.source.Control) anyerror!void {
                return error.UnexpectedFrame;
            }
        };
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        try std.testing.expectError(error.Cancelled, embedding_video.observeConfigured(a, bytes, null, control, .{ .observer = .{ .context = &unused, .accept = Refuse.accept }, .sampling = sampling, .pool = &pool }));
    }
}

test "gemma4 video corrupt input is a client error while cancellation and capacity keep their identity" {
    const a = std.testing.allocator;
    const truncated = @embedFile("../testdata/video/bframes.mp4")[0..64];
    inline for (.{ "not a video at all", truncated }) |bytes| {
        const failure = if (embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, null)) |times| blk: {
            a.free(times);
            break :blk error.TestExpectedError;
        } else |err| err;
        try std.testing.expectEqual(error.InvalidVideoInput, inputError(failure));
    }
    for ([_]anyerror{ error.Cancelled, error.Timeout, error.SharedAdmissionExceeded, error.OutOfMemory }) |err| {
        try std.testing.expectEqual(err, inputError(err));
    }
}

test "gemma4 video indexing and decoding share one bounded pool" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/av1.webm");
    var tiny = media.admission.Pool{ .limits = .{ .host_bytes = 1 } };
    try std.testing.expectError(error.SharedAdmissionExceeded, embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, &tiny));
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes } };
    const times = try embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, &pool);
    defer a.free(times);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    try std.testing.expect(pool.high_water.host_bytes > 0);
}

test "gemma4 video reserves portable memory through generation admission" {
    const generation = @import("generation.zig");
    const messages = [_]generation.Message{.{ .role = "user", .content = "describe", .video_bytes = "clip" }};
    const portable = try generation.nativeGenerationMediaAdmissionForBackend(std.testing.allocator, "", &messages, .{ .family = .gemma }, .generic);
    try std.testing.expectEqual(portable_host_bytes, portable.host_scratch_bytes);
    try std.testing.expect(portable.token_allowance >= 60 * 80);
    const cuda = try generation.nativeGenerationMediaAdmissionForBackend(std.testing.allocator, "", &messages, .{ .family = .gemma }, .cuda);
    try std.testing.expectEqual(portable_host_bytes, cuda.host_scratch_bytes);
    const text = [_]generation.Message{.{ .role = "user", .content = "describe" }};
    try std.testing.expectEqual(@as(usize, 0), (try generation.nativeGenerationMediaAdmissionForBackend(std.testing.allocator, "", &text, .{ .family = .gemma }, .generic)).host_scratch_bytes);
}

test "gemma4 video reports presentation size for anamorphic containers" {
    const a = std.testing.allocator;
    inline for (.{ "anamorphic-2x1.mp4", "anamorphic-2x1.webm", "bframes.mp4" }, .{ true, true, false }) |name, anamorphic| {
        const Record = struct {
            width: u32 = 0,
            height: u32 = 0,
            raster_width: u32 = 0,
            raster_height: u32 = 0,
            fn accept(raw: *anyopaque, _: usize, frame: video.preparation.BorrowedFrame, options: video.preparation.Options, control: media.source.Control) anyerror!void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                var displayed = try frame.display(std.testing.allocator, options, control);
                defer displayed.deinit(std.testing.allocator);
                self.width = options.presentation_width;
                self.height = options.presentation_height;
                self.raster_width = displayed.width;
                self.raster_height = displayed.height;
            }
        };
        var record: Record = .{};
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes } };
        try embedding_video.observeConfigured(a, @embedFile("../testdata/video/" ++ name), null, .{}, .{ .observer = .{ .context = &record, .accept = Record.accept }, .sampling = sampling, .pool = &pool });
        try std.testing.expectEqual(record.raster_height, record.height);
        if (anamorphic) {
            try std.testing.expectEqual(@as(u32, 64), record.raster_width);
            try std.testing.expectEqual(@as(u32, 128), record.width);
        } else {
            try std.testing.expectEqual(record.raster_width, record.width);
        }
    }
}
