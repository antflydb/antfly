// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const video = @import("antfly_video");
const media = @import("antfly_media");
const image = @import("antfly_image");
test "video consumers share the same media Reader and image coefficient types" {
    const reader_type = @typeInfo(@TypeOf(video.apple.decodeSelected)).@"fn".param_types[1].?;
    comptime {
        if (reader_type != *media.mp4.Reader) @compileError("video and consumer must share the media module");
    }
    // Both paths must resolve the same source files through one shared module.
    _ = image.processing.BicubicAxis;
    _ = video.preparation.referenceHost;
    _ = video.decode_plan.create;
    _ = video.windows.create;
    _ = video.apple_jobs.prepareWindows;
    _ = video.h264.decodeFrame;
    _ = video.software.prepareWindows;
    _ = video.mjpeg.prepareWindows;
    _ = video.mjpeg_metal.prepareWindows;
    _ = video.preparation.Metal.submitRgba;
    _ = video.preparation.referenceRgba;
}
