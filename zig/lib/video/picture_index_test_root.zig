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

//! Metadata-only long-clip benchmark; excludes decoder and model execution.
//! Differential transport indexing across the maintained AVC qualification corpus.
const std = @import("std");
const media = @import("antfly_media");
const pictures = @import("src/h264_pictures.zig");
const h264 = @import("src/h264.zig");
test "AVC header corpus matches reconstruction" {
    const a = std.testing.allocator;
    inline for (.{
        "h264-baseline-p",
        "h264-dynamic-geometry",
        "h264-dynamic-pps",
        "h264-high10-422-cabac",
        "h264-high10-422-cavlc",
        "h264-high10-444-cabac",
        "h264-high10-444-cavlc",
        "h264-high10-cabac",
        "h264-high10-cavlc",
        "h264-high10-lossless-422-cabac",
        "h264-high10-lossless-422-cavlc",
        "h264-high10-lossless-444-cabac",
        "h264-high10-lossless-444-cavlc",
        "h264-high10-mbaff-cabac",
        "h264-high10-mbaff-cavlc",
        "h264-high10-mbaff-unfiltered-cabac",
        "h264-high10-mbaff-unfiltered-cavlc",
        "h264-high8-422-cabac",
        "h264-high8-422-cavlc",
        "h264-high8-444-cabac",
        "h264-high8-444-cavlc",
        "h264-high8-lossless-444-cabac",
        "h264-high8-lossless-444-cavlc",
        "h264-high8-lossless-cabac",
        "h264-high8-lossless-cavlc",
        "h264-high8-mbaff-cabac",
        "h264-high8-mbaff-cavlc",
        "h264-high8-mbaff-unfiltered-cabac",
        "h264-high8-mbaff-unfiltered-cavlc",
        "h264-jm-mono-10",
        "h264-jm-mono-8",
        "h264-jm-partitions",
        "h264-jm-primary-sp",
        "h264-jm-secondary-sp",
        "h264-jm-separate",
        "h264-jm-separate-10",
        "h264-jm-separate-14",
        "h264-jm-si",
        "h264-main-b",
        "h264-main-intra",
        "h264-mbaff-pcm-10-2",
        "h264-mbaff-pcm-10-3",
        "h264-mbaff-pcm-8-1",
        "h264-paff-adaptive-packets-10-2",
        "h264-paff-adaptive-packets-14-3",
        "h264-paff-adaptive-packets-8-1",
        "h264-paff-adaptive-paired-10-2",
        "h264-paff-adaptive-paired-14-3",
        "h264-paff-adaptive-paired-8-1",
        "h264-paff-b-10-2-spatial",
        "h264-paff-b-10-2-temporal",
        "h264-paff-b-14-3-spatial",
        "h264-paff-b-14-3-temporal",
        "h264-paff-b-8-1-spatial",
        "h264-paff-b-8-1-temporal",
        "h264-paff-b-spatial-packets-10-2",
        "h264-paff-b-spatial-packets-14-3",
        "h264-paff-b-spatial-packets-8-1",
        "h264-paff-b-temporal-packets-10-2",
        "h264-paff-b-temporal-packets-14-3",
        "h264-paff-b-temporal-packets-8-1",
        "h264-paff-bottom-packets-10-2",
        "h264-paff-bottom-packets-14-3",
        "h264-paff-bottom-packets-8-1",
        "h264-paff-cabac-packets-10-2",
        "h264-paff-cabac-packets-14-3",
        "h264-paff-cabac-packets-8-1",
        "h264-paff-fragments-cabac-bottom-10-2",
        "h264-paff-fragments-cabac-bottom-14-3",
        "h264-paff-fragments-cabac-bottom-8-1",
        "h264-paff-fragments-cavlc-10-2",
        "h264-paff-fragments-cavlc-14-3",
        "h264-paff-fragments-cavlc-8-1",
        "h264-paff-frozen-prediction-10-2",
        "h264-paff-frozen-prediction-14-3",
        "h264-paff-frozen-prediction-8-1",
        "h264-paff-idr-long-packets-10-2",
        "h264-paff-idr-long-packets-14-3",
        "h264-paff-idr-long-packets-8-1",
        "h264-paff-interleaved-10-2",
        "h264-paff-interleaved-14-3",
        "h264-paff-interleaved-8-1",
        "h264-paff-interleaved-fragments-10-2",
        "h264-paff-interleaved-fragments-14-3",
        "h264-paff-interleaved-fragments-8-1",
        "h264-paff-long-packets-10-2",
        "h264-paff-long-packets-14-3",
        "h264-paff-long-packets-8-1",
        "h264-paff-long-paired-10-2",
        "h264-paff-long-paired-14-3",
        "h264-paff-long-paired-8-1",
        "h264-paff-pcm-12-2",
        "h264-paff-pcm-14-3",
        "h264-paff-pcm-8-1",
        "h264-paff-prediction-10-2",
        "h264-paff-prediction-14-3",
        "h264-paff-prediction-8-1",
        "h264-paff-split-packets-10-2",
        "h264-paff-split-packets-14-3",
        "h264-paff-split-packets-8-1",
    }) |name| {
        var source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = @embedFile("testdata/" ++ name ++ ".mp4") } };
        var reader = try media.mp4.Reader.init(a, &source, .{});
        defer reader.deinit();
        var decoded = try pictures.index(a, &reader, .{});
        defer decoded.deinit();
        var planned = try pictures.planning(a, &reader, .{}, null);
        defer planned.deinit();
        try std.testing.expectEqual(@as(usize, 0), planned.decoded_packets);
        std.testing.expectEqualSlices(h264.Picture, decoded.pictures, planned.pictures) catch |err| {
            std.debug.print("AVC header mismatch fixture={s}\n", .{name});
            return err;
        };
    }
}

test "AVC header planning cancellation precedes metadata allocation" {
    const Cancel = struct {
        fn check(_: ?*const anyopaque) !void {
            return error.Cancelled;
        }
    };
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 64 * 1024 * 1024 } };
    var source = media.source.Source{ .allocator = a, .identity = "cancel", .storage = .{ .borrowed = @embedFile("testdata/h264-paff-adaptive-packets-8-1.mp4") }, .admission_pool = &pool };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const before = pool.snapshot();
    source.control = .{ .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, pictures.planning(a, &reader, .{}, null));
    try std.testing.expectEqual(before, pool.snapshot());
}

test "AVC header field duration overflow rejects without leaking" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .identity = "overflow", .storage = .{ .borrowed = @embedFile("testdata/h264-paff-adaptive-packets-8-1.mp4") } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    reader.packets[0].pts = std.math.minInt(i64);
    reader.packets[1].pts = std.math.maxInt(i64) - 1;
    try std.testing.expectError(error.TimestampOverflow, pictures.planning(a, &reader, .{}, null));
}
