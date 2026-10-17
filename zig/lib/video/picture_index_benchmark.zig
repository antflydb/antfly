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
const std = @import("std");
const media = @import("antfly_media");
const pictures = @import("src/h264_pictures.zig");
fn now() i96 {
    return std.Io.Clock.awake.now(std.Options.debug_io).nanoseconds;
}
pub fn main() !void {
    const a = std.heap.smp_allocator;
    inline for (.{ "h264-paff-adaptive-packets-8-1", "h264-paff-interleaved-fragments-10-2", "h264-jm-partitions", "h264-jm-separate", "h264-dynamic-geometry" }) |name| {
        var source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = @embedFile("testdata/" ++ name ++ ".mp4") } };
        var reader = try media.mp4.Reader.init(a, &source, .{});
        defer reader.deinit();
        var header_ns: i96 = 0;
        var decode_ns: i96 = 0;
        for (0..8) |iteration| {
            var planned: pictures.Index = undefined;
            var decoded: pictures.Index = undefined;
            if (iteration % 2 == 0) {
                const start = now();
                planned = try pictures.planning(a, &reader, .{}, null);
                const middle = now();
                decoded = try pictures.index(a, &reader, .{});
                header_ns += middle - start;
                decode_ns += now() - middle;
            } else {
                const start = now();
                decoded = try pictures.index(a, &reader, .{});
                const middle = now();
                planned = try pictures.planning(a, &reader, .{}, null);
                decode_ns += middle - start;
                header_ns += now() - middle;
            }
            defer planned.deinit();
            defer decoded.deinit();
            if (planned.pictures.len != decoded.pictures.len) return error.IndexSamplingMismatch;
            for (planned.pictures, decoded.pictures) |left, right| if (!std.meta.eql(left, right)) return error.IndexSamplingMismatch;
        }
        std.debug.print("{{\"fixture\":\"{s}\",\"iterations\":8,\"packets\":{d},\"header_ns\":{d},\"reconstruction_ns\":{d}}}\n", .{ name, reader.packets.len, header_ns, decode_ns });
    }
}
