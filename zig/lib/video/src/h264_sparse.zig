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

//! Bounded software decoding across independently verified IDR runs.
const std = @import("std");
const media = @import("antfly_media");
const h264 = @import("h264.zig");
const planning = @import("decode_plan.zig");
pub const Options = struct {
    decode: h264.Options = .{},
    max_selections: usize = 64,
    max_total_packets: usize = 8192,
};

/// Callback slots retain the original order and duplicates. Pictures are
/// borrowed only during callbacks; each IDR run releases its decoder workspace.
/// In-band configuration changes use h264_dynamic instead of this static API.
pub fn decodeSelected(a: std.mem.Allocator, reader: *media.mp4.Reader, selected: []const usize, options: Options, context: *anyopaque, callback: h264.SelectionCallback) !h264.Statistics {
    try reader.input.control.check();
    if (reader.track.inband_parameter_sets) return error.UnsupportedDynamicVideoConfig;
    if (selected.len == 0 or selected.len > options.max_selections) return error.ResourceLimitExceeded;
    const unique = try a.alloc(usize, selected.len);
    defer a.free(unique);
    var count: usize = 0;
    for (selected) |index| {
        if (std.mem.indexOfScalar(usize, unique[0..count], index) != null) continue;
        unique[count] = index;
        count += 1;
    }
    var plan = try planning.create(a, reader, unique[0..count], .{ .portable_config = true, .max_run_packets = options.decode.max_dependency_packets, .max_decode_packets = options.max_total_packets, .max_selections = options.max_selections });
    defer plan.deinit();
    const indexes = try a.alloc(usize, selected.len);
    defer a.free(indexes);
    const slots = try a.alloc(usize, selected.len);
    defer a.free(slots);
    const Forward = struct {
        context: *anyopaque,
        callback: h264.SelectionCallback,
        slots: []const usize,
        fn publish(raw: *anyopaque, slot: usize, frame: *const h264.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.callback(self.context, self.slots[slot], frame);
        }
    };
    var stats = h264.Statistics{ .decoded_packets = 0, .payload_bytes = 0, .decode_high_water = 0 };
    for (plan.runs) |run| {
        try reader.input.control.check();
        var n: usize = 0;
        for (selected, 0..) |index, slot| {
            if (index < run.first or index > run.last) continue;
            indexes[n] = index;
            slots[n] = slot;
            n += 1;
        }
        var forward = Forward{ .context = context, .callback = callback, .slots = slots[0..n] };
        const result = try h264.decodeSelected(a, reader, indexes[0..n], options.decode, &forward, Forward.publish);
        stats.decoded_packets = try std.math.add(usize, stats.decoded_packets, result.decoded_packets);
        stats.payload_bytes = try std.math.add(u64, stats.payload_bytes, result.payload_bytes);
        stats.decode_high_water = @max(stats.decode_high_water, result.decode_high_water);
    }
    return stats;
}

test "video sparse software IDR runs preserve reordered duplicate slots within dependency bounds" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .identity = "sparse-software", .storage = .{ .borrowed = @embedFile("../testdata/decode-bframes.mp4") } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const selected = [_]usize{ 19, 1, 19 };
    const Capture = struct {
        expected: [3][32]u8,
        seen: [3]bool = @splat(false),
        fail: bool = false,
        fn publish(raw: *anyopaque, slot: usize, frame: *const h264.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail) return error.ConsumerFailure;
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            hash.update(frame.nv12);
            var actual: [32]u8 = undefined;
            hash.final(&actual);
            try std.testing.expectEqualSlices(u8, &self.expected[slot], &actual);
            try std.testing.expect(!self.seen[slot]);
            self.seen[slot] = true;
        }
    };
    var capture: Capture = .{ .expected = undefined };
    for (selected, 0..) |index, slot| {
        var frame = try h264.decodeFrame(a, &reader, index, .{});
        defer frame.deinit();
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(frame.nv12);
        hash.final(&capture.expected[slot]);
    }
    const before = source.retained_bytes;
    try std.testing.expectError(error.ResourceLimitExceeded, h264.decodeSelected(a, &reader, &selected, .{ .max_dependency_packets = 12 }, &capture, Capture.publish));
    const stats = try decodeSelected(a, &reader, &selected, .{ .decode = .{ .max_dependency_packets = 12 } }, &capture, Capture.publish);
    for (capture.seen) |seen| try std.testing.expect(seen);
    try std.testing.expect(stats.decoded_packets < 20);
    try std.testing.expectEqual(before, source.retained_bytes);
    capture.fail = true;
    try std.testing.expectError(error.ConsumerFailure, decodeSelected(a, &reader, &selected, .{}, &capture, Capture.publish));
    try std.testing.expectEqual(before, source.retained_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, decodeSelected(a, &reader, &selected, .{ .max_total_packets = 1 }, &capture, Capture.publish));
    try std.testing.expectEqual(before, source.retained_bytes);
}
