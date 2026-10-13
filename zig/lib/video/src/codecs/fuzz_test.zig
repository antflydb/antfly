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

//! Coverage-guided reconstruction targets with native allocation/control limits.
const std = @import("std");
const decoder = @import("decoder.zig");
const Guard = struct {
    calls: usize = 0,
    fn check(raw: ?*const anyopaque) !void {
        const self: *Guard = @ptrCast(@alignCast(@constCast(raw.?)));
        self.calls += 1;
        if (self.calls > 10000) return error.DeadlineExceeded;
    }
};
fn runFramed(codec: decoder.Codec, bytes: []const u8, framing: decoder.Av1Framing) !void {
    if (bytes.len == 0 or bytes.len > 128 * 1024) return;
    var guard = Guard{};
    const session = decoder.Session.create(std.testing.allocator, codec, .{
        .av1_framing = framing,
        .max_pixels = 512 * 512,
        .max_packet_bytes = 128 * 1024,
        .max_decode_bytes = 32 * 1024 * 1024,
        .max_output_bytes = 4 * 1024 * 1024,
        .control = .{ .context = &guard, .check_fn = Guard.check },
    }) catch return;
    defer session.destroy();
    var batch = session.decodeOutputs(bytes, 0, null, if (bytes[bytes.len - 1] & 1 == 0) .{ .indexes = &.{0} } else .none) catch return;
    defer batch.deinit();
    for (batch.frames) |frame| try frame.host().validate();
}
fn run(codec: decoder.Codec, bytes: []const u8) !void {
    try runFramed(codec, bytes, .low_overhead);
}
fn seed(comptime name: []const u8) []const u8 {
    return comptime blk: {
        @setEvalBranchQuota(1_000_000);
        const ivf = @embedFile("../../testdata/vpx-av1/" ++ name ++ ".ivf");
        const size = std.mem.readInt(u32, ivf[32..36], .little);
        // Smith consumes the serialized length before supplying raw payload.
        var encoded: [4 + size]u8 = undefined;
        std.mem.writeInt(u32, encoded[0..4], size, .little);
        @memcpy(encoded[4..], ivf[44..][0..size]);
        const frozen = encoded;
        break :blk &frozen;
    };
}
test "video fuzz VP9 reconstruction and native allocation cleanup" {
    const Harness = struct {
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var bytes: [128 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&bytes, 0xA17F9001);
            try run(.vp9, bytes[0..length]);
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &.{ seed("vp9"), seed("vp9-yuv420p12le"), seed("vp9-yuv444p10le"), seed("vp9-yuv422p12le") } });
}
test "video fuzz AV1 reconstruction and native allocation cleanup" {
    const Harness = struct {
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var bytes: [128 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&bytes, 0xA17FA001);
            try run(.av1, bytes[0..length]);
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &.{ seed("av1"), seed("av1-10"), seed("av1-grain"), seed("av1-odd"), extendedSeed("av1-c0-12"), extendedSeed("av1-c2-12"), extendedSeed("av1-c3-12"), extendedSeed("av1-svc") } });
}

fn extendedSeed(comptime name: []const u8) []const u8 {
    return comptime blk: {
        @setEvalBranchQuota(1_000_000);
        const ivf = @embedFile("../../testdata/av1-extended/" ++ name ++ ".ivf");
        const size = std.mem.readInt(u32, ivf[32..36], .little);
        // Smith consumes the serialized length before supplying raw payload.
        var encoded: [4 + size]u8 = undefined;
        std.mem.writeInt(u32, encoded[0..4], size, .little);
        @memcpy(encoded[4..], ivf[44..][0..size]);
        const frozen = encoded;
        break :blk &frozen;
    };
}
test "video fuzz AV1 Annex B framing reconstruction and cleanup" {
    const Harness = struct {
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var bytes: [128 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&bytes, 0xA17FAB01);
            try runFramed(.av1, bytes[0..length], .annex_b);
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &.{extendedSeed("av1-annex-b")} });
}

test "codec fuzz seed packets reach reconstruction successfully" {
    const Case = struct { codec: decoder.Codec, framing: decoder.Av1Framing = .low_overhead, bytes: []const u8 };
    const cases = [_]Case{
        .{ .codec = .vp9, .bytes = seed("vp9") },
        .{ .codec = .vp9, .bytes = seed("vp9-yuv420p12le") },
        .{ .codec = .vp9, .bytes = seed("vp9-yuv444p10le") },
        .{ .codec = .vp9, .bytes = seed("vp9-yuv422p12le") },
        .{ .codec = .av1, .bytes = seed("av1") },
        .{ .codec = .av1, .bytes = seed("av1-10") },
        .{ .codec = .av1, .bytes = seed("av1-grain") },
        .{ .codec = .av1, .bytes = seed("av1-odd") },
        .{ .codec = .av1, .bytes = extendedSeed("av1-c0-12") },
        .{ .codec = .av1, .bytes = extendedSeed("av1-c2-12") },
        .{ .codec = .av1, .bytes = extendedSeed("av1-c3-12") },
        .{ .codec = .av1, .bytes = extendedSeed("av1-svc") },
        .{ .codec = .av1, .framing = .annex_b, .bytes = extendedSeed("av1-annex-b") },
    };
    for (cases) |case| {
        var guard = Guard{};
        var smith = std.testing.Smith{ .in = case.bytes };
        var payload: [128 * 1024]u8 = undefined;
        const length = smith.sliceWithHash(&payload, 0xA17F9001);
        try std.testing.expectEqual(case.bytes.len - 4, length);
        try std.testing.expectEqualSlices(u8, case.bytes[4..], payload[0..length]);
        const session = try decoder.Session.create(std.testing.allocator, case.codec, .{ .av1_framing = case.framing, .max_pixels = 512 * 512, .max_packet_bytes = 128 * 1024, .max_decode_bytes = 32 * 1024 * 1024, .max_output_bytes = 4 * 1024 * 1024, .control = .{ .context = &guard, .check_fn = Guard.check } });
        defer session.destroy();
        var batch = try session.decode(payload[0..length], 0, null);
        defer batch.deinit();
        try std.testing.expect(batch.frames.len != 0);
        for (batch.frames) |frame| try frame.host().validate();
    }
}
