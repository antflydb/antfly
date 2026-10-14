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

/// Packet tapes preserve all IVF packet headers/payloads without requiring an
/// intact container header. Smith can mutate interframes and framing lengths.
fn tapeSeed(comptime directory: []const u8, comptime name: []const u8) []const u8 {
    return comptime blk: {
        @setEvalBranchQuota(2_000_000);
        const ivf = @embedFile("../../testdata/" ++ directory ++ "/" ++ name ++ ".ivf");
        var encoded: [4 + ivf.len - 32]u8 = undefined;
        std.mem.writeInt(u32, encoded[0..4], ivf.len - 32, .little);
        @memcpy(encoded[4..], ivf[32..]);
        const frozen = encoded;
        break :blk &frozen;
    };
}
const TapeResult = struct { packets: usize = 0, outputs: usize = 0, materialized: usize = 0, retained_checks: usize = 0 };
fn batchHash(batch: decoder.Batch) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (batch.frames) |picture| hash.update(picture.bytes);
    return hash.finalResult();
}
fn verifyHeld(held: *[4]?decoder.Batch, hashes: *const [4][32]u8, result: *TapeResult) !void {
    for (held, hashes) |entry, expected| if (entry) |batch| {
        for (batch.frames) |picture| try picture.host().validate();
        try std.testing.expectEqual(expected, batchHash(batch));
        result.retained_checks += 1;
    };
}
fn runTape(codec: decoder.Codec, framing: decoder.Av1Framing, tape: []const u8, strict: bool) !TapeResult {
    const SequenceGuard = struct {
        calls: usize = 0,
        // Textured 480p seeds exceed the tiny-fixture guard; still bound
        // work independently of the packet/pixel/allocation limits.
        limit: usize = 2_000_000,
        fn check(raw: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.calls += 1;
            if (self.calls > self.limit) return error.Canceled;
        }
    };
    var guard = SequenceGuard{};
    var session: ?*decoder.Session = try decoder.Session.create(std.testing.allocator, codec, .{
        .av1_framing = framing,
        .max_pixels = 1024 * 1024,
        .max_packet_bytes = 128 * 1024,
        .max_decode_bytes = 32 * 1024 * 1024,
        .max_output_bytes = 4 * 1024 * 1024,
        .control = .{ .context = &guard, .check_fn = SequenceGuard.check },
    });
    defer if (session) |owner| owner.destroy();
    var held: [4]?decoder.Batch = @splat(null);
    defer for (&held) |*entry| if (entry.*) |*batch| batch.deinit();
    var hashes: [4][32]u8 = @splat(@splat(0));
    var result = TapeResult{};
    var offset: usize = 0;
    var rejected: usize = 0;
    while (offset <= tape.len and tape.len - offset >= 12 and result.packets < 32) {
        const length: usize = std.mem.readInt(u32, tape[offset..][0..4], .little);
        const flags = tape[offset + 11];
        offset += 12;
        if (length == 0 or length > tape.len - offset or length > 128 * 1024) {
            if (strict) return error.InvalidTape;
            break;
        }
        const packet = tape[offset..][0..length];
        offset += length;
        if (!strict and flags & 0x80 != 0) guard.limit = guard.calls + 1;
        try verifyHeld(&held, &hashes, &result);
        const selection: decoder.OutputSelection = switch (flags & 3) {
            1 => .none,
            2 => .{ .indexes = &.{0} },
            else => .all,
        };
        result.packets += 1;
        var batch = session.?.decodeOutputs(packet, @intCast(result.packets), null, selection) catch |err| {
            try verifyHeld(&held, &hashes, &result);
            if (strict) return err;
            rejected += 1;
            // Never reenter a poisoned core. Pre-entry rejection may leave the
            // session retryable, but control exhaustion remains terminal.
            if (session.?.failed or guard.calls > guard.limit or rejected >= 4) break;
            continue;
        };
        result.outputs += batch.output_count;
        result.materialized += batch.frames.len;
        try verifyHeld(&held, &hashes, &result);
        for (batch.frames) |picture| try picture.host().validate();
        if (batch.frames.len == 0) {
            batch.deinit();
        } else {
            const slot = (result.packets - 1) % held.len;
            if (held[slot]) |*old| old.deinit();
            hashes[slot] = batchHash(batch);
            held[slot] = batch;
        }
    }
    // Retained output is independently owned, including on decoder failure.
    session.?.destroy();
    session = null;
    try verifyHeld(&held, &hashes, &result);
    return result;
}

test "video fuzz sequence VP9 interframes retained outputs and cleanup" {
    const Harness = struct {
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var bytes: [128 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&bytes, 0xA17F9002);
            _ = try runTape(.vp9, .low_overhead, bytes[0..length], false);
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &.{ tapeSeed("vpx-av1", "vp9"), tapeSeed("vpx-av1", "vp9-yuv420p12le"), tapeSeed("vpx-av1", "vp9-yuv444p10le"), tapeSeed("vpx-av1", "vp9-yuv422p12le"), tapeSeed("sequence-fuzz", "sintel-vp9-detail") } });
}
test "video fuzz sequence AV1 interframes layers retained outputs and cleanup" {
    const Harness = struct {
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var bytes: [128 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&bytes, 0xA17FA002);
            _ = try runTape(.av1, .low_overhead, bytes[0..length], false);
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &.{ tapeSeed("vpx-av1", "av1"), tapeSeed("vpx-av1", "av1-10"), tapeSeed("vpx-av1", "av1-grain"), tapeSeed("vpx-av1", "av1-odd"), tapeSeed("av1-extended", "av1-c0-12"), tapeSeed("av1-extended", "av1-c2-12"), tapeSeed("av1-extended", "av1-c3-12"), tapeSeed("av1-extended", "av1-svc"), tapeSeed("sequence-fuzz", "sintel-av1-detail") } });
}
test "video fuzz sequence Annex B retained outputs and cleanup" {
    const Harness = struct {
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var bytes: [128 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&bytes, 0xA17FAB02);
            _ = try runTape(.av1, .annex_b, bytes[0..length], false);
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &.{tapeSeed("av1-extended", "av1-annex-b")} });
}
test "codec fuzz sequence seeds consume complete streams and retain independent pictures" {
    inline for (.{ .{ "vpx-av1", "vp9", decoder.Codec.vp9, decoder.Av1Framing.low_overhead }, .{ "vpx-av1", "vp9-yuv420p12le", decoder.Codec.vp9, decoder.Av1Framing.low_overhead }, .{ "vpx-av1", "vp9-yuv444p10le", decoder.Codec.vp9, decoder.Av1Framing.low_overhead }, .{ "vpx-av1", "vp9-yuv422p12le", decoder.Codec.vp9, decoder.Av1Framing.low_overhead }, .{ "vpx-av1", "av1", decoder.Codec.av1, decoder.Av1Framing.low_overhead }, .{ "vpx-av1", "av1-10", decoder.Codec.av1, decoder.Av1Framing.low_overhead }, .{ "vpx-av1", "av1-grain", decoder.Codec.av1, decoder.Av1Framing.low_overhead }, .{ "vpx-av1", "av1-odd", decoder.Codec.av1, decoder.Av1Framing.low_overhead }, .{ "av1-extended", "av1-c0-12", decoder.Codec.av1, decoder.Av1Framing.low_overhead }, .{ "av1-extended", "av1-c2-12", decoder.Codec.av1, decoder.Av1Framing.low_overhead }, .{ "av1-extended", "av1-c3-12", decoder.Codec.av1, decoder.Av1Framing.low_overhead }, .{ "av1-extended", "av1-svc", decoder.Codec.av1, decoder.Av1Framing.low_overhead }, .{ "av1-extended", "av1-annex-b", decoder.Codec.av1, decoder.Av1Framing.annex_b }, .{ "sequence-fuzz", "sintel-vp9-detail", decoder.Codec.vp9, decoder.Av1Framing.low_overhead }, .{ "sequence-fuzz", "sintel-av1-detail", decoder.Codec.av1, decoder.Av1Framing.low_overhead } }) |case| {
        const encoded = tapeSeed(case[0], case[1]);
        const result = runTape(case[2], case[3], encoded[4..], true) catch |err| {
            std.debug.print("sequence seed {s}: {s}\n", .{ case[1], @errorName(err) });
            return err;
        };
        try std.testing.expectEqual(@as(usize, 12), result.packets);
        try std.testing.expect(result.outputs >= 12);
        try std.testing.expect(result.retained_checks > 4);
    }
}

test "codec sequence selection and cancellation preserve retained pictures" {
    inline for (.{ .{ "vp9", decoder.Codec.vp9 }, .{ "av1", decoder.Codec.av1 } }) |case| {
        const encoded = comptime tapeSeed("vpx-av1", case[0]);
        var tape: [encoded.len - 4]u8 = undefined;
        @memcpy(&tape, encoded[4..]);
        var offset: usize = 0;
        var index: usize = 0;
        while (offset < tape.len) : (index += 1) {
            const length = std.mem.readInt(u32, tape[offset..][0..4], .little);
            tape[offset + 11] = if (index % 2 == 1) 1 else 2;
            offset += 12 + length;
        }
        const selected = try runTape(case[1], .low_overhead, &tape, true);
        try std.testing.expectEqual(@as(usize, 12), selected.outputs);
        try std.testing.expectEqual(@as(usize, 6), selected.materialized);
        try std.testing.expect(selected.retained_checks > 4);
        const first_length = std.mem.readInt(u32, tape[0..4], .little);
        tape[12 + first_length + 11] = 0x80;
        const canceled = try runTape(case[1], .low_overhead, &tape, false);
        try std.testing.expectEqual(@as(usize, 2), canceled.packets);
        try std.testing.expectEqual(@as(usize, 1), canceled.outputs);
        try std.testing.expectEqual(@as(usize, 1), canceled.materialized);
        try std.testing.expect(canceled.retained_checks >= 3);
    }
}
