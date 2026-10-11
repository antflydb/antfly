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

const std = @import("std");
const decoder = @import("decoder.zig");
const media = @import("antfly_media");
const Golden = struct { name: []const u8, codec: decoder.Codec, width: u32, height: u32, depth: u8, chroma: u8, packet_sha256: []const u8, frame_sha256: [][]const u8 };
const fixtures = .{ "av1", "av1-10", "av1-grain", "av1-odd", "vp9", "vp9-yuv420p", "vp9-yuv420p10le", "vp9-yuv420p12le", "vp9-yuv422p", "vp9-yuv422p12le", "vp9-yuv444p", "vp9-yuv444p10le" };
fn first(comptime name: []const u8) []const u8 {
    const b = @embedFile("../../testdata/vpx-av1/" ++ name ++ ".ivf");
    return b[44..][0..std.mem.readInt(u32, b[32..36], .little)];
}
fn hashFrame(item: decoder.Frame) [32]u8 {
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    const host = item.host();
    h.update(host.planes[0].bytes);
    const n: usize = if (item.bit_depth == 8) 1 else 2;
    const uv = host.planes[1].bytes;
    for (0..2) |p| {
        var i: usize = p * n;
        while (i < uv.len) : (i += 2 * n) h.update(uv[i..][0..n]);
    }
    var out: [32]u8 = undefined;
    h.final(&out);
    return out;
}
test "VP9 AV1 public sessions match independent per-picture hashes" {
    const a = std.testing.allocator;
    const parsed = try std.json.parseFromSlice([]Golden, a, @embedFile("../../testdata/vpx-av1/golden.json"), .{});
    defer parsed.deinit();
    inline for (fixtures) |name| {
        const coded = @embedFile("../../testdata/vpx-av1/" ++ name ++ ".ivf");
        var golden: ?Golden = null;
        for (parsed.value) |g| {
            if (std.mem.eql(u8, g.name, name)) golden = g;
        }
        const g = golden.?;
        const session = try decoder.Session.create(a, g.codec, .{});
        defer session.destroy();
        var at: usize = 32;
        var displayed: usize = 0;
        while (at < coded.len) {
            const size = std.mem.readInt(u32, coded[at..][0..4], .little);
            at += 12;
            var batch = try session.decode(coded[at..][0..size], @intCast(displayed), null);
            defer batch.deinit();
            at += size;
            for (batch.frames) |item| {
                try std.testing.expect(displayed < g.frame_sha256.len);
                try std.testing.expectEqual(g.width, item.width);
                try std.testing.expectEqual(g.height, item.height);
                try std.testing.expectEqual(g.depth, item.bit_depth);
                try std.testing.expectEqual(g.chroma, item.chroma_format);
                try item.host().validate();
                var expected: [32]u8 = undefined;
                _ = try std.fmt.hexToBytes(&expected, g.frame_sha256[displayed]);
                try std.testing.expectEqualSlices(u8, &expected, &hashFrame(item));
                displayed += 1;
            }
        }
        try std.testing.expectEqual(g.frame_sha256.len, displayed);
    }
}
test "VP9 AV1 output survives session destruction and releases admission" {
    const a = std.testing.allocator;
    inline for (.{ decoder.Codec.vp9, decoder.Codec.av1 }) |codec| {
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 32 * 1024 * 1024 } };
        const session = try decoder.Session.create(a, codec, .{ .max_decode_bytes = 16 * 1024 * 1024, .admission_pool = &pool });
        var batch = try session.decode(if (codec == .vp9) first("vp9") else first("av1"), 42, 7);
        session.destroy();
        try std.testing.expectEqual(@as(usize, 1), batch.frames.len);
        const item = batch.frames[0];
        try std.testing.expectEqual(@as(i64, 42), item.pts_ns);
        try std.testing.expectEqual(@as(?u64, 7), item.duration_ns);
        try std.testing.expectEqual(@as(u64, item.bytes.len), pool.snapshot().host_bytes);
        try item.host().validate();
        batch.deinit();
        try std.testing.expectEqual(@as(u64, 0), pool.snapshot().host_bytes);
    }
}
test "VP9 AV1 bounded allocation and preflight failures" {
    const a = std.testing.allocator;
    inline for (.{ decoder.Codec.vp9, decoder.Codec.av1 }) |codec| {
        try std.testing.expectError(error.ResourceLimitExceeded, decoder.Session.create(a, codec, .{ .max_decode_bytes = 1 }));
        const session = try decoder.Session.create(a, codec, .{ .max_packet_bytes = 1 });
        defer session.destroy();
        try std.testing.expectError(error.ResourceLimitExceeded, session.decode(if (codec == .vp9) first("vp9") else first("av1"), 0, null));
        try std.testing.expect(!session.failed);
    }
}
test "VP9 AV1 post-entry output denial invalidates and reclaims core" {
    const a = std.testing.allocator;
    inline for (.{ decoder.Codec.vp9, decoder.Codec.av1 }) |codec| {
        const session = try decoder.Session.create(a, codec, .{ .max_output_bytes = 1 });
        defer session.destroy();
        const packet = if (codec == .vp9) first("vp9") else first("av1");
        try std.testing.expectError(error.ResourceLimitExceeded, session.decode(packet, 0, null));
        try std.testing.expect(session.failed);
        try std.testing.expectEqual(@as(usize, 0), session.owner.budget.live);
        try std.testing.expectError(error.InvalidDecoderSession, session.decode(packet, 0, null));
    }
}
const Cancellation = struct {
    calls: usize = 0,
    limit: usize = std.math.maxInt(usize),
    fn check(context: ?*const anyopaque) !void {
        const self: *Cancellation = @ptrCast(@alignCast(@constCast(context.?)));
        self.calls += 1;
        if (self.calls >= self.limit) return error.Cancelled;
    }
};
test "VP9 AV1 cancellation before and during reconstruction" {
    inline for (.{ decoder.Codec.vp9, decoder.Codec.av1 }) |codec| {
        var cancellation = Cancellation{};
        const session = try decoder.Session.create(std.testing.allocator, codec, .{ .control = .{ .context = &cancellation, .check_fn = Cancellation.check } });
        defer session.destroy();
        const packet = if (codec == .vp9) first("vp9") else first("av1");
        cancellation.limit = cancellation.calls + 1;
        try std.testing.expectError(error.Cancelled, session.decode(packet, 0, null));
        try std.testing.expect(!session.failed);
        cancellation.calls = 0;
        cancellation.limit = 20;
        try std.testing.expectError(error.Cancelled, session.decode(packet, 0, null));
        try std.testing.expect(session.failed);
        try std.testing.expectEqual(@as(usize, 0), session.owner.budget.live);
    }
}
fn allocationFailure(codec: decoder.Codec) !void {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(a: std.mem.Allocator, c: decoder.Codec) !void {
            const session = try decoder.Session.create(a, c, .{});
            defer session.destroy();
            var batch = try session.decode(if (c == .vp9) first("vp9") else first("av1"), 0, null);
            defer batch.deinit();
        }
    }.run, .{codec});
}
test "VP9 AV1 every first-picture allocation failure is reclaimed" {
    try allocationFailure(.vp9);
    try allocationFailure(.av1);
}
test "VP9 AV1 truncated packets fail without retained allocations" {
    inline for (.{ decoder.Codec.vp9, decoder.Codec.av1 }) |codec| {
        const packet = if (codec == .vp9) first("vp9") else first("av1");
        for ([_]usize{ 1, 4, 16, 32, packet.len / 2 }) |length| {
            const session = try decoder.Session.create(std.testing.allocator, codec, .{});
            defer session.destroy();
            if (session.decode(packet[0..length], 0, null)) |value| {
                var batch = value;
                batch.deinit();
            } else |_| {}
        }
    }
}
const Worker = struct {
    codec: decoder.Codec,
    pool: *media.admission.Pool,
    failed: std.atomic.Value(bool) = .init(false),
    fn run(self: *Worker) void {
        self.decode() catch {
            self.failed.store(true, .release);
        };
    }
    fn decode(self: *Worker) !void {
        for (0..3) |_| {
            const session = try decoder.Session.create(std.heap.page_allocator, self.codec, .{ .max_decode_bytes = 16 * 1024 * 1024, .admission_pool = self.pool });
            defer session.destroy();
            var batch = try session.decode(if (self.codec == .vp9) first("vp9") else first("av1"), 0, null);
            defer batch.deinit();
            if (batch.frames.len != 1) return error.MissingPicture;
        }
    }
};
test "VP9 AV1 independent concurrent sessions share admission safely" {
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 80 * 1024 * 1024 } };
    var workers = [_]Worker{ .{ .codec = .vp9, .pool = &pool }, .{ .codec = .av1, .pool = &pool }, .{ .codec = .vp9, .pool = &pool }, .{ .codec = .av1, .pool = &pool } };
    var threads: [4]std.Thread = undefined;
    var started: usize = 0;
    defer for (threads[0..started]) |thread| thread.join();
    for (&workers, &threads) |*worker, *thread| {
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{worker});
        started += 1;
    }
    // Join here so admission assertions observe completed workers.
    for (threads) |thread| thread.join();
    started = 0;
    for (&workers) |*worker| try std.testing.expect(!worker.failed.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), pool.snapshot().host_bytes);
}
test "VP9 AV1 bounded deterministic header mutations" {
    inline for (.{ decoder.Codec.vp9, decoder.Codec.av1 }) |codec| {
        const packet = if (codec == .vp9) first("vp9") else first("av1");
        const mutated = try std.testing.allocator.dupe(u8, packet);
        defer std.testing.allocator.free(mutated);
        for (0..64) |i| {
            @memcpy(mutated, packet);
            mutated[i / 8] ^= @as(u8, 1) << @intCast(i % 8);
            const session = try decoder.Session.create(std.testing.allocator, codec, .{ .max_decode_bytes = 16 * 1024 * 1024, .max_pixels = 64 * 1024 });
            defer session.destroy();
            if (session.decode(mutated, 0, null)) |result| {
                var batch = result;
                batch.deinit();
            } else |_| {}
        }
    }
}
test "VP9 AV1 superframe exposes every visible picture" {
    const coded = @embedFile("../../testdata/vpx-av1/vp9.ivf");
    const n = std.mem.readInt(u32, coded[32..36], .little);
    const m = std.mem.readInt(u32, coded[44 + n ..][0..4], .little);
    const combined = try std.testing.allocator.alloc(u8, n + m + 6);
    defer std.testing.allocator.free(combined);
    @memcpy(combined[0..n], coded[44..][0..n]);
    @memcpy(combined[n..][0..m], coded[56 + n ..][0..m]);
    combined[n + m] = 0xc9;
    std.mem.writeInt(u16, combined[n + m + 1 ..][0..2], @intCast(n), .little);
    std.mem.writeInt(u16, combined[n + m + 3 ..][0..2], @intCast(m), .little);
    combined[n + m + 5] = 0xc9;
    const session = try decoder.Session.create(std.testing.allocator, .vp9, .{});
    defer session.destroy();
    var batch = try session.decode(combined, 0, null);
    defer batch.deinit();
    try std.testing.expectEqual(@as(usize, 2), batch.frames.len);
    const parsed = try std.json.parseFromSlice([]Golden, std.testing.allocator, @embedFile("../../testdata/vpx-av1/golden.json"), .{});
    defer parsed.deinit();
    for (parsed.value) |g| {
        if (std.mem.eql(u8, g.name, "vp9")) {
            for (batch.frames, 0..) |item, i| {
                var expected: [32]u8 = undefined;
                _ = try std.fmt.hexToBytes(&expected, g.frame_sha256[i]);
                try std.testing.expectEqualSlices(u8, &expected, &hashFrame(item));
            }
        }
    }
}
test "VP9 AV1 WebM reader routes coded packets and timestamps" {
    inline for (.{ "vp9", "av1" }) |name| {
        const a = std.testing.allocator;
        var input = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = @embedFile("../../testdata/vpx-av1/" ++ name ++ ".webm") } };
        var reader = try media.webm.Reader.init(a, &input, .{});
        defer reader.deinit();
        const session = try decoder.Session.create(a, if (std.mem.eql(u8, name, "vp9")) .vp9 else .av1, .{});
        defer session.destroy();
        var displayed: usize = 0;
        for (0..reader.packets.len) |index| {
            var batch = try session.decodeWebmPacket(&reader, index);
            defer batch.deinit();
            for (batch.frames) |item| {
                try std.testing.expectEqual(reader.packets[index].pts, item.pts_ns);
                try item.host().validate();
                displayed += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 12), displayed);
    }
}
extern fn av_test_device_create() ?*anyopaque;
extern fn av_test_device_destroy(*anyopaque) void;
test "VP9 AV1 native-depth decoded frames preserve CPU Metal preparation parity" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const prep = @import("../preparation.zig");
    const a = std.testing.allocator;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    var metal = try prep.Metal.init(device);
    defer metal.deinit();
    inline for (.{ "av1-10", "av1-odd", "vp9-yuv420p12le", "vp9-yuv422p12le", "vp9-yuv444p10le" }) |name| {
        const session = try decoder.Session.create(a, if (std.mem.startsWith(u8, name, "av1")) .av1 else .vp9, .{});
        defer session.destroy();
        var batch = try session.decode(first(name), 0, null);
        defer batch.deinit();
        const host = batch.frames[0].host();
        const options = prep.Options{ .width = 48, .height = 48, .matrix = .bt709 };
        const expected = try prep.referenceHost(a, host, options, .{});
        defer a.free(expected);
        var job = try metal.submitHost(a, host, options, .{});
        defer job.deinit();
        try job.wait(std.testing.io, .{});
        const actual = try job.readback(a);
        defer a.free(actual);
        for (expected, actual) |e, v| try std.testing.expectApproxEqAbs(e, v, 0.000001);
    }
}
