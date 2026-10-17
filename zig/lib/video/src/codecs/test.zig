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
test {
    _ = @import("annex_b.zig");
}
const decoder = @import("decoder.zig");
const media = @import("antfly_media");
const Golden = struct { framing: decoder.Av1Framing = .low_overhead, name: []const u8, codec: decoder.Codec, width: u32, height: u32, depth: u8, chroma: u8, packet_sha256: []const u8, frame_sha256: [][]const u8 };
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
test "VP9 AV1 real sequence pictures match independent decoder hashes" {
    const a = std.testing.allocator;
    const parsed = try std.json.parseFromSlice([]Golden, a, @embedFile("../../testdata/sequence-fuzz/golden.json"), .{});
    defer parsed.deinit();
    inline for (.{ "sintel-vp9-detail", "sintel-av1-detail" }) |name| {
        const coded = @embedFile("../../testdata/sequence-fuzz/" ++ name ++ ".ivf");
        var golden: ?Golden = null;
        for (parsed.value) |entry| if (std.mem.eql(u8, entry.name, name)) {
            golden = entry;
        };
        const g = golden.?;
        var expected_packet_hash: [32]u8 = undefined;
        _ = try std.fmt.hexToBytes(&expected_packet_hash, g.packet_sha256);
        var packet_hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(coded, &packet_hash, .{});
        try std.testing.expectEqual(expected_packet_hash, packet_hash);
        const session = try decoder.Session.create(a, g.codec, .{});
        defer session.destroy();
        var offset: usize = 32;
        var displayed: usize = 0;
        while (offset < coded.len) {
            const length = std.mem.readInt(u32, coded[offset..][0..4], .little);
            offset += 12;
            var batch = try session.decode(coded[offset..][0..length], @intCast(displayed), null);
            defer batch.deinit();
            offset += length;
            for (batch.frames) |picture| {
                try std.testing.expect(displayed < g.frame_sha256.len);
                try std.testing.expectEqual(g.width, picture.width);
                try std.testing.expectEqual(g.height, picture.height);
                try std.testing.expectEqual(g.depth, picture.bit_depth);
                try std.testing.expectEqual(g.chroma, picture.chroma_format);
                var expected: [32]u8 = undefined;
                _ = try std.fmt.hexToBytes(&expected, g.frame_sha256[displayed]);
                try std.testing.expectEqual(expected, hashFrame(picture));
                displayed += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 12), displayed);
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
        try std.testing.expectEqual(@as(u64, item.bytes.len + batch.frames.len * @sizeOf(decoder.Frame)), pool.snapshot().host_bytes);
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
            const session = try decoder.Session.create(std.heap.page_allocator, self.codec, .{ .admission_pool = self.pool });
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
    const filtered = try decoder.Session.create(std.testing.allocator, .vp9, .{});
    defer filtered.destroy();
    var chosen = try filtered.decodeOutputs(combined, 0, null, .{ .indexes = &.{ 1, 1 } });
    defer chosen.deinit();
    try std.testing.expectEqual(@as(usize, 2), chosen.output_count);
    try std.testing.expectEqual(@as(usize, 1), chosen.frames.len);
    try std.testing.expectEqual(@as(usize, 1), chosen.frames[0].output_index);
    try std.testing.expectEqualSlices(u8, batch.frames[1].bytes, chosen.frames[0].bytes);
    const limited = try decoder.Session.create(std.testing.allocator, .vp9, .{ .max_output_frames = 1 });
    defer limited.destroy();
    try std.testing.expectError(error.ResourceLimitExceeded, limited.decodeOutputs(combined, 0, null, .none));
    try std.testing.expect(limited.failed);
    try std.testing.expectEqual(@as(usize, 0), limited.owner.budget.live);
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

test "VP9 AV1 real Sintel WebM pictures match independent reference hashes" {
    const a = std.testing.allocator;
    const Stream = struct { name: []const u8, codec: decoder.Codec, sha256: []const u8, width: u32, height: u32, frame_sha256: [][]const u8 };
    const Report = struct { streams: []Stream };
    const parsed = try std.json.parseFromSlice(Report, a, @embedFile("../../testdata/vpx-av1/real-streams.json"), .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    inline for (.{ "vp9-sintel", "av1-sintel" }) |name| {
        const bytes = @embedFile("../../testdata/vpx-av1/" ++ name ++ ".webm");
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = name };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        const golden = for (parsed.value.streams) |stream| {
            if (std.mem.eql(u8, stream.name, name)) break stream;
        } else return error.MissingGoldenStream;
        var file_hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &file_hash, .{});
        var expected: [32]u8 = undefined;
        _ = try std.fmt.hexToBytes(&expected, golden.sha256);
        try std.testing.expectEqualSlices(u8, &expected, &file_hash);
        const session = try decoder.Session.create(a, golden.codec, .{});
        defer session.destroy();
        var displayed: usize = 0;
        for (reader.packets, 0..) |_, id| {
            var batch = try session.decodeWebmPacket(&reader, id);
            defer batch.deinit();
            for (batch.frames) |frame| {
                try std.testing.expect(displayed < golden.frame_sha256.len);
                try std.testing.expectEqual(golden.width, frame.width);
                try std.testing.expectEqual(golden.height, frame.height);
                _ = try std.fmt.hexToBytes(&expected, golden.frame_sha256[displayed]);
                try std.testing.expectEqualSlices(u8, &expected, &hashFrame(frame));
                displayed += 1;
            }
        }
        try std.testing.expectEqual(golden.frame_sha256.len, displayed);
    }
}

fn leb(out: *std.array_list.Managed(u8), size: usize) !void {
    var value = size;
    while (true) {
        const byte: u8 = @intCast(value & 127);
        value >>= 7;
        try out.append(byte | @as(u8, if (value == 0) 0 else 128));
        if (value == 0) break;
    }
}
fn annexPacket(a: std.mem.Allocator, packet: []const u8) ![]u8 {
    var obu: std.array_list.Managed(u8) = .init(a);
    defer obu.deinit();
    var frame_unit: std.array_list.Managed(u8) = .init(a);
    defer frame_unit.deinit();
    var has_frame_header = false;
    var at: usize = 0;
    while (at < packet.len) {
        const start = at;
        const kind = (packet[start] >> 3) & 15;
        if (kind == 3 or kind == 6) {
            if (has_frame_header) {
                try leb(&frame_unit, obu.items.len);
                try frame_unit.appendSlice(obu.items);
                obu.clearRetainingCapacity();
            }
            has_frame_header = true;
        }
        at += 1 + @as(usize, if (packet[start] & 4 != 0) 1 else 0);
        var size: usize = 0;
        var shift: u6 = 0;
        while (true) {
            const byte = packet[at];
            at += 1;
            size |= @as(usize, byte & 127) << shift;
            if (byte & 128 == 0) break;
            shift += 7;
        }
        const header_length: usize = if (packet[start] & 4 != 0) 2 else 1;
        try leb(&obu, header_length + size);
        try obu.append(packet[start] & ~@as(u8, 2));
        if (header_length == 2) try obu.append(packet[start + 1]);
        try obu.appendSlice(packet[at..][0..size]);
        at += size;
    }
    try leb(&frame_unit, obu.items.len);
    try frame_unit.appendSlice(obu.items);
    var temporal: std.array_list.Managed(u8) = .init(a);
    defer temporal.deinit();
    try leb(&temporal, frame_unit.items.len);
    try temporal.appendSlice(frame_unit.items);
    return temporal.toOwnedSlice();
}
test "AV1 Annex B session matches low-overhead and preflight errors are retryable" {
    const a = std.testing.allocator;
    const reference = try decoder.Session.create(a, .av1, .{});
    defer reference.destroy();
    const session = try decoder.Session.create(a, .av1, .{ .av1_framing = .annex_b });
    defer session.destroy();
    const live = session.owner.budget.live;
    try std.testing.expectError(error.InvalidVideoData, session.decode(&.{ 3, 2, 1, 0x08 }, 0, null));
    try std.testing.expect(!session.failed);
    try std.testing.expectEqual(live, session.owner.budget.live);
    const coded = @embedFile("../../testdata/vpx-av1/av1.ivf");
    var at: usize = 32;
    var count: usize = 0;
    while (at < coded.len) {
        const size = std.mem.readInt(u32, coded[at..][0..4], .little);
        at += 12;
        const packet = coded[at..][0..size];
        at += size;
        const framed = try annexPacket(a, packet);
        defer a.free(framed);
        var expected = try reference.decode(packet, 11, 17);
        defer expected.deinit();
        var actual = try session.decode(framed, 11, 17);
        defer actual.deinit();
        try std.testing.expectEqual(expected.frames.len, actual.frames.len);
        for (expected.frames, actual.frames) |left, right| {
            try std.testing.expectEqualSlices(u8, &hashFrame(left), &hashFrame(right));
            try std.testing.expectEqual(@as(i64, 11), right.pts_ns);
            try std.testing.expectEqual(@as(?u64, 17), right.duration_ns);
            count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 12), count);
}

test "AV1 profiles native-depth monochrome and chroma match AOM reference hashes" {
    const a = std.testing.allocator;
    const parsed = try std.json.parseFromSlice([]Golden, a, @embedFile("../../testdata/av1-extended/golden.json"), .{});
    defer parsed.deinit();
    inline for (.{ "av1-c0-8", "av1-c0-12", "av1-c1-12", "av1-c2-8", "av1-c2-10", "av1-c2-12", "av1-c3-8", "av1-c3-10", "av1-c3-12", "av1-annex-b" }) |name| {
        const coded = @embedFile("../../testdata/av1-extended/" ++ name ++ ".ivf");
        var golden: ?Golden = null;
        for (parsed.value) |g| {
            if (std.mem.eql(u8, g.name, name)) golden = g;
        }
        const g = golden.?;
        var coded_hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(coded, &coded_hash, .{});
        var expected: [32]u8 = undefined;
        _ = try std.fmt.hexToBytes(&expected, g.packet_sha256);
        try std.testing.expectEqualSlices(u8, &expected, &coded_hash);
        const session = try decoder.Session.create(a, .av1, .{ .av1_framing = g.framing });
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
                _ = try std.fmt.hexToBytes(&expected, g.frame_sha256[displayed]);
                try std.testing.expectEqualSlices(u8, &expected, &hashFrame(item));
                displayed += 1;
            }
        }
        try std.testing.expectEqual(g.frame_sha256.len, displayed);
    }
}

const LayerGolden = struct {
    packet_sha256: []const u8,
    cases: []struct {
        operating_point: u5,
        all_layers: bool,
        frames: []struct { width: u32, height: u32, sha256: []const u8 },
    },
};
test "AV1 spatial temporal operating points match independent AOM outputs" {
    const a = std.testing.allocator;
    const parsed = try std.json.parseFromSlice(LayerGolden, a, @embedFile("../../testdata/av1-extended/layers.json"), .{});
    defer parsed.deinit();
    const coded = @embedFile("../../testdata/av1-extended/av1-svc.ivf");
    var actual_hash: [32]u8 = undefined;
    var expected: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(coded, &actual_hash, .{});
    _ = try std.fmt.hexToBytes(&expected, parsed.value.packet_sha256);
    try std.testing.expectEqualSlices(u8, &expected, &actual_hash);
    for (parsed.value.cases) |g| {
        inline for (.{ decoder.Av1Framing.low_overhead, decoder.Av1Framing.annex_b }) |framing| {
            const session = try decoder.Session.create(a, .av1, .{ .av1_framing = framing, .av1_operating_point = g.operating_point, .av1_all_layers = g.all_layers });
            defer session.destroy();
            var at: usize = 32;
            var displayed: usize = 0;
            while (at < coded.len) {
                const size = std.mem.readInt(u32, coded[at..][0..4], .little);
                at += 12;
                const packet = coded[at..][0..size];
                const framed: ?[]u8 = if (framing == .annex_b) try annexPacket(a, packet) else null;
                defer if (framed) |bytes| a.free(bytes);
                var batch = try session.decode(framed orelse packet, @intCast(displayed), null);
                defer batch.deinit();
                at += size;
                for (batch.frames) |item| {
                    try std.testing.expect(displayed < g.frames.len);
                    try std.testing.expectEqual(g.frames[displayed].width, item.width);
                    try std.testing.expectEqual(g.frames[displayed].height, item.height);
                    _ = try std.fmt.hexToBytes(&expected, g.frames[displayed].sha256);
                    try std.testing.expectEqualSlices(u8, &expected, &hashFrame(item));
                    displayed += 1;
                }
            }
            try std.testing.expectEqual(g.frames.len, displayed);
        }
    }
}

test "AV1 Annex B normalization uses shared decode budget and survives denial" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 32 * 1024 * 1024 } };
    const session = try decoder.Session.create(a, .av1, .{ .av1_framing = .annex_b, .max_decode_bytes = 16 * 1024 * 1024, .admission_pool = &pool });
    defer session.destroy();
    const live = session.owner.budget.live;
    const limit = session.owner.budget.limit;
    session.owner.budget.limit = live + 1;
    try std.testing.expectError(error.ResourceLimitExceeded, session.decode(&.{ 3, 2, 1, 0x10 }, 0, null));
    try std.testing.expect(!session.failed);
    try std.testing.expectEqual(live, session.owner.budget.live);
    session.owner.budget.limit = limit;
    const packet = try annexPacket(a, first("av1"));
    defer a.free(packet);
    var batch = try session.decode(packet, 0, null);
    defer batch.deinit();
    try std.testing.expectEqual(@as(usize, 1), batch.frames.len);
}
test "AV1 explicit unavailable operating point fails without silent fallback" {
    const session = try decoder.Session.create(std.testing.allocator, .av1, .{ .av1_operating_point = 31 });
    defer session.destroy();
    try std.testing.expectError(error.InvalidDecodeOptions, session.decode(first("av1"), 0, null));
    try std.testing.expect(session.failed);
    try std.testing.expectEqual(@as(usize, 0), session.owner.budget.live);
}

test "AV1 Annex B cancellation and post-entry denial release normalization" {
    const a = std.testing.allocator;
    const packet = try annexPacket(a, first("av1"));
    defer a.free(packet);
    var cancellation = Cancellation{};
    const session = try decoder.Session.create(a, .av1, .{ .av1_framing = .annex_b, .control = .{ .context = &cancellation, .check_fn = Cancellation.check } });
    defer session.destroy();
    const live = session.owner.budget.live;
    cancellation.limit = cancellation.calls + 3;
    try std.testing.expectError(error.Cancelled, session.decode(packet, 0, null));
    try std.testing.expect(!session.failed);
    try std.testing.expectEqual(live, session.owner.budget.live);
    cancellation.limit = std.math.maxInt(usize);
    var batch = try session.decode(packet, 0, null);
    defer batch.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 32 * 1024 * 1024 } };
    const denied = try decoder.Session.create(a, .av1, .{ .av1_framing = .annex_b, .max_decode_bytes = 16 * 1024 * 1024, .max_output_bytes = 1, .admission_pool = &pool });
    defer denied.destroy();
    try std.testing.expectError(error.ResourceLimitExceeded, denied.decode(packet, 0, null));
    try std.testing.expect(denied.failed);
    try std.testing.expectEqual(@as(usize, 0), denied.owner.budget.live);
    try std.testing.expectEqual(@as(u64, @sizeOf(decoder.Session)), pool.snapshot().host_bytes);
}

fn selectedOutputParity(coded: []const u8, codec: decoder.Codec, options: decoder.Options) !void {
    const a = std.testing.allocator;
    const baseline = try decoder.Session.create(a, codec, options);
    defer baseline.destroy();
    const selected = try decoder.Session.create(a, codec, options);
    defer selected.destroy();
    var at: usize = 32;
    var packet: usize = 0;
    var expected_bytes: u64 = 0;
    var skipped: usize = 0;
    while (at < coded.len) : (packet += 1) {
        const size = std.mem.readInt(u32, coded[at..][0..4], .little);
        at += 12;
        const bytes = coded[at..][0..size];
        at += size;
        var expected = try baseline.decode(bytes, @intCast(packet), 123);
        defer expected.deinit();
        // A skipped key/reference output must still predict following outputs.
        const selection: decoder.OutputSelection = switch (packet % 3) {
            0 => .none,
            1 => .{ .indexes = &.{0} },
            else => .all,
        };
        var actual = try selected.decodeOutputs(bytes, @intCast(packet), 123, selection);
        defer actual.deinit();
        try std.testing.expectEqual(expected.frames.len, actual.output_count);
        var count: usize = 0;
        for (expected.frames, 0..) |frame, i| {
            const wanted = selection == .all or (selection == .indexes and i == 0);
            if (!wanted) {
                skipped += 1;
                continue;
            }
            const item = actual.frames[count];
            count += 1;
            expected_bytes += frame.bytes.len;
            try std.testing.expectEqual(i, item.output_index);
            try std.testing.expectEqual(frame.pts_ns, item.pts_ns);
            try std.testing.expectEqual(frame.duration_ns, item.duration_ns);
            try std.testing.expectEqual(frame.width, item.width);
            try std.testing.expectEqual(frame.height, item.height);
            try std.testing.expectEqual(frame.bit_depth, item.bit_depth);
            try std.testing.expectEqual(frame.chroma_format, item.chroma_format);
            try std.testing.expectEqual(frame.color, item.color);
            try std.testing.expectEqualSlices(u8, frame.bytes, item.bytes);
        }
        try std.testing.expectEqual(count, actual.frames.len);
    }
    try std.testing.expect(skipped > 0);
    try std.testing.expect(expected_bytes > 0);
    try std.testing.expectEqual(expected_bytes, selected.output_stats.copied_bytes);
    try std.testing.expectEqual(skipped, selected.output_stats.skipped_outputs);
    try std.testing.expect(selected.output_stats.copied_bytes < baseline.output_stats.copied_bytes);
}
test "VP9 AV1 filtered outputs preserve skipped references depth chroma grain and layers" {
    inline for (fixtures) |name| try selectedOutputParity(@embedFile("../../testdata/vpx-av1/" ++ name ++ ".ivf"), if (std.mem.startsWith(u8, name, "vp9")) .vp9 else .av1, .{});
    inline for (.{ "av1-c0-8", "av1-c0-12", "av1-c1-12", "av1-c2-8", "av1-c2-10", "av1-c2-12", "av1-c3-8", "av1-c3-10", "av1-c3-12", "av1-annex-b" }) |name|
        try selectedOutputParity(@embedFile("../../testdata/av1-extended/" ++ name ++ ".ivf"), .av1, .{ .av1_framing = if (std.mem.eql(u8, name, "av1-annex-b")) .annex_b else .low_overhead });
    for (0..2) |point| inline for (.{ true, false }) |layers| try selectedOutputParity(@embedFile("../../testdata/av1-extended/av1-svc.ivf"), .av1, .{ .av1_operating_point = @intCast(point), .av1_all_layers = layers });
}
test "VP9 AV1 skipped outputs use no output byte budget and selector errors remain retryable" {
    inline for (.{ decoder.Codec.vp9, decoder.Codec.av1 }) |codec| {
        const session = try decoder.Session.create(std.testing.allocator, codec, .{ .max_output_bytes = 0 });
        defer session.destroy();
        const bytes = if (codec == .vp9) first("vp9") else first("av1");
        try std.testing.expectError(error.InvalidPacketIndex, session.decodeOutputs(bytes, 0, null, .{ .indexes = &.{8} }));
        try std.testing.expect(!session.failed);
        var empty = try session.decodeOutputs(bytes, 0, null, .none);
        defer empty.deinit();
        try std.testing.expect(empty.output_count > 0);
        try std.testing.expectEqual(@as(usize, 0), empty.frames.len);
        try std.testing.expectEqual(@as(u64, 0), session.output_stats.copied_bytes);
        try std.testing.expect(session.owner.budget.live > 0);
    }
}
test "VP9 AV1 filtered allocation failures cancellation and corrupt output release owners" {
    inline for (.{ decoder.Codec.vp9, decoder.Codec.av1 }) |codec| {
        try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
            fn run(a: std.mem.Allocator, c: decoder.Codec) !void {
                const session = try decoder.Session.create(a, c, .{});
                defer session.destroy();
                var batch = try session.decodeOutputs(if (c == .vp9) first("vp9") else first("av1"), 0, null, .none);
                defer batch.deinit();
            }
        }.run, .{codec});
        var control = Cancellation{};
        const session = try decoder.Session.create(std.testing.allocator, codec, .{ .control = .{ .context = &control, .check_fn = Cancellation.check } });
        defer session.destroy();
        const bytes = if (codec == .vp9) first("vp9") else first("av1");
        control.calls = 0;
        control.limit = 20;
        try std.testing.expectError(error.Cancelled, session.decodeOutputs(bytes, 0, null, .none));
        try std.testing.expect(session.failed);
        try std.testing.expectEqual(@as(usize, 0), session.owner.budget.live);
    }
}

test "VP9 AV1 corruption errors are preserved when every output is skipped" {
    inline for (.{ decoder.Codec.vp9, decoder.Codec.av1 }) |codec| {
        const bytes = if (codec == .vp9) first("vp9") else first("av1");
        var failures: usize = 0;
        for ([_]usize{ 1, 4, 16, bytes.len / 2 }) |length| {
            const all = try decoder.Session.create(std.testing.allocator, codec, .{});
            defer all.destroy();
            const none = try decoder.Session.create(std.testing.allocator, codec, .{});
            defer none.destroy();
            if (all.decode(bytes[0..length], 0, null)) |value| {
                var expected = value;
                defer expected.deinit();
                var actual = try none.decodeOutputs(bytes[0..length], 0, null, .none);
                defer actual.deinit();
                try std.testing.expectEqual(expected.output_count, actual.output_count);
            } else |err| {
                try std.testing.expectError(err, none.decodeOutputs(bytes[0..length], 0, null, .none));
                try std.testing.expectEqual(all.failed, none.failed);
                failures += 1;
            }
        }
        try std.testing.expect(failures > 0);
    }
}

test "VP9 AV1 default ceilings admit actual core and independently retained batch bytes" {
    inline for (.{ decoder.Codec.vp9, decoder.Codec.av1 }) |codec| {
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 32 * 1024 * 1024 } };
        {
            const session = try decoder.Session.create(std.testing.allocator, codec, .{ .admission_pool = &pool });
            defer session.destroy();
            try std.testing.expectEqual(@as(u64, @sizeOf(decoder.Session) + session.owner.budget.live), pool.snapshot().host_bytes);
            try std.testing.expectEqual(session.owner.budget.live, session.owner.allocations.?.live);
            {
                var batch = try session.decode(if (codec == .vp9) first("vp9") else first("av1"), 0, null);
                defer batch.deinit();
                var output: usize = batch.frames.len * @sizeOf(decoder.Frame);
                for (batch.frames) |item| output += item.bytes.len;
                try std.testing.expectEqual(@as(u64, output + @sizeOf(decoder.Session) + session.owner.budget.live), pool.snapshot().host_bytes);
                try std.testing.expectEqual(@as(u64, batch.frames.len * @sizeOf(decoder.Frame)), batch.reservation.resources.host_bytes);
                try std.testing.expect(pool.snapshot().host_bytes < session.options.max_decode_bytes);
            }
            try std.testing.expectEqual(@as(u64, @sizeOf(decoder.Session) + session.owner.budget.live), pool.snapshot().host_bytes);
        }
        try std.testing.expectEqual(@as(u64, 0), pool.snapshot().host_bytes);
    }
}
test "VP9 AV1 shared denial sweeps core preserves peer charge and retains failed session object" {
    inline for (.{ decoder.Codec.vp9, decoder.Codec.av1 }) |codec| {
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 32 * 1024 * 1024 } };
        {
            const session = try decoder.Session.create(std.testing.allocator, codec, .{ .admission_pool = &pool });
            defer session.destroy();
            const remaining = pool.limits.host_bytes - pool.snapshot().host_bytes;
            var peer = try pool.acquire(.{ .host_bytes = remaining });
            defer peer.deinit();
            try std.testing.expectError(error.SharedAdmissionExceeded, session.decodeOutputs(if (codec == .vp9) first("vp9") else first("av1"), 0, null, .none));
            try std.testing.expect(session.failed);
            try std.testing.expectEqual(@as(usize, 0), session.owner.budget.live);
            try std.testing.expectEqual(@as(usize, 0), session.owner.allocations.?.live);
            try std.testing.expectEqual(remaining, peer.resources.host_bytes);
            try std.testing.expectEqual(remaining + @sizeOf(decoder.Session), pool.snapshot().host_bytes);
            try std.testing.expectError(error.InvalidDecoderSession, session.decodeOutputs(if (codec == .vp9) first("vp9") else first("av1"), 0, null, .none));
        }
        try std.testing.expectEqual(@as(u64, 0), pool.snapshot().host_bytes);
    }
}
test "VP9 AV1 every shared allocation failure releases core batch and session charges" {
    inline for (.{ decoder.Codec.vp9, decoder.Codec.av1 }) |codec| try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(a: std.mem.Allocator, c: decoder.Codec) !void {
            var pool = media.admission.Pool{ .limits = .{ .host_bytes = 32 * 1024 * 1024 } };
            defer std.debug.assert(pool.snapshot().host_bytes == 0);
            const session = try decoder.Session.create(a, c, .{ .admission_pool = &pool });
            defer session.destroy();
            var batch = try session.decode(if (c == .vp9) first("vp9") else first("av1"), 0, null);
            defer batch.deinit();
        }
    }.run, .{codec});
}

test "AV1 shared Annex B preflight denial is retryable and does not mask later corruption" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 32 * 1024 * 1024 } };
    {
        const session = try decoder.Session.create(a, .av1, .{ .admission_pool = &pool, .av1_framing = .annex_b });
        defer session.destroy();
        const live = session.owner.budget.live;
        const coded = @embedFile("../../testdata/av1-extended/av1-annex-b.ivf");
        const packet = coded[44..][0..std.mem.readInt(u32, coded[32..36], .little)];
        var peer = try pool.acquire(.{ .host_bytes = pool.limits.host_bytes - pool.snapshot().host_bytes });
        defer peer.deinit();
        try std.testing.expectError(error.SharedAdmissionExceeded, session.decodeOutputs(packet, 0, null, .none));
        try std.testing.expect(!session.failed);
        try std.testing.expectEqual(live, session.owner.budget.live);
        peer.deinit();
        try std.testing.expectError(error.InvalidVideoData, session.decodeOutputs(packet[0..1], 0, null, .none));
        try std.testing.expect(!session.failed);
        var result = try session.decodeOutputs(packet, 0, null, .none);
        defer result.deinit();
        try std.testing.expect(result.output_count > 0);
        try std.testing.expectEqual(@as(usize, 0), result.frames.len);
    }
    try std.testing.expectEqual(@as(u64, 0), pool.snapshot().host_bytes);
}

test "identity-matrix AV1 VP9 WebM pictures preserve independent RGB planes and colour metadata" {
    const a = std.testing.allocator;
    const pics = @import("../webm_pictures.zig");
    const G = struct { name: []const u8, codec: decoder.Codec, width: u32, height: u32, depth: u8, frame_sha256: []const []const u8 };
    const parsed = try std.json.parseFromSlice([]G, a, @embedFile("../../testdata/gbr/golden.json"), .{});
    defer parsed.deinit();
    const Capture = struct {
        golden: G,
        count: usize = 0,
        fn accept(raw: *anyopaque, id: usize, frame: *const decoder.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.count, id);
            try std.testing.expectEqual(self.golden.width, frame.width);
            try std.testing.expectEqual(self.golden.height, frame.height);
            try std.testing.expectEqual(self.golden.depth, frame.bit_depth);
            try std.testing.expectEqual(@as(u8, 3), frame.chroma_format);
            try std.testing.expect(frame.color.full_range);
            try std.testing.expectEqual(@as(u8, 0), frame.color.matrix);
            try std.testing.expectEqual(@as(u8, 1), frame.color.primaries);
            try std.testing.expectEqual(@as(u8, 13), frame.color.transfer);
            var expected: [32]u8 = undefined;
            _ = try std.fmt.hexToBytes(&expected, self.golden.frame_sha256[id]);
            try std.testing.expectEqualSlices(u8, &expected, &hashFrame(frame.*));
            self.count += 1;
        }
    };
    inline for (.{ "av1-gbr-8-127", "vp9-gbr-8-127", "av1-gbr-10-127", "vp9-gbr-10-127", "av1-gbr-12-127", "vp9-gbr-12-127", "av1-gbr-8-128", "vp9-gbr-8-128", "av1-gbr-10-128", "vp9-gbr-10-128", "av1-gbr-12-128", "vp9-gbr-12-128" }) |name| {
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../../testdata/gbr/" ++ name ++ ".webm") }, .identity = name };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        var g: ?G = null;
        for (parsed.value) |row| if (std.mem.eql(u8, row.name, name)) {
            g = row;
        };
        var capture = Capture{ .golden = g.? };
        var ids: [12]usize = undefined;
        for (&ids, 0..) |*id, i| id.* = i;
        try pics.walk(a, &reader, .{}, &ids, &capture, Capture.accept);
        try std.testing.expectEqual(@as(usize, 12), capture.count);
    }
}
