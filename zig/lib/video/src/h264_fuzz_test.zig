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

//! Native Zig coverage-guided targets plus checked-in corpus replay.
const std = @import("std");
const media = @import("antfly_media");
const h264 = @import("h264.zig");
const Guard = struct {
    checks: usize = 0,
    fn check(context: ?*const anyopaque) !void {
        const self: *Guard = @ptrCast(@alignCast(@constCast(context.?)));
        self.checks += 1;
        if (self.checks > 10_000) return error.DeadlineExceeded;
    }
};
fn decode(bytes: []const u8) !void {
    if (bytes.len > 1024 * 1024) return;
    const a = std.testing.allocator;
    var guard = Guard{};
    var src = media.source.Source{ .allocator = a, .identity = "fuzz-corpus", .storage = .{ .borrowed = bytes }, .control = .{ .context = &guard, .check_fn = Guard.check }, .limits = .{ .max_total_bytes = 16 * 1024 * 1024 } };
    {
        var reader = media.mp4.Reader.init(a, &src, .{ .max_samples = 128, .max_boxes = 2048, .max_index_bytes = 1024 * 1024, .max_metadata_bytes = 1024 * 1024 }) catch return;
        defer reader.deinit();
        if (reader.packets.len == 0) return;
        var frame = h264.decodeFrame(a, &reader, reader.packets.len - 1, .{ .max_pixels = 1024 * 1024, .max_packet_bytes = 1024 * 1024, .max_decode_bytes = 64 * 1024 * 1024, .max_dependency_packets = 128, .max_slices = 64, .max_parameter_packets = 128, .max_parameter_bytes = 1024 * 1024 }) catch return;
        frame.deinit();
    }
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
}
fn corpus(comptime bytes: []const u8) []const u8 {
    return comptime blk: {
        var encoded: [4 + bytes.len]u8 = undefined;
        std.mem.writeInt(u32, encoded[0..4], @intCast(bytes.len), .little);
        @memcpy(encoded[4..], bytes);
        const frozen = encoded;
        break :blk &frozen;
    };
}
test "video fuzz H264 containers reconstruction and allocation cleanup" {
    const Harness = struct {
        fn run(_: void, smith: *std.testing.Smith) !void {
            var buffer: [128 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&buffer, 0xA17F1001);
            try decode(buffer[0..length]);
        }
    };
    try std.testing.fuzz({}, Harness.run, .{ .corpus = &.{ corpus(@embedFile("../testdata/h264-sintel-original.mp4")), corpus(@embedFile("../testdata/h264-dynamic-geometry.mp4")), corpus(@embedFile("../testdata/h264-mono-14-cabac.mp4")), corpus(@embedFile("../testdata/h264-gaps-poc1-cavlc.mp4")), corpus(@embedFile("../testdata/h264-jm-partitions.mp4")), corpus(@embedFile("../testdata/h264-jm-separate-14.mp4")), corpus(@embedFile("../testdata/h264-jm-secondary-sp.mp4")) } });
}
fn configCorpus(comptime bytes: []const u8) []const u8 {
    return comptime blk: {
        @setEvalBranchQuota(1_000_000);
        const offset = std.mem.indexOf(u8, bytes, "avcC").?;
        const size = std.mem.readInt(u32, bytes[offset - 4 ..][0..4], .big) - 8;
        break :blk corpus(bytes[offset + 4 ..][0..size]);
    };
}
test "video fuzz H264 parameter sets reject arbitrary syntax without leaks" {
    const Harness = struct {
        fn run(_: void, smith: *std.testing.Smith) !void {
            {
                var buffer: [4096]u8 = undefined;
                const length = smith.sliceWithHash(&buffer, 0xA17F1002);
                const bytes = buffer[0..length];
                const cfg = h264.configParse(std.testing.allocator, bytes) catch return;
                cfg.groups.deinit(std.testing.allocator);
            }
        }
    };
    try std.testing.fuzz({}, Harness.run, .{ .corpus = &.{ configCorpus(@embedFile("../testdata/h264-sintel-original.mp4")), configCorpus(@embedFile("../testdata/h264-jm-partitions.mp4")), configCorpus(@embedFile("../testdata/h264-jm-separate-14.mp4")), configCorpus(@embedFile("../testdata/h264-mono-10-cavlc.mp4")) } });
}

const pictures = @import("h264_pictures.zig");
const persistent_seeds = .{
    @embedFile("../testdata/h264-baseline-p.mp4"),
    @embedFile("../testdata/h264-dynamic-geometry.mp4"),
    @embedFile("../testdata/h264-dynamic-pps.mp4"),
    @embedFile("../testdata/h264-paff-interleaved-fragments-10-2.mp4"),
    @embedFile("../testdata/h264-jm-partitions.mp4"),
    @embedFile("../testdata/h264-jm-separate-14.mp4"),
};
fn persistentCorpus(comptime id: u8, comptime bytes: []const u8) []const u8 {
    return comptime blk: {
        var input: [1 + bytes.len]u8 = undefined;
        input[0] = id;
        @memcpy(input[1..], bytes);
        const frozen = input;
        break :blk corpus(&frozen);
    };
}
fn persistent(bytes: []const u8) !void {
    if (bytes.len == 0) return;
    const a = std.testing.allocator;
    const seeds = [_][]const u8{ persistent_seeds[0], persistent_seeds[1], persistent_seeds[2], persistent_seeds[3], persistent_seeds[4], persistent_seeds[5] };
    // Keep the original index while mutating encoded payloads. Container fuzz
    // above exercises parser rejection; this target reaches native ownership
    // and continuation even when mutated bytes would destroy the MP4 boxes.
    const input = try a.dupe(u8, seeds[bytes[0] % seeds.len]);
    defer a.free(input);
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 192 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var guard = Guard{};
    var source = media.source.Source{ .allocator = a, .identity = "persistent-fuzz", .admission_pool = &pool, .storage = .{ .borrowed = input }, .control = .{ .context = &guard, .check_fn = Guard.check } };
    defer std.debug.assert(source.retained_bytes == 0);
    var reader = try media.mp4.Reader.init(a, &source, .{ .max_samples = 128 });
    defer reader.deinit();
    const packets = reader.packets;
    defer reader.packets = packets;
    for (packets) |packet| {
        const at: usize = @intCast(packet.offset);
        if (at >= bytes.len - 1) continue;
        const count = @min(packet.size, bytes.len - 1 - at);
        @memcpy(input[at..][0..count], bytes[1 + at ..][0..count]);
    }
    const options = pictures.Options{ .max_packets = 128, .max_pictures = 128, .decode = .{ .max_pixels = 1024 * 1024, .max_packet_bytes = 1024 * 1024, .max_decode_bytes = 64 * 1024 * 1024, .max_dependency_packets = 128, .max_slices = 64, .max_parameter_packets = 128, .max_parameter_bytes = 1024 * 1024 } };
    const chunk = @max(1, (packets.len + 7) / 8);
    reader.packets = packets[0..@min(chunk, packets.len)];
    const owner = pictures.Stream.init(a, &reader, options) catch return;
    defer owner.deinit();
    var held: ?pictures.Index = null;
    defer if (held) |*snapshot| snapshot.deinit();
    var held_copy: ?[]h264.Picture = null;
    defer if (held_copy) |copy| a.free(copy);
    var base: usize = 0;
    var end = reader.packets.len;
    while (true) {
        guard.checks = 0;
        reader.packets = packets[base..end];
        const incremental_error: ?anyerror = if (owner.advance(&reader)) |_| null else |err| err;
        guard.checks = 0;
        var complete = reader;
        complete.packets = packets[0..end];
        var oracle = pictures.streaming(a, &complete, options) catch |err| {
            // Always execute the persistent path, including malformed appends.
            // Rejected input may have different first-error ordering. Completed
            // persistent output must also have a successful replay oracle.
            if (incremental_error == null and err != error.ResourceLimitExceeded and err != error.DeadlineExceeded and err != error.OutOfMemory) return err;
            return;
        };
        defer oracle.deinit();
        if (incremental_error) |err| switch (err) {
            error.ResourceLimitExceeded, error.DeadlineExceeded, error.OutOfMemory => return,
            else => return err,
        };
        var expected = try oracle.cloneRebased(a, base, end - base, &pool, .{});
        defer expected.deinit();
        var actual = try owner.snapshot(&reader);
        defer actual.deinit();
        try std.testing.expectEqualSlices(h264.Picture, expected.pictures, actual.pictures);
        try std.testing.expectEqual(expected.pending_pts, actual.pending_pts);
        try std.testing.expectEqual(expected.pending_first_packet, actual.pending_first_packet);
        if (held) |snapshot| {
            try std.testing.expectEqualSlices(h264.Picture, held_copy.?, snapshot.pictures);
        } else {
            held = try owner.snapshot(&reader);
            held_copy = try a.dupe(h264.Picture, held.?.pictures);
        }
        // Retain the newest completed picture and all unfinished assemblies.
        // Rejected eviction must leave the owner available for the next append.
        if (actual.pictures.len > 1) {
            var remove = actual.pictures[actual.pictures.len - 1].first_packet;
            if (actual.pending_first_packet) |pending| remove = @min(remove, pending);
            if (remove != 0) {
                guard.checks = 0;
                if (owner.rebase(&reader, remove)) |_| {
                    base += remove;
                } else |err| switch (err) {
                    error.InvalidLivePictureIndex, error.ResourceLimitExceeded, error.DeadlineExceeded, error.OutOfMemory => {},
                    else => return err,
                }
            }
        }
        if (end == packets.len) break;
        end = @min(packets.len, end + chunk);
    }
}
test "video fuzz H264 persistent payloads append eviction snapshots and replay" {
    const Harness = struct {
        fn run(_: void, smith: *std.testing.Smith) !void {
            var buffer: [128 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&buffer, 0xA17F1003);
            try persistent(buffer[0..length]);
        }
    };
    try std.testing.fuzz({}, Harness.run, .{ .corpus = &.{
        persistentCorpus(0, persistent_seeds[0]), persistentCorpus(1, persistent_seeds[1]),
        persistentCorpus(2, persistent_seeds[2]), persistentCorpus(3, persistent_seeds[3]),
        persistentCorpus(4, persistent_seeds[4]), persistentCorpus(5, persistent_seeds[5]),
    } });
}

// Recovered from the stateful campaign: an incomplete corrupt colour plane
// must not be swallowed because a preceding plane reported pending metadata.
test "H264 persistent corrupt plane is not a recoverable pending tail" {
    var smith = std.testing.Smith{ .in = @embedFile("../testdata/h264-persistent-tail.smith") };
    var buffer: [128 * 1024]u8 = undefined;
    const n = smith.sliceWithHash(&buffer, 0xA17F1003);
    try persistent(buffer[0..n]);
    const a = std.testing.allocator;
    const input = try a.dupe(u8, persistent_seeds[5]);
    defer a.free(input);
    var source = media.source.Source{ .allocator = a, .identity = "corrupt-plane-regression", .storage = .{ .borrowed = input } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const packets = reader.packets;
    defer reader.packets = packets;
    for (packets) |packet| {
        const at: usize = @intCast(packet.offset);
        if (at >= n - 1) continue;
        const count = @min(packet.size, n - 1 - at);
        @memcpy(input[at..][0..count], buffer[1 + at ..][0..count]);
    }
    reader.packets = packets[0..4];
    try std.testing.expectError(error.IncompleteVideoPicture, pictures.streaming(a, &reader, .{}));
}
