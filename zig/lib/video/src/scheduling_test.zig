// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const media = @import("antfly_media");
const video = @import("mod.zig");
const a = std.testing.allocator;
const clip = @embedFile("../testdata/decode-bframes.mp4");
fn source() media.source.Source {
    return .{ .allocator = a, .identity = "scheduling-v1", .storage = .{ .borrowed = clip } };
}
const clips = [_]video.windows.Window{
    .{ .interval = .{ .start = 0, .end = 16384 }, .step = 4096 },
    .{ .interval = .{ .start = 8192, .end = 24576 }, .step = 4096 },
};
test "video IDR qualification rejects ordinary I mixed VCL and SEI-only samples" {
    try std.testing.expect(try video.avc.isIdr(&.{ 1, 0x65 }, 1));
    try std.testing.expect(!try video.avc.isIdr(&.{ 1, 0x61 }, 1));
    try std.testing.expect(!try video.avc.isIdr(&.{ 1, 0x65, 1, 0x61 }, 1));
    try std.testing.expect(!try video.avc.isIdr(&.{ 1, 0x06 }, 1));
    try std.testing.expectError(error.MalformedVideoPacket, video.avc.isIdr(&.{ 2, 0x65 }, 1));
}
test "video dependency plan verifies sync hints budgets and retains anchor leases" {
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    var baseline = try video.decode_plan.create(a, &reader, &.{19}, .{ .mode = .from_start });
    defer baseline.deinit();
    try std.testing.expectEqual(@as(usize, 20), baseline.submitted_packets);
    var plan = try video.decode_plan.create(a, &reader, &.{ 19, 11 }, .{});
    try std.testing.expect(plan.submitted_packets < 20);
    for (plan.runs) |run| if (run.first != 0) {
        try std.testing.expect(try video.avc.isIdr(plan.anchorBytes(run.first).?, reader.track.nal_length_bytes));
    };
    try std.testing.expect(plan.probe_candidates < 3);
    plan.deinit();
    try std.testing.expectEqual(retained, src.retained_bytes);
    // A lying container hint on a non-IDR must not become a decode start.
    reader.packets[19].sync = true;
    plan = try video.decode_plan.create(a, &reader, &.{19}, .{});
    defer plan.deinit();
    try std.testing.expect(plan.runs[0].first != 19);
    try std.testing.expectError(error.ResourceLimitExceeded, video.decode_plan.create(a, &reader, &.{19}, .{ .max_probe_candidates = 0 }));
    try std.testing.expectError(error.ResourceLimitExceeded, video.decode_plan.create(a, &reader, &.{19}, .{ .max_probe_bytes = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, video.decode_plan.create(a, &reader, &.{19}, .{ .max_search_steps = 1 }));
    try std.testing.expectError(error.DuplicateFrameSelection, video.decode_plan.create(a, &reader, &.{ 0, 0 }, .{}));
}
fn plannerAlloc(allocator: std.mem.Allocator) !void {
    var provider = Provider{ .bytes = clip };
    var src = source();
    src.storage = .{ .range = .{ .context = &provider, .read_at = Provider.read, .length = clip.len } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    defer std.debug.assert(src.retained_bytes == retained);
    var plan = try video.decode_plan.create(allocator, &reader, &.{ 1, 19 }, .{});
    defer plan.deinit();
    var windows = try video.windows.create(allocator, &reader, &clips, .{});
    defer windows.deinit();
}
test "video scheduling plans release all allocations and leases on failure" {
    var stable = NoResize{ .backing = a };
    try std.testing.checkAllAllocationFailures(stable.allocator(), plannerAlloc, .{});
}
test "video overlapping windows preserve presentation order and share picture indexes" {
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    var plan = try video.windows.create(a, &reader, &clips, .{});
    defer plan.deinit();
    try std.testing.expect(plan.references.len > plan.unique_indexes.len);
    for (0..clips.len) |i| {
        const refs = try plan.window(i);
        for (refs[1..], refs[0 .. refs.len - 1]) |next, previous| try std.testing.expect(plan.unique_pts[next] > plan.unique_pts[previous]);
    }
    try std.testing.expectError(error.InvalidWindowIndex, plan.window(2));
    try std.testing.expectError(error.ResourceLimitExceeded, video.windows.create(a, &reader, &clips, .{ .max_unique_frames = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, video.windows.create(a, &reader, &clips, .{ .max_total_selections = 1 }));
    try std.testing.expectError(error.EmptyVideoWindow, video.windows.create(a, &reader, &.{.{ .interval = .{ .start = 999999, .end = 1000000 }, .step = 1 }}, .{}));
}
extern fn av_test_device_create() ?*anyopaque;
extern fn av_test_device_destroy(*anyopaque) void;
test "video verified IDR native decoding matches baseline and streams borrowed pictures" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const indexes = [_]usize{ 1, 19 };
    var baseline = try video.apple.decodeSelected(a, &reader, &indexes, .{ .require_hardware = false });
    defer baseline.deinit();
    var optimized = try video.apple.decodeSelected(a, &reader, &indexes, .{ .require_hardware = false, .seek_mode = .verified_idr });
    defer optimized.deinit();
    try std.testing.expect(optimized.submitted_packets < baseline.submitted_packets);
    for (baseline.frames, optimized.frames) |*expected, *actual| {
        try std.testing.expectEqual(expected.pts, actual.pts);
        var e = try expected.surface.map();
        defer e.deinit();
        var v = try actual.surface.map();
        defer v.deinit();
        for (e.planes, v.planes, 0..) |ep, vp, plane| for (0..ep.height) |row| {
            const width = ep.width * @as(usize, if (plane == 0) 1 else 2);
            try std.testing.expectEqualSlices(u8, ep.bytes[row * ep.stride ..][0..width], vp.bytes[row * vp.stride ..][0..width]);
        };
    }
    const Consumer = struct {
        count: usize = 0,
        fail: bool = false,
        fn accept(context: *anyopaque, frame: *const video.apple.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(context));
            _ = frame;
            self.count += 1;
            if (self.fail) return error.ConsumerFailure;
        }
    };
    var consumer = Consumer{ .fail = true };
    try std.testing.expectError(error.ConsumerFailure, video.apple.decodeTo(a, &reader, &indexes, .{ .require_hardware = false }, .{ .context = &consumer, .accept = Consumer.accept }));
    consumer = .{};
    var receipt = try video.apple.decodeTo(a, &reader, &indexes, .{ .require_hardware = false, .seek_mode = .verified_idr }, .{ .context = &consumer, .accept = Consumer.accept });
    defer receipt.deinit();
    try std.testing.expectEqual(@as(usize, 2), consumer.count);
    try std.testing.expectEqual(@as(usize, 0), receipt.frames.len);
}
test "video bounded Metal jobs reuse overlap and survive source teardown" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.SkipZigTest;
    defer av_test_device_destroy(device);
    var metal = try video.preparation.Metal.init(device);
    defer metal.deinit();
    var src = source();
    var jobs = blk: {
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        var result = try video.apple_jobs.prepareWindows(a, std.testing.io, &reader, &clips, &metal, .{ .decode = .{ .require_hardware = false, .seek_mode = .verified_idr, .max_frames = 64 }, .preparation = .{ .width = 48, .height = 48, .matrix = .bt709 }, .queue_depth = 1 });
        errdefer result.deinit();
        var baseline = try video.apple.decodeSelected(a, &reader, result.plan.unique_indexes, .{ .require_hardware = false });
        defer baseline.deinit();
        for (result.frames, baseline.frames) |*prepared, *frame| {
            const expected = try video.preparation.reference(a, &frame.surface, .{ .width = 48, .height = 48, .matrix = .bt709 }, .{});
            defer a.free(expected);
            const actual = try prepared.readback(a);
            defer a.free(actual);
            for (expected, actual) |e, v| try std.testing.expect(@abs(e - v) <= 2.01 / 255.0);
        }
        break :blk result;
    };
    defer jobs.deinit();
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    try std.testing.expect(jobs.reusedSelections() > 0);
    try std.testing.expectEqual(@as(usize, 1), jobs.queue_high_water);
    for (jobs.frames) |*prepared| {
        try prepared.releaseSource();
        _ = try prepared.buffer();
    }
    try std.testing.expectEqual((try jobs.window(0))[2], (try jobs.window(1))[0]);
}

const long_clip = @embedFile("../testdata/schedule-closed.mp4");
const open_clip = @embedFile("../testdata/schedule-open.mp4");
test "video closed GOP plans save packets but open GOP recovery hints need preroll" {
    inline for (.{ long_clip, open_clip }, 0..) |bytes, i| {
        var src = source();
        src.storage = .{ .borrowed = bytes };
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        var plan = try video.decode_plan.create(a, &reader, &.{ 12, 59 }, .{});
        defer plan.deinit();
        if (i == 0) {
            try std.testing.expectEqual(@as(usize, 13), plan.submitted_packets);
            try std.testing.expectEqual(@as(usize, 2), plan.runs.len);
            var bridged = try video.decode_plan.create(a, &reader, &.{ 12, 59 }, .{ .merge_gap_packets = 40 });
            defer bridged.deinit();
            try std.testing.expectEqual(@as(usize, 1), bridged.runs.len);
            try std.testing.expectEqual(@as(usize, 50), bridged.submitted_packets);
            try std.testing.expectError(error.ResourceLimitExceeded, video.decode_plan.create(a, &reader, &.{ 12, 59 }, .{ .max_decode_packets = 12 }));
        } else {
            try std.testing.expectEqual(@as(usize, 60), plan.submitted_packets);
            try std.testing.expectEqual(@as(usize, 0), plan.runs[0].first);
            try std.testing.expect(plan.probe_candidates > 1);
        }
    }
}
test "video scheduling fixture hashes and packet clocks match FFprobe receipts" {
    const receipt = try std.json.parseFromSlice(struct { files: []const struct { sha256: []const u8, bytes: usize, packets: []const struct { pts: i64, dts: i64, size: []const u8, pos: []const u8, flags: []const u8 } } }, a, @embedFile("../testdata/scheduling-oracle.json"), .{ .ignore_unknown_fields = true });
    defer receipt.deinit();
    inline for (.{ long_clip, open_clip }, 0..) |bytes, i| {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        const expected = receipt.value.files[i];
        try std.testing.expectEqualStrings(expected.sha256, &std.fmt.bytesToHex(digest, .lower));
        try std.testing.expectEqual(expected.bytes, bytes.len);
        var src = source();
        src.storage = .{ .borrowed = bytes };
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        try std.testing.expectEqual(expected.packets.len, reader.packets.len);
        for (reader.packets, expected.packets) |packet, oracle_packet| {
            try std.testing.expectEqual(oracle_packet.pts, packet.pts);
            try std.testing.expectEqual(oracle_packet.dts, packet.dts);
            try std.testing.expectEqual(try std.fmt.parseInt(u32, oracle_packet.size, 10), packet.size);
            try std.testing.expectEqual(try std.fmt.parseInt(u64, oracle_packet.pos, 10), packet.offset);
            try std.testing.expectEqual(std.mem.indexOfScalar(u8, oracle_packet.flags, 'K') != null, packet.sync);
        }
    }
}
const Provider = struct {
    bytes: []const u8,
    fn read(context: *anyopaque, offset: u64, out: []u8, control: media.source.Control) !usize {
        try control.check();
        const self: *@This() = @ptrCast(@alignCast(context));
        const n = @min(out.len, self.bytes.len - @as(usize, @intCast(offset)));
        @memcpy(out[0..n], self.bytes[@intCast(offset)..][0..n]);
        return n;
    }
};
test "video sparse native range decode saves actual reads and bytes with exact output parity" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    inline for (.{ long_clip, open_clip }, 0..) |bytes, i| {
        var provider = Provider{ .bytes = bytes };
        var src = source();
        src.storage = .{ .range = .{ .context = &provider, .read_at = Provider.read, .length = bytes.len } };
        var reader = try media.mp4.Reader.init(a, &src, .{});
        defer reader.deinit();
        const before_bytes = src.total_bytes;
        const before_reads = src.reads;
        var baseline = try video.apple.decodeSelected(a, &reader, &.{ 12, 59 }, .{ .require_hardware = false });
        defer baseline.deinit();
        const baseline_bytes = src.total_bytes - before_bytes;
        const baseline_reads = src.reads - before_reads;
        const next_bytes = src.total_bytes;
        const next_reads = src.reads;
        var optimized = try video.apple.decodeSelected(a, &reader, &.{ 12, 59 }, .{ .require_hardware = false, .seek_mode = .verified_idr, .max_decode_packets = if (i == 0) 13 else 60 });
        defer optimized.deinit();
        const optimized_bytes = src.total_bytes - next_bytes;
        const optimized_reads = src.reads - next_reads;
        if (i == 0) {
            try std.testing.expectEqual(@as(usize, 13), optimized.submitted_packets);
            try std.testing.expectEqual(@as(usize, 13), optimized_reads);
            try std.testing.expectEqual(@as(u64, 61067), baseline_bytes);
            try std.testing.expectEqual(@as(u64, 14137), optimized_bytes);
        } else {
            try std.testing.expectEqual(@as(usize, 60), optimized.submitted_packets);
        }
        try std.testing.expectEqual(@as(usize, 60), baseline_reads);
        for (baseline.frames, optimized.frames) |*expected, *actual| {
            try std.testing.expectEqual(expected.pts, actual.pts);
            var e = try expected.surface.map();
            defer e.deinit();
            var v = try actual.surface.map();
            defer v.deinit();
            for (e.planes, v.planes, 0..) |ep, vp, plane| for (0..ep.height) |row| {
                const width = ep.width * @as(usize, if (plane == 0) 1 else 2);
                try std.testing.expectEqualSlices(u8, ep.bytes[row * ep.stride ..][0..width], vp.bytes[row * vp.stride ..][0..width]);
            };
        }
    }
}

const job_options = video.apple_jobs.Options{
    .decode = .{ .require_hardware = false, .seek_mode = .verified_idr, .max_frames = 64 },
    .preparation = .{ .width = 48, .height = 48, .matrix = .bt709 },
    .queue_depth = 2,
};
fn jobsAlloc(allocator: std.mem.Allocator, metal: *video.preparation.Metal) !void {
    metal.clearCoefficients();
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    defer std.debug.assert(src.retained_bytes == retained);
    var jobs = try video.apple_jobs.prepareWindows(allocator, std.testing.io, &reader, &clips, metal, job_options);
    defer jobs.deinit();
}
test "video queued Metal job allocator failures fence work and release ownership" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.SkipZigTest;
    defer av_test_device_destroy(device);
    var metal = try video.preparation.Metal.init(device);
    defer metal.deinit();
    var stable = NoResize{ .backing = a };
    try std.testing.checkAllAllocationFailures(stable.allocator(), jobsAlloc, .{&metal});
}
test "video Metal queue admission late cancellation and retry stay bounded" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.SkipZigTest;
    defer av_test_device_destroy(device);
    var metal = try video.preparation.Metal.init(device);
    defer metal.deinit();
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    var options = job_options;
    options.queue_depth = 0;
    try std.testing.expectError(error.ResourceLimitExceeded, video.apple_jobs.prepareWindows(a, std.testing.io, &reader, &clips, &metal, options));
    options = job_options;
    options.max_output_bytes = 1;
    try std.testing.expectError(error.ResourceLimitExceeded, video.apple_jobs.prepareWindows(a, std.testing.io, &reader, &clips, &metal, options));
    options = job_options;
    options.max_inflight_surface_bytes = 1;
    try std.testing.expectError(error.ResourceLimitExceeded, video.apple_jobs.prepareWindows(a, std.testing.io, &reader, &clips, &metal, options));
    var jobs = try video.apple_jobs.prepareWindows(a, std.testing.io, &reader, &clips, &metal, job_options);
    try std.testing.expectEqual(@as(usize, 2), jobs.queue_high_water);
    try std.testing.expect(jobs.inflight_surface_high_water <= job_options.max_inflight_surface_bytes);
    var end: usize = 0;
    for (jobs.plan.unique_indexes) |index| end = @max(end, index);
    jobs.deinit();
    const Cancel = struct {
        source: *const media.source.Source,
        cutoff: u64,
        fn check(context: ?*const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(context.?));
            if (self.source.total_bytes >= self.cutoff) return error.Cancelled;
        }
    };
    // Cancel after reading the last selected packet on the baseline path. Earlier
    // pictures have submitted Metal work; this trigger is independent of poll timing.
    var cutoff = src.total_bytes;
    for (reader.packets[0 .. end + 1]) |packet| cutoff += packet.size;
    const cancel = Cancel{ .source = &src, .cutoff = cutoff };
    src.control = .{ .context = &cancel, .check_fn = Cancel.check };
    options = job_options;
    options.decode.seek_mode = .from_start;
    try std.testing.expectError(error.Cancelled, video.apple_jobs.prepareWindows(a, std.testing.io, &reader, &clips, &metal, options));
    try std.testing.expectEqual(retained, src.retained_bytes);
    src.control = .{};
    jobs = try video.apple_jobs.prepareWindows(a, std.testing.io, &reader, &clips, &metal, job_options);
    jobs.deinit();
    try std.testing.expectEqual(retained, src.retained_bytes);
}
test "video portable planner cancellation does not leak probed source leases" {
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    const Cancel = struct {
        calls: usize = 0,
        fn check(context: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(context.?)));
            self.calls += 1;
            if (self.calls == 5) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    src.control = .{ .context = &cancel, .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, video.decode_plan.create(a, &reader, &.{ 1, 19 }, .{}));
    try std.testing.expectEqual(retained, src.retained_bytes);
    src.control = .{};
    var plan = try video.decode_plan.create(a, &reader, &.{ 1, 19 }, .{});
    plan.deinit();
    try std.testing.expectEqual(retained, src.retained_bytes);
}

// Force relocation for ArrayList growth/shrink so allocation failure indices do
// not depend on whether the backing allocator can resize adjacent pages in place.
const NoResize = struct {
    backing: std.mem.Allocator,
    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn alloc(context: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(context));
        return self.backing.rawAlloc(len, alignment, ra);
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(context: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(context));
        self.backing.rawFree(bytes, alignment, ra);
    }
};
