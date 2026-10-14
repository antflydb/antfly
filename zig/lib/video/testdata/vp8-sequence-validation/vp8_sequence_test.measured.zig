// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Stateful VP8 interframe ownership and independent real-picture qualification.
const std = @import("std");
const image = @import("antfly_image");
const Budget = @import("decode_budget.zig").Budget;
const Planes = image.webp.Vp8FramePlanes;
const coded = @embedFile("../testdata/sequence-fuzz/sintel-vp8-detail.ivf");
const encoded = blk: {
    var value: [4 + coded.len - 32]u8 = undefined;
    std.mem.writeInt(u32, value[0..4], coded.len - 32, .little);
    @memcpy(value[4..], coded[32..]);
    break :blk value;
};
const Guard = struct {
    calls: usize = 0,
    limit: usize = 2_000_000,
    fn check(raw: ?*const anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
        self.calls += 1;
        if (self.calls > self.limit) return error.Canceled;
    }
};
fn hashPicture(p: Planes, padded: bool) [32]u8 {
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_][]const u8{ p.y, p.u, p.v }, [_]usize{ p.y_stride, p.uv_stride, p.uv_stride }, [_]usize{ p.width, (p.width + 1) / 2, (p.width + 1) / 2 }, [_]usize{ p.height, (p.height + 1) / 2, (p.height + 1) / 2 }) |plane, stride, width, height| {
        if (padded) h.update(plane) else for (0..height) |row| h.update(plane[row * stride ..][0..width]);
    }
    return h.finalResult();
}
fn verify(held: *const [4]?Planes, hashes: *const [4][32]u8) !void {
    for (held, hashes) |entry, digest| if (entry) |p| {
        try std.testing.expect(p.width > 0 and p.height > 0);
        try std.testing.expect(p.y_stride >= p.width and p.uv_stride >= (p.width + 1) / 2);
        try std.testing.expect(p.y_padded_height >= p.height and p.uv_padded_height >= (p.height + 1) / 2);
        try std.testing.expectEqual(p.y_stride * p.y_padded_height, p.y.len);
        try std.testing.expectEqual(p.uv_stride * p.uv_padded_height, p.u.len);
        try std.testing.expectEqual(p.u.len, p.v.len);
        try std.testing.expectEqual(digest, hashPicture(p, true));
    };
}
const Result = struct { packets: usize = 0, retained: usize = 0 };
fn runTape(tape: []const u8, strict: bool, oracle: ?[]const []const u8) !Result {
    var budget = Budget{ .backing = std.testing.allocator, .limit = 32 * 1024 * 1024 };
    const a = budget.allocator();
    var state = image.webp.Vp8Decoder{};
    defer state.deinit(a);
    var guard = Guard{};
    var scope = image.work_control.Scope.enter(.{ .context = &guard, .check_fn = Guard.check });
    defer scope.deinit();
    var held: [4]?Planes = @splat(null);
    defer for (&held) |*entry| if (entry.*) |*p| p.deinit(a);
    var hashes: [4][32]u8 = @splat(@splat(0));
    var result = Result{};
    var offset: usize = 0;
    while (offset <= tape.len and tape.len - offset >= 12 and result.packets < 32) {
        const length = std.mem.readInt(u32, tape[offset..][0..4], .little);
        const flags = tape[offset + 11];
        offset += 12;
        if (length == 0 or length > tape.len - offset or length > 128 * 1024) {
            if (strict) return error.InvalidTape;
            break;
        }
        const packet = tape[offset..][0..length];
        offset += length;
        if (!strict and flags & 0x80 != 0) guard.limit = guard.calls + 1;
        try verify(&held, &hashes);
        result.packets += 1;
        var p = state.decode(a, packet, .{ .max_pixels = 1024 * 1024, .max_rgba_bytes = 32 * 1024 * 1024 }) catch |err| {
            try verify(&held, &hashes);
            if (strict) return err;
            break;
        };
        errdefer p.deinit(a);
        try verify(&held, &hashes);
        if (oracle) |expected| {
            try std.testing.expect(result.packets <= expected.len);
            try std.testing.expectEqual(@as(u32, 854), p.width);
            try std.testing.expectEqual(@as(u32, 480), p.height);
            var hash: [32]u8 = undefined;
            _ = try std.fmt.hexToBytes(&hash, expected[result.packets - 1]);
            if (!std.mem.eql(u8, &hash, &hashPicture(p, false))) {
                return error.PictureOracleMismatch;
            }
        }
        // Output selection must not skip reference/entropy updates.
        if (flags & 1 != 0 or packet[0] & 16 == 0) {
            p.deinit(a);
        } else {
            const slot = (result.packets - 1) % held.len;
            if (held[slot]) |*old| old.deinit(a);
            hashes[slot] = hashPicture(p, true);
            held[slot] = p;
            result.retained += 1;
        }
    }
    state.deinit(a);
    try verify(&held, &hashes);
    return result;
}
test "video fuzz sequence VP8 interframes retained outputs and cleanup" {
    const Harness = struct {
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var bytes: [128 * 1024]u8 = undefined;
            const length = smith.sliceWithHash(&bytes, 0xA17F8002);
            _ = try runTape(bytes[0..length], false, null);
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &.{&encoded} });
}
test "VP8 real sequence matches independent hashes and keeps retained output across cancellation" {
    const Golden = struct { width: u32, height: u32, frame_sha256: []const []const u8 };
    const golden = try std.json.parseFromSlice(Golden, std.testing.allocator, @embedFile("../testdata/sequence-fuzz/vp8-golden.json"), .{});
    defer golden.deinit();
    const result = try runTape(encoded[4..], true, golden.value.frame_sha256);
    try std.testing.expectEqual(@as(usize, 12), result.packets);
    try std.testing.expectEqual(@as(usize, 12), result.retained);
    var tape = encoded;
    var offset: usize = 4;
    var index: usize = 0;
    while (offset < tape.len) : (index += 1) {
        const length = std.mem.readInt(u32, tape[offset..][0..4], .little);
        tape[offset + 11] = @intCast(index % 2);
        offset += 12 + length;
    }
    const skipped = try runTape(tape[4..], true, golden.value.frame_sha256);
    try std.testing.expectEqual(@as(usize, 12), skipped.packets);
    try std.testing.expectEqual(@as(usize, 6), skipped.retained);
    const first_length = std.mem.readInt(u32, tape[4..8], .little);
    tape[4 + 12 + first_length + 11] = 0x80;
    const canceled = try runTape(tape[4..], false, null);
    try std.testing.expectEqual(@as(usize, 2), canceled.packets);
    try std.testing.expectEqual(@as(usize, 1), canceled.retained);
}
