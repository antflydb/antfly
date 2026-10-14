// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const network = @import("http_stream.zig");
const live = @import("live_mp4.zig");
const media_source = @import("source.zig");
const mp4 = @import("mp4.zig");
const admission = @import("admission.zig");
const original = @embedFile("../testdata/fragmented.mp4");
const init_end = 797;
const Fake = struct {
    pub const Client = struct {
        complete: bool = false,
        fail_after_prefix: bool = false,
        status: u16 = 200,
        encoding: ?[]const u8 = null,
        content_range: ?[]const u8 = null,
        calls: usize = 0,
        pub fn getToWriter(self: *@This(), url: []const u8, options: anytype, writer: anytype, _: anytype, _: anytype) !Response {
            self.calls += 1;
            try std.testing.expectEqualStrings("https://example.test/live/media.m4s", url);
            try std.testing.expect(!options.follow_redirects and !options.cookies_enabled);
            try std.testing.expectEqual(@as(u32, 0), options.max_retries);
            const response = Response{ .status = .{ .code = self.status }, .encoding = self.encoding, .content_range = self.content_range };
            try writer.startResponse(response);
            const body = original[init_end..];
            var offset: usize = 0;
            while (offset < body.len) {
                if (self.fail_after_prefix and offset >= 11206 - init_end + 8) return error.ConnectionReset;
                const end = @min(body.len, offset + 128);
                try writer.writeAll(body[offset..end]);
                offset = end;
            }
            self.complete = true;
            return response;
        }
    };
    const Response = struct {
        status: struct { code: u16 },
        encoding: ?[]const u8,
        content_range: ?[]const u8,
        pub fn deinit(_: *@This()) void {}
        pub fn header(self: @This(), name: []const u8) ?[]const u8 {
            return if (std.ascii.eqlIgnoreCase(name, "Content-Encoding")) self.encoding else if (std.ascii.eqlIgnoreCase(name, "Content-Range")) self.content_range else null;
        }
    };
};
const Capture = struct {
    client: *Fake.Client,
    held: [8]?live.Segment = @splat(null),
    count: usize = 0,
    early: usize = 0,
    fail: bool = false,
    cancel: ?*bool = null,
    fn deinit(self: *@This()) void {
        for (&self.held) |*entry| if (entry.*) |*owner| owner.deinit();
    }
    fn accept(raw: *anyopaque, delivery: *network.FragmentDelivery) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.fail) return error.ConsumerFailure;
        try std.testing.expect(self.count < self.held.len);
        if (!self.client.complete) self.early += 1;
        self.held[self.count] = try delivery.take();
        try std.testing.expectError(error.FragmentAlreadyTaken, delivery.take());
        self.count += 1;
        if (self.cancel) |value| value.* = true;
    }
};
test "HTTP streamed fragments publish before completion preserve independent packets and bound work" {
    const a = std.testing.allocator;
    var pool = admission.Pool{ .limits = .{ .host_bytes = 4 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var client = Fake.Client{};
    var capture = Capture{ .client = &client };
    defer capture.deinit();
    var ingest = try live.Ingest.init(a, "http-stream", original[0..init_end], .{ .stream = .{ .admission_pool = &pool } });
    var active = true;
    defer if (active) ingest.deinit();
    var adapter = network.Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd", .pool = &pool };
    const stats = try adapter.streamFmp4(adapter.root_url, "media.m4s", null, original.len, &ingest, &capture, Capture.accept);
    try std.testing.expectEqual(original.len - init_end, stats.bytes);
    try std.testing.expectEqual(capture.count, stats.fragments);
    try std.testing.expect(capture.early > 0 and capture.early < capture.count);
    try std.testing.expect(stats.largest_pending_capacity < stats.bytes);
    try std.testing.expectEqual(stats.bytes, adapter.charged_bytes);
    try std.testing.expectEqual(stats.bytes, adapter.transferred_bytes);
    try std.testing.expectEqual(@as(usize, 0), ingest.pending.items.len);
    ingest.deinit();
    active = false;
    var input = media_source.Source{ .allocator = a, .identity = "stream-baseline", .storage = .{ .borrowed = original } };
    var baseline = try mp4.Reader.init(a, &input, .{});
    defer baseline.deinit();
    var index: usize = 0;
    for (capture.held[0..capture.count]) |entry| {
        var owned = entry.?;
        for (owned.reader.packets, 0..) |packet, local| {
            try std.testing.expectEqual(baseline.packets[index].media_dts, packet.media_dts);
            var expected = try baseline.readPacket(index);
            defer expected.deinit();
            var actual = try owned.reader.readPacket(local);
            defer actual.deinit();
            try std.testing.expectEqualSlices(u8, expected.bytes, actual.bytes);
            index += 1;
        }
    }
    try std.testing.expectEqual(baseline.packets.len, index);
}
test "HTTP streamed fragments reject headers before delivery and poison entered failures" {
    const a = std.testing.allocator;
    inline for (.{ .{ @as(u16, 404), @as(?[]const u8, null), false, false, error.UnexpectedHttpStatus }, .{ @as(u16, 200), @as(?[]const u8, "gzip"), false, false, error.UnsupportedMediaEncoding }, .{ @as(u16, 200), @as(?[]const u8, null), true, false, error.ConnectionReset }, .{ @as(u16, 200), @as(?[]const u8, null), false, true, error.ConsumerFailure } }) |case| {
        var pool = admission.Pool{ .limits = .{ .host_bytes = 4 * 1024 * 1024 } };
        defer std.debug.assert(pool.snapshot().host_bytes == 0);
        var client = Fake.Client{ .status = case[0], .encoding = case[1], .fail_after_prefix = case[2] };
        var capture = Capture{ .client = &client, .fail = case[3] };
        defer capture.deinit();
        var ingest = try live.Ingest.init(a, "http-failure", original[0..init_end], .{ .stream = .{ .admission_pool = &pool } });
        defer ingest.deinit();
        var adapter = network.Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd", .pool = &pool };
        try std.testing.expectError(case[4], adapter.streamFmp4(adapter.root_url, "media.m4s", null, original.len, &ingest, &capture, Capture.accept));
        try std.testing.expect(ingest.ended);
        try std.testing.expectEqual(@as(usize, 0), ingest.pending.items.len);
        try std.testing.expectEqual(original.len, adapter.charged_bytes);
        try std.testing.expectError(error.InvalidMediaStreamState, adapter.streamFmp4(adapter.root_url, "media.m4s", null, original.len, &ingest, &capture, Capture.accept));
        try std.testing.expectEqual(@as(usize, 1), client.calls);
        if (case[2]) try std.testing.expectEqual(@as(usize, 1), capture.count) else try std.testing.expectEqual(@as(usize, 0), capture.count);
    }
}
fn allocationRun(a: std.mem.Allocator) !void {
    var client = Fake.Client{};
    var capture = Capture{ .client = &client };
    defer capture.deinit();
    var ingest = try live.Ingest.init(a, "http-allocation", original[0..init_end], .{});
    defer ingest.deinit();
    var adapter = network.Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd" };
    _ = try adapter.streamFmp4(adapter.root_url, "media.m4s", null, original.len, &ingest, &capture, Capture.accept);
}
test "HTTP streamed fragments release every failing allocation including taken snapshots" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationRun, .{});
}
test "HTTP streamed cancellation preserves taken fragments and preflight admission does not enter transport" {
    const a = std.testing.allocator;
    const Cancel = struct {
        fn check(raw: ?*const anyopaque) !void {
            if (@as(*const bool, @ptrCast(@alignCast(raw.?))).*) return error.Cancelled;
        }
    };
    var canceled = false;
    var client = Fake.Client{};
    var capture = Capture{ .client = &client, .cancel = &canceled };
    defer capture.deinit();
    var ingest = try live.Ingest.init(a, "http-cancellation", original[0..init_end], .{});
    defer ingest.deinit();
    var pool = admission.Pool{ .limits = .{} };
    var adapter = network.Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd", .pool = &pool, .control = .{ .context = &canceled, .check_fn = Cancel.check } };
    try std.testing.expectError(error.SharedAdmissionExceeded, adapter.streamFmp4(adapter.root_url, "media.m4s", null, original.len, &ingest, &capture, Capture.accept));
    try std.testing.expectEqual(@as(usize, 0), client.calls);
    try std.testing.expectEqual(@as(usize, 0), adapter.requests);
    try std.testing.expect(!ingest.ended);
    adapter.pool = null;
    try std.testing.expectError(error.Cancelled, adapter.streamFmp4(adapter.root_url, "media.m4s", null, original.len, &ingest, &capture, Capture.accept));
    try std.testing.expect(ingest.ended);
    try std.testing.expectEqual(@as(usize, 1), capture.count);
    var lease = try capture.held[0].?.reader.readPacket(0);
    defer lease.deinit();
    try std.testing.expect(lease.bytes.len > 0);
}
test "HTTP fragment growth respects exact tight-budget fallback and complete pending byte ownership" {
    const a = std.testing.allocator;
    var pool = admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
    var ingest = try live.Ingest.init(a, "tight-growth", original[0..init_end], .{ .stream = .{ .admission_pool = &pool } });
    defer ingest.deinit();
    const initial = pool.snapshot().host_bytes;
    try ingest.push("12345678");
    pool.limits.host_bytes = initial + 17; // old eight + new exact nine; doubling would need 24.
    try ingest.push("9");
    try std.testing.expectEqualStrings("123456789", ingest.pending.items);
    try std.testing.expectEqual(@as(usize, 9), ingest.pending.capacity);
    try std.testing.expectEqual(initial + 9, pool.snapshot().host_bytes);
    try std.testing.expectError(error.SharedAdmissionExceeded, ingest.push("0"));
    try std.testing.expectEqualStrings("123456789", ingest.pending.items);
    try std.testing.expectEqual(initial + 9, pool.snapshot().host_bytes);
}
test "HTTP streamed ranges validate headers before delivery and consume exactly the requested extent" {
    const a = std.testing.allocator;
    var header: [96]u8 = undefined;
    const content_range = try std.fmt.bufPrint(&header, "bytes={d}-{d}/{d}", .{ init_end, original.len - 1, original.len });
    // Content-Range uses a space after its unit, independently of Range syntax.
    header[5] = ' ';
    inline for (.{ true, false }) |valid| {
        var client = Fake.Client{ .status = 206, .content_range = if (valid) content_range else null };
        var capture = Capture{ .client = &client };
        defer capture.deinit();
        var ingest = try live.Ingest.init(a, "http-range", original[0..init_end], .{});
        defer ingest.deinit();
        var adapter = network.Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd" };
        const result = adapter.streamFmp4(adapter.root_url, "media.m4s", .{ .offset = init_end, .length = original.len - init_end }, original.len, &ingest, &capture, Capture.accept);
        if (valid) {
            try std.testing.expectEqual(original.len - init_end, (try result).bytes);
            try std.testing.expectEqual(@as(usize, 6), capture.count);
        } else {
            try std.testing.expectError(error.InvalidRemoteRange, result);
            try std.testing.expectEqual(@as(usize, 0), capture.count);
        }
    }
}
test "HTTP fragment one-byte input uses bounded allocations and retains exact coded bytes" {
    var tracking = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var ingest = try live.Ingest.init(tracking.allocator(), "one-byte-growth", original[0..init_end], .{});
    defer ingest.deinit();
    const before_count = tracking.alloc_index;
    const before_bytes = tracking.allocated_bytes;
    const fragment = original[init_end..11206];
    for (fragment) |byte| try ingest.push(&.{byte});
    const allocations = tracking.alloc_index - before_count;
    const allocated_bytes = tracking.allocated_bytes - before_bytes;
    try std.testing.expect(allocations <= 15);
    try std.testing.expect(allocated_bytes < fragment.len * 4);
    try std.testing.expectEqualSlices(u8, fragment, ingest.pending.items);
    std.debug.print("one-byte fragment: {d} bytes, {d} allocations, {d} allocated bytes, {d} retained capacity\n", .{ fragment.len, allocations, allocated_bytes, ingest.pending.capacity });
}
