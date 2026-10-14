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
        body: ?[]const u8 = null,
        fail_after_bytes: ?usize = null,
        pub fn getToWriter(self: *@This(), url: []const u8, options: anytype, writer: anytype, _: anytype, _: anytype) !Response {
            self.calls += 1;
            self.complete = false;
            try std.testing.expectEqualStrings("https://example.test/live/media.m4s", url);
            try std.testing.expect(!options.follow_redirects and !options.cookies_enabled);
            try std.testing.expectEqual(@as(u32, 0), options.max_retries);
            const response = Response{ .status = .{ .code = self.status }, .encoding = self.encoding, .content_range = self.content_range };
            try writer.startResponse(response);
            const body = self.body orelse original[init_end..];
            var offset: usize = 0;
            while (offset < body.len) {
                if (self.fail_after_prefix and offset >= 11206 - init_end + 8) return error.ConnectionReset;
                if (self.fail_after_bytes) |limit| if (offset >= limit) return error.ConnectionReset;
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

test "HTTP fragment replay suppresses acknowledged prefix and preserves taken snapshots after restart" {
    const a = std.testing.allocator;
    var pool = admission.Pool{ .limits = .{ .host_bytes = 4 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var client = Fake.Client{ .fail_after_prefix = true };
    var capture = Capture{ .client = &client };
    defer capture.deinit();
    var adapter = network.Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd", .pool = &pool };
    var ledger = network.FragmentLedger{};
    try std.testing.expectError(error.ConnectionReset, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{ .epoch = 7 }, &capture, Capture.accept));
    try std.testing.expectEqual(@as(usize, 1), ledger.acknowledged);
    try std.testing.expectEqual(@as(usize, 1), ledger.attempts);
    try std.testing.expect(!ledger.complete and !ledger.invalid and !ledger.active);
    client.fail_after_prefix = false;
    const stats = try adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{ .epoch = 7 }, &capture, Capture.accept);
    try std.testing.expectEqual(@as(usize, 1), stats.replayed);
    try std.testing.expectEqual(@as(usize, 5), stats.acknowledged);
    try std.testing.expectEqual(@as(usize, 6), capture.count);
    try std.testing.expect(ledger.complete);
    for (capture.held[0..capture.count], 0..) |entry, i| {
        const segment = entry.?;
        try std.testing.expectEqual(i, segment.ordinal);
        try std.testing.expectEqual(@as(usize, 7), segment.epoch);
        var packet = try capture.held[i].?.reader.readPacket(0);
        defer packet.deinit();
        try std.testing.expect(packet.bytes.len > 0);
    }
    _ = try adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{ .epoch = 7 }, &capture, Capture.accept);
    try std.testing.expectEqual(@as(usize, 2), client.calls);
    var input = @import("source.zig").Source{ .allocator = a, .identity = "replay-baseline", .storage = .{ .borrowed = original } };
    var baseline = try @import("mp4.zig").Reader.init(a, &input, .{});
    defer baseline.deinit();
    var index: usize = 0;
    for (capture.held[0..capture.count]) |entry| {
        var segment = entry.?;
        for (segment.reader.packets, 0..) |packet, local| {
            const expected = baseline.packets[index];
            try std.testing.expectEqual(expected.media_dts, packet.media_dts);
            try std.testing.expectEqual(expected.media_pts, packet.media_pts);
            try std.testing.expectEqual(expected.duration, packet.duration);
            try std.testing.expectEqual(expected.sync, packet.sync);
            var original_packet = try baseline.readPacket(index);
            defer original_packet.deinit();
            var replay_packet = try segment.reader.readPacket(local);
            defer replay_packet.deinit();
            try std.testing.expectEqualSlices(u8, original_packet.bytes, replay_packet.bytes);
            index += 1;
        }
    }
    try std.testing.expectEqual(baseline.packets.len, index);
    // Completed ledgers still reject a different resource/configuration.
    try std.testing.expectError(error.SourceVersionChanged, adapter.streamFmp4Resuming(adapter.root_url, "other.m4s", null, original.len, original[0..init_end], &ledger, .{ .epoch = 7 }, &capture, Capture.accept));
    try std.testing.expect(ledger.invalid);
}

const ReplayConsumer = struct {
    client: *Fake.Client,
    capture: Capture,
    fail: bool = true,
    take_on_failure: bool = false,
    acknowledge_failure: bool = false,
    fn accept(raw: *anyopaque, delivery: *network.FragmentDelivery) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.fail) {
            if (self.take_on_failure) try Capture.accept(&self.capture, delivery);
            if (self.acknowledge_failure) delivery.acknowledge();
            return error.ConsumerFailure;
        }
        return Capture.accept(&self.capture, delivery);
    }
};
test "HTTP fragment replay distinguishes ownership from accepted irreversible side effects" {
    inline for (.{ .{ false, false }, .{ true, false }, .{ true, true }, .{ false, true } }) |policy| {
        const a = std.testing.allocator;
        var client = Fake.Client{};
        var consumer = ReplayConsumer{ .client = &client, .capture = .{ .client = &client }, .take_on_failure = policy[0], .acknowledge_failure = policy[1] };
        defer consumer.capture.deinit();
        var ledger = network.FragmentLedger{};
        var adapter = network.Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd" };
        try std.testing.expectError(error.ConsumerFailure, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &consumer, ReplayConsumer.accept));
        try std.testing.expectEqual(@as(usize, @intFromBool(policy[1])), ledger.acknowledged);
        consumer.fail = false;
        const result = try adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &consumer, ReplayConsumer.accept);
        try std.testing.expectEqual(@as(usize, @intFromBool(policy[1])), result.replayed);
        try std.testing.expectEqual(@as(usize, 6) - @intFromBool(policy[1]) + @intFromBool(policy[0]), consumer.capture.count);
        if (policy[0] and !policy[1]) try std.testing.expectEqual(consumer.capture.held[0].?.ordinal, consumer.capture.held[1].?.ordinal);
    }
}

test "HTTP fragment replay rejects changed coded prefix before any new delivery" {
    const a = std.testing.allocator;
    var input = @import("source.zig").Source{ .allocator = a, .identity = "baseline", .storage = .{ .borrowed = original } };
    var reader = try @import("mp4.zig").Reader.init(a, &input, .{});
    defer reader.deinit();
    const changed = try a.dupe(u8, original[init_end..]);
    defer a.free(changed);
    changed[@as(usize, @intCast(reader.packets[0].offset)) - init_end + 8] ^= 1;
    var client = Fake.Client{ .fail_after_prefix = true };
    var capture = Capture{ .client = &client };
    defer capture.deinit();
    var ledger = network.FragmentLedger{};
    var adapter = network.Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd" };
    try std.testing.expectError(error.ConnectionReset, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept));
    client.fail_after_prefix = false;
    client.body = changed;
    try std.testing.expectError(error.SourceVersionChanged, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept));
    try std.testing.expect(ledger.invalid and !ledger.complete);
    try std.testing.expectEqual(@as(usize, 1), capture.count);
    try std.testing.expectEqual(@as(usize, 1), ledger.acknowledged);
    try std.testing.expectError(error.SourceVersionChanged, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept));
    try std.testing.expectEqual(@as(usize, 2), client.calls);
}

test "HTTP fragment replay fails a shorter clean resource and preserves incomplete transport retries" {
    const a = std.testing.allocator;
    var client = Fake.Client{ .fail_after_bytes = 20811 - init_end + 8 };
    var capture = Capture{ .client = &client };
    defer capture.deinit();
    var ledger = network.FragmentLedger{};
    var adapter = network.Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd" };
    try std.testing.expectError(error.ConnectionReset, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept));
    try std.testing.expectEqual(@as(usize, 2), ledger.acknowledged);
    client.fail_after_bytes = 11206 - init_end + 8;
    try std.testing.expectError(error.ConnectionReset, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept));
    try std.testing.expect(!ledger.invalid);
    try std.testing.expectEqual(@as(usize, 2), capture.count);
    client.fail_after_bytes = null;
    client.body = original[init_end..11206];
    try std.testing.expectError(error.SourceVersionChanged, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept));
    try std.testing.expect(ledger.invalid);
    try std.testing.expectEqual(@as(usize, 2), capture.count);
}

test "HTTP fragment replay pre-entry admission does not bind or spend attempts and retry limits persist" {
    const a = std.testing.allocator;
    var pool = admission.Pool{ .limits = .{ .host_bytes = 1 } };
    var client = Fake.Client{ .fail_after_prefix = true };
    var capture = Capture{ .client = &client };
    defer capture.deinit();
    var ledger = network.FragmentLedger{};
    var adapter = network.Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd", .pool = &pool };
    try std.testing.expectError(error.SharedAdmissionExceeded, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{ .max_attempts = 1 }, &capture, Capture.accept));
    try std.testing.expect(ledger.binding == null);
    try std.testing.expectEqual(@as(usize, 0), ledger.attempts);
    try std.testing.expectEqual(@as(usize, 0), client.calls);
    try std.testing.expectEqual(@as(usize, 0), pool.snapshot().host_bytes);
    adapter.pool = null;
    try std.testing.expectError(error.ConnectionReset, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{ .max_attempts = 1 }, &capture, Capture.accept));
    try std.testing.expectError(error.ResourceLimitExceeded, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{ .max_attempts = 1 }, &capture, Capture.accept));
    try std.testing.expectEqual(@as(usize, 1), client.calls);
}

fn replayAllocationRun(a: std.mem.Allocator) !void {
    var client = Fake.Client{ .fail_after_prefix = true };
    var capture = Capture{ .client = &client };
    defer capture.deinit();
    var ledger = network.FragmentLedger{};
    var adapter = network.Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd" };
    _ = adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept) catch |err| {
        if (err != error.ConnectionReset) return err;
    };
    client.fail_after_prefix = false;
    _ = try adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept);
    try std.testing.expect(!ledger.active);
}
test "HTTP fragment replay allocation failures release rebuilt snapshots prefix leases and taken owners" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, replayAllocationRun, .{});
}

test "HTTP fragment replay binds initialization epoch and byte-range before requesting replacements" {
    inline for (.{ "init", "epoch", "range" }) |change| {
        const a = std.testing.allocator;
        var client = Fake.Client{ .fail_after_prefix = true };
        var capture = Capture{ .client = &client };
        defer capture.deinit();
        var ledger = network.FragmentLedger{};
        var adapter = network.Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd" };
        try std.testing.expectError(error.ConnectionReset, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept));
        var changed_init: [init_end]u8 = original[0..init_end].*;
        changed_init[0] ^= 1;
        const initialization = if (std.mem.eql(u8, change, "init")) &changed_init else original[0..init_end];
        const range: ?@import("hls.zig").Range = if (std.mem.eql(u8, change, "range")) .{ .offset = init_end, .length = original.len - init_end } else null;
        const epoch: usize = if (std.mem.eql(u8, change, "epoch")) 1 else 0;
        try std.testing.expectError(error.SourceVersionChanged, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", range, original.len, initialization, &ledger, .{ .epoch = epoch }, &capture, Capture.accept));
        try std.testing.expectEqual(@as(usize, 1), client.calls);
        try std.testing.expect(ledger.invalid);
    }
}

test "HTTP fragment replay compares native timing even when coded picture bytes are unchanged" {
    const a = std.testing.allocator;
    const changed = try a.dupe(u8, original[init_end..]);
    defer a.free(changed);
    const tfdt = std.mem.indexOf(u8, changed, "tfdt").?;
    const size: usize = if (changed[tfdt + 4] == 1) 16 else 12;
    changed[tfdt + size - 1] ^= 1;
    var client = Fake.Client{ .fail_after_prefix = true };
    var capture = Capture{ .client = &client };
    defer capture.deinit();
    var ledger = network.FragmentLedger{};
    var adapter = network.Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd" };
    try std.testing.expectError(error.ConnectionReset, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept));
    client.fail_after_prefix = false;
    client.body = changed;
    try std.testing.expectError(error.SourceVersionChanged, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept));
    try std.testing.expect(ledger.invalid);
    try std.testing.expectEqual(@as(usize, 1), capture.count);
}

test "HTTP fragment replay cancellation preserves acknowledgement and resumes without repeated callbacks" {
    const Cancel = struct {
        fn check(raw: ?*const anyopaque) !void {
            if (@as(*const bool, @ptrCast(@alignCast(raw.?))).*) return error.Cancelled;
        }
    };
    var canceled = false;
    var client = Fake.Client{};
    var capture = Capture{ .client = &client, .cancel = &canceled };
    defer capture.deinit();
    var ledger = network.FragmentLedger{};
    var adapter = network.Adapter(Fake){ .allocator = std.testing.allocator, .client = &client, .root_url = "https://example.test/live/list.mpd", .control = .{ .context = &canceled, .check_fn = Cancel.check } };
    try std.testing.expectError(error.Cancelled, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept));
    try std.testing.expectEqual(@as(usize, 1), ledger.acknowledged);
    try std.testing.expectError(error.Cancelled, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept));
    try std.testing.expectEqual(@as(usize, 1), ledger.attempts);
    canceled = false;
    capture.cancel = null;
    const stats = try adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept);
    try std.testing.expectEqual(@as(usize, 1), stats.replayed);
    try std.testing.expectEqual(@as(usize, 6), capture.count);
}

test "HTTP fragment replay bounds published work and rejects recursive use without spending a request" {
    const Consumer = struct {
        adapter: *network.Adapter(Fake),
        ledger: *network.FragmentLedger,
        calls: usize = 0,
        fn accept(raw: *anyopaque, _: *network.FragmentDelivery) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectError(error.InvalidMediaStreamState, self.adapter.streamFmp4Resuming(self.adapter.root_url, "media.m4s", null, original.len, original[0..init_end], self.ledger, .{}, self, accept));
            self.calls += 1;
        }
    };
    var client = Fake.Client{};
    var ledger = network.FragmentLedger{};
    var adapter = network.Adapter(Fake){ .allocator = std.testing.allocator, .client = &client, .root_url = "https://example.test/live/list.mpd" };
    var consumer = Consumer{ .adapter = &adapter, .ledger = &ledger };
    try std.testing.expectError(error.ResourceLimitExceeded, adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, original.len, original[0..init_end], &ledger, .{ .max_fragments = 1 }, &consumer, Consumer.accept));
    try std.testing.expectEqual(@as(usize, 1), ledger.acknowledged);
    try std.testing.expectEqual(@as(usize, 1), client.calls);
    try std.testing.expectEqual(@as(usize, 1), consumer.calls);
    try std.testing.expect(!ledger.active and !ledger.invalid);
}

test "HTTP fragment replay reopens append-only live bodies and publishes only new coded fragments" {
    const a = std.testing.allocator;
    var input = @import("source.zig").Source{ .allocator = a, .identity = "grow-baseline", .storage = .{ .borrowed = original } };
    var baseline = try @import("mp4.zig").Reader.init(a, &input, .{});
    defer baseline.deinit();
    const last = baseline.packets[baseline.packets.len - 1];
    const next: u64 = @intCast(last.media_dts + last.duration);
    const fragment = try a.dupe(u8, original[init_end..11206]);
    defer a.free(fragment);
    const tfdt = std.mem.indexOf(u8, fragment, "tfdt").?;
    if (fragment[tfdt + 4] == 1) {
        std.mem.writeInt(u64, fragment[tfdt + 8 ..][0..8], next, .big);
    } else {
        std.mem.writeInt(u32, fragment[tfdt + 8 ..][0..4], @intCast(next), .big);
    }
    const grown = try std.mem.concat(a, u8, &.{ original[init_end..], fragment });
    defer a.free(grown);
    var client = Fake.Client{};
    var capture = Capture{ .client = &client };
    defer capture.deinit();
    var ledger = network.FragmentLedger{};
    try std.testing.expectError(error.InvalidMediaStreamState, ledger.reopen());
    var adapter = network.Adapter(Fake){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.mpd" };
    _ = try adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, grown.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept);
    try std.testing.expectEqual(@as(usize, 6), capture.count);
    try ledger.reopen();
    const unchanged = try adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, grown.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept);
    try std.testing.expectEqual(@as(usize, 6), unchanged.replayed);
    try std.testing.expectEqual(@as(usize, 0), unchanged.acknowledged);
    try std.testing.expectEqual(@as(usize, 6), capture.count);
    const previous_identity = try a.dupe(u8, capture.held[5].?.identity);
    defer a.free(previous_identity);
    try ledger.reopen();
    client.body = grown;
    const appended = try adapter.streamFmp4Resuming(adapter.root_url, "media.m4s", null, grown.len, original[0..init_end], &ledger, .{}, &capture, Capture.accept);
    try std.testing.expectEqual(@as(usize, 6), appended.replayed);
    try std.testing.expectEqual(@as(usize, 1), appended.acknowledged);
    try std.testing.expectEqual(@as(usize, 7), capture.count);
    try std.testing.expectEqual(@as(usize, 7), ledger.acknowledged);
    try std.testing.expectEqual(@as(usize, 3), ledger.attempts);
    try std.testing.expectEqual(@as(i64, @intCast(next)), capture.held[6].?.reader.packets[0].media_dts);
    try std.testing.expect(!std.mem.eql(u8, previous_identity, capture.held[6].?.identity));
    var old_packet = try baseline.readPacket(0);
    defer old_packet.deinit();
    var new_packet = try capture.held[6].?.reader.readPacket(0);
    defer new_packet.deinit();
    try std.testing.expectEqualSlices(u8, old_packet.bytes, new_packet.bytes);
}
