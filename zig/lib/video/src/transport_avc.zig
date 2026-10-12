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

//! Native TS AVC packet adapter. PES clocks remain in the 90 kHz track domain.
const std = @import("std");
const media = @import("antfly_media");
const h264 = @import("h264.zig");
pub const Options = struct {
    transport: media.transport_stream.Limits = .{},
    stream: media.stream.Options = .{},
    index: media.mp4.Limits = .{},
    /// Bootstrap a continuation segment whose payload omits parameter sets.
    configuration: ?[]const u8 = null,
    last_duration_ticks: ?u32 = null,
};
pub const Reader = struct {
    snapshot: *media.stream.Snapshot,
    reader: media.mp4.Reader,
    next_state: media.transport_stream.State,
    pub fn deinit(self: *@This()) void {
        self.reader.deinit();
        self.snapshot.deinit();
        self.* = undefined;
    }
    pub fn init(a: std.mem.Allocator, identity: []const u8, bytes: []const u8, state: media.transport_stream.State, options: Options) !Reader {
        var demux = try media.transport_stream.demux(a, bytes, state, options.transport, options.stream.control, options.stream.admission_pool);
        defer demux.deinit();
        return fromDemux(a, identity, demux, options);
    }
};
const Nals = struct {
    bytes: []const u8,
    cursor: usize = 0,
    fn start(bytes: []const u8, offset: usize) ?struct { at: usize, payload: usize } {
        var i = offset;
        while (i + 3 <= bytes.len) : (i += 1) {
            if (bytes[i] != 0 or bytes[i + 1] != 0) continue;
            if (bytes[i + 2] == 1) return .{ .at = i, .payload = i + 3 };
            if (i + 4 <= bytes.len and bytes[i + 2] == 0 and bytes[i + 3] == 1) return .{ .at = i, .payload = i + 4 };
        }
        return null;
    }
    fn next(self: *Nals) !?[]const u8 {
        if (self.cursor == self.bytes.len) return null;
        const first = start(self.bytes, self.cursor) orelse return error.MalformedAnnexB;
        for (self.bytes[self.cursor..first.at]) |value| if (value != 0) return error.MalformedAnnexB;
        const following = start(self.bytes, first.payload);
        var end = if (following) |value| value.at else self.bytes.len;
        while (end > first.payload and self.bytes[end - 1] == 0) end -= 1;
        if (end == first.payload or self.bytes[first.payload] & 128 != 0) return error.MalformedAnnexB;
        self.cursor = if (following) |value| value.at else self.bytes.len;
        return self.bytes[first.payload..end];
    }
};
fn configuration(a: std.mem.Allocator, demux: media.transport_stream.Demux, options: Options) ![]u8 {
    var sps: ?[]const u8 = null;
    var pps: ?[]const u8 = null;
    for (demux.packets) |packet| {
        try options.stream.control.check();
        var nals = Nals{ .bytes = demux.bytes[packet.offset..][0..packet.size] };
        while (try nals.next()) |nal| {
            try options.stream.control.check();
            if (nal[0] & 31 == 7 and sps == null) sps = nal;
            if (nal[0] & 31 == 8 and pps == null) pps = nal;
            if (nal[0] & 31 == 1 or nal[0] & 31 == 5) break;
        }
        if (sps != null and pps != null) break;
    }
    if (sps == null or pps == null) return a.dupe(u8, options.configuration orelse return error.MissingVideoParameterSet);
    if (sps.?.len < 4 or sps.?.len > 65535 or pps.?.len > 65535) return error.MalformedVideoConfig;
    const out = try a.alloc(u8, 11 + sps.?.len + pps.?.len);
    out[0..6].* = .{ 1, sps.?[1], sps.?[2], sps.?[3], 0xff, 0xe1 };
    std.mem.writeInt(u16, out[6..8], @intCast(sps.?.len), .big);
    @memcpy(out[8..][0..sps.?.len], sps.?);
    var offset = 8 + sps.?.len;
    out[offset] = 1;
    offset += 1;
    std.mem.writeInt(u16, out[offset..][0..2], @intCast(pps.?.len), .big);
    @memcpy(out[offset + 2 ..], pps.?);
    return out;
}
pub fn fromDemux(a: std.mem.Allocator, identity: []const u8, demux: media.transport_stream.Demux, options: Options) !Reader {
    if (demux.packets.len == 0 or demux.packets.len > options.index.max_samples) return error.ResourceLimitExceeded;
    try options.stream.control.check();
    if (options.configuration) |bytes| if (bytes.len > options.stream.max_bytes) return error.ResourceLimitExceeded;
    // Two u16-length parameter sets plus AVCC overhead. Config parsing copies
    // RBSP bytes and may retain an FMO map: at least one coded bit per map entry
    // bounds that map by eight times the parameter bytes.
    const config_bound = @max(@min(@as(usize, 131081), try std.math.add(usize, demux.bytes.len, 11)), if (options.configuration) |bytes| bytes.len else 0);
    var config_reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.mul(usize, config_bound, 12) }) else media.admission.Token{};
    defer config_reservation.deinit();
    const avcc = try configuration(a, demux, options);
    defer a.free(avcc);
    const config = try h264.configParse(a, avcc);
    defer config.groups.deinit(a);
    if (config.width > options.index.max_dimension or config.height > options.index.max_dimension) return error.ResourceLimitExceeded;
    const normalized_bound = @min(options.stream.max_bytes, try std.math.add(usize, avcc.len, try std.math.add(usize, demux.bytes.len, demux.bytes.len / 4)));
    const packet_bytes = try std.math.mul(usize, demux.packets.len, @sizeOf(media.mp4.Packet));
    // Normalization capacity is bounded by actual input, not a blanket stream
    // maximum. Snapshot storage has its own reservation during copy overlap.
    var reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.add(usize, try std.math.add(usize, try std.math.mul(usize, normalized_bound, 2), 64), packet_bytes) }) else media.admission.Token{};
    errdefer reservation.deinit();
    const packets = try a.alloc(media.mp4.Packet, demux.packets.len);
    errdefer a.free(packets);
    var normalized: std.ArrayList(u8) = .empty;
    var normalized_live = true;
    defer if (normalized_live) normalized.deinit(a);
    try normalized.appendSlice(a, avcc);
    for (demux.packets, 0..) |packet, i| {
        try options.stream.control.check();
        const offset = normalized.items.len;
        var nals = Nals{ .bytes = demux.bytes[packet.offset..][0..packet.size] };
        var sync = false;
        var auds: usize = 0;
        var count: usize = 0;
        while (try nals.next()) |nal| {
            try options.stream.control.check();
            count += 1;
            if (count > 4096) return error.ResourceLimitExceeded;
            if (nal[0] & 31 == 9) auds += 1;
            // More than one independently delimited picture needs additional
            // timestamp mapping; do not assign a PES clock to every picture.
            if (auds > 1) return error.AmbiguousTransportPictureTiming;
            if (nal[0] & 31 == 5) sync = true;
            if (nal.len + 4 > options.stream.max_bytes -| normalized.items.len) return error.ResourceLimitExceeded;
            var prefix: [4]u8 = undefined;
            std.mem.writeInt(u32, &prefix, std.math.cast(u32, nal.len) orelse return error.ResourceLimitExceeded, .big);
            try normalized.appendSlice(a, &prefix);
            try normalized.appendSlice(a, nal);
        }
        const size = normalized.items.len - offset;
        if (size == 0 or size > options.index.max_packet_bytes) return error.ResourceLimitExceeded;
        const duration: u32 = if (i + 1 < demux.packets.len)
            std.math.cast(u32, @as(i128, demux.packets[i + 1].dts) - packet.dts) orelse return error.InvalidSamplingMetadata
        else
            options.last_duration_ticks orelse if (i != 0) packets[i - 1].duration else return error.MissingTransportDuration;
        if (duration == 0) return error.InvalidSamplingMetadata;
        packets[i] = .{ .offset = offset, .size = @intCast(size), .dts = packet.dts, .media_dts = packet.dts, .pts = packet.pts, .media_pts = packet.pts, .duration = duration, .sync = sync };
    }
    const snapshot = try media.stream.Snapshot.copy(a, identity, &.{normalized.items}, options.stream);
    errdefer snapshot.deinit();
    var metadata = try snapshot.input.read(0, avcc.len);
    errdefer metadata.deinit();
    normalized.deinit(a);
    normalized_live = false;
    try reservation.resize(.{ .host_bytes = packets.len * @sizeOf(media.mp4.Packet) });
    const track = media.mp4.Track{ .codec = .avc, .inband_parameter_sets = true, .id = 1, .timescale = 90000, .width = @intCast(config.width), .height = @intCast(config.height), .avcc = metadata.bytes, .nal_length_bytes = 4, .display_matrix = .{ 65536, 0, 0, 0, 65536, 0, 0, 0, 1073741824 } };
    return .{ .snapshot = snapshot, .reader = .{ .allocator = a, .input = &snapshot.input, .metadata = metadata, .index_reservation = reservation, .track = track, .packets = packets, .limits = options.index }, .next_state = demux.next_state };
}

/// Request-local state for HLS TS video. Media controls the transport and AES;
/// this adapter supplies codec configuration and the existing packet-reader ABI.
pub const Hls = struct {
    allocator: std.mem.Allocator,
    state: media.transport_stream.State = .{},
    configuration: ?[]u8 = null,
    configuration_reservation: media.admission.Token = .{},
    discontinuity: ?u64 = null,
    ordinal: usize = 0,
    epoch: usize = 0,
    ingested_bytes: u64 = 0,
    pub fn deinit(self: *Hls) void {
        if (self.configuration) |bytes| self.allocator.free(bytes);
        self.configuration_reservation.deinit();
        self.* = undefined;
    }
    pub fn factory(self: *Hls) media.hls_live.TransportFactory {
        return .{ .context = self, .accept = accept };
    }
    fn accept(raw: *anyopaque, identity: []const u8, bytes: []const u8, item: media.hls.Segment, limits: media.live_mp4.Options, control: media.source.Control, pool: ?*media.admission.Pool) anyerror!media.live_mp4.Segment {
        const self: *Hls = @ptrCast(@alignCast(raw));
        try control.check();
        if (self.ordinal >= limits.max_segments or bytes.len > limits.max_ingested_bytes -| self.ingested_bytes) return error.ResourceLimitExceeded;
        const reset = self.discontinuity != null and self.discontinuity.? != item.discontinuity_sequence;
        const epoch = std.math.add(usize, self.epoch, @intFromBool(reset)) catch return error.ResourceLimitExceeded;
        if (epoch > limits.max_reconfigurations) return error.ResourceLimitExceeded;
        var identity_reservation = if (pool) |p| try p.acquire(.{ .host_bytes = identity.len }) else media.admission.Token{};
        errdefer identity_reservation.deinit();
        const owned_identity = try self.allocator.dupe(u8, identity);
        errdefer self.allocator.free(owned_identity);
        var options = Options{ .index = limits.index, .stream = limits.stream, .configuration = if (reset) null else self.configuration };
        options.stream.control = control;
        if (options.stream.admission_pool == null) options.stream.admission_pool = pool;
        options.transport.max_bytes = @min(options.transport.max_bytes, limits.stream.max_bytes);
        // A single PES needs an explicit duration. Multi-PES durations use their
        // decode clock differences; no extra picture timestamps are fabricated.
        var demux = try media.transport_stream.demux(self.allocator, bytes, if (reset) .{} else self.state, options.transport, control, options.stream.admission_pool);
        defer demux.deinit();
        if (demux.packets.len == 1) {
            const ticks = item.duration_seconds * 90000;
            if (!std.math.isFinite(ticks) or ticks < 1 or ticks > std.math.maxInt(u32)) return error.InvalidSamplingMetadata;
            options.last_duration_ticks = @intFromFloat(@round(ticks));
        }
        var source = try fromDemux(self.allocator, owned_identity, demux, options);
        errdefer source.deinit();
        var configuration_reservation = if (pool) |p| try p.acquire(.{ .host_bytes = source.reader.track.avcc.len }) else media.admission.Token{};
        errdefer configuration_reservation.deinit();
        const configuration_copy = try self.allocator.dupe(u8, source.reader.track.avcc);
        errdefer self.allocator.free(configuration_copy);
        if (!reset and self.configuration != null and !std.mem.eql(u8, self.configuration.?, configuration_copy)) return error.SourceVersionChanged;
        try control.check();
        if (self.configuration) |previous| self.allocator.free(previous);
        self.configuration_reservation.deinit();
        self.configuration = configuration_copy;
        self.configuration_reservation = configuration_reservation;
        self.state = source.next_state;
        self.discontinuity = item.discontinuity_sequence;
        self.epoch = epoch;
        self.ingested_bytes += bytes.len;
        const ordinal = self.ordinal;
        self.ordinal += 1;
        return .{ .origin = .normalized_transport, .backing = source.snapshot, .reader = source.reader, .ordinal = ordinal, .epoch = epoch, .identity = owned_identity, .identity_reservation = identity_reservation };
    }
};

test "native TS AVC packet adapter decodes every selected picture against independent FFmpeg" {
    const Oracle = struct {
        packets: []const struct { offset: usize, size: usize, pts: i64, dts: i64, duration: u32, sync: bool, sha256: []const u8 },
        width: u32,
        height: u32,
        time_base: []const u8,
        decoded_sha256: []const []const u8,
    };
    const Capture = struct {
        oracle: Oracle,
        count: usize = 0,
        fn accept(raw: *anyopaque, slot: usize, frame: *const h264.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const packet = self.oracle.packets[slot];
            var rank: usize = 0;
            for (self.oracle.packets, 0..) |other, index| if (other.pts < packet.pts or (other.pts == packet.pts and index < slot)) {
                rank += 1;
            };
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(frame.nv12, &hash, .{});
            try std.testing.expectEqualStrings(self.oracle.decoded_sha256[rank], &std.fmt.bytesToHex(hash, .lower));
            try std.testing.expectEqual(self.oracle.width, frame.width);
            try std.testing.expectEqual(self.oracle.height, frame.height);
            try std.testing.expectEqual(packet.pts, frame.pts);
            self.count += 1;
        }
    };
    const a = std.testing.allocator;
    inline for (.{ "closed", "bframes" }) |name| {
        const oracle = try std.json.parseFromSlice(Oracle, a, @embedFile("../testdata/transport/" ++ name ++ ".json"), .{});
        defer oracle.deinit();
        var source = try Reader.init(a, "transport-qualification", @embedFile("../testdata/transport/" ++ name ++ ".ts"), .{}, .{});
        defer source.deinit();
        const indexes = try a.alloc(usize, source.reader.packets.len);
        defer a.free(indexes);
        for (indexes, 0..) |*index, i| index.* = i;
        const session = try @import("h264_dynamic.zig").Session.init(a, &source.reader, .{});
        defer session.deinit();
        var capture = Capture{ .oracle = oracle.value };
        _ = try session.decodeSelected(indexes, &capture, Capture.accept);
        try std.testing.expectEqual(oracle.value.decoded_sha256.len, capture.count);
    }
}
fn adapterAllocationRun(backing: std.mem.Allocator, pool: *media.admission.Pool) !void {
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    var source = try Reader.init(no_resize.allocator(), "transport-allocation", @embedFile("../testdata/transport/closed.ts"), .{}, .{ .stream = .{ .admission_pool = pool } });
    defer source.deinit();
    var lease = try source.reader.readPacket(0);
    defer lease.deinit();
    try std.testing.expect(lease.bytes.len > 0);
}
test "native TS AVC adapter allocation failures release normalized payload metadata and admission" {
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, adapterAllocationRun, .{&pool});
}

const TestHttp = struct {
    const initial = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:8\n#EXT-X-MEDIA-SEQUENCE:10\n#EXTINF:4,\nclosed.ts\n";
    const changed = initial ++ "#EXT-X-DISCONTINUITY\n#EXTINF:2,\nbframes.ts\n#EXT-X-ENDLIST\n";
    pub const Client = struct {
        manifest: []const u8 = initial,
        calls: usize = 0,
        fail: bool = false,
        pub fn get(self: *@This(), uri: []const u8, _: anytype) !Response {
            self.calls += 1;
            if (std.mem.endsWith(u8, uri, "list.m3u8")) return .{ .body = self.manifest };
            if (self.fail) return error.ConnectionReset;
            if (std.mem.endsWith(u8, uri, "closed.ts")) return .{ .body = @embedFile("../testdata/transport/closed.ts") };
            if (std.mem.endsWith(u8, uri, "bframes.ts")) return .{ .body = @embedFile("../testdata/transport/bframes.ts") };
            return error.UnexpectedTransportResource;
        }
    };
    const Response = struct {
        body: ?[]const u8,
        status: struct { code: u16 } = .{ .code = 200 },
        pub fn header(_: *const @This(), _: []const u8) ?[]const u8 {
            return null;
        }
        pub fn deinit(_: *@This()) void {}
    };
};
test "native TS AVC HLS factory preserves packet leases through discontinuity and transport retry" {
    const a = std.testing.allocator;
    var codec = Hls{ .allocator = a };
    defer codec.deinit();
    var client = TestHttp.Client{ .fail = true };
    var transport = media.http_stream.Adapter(TestHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8" };
    var reader = try media.hls_live.Reader(TestHttp).init(a, &transport, transport.root_url, .{ .transport_stream = codec.factory() });
    defer reader.deinit();
    try reader.reload();
    try std.testing.expectError(error.ConnectionReset, reader.next());
    try std.testing.expectEqual(@as(usize, 0), codec.ordinal);
    client.fail = false;
    var first = (try reader.next()).?;
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 0), first.epoch);
    try std.testing.expect(first.origin == .normalized_transport);
    var held = try first.reader.readPacket(0);
    defer held.deinit();
    const original = try a.dupe(u8, held.bytes);
    defer a.free(original);
    client.manifest = TestHttp.changed;
    try reader.reload();
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), second.epoch);
    try std.testing.expectEqualSlices(u8, original, held.bytes);
    try std.testing.expect(reader.ended());
    try std.testing.expectEqual(@as(usize, 2), codec.ordinal);
}
fn hlsAllocationRun(backing: std.mem.Allocator, pool: *media.admission.Pool) !void {
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const a = no_resize.allocator();
    var codec = Hls{ .allocator = a };
    defer codec.deinit();
    var client = TestHttp.Client{};
    var transport = media.http_stream.Adapter(TestHttp){ .allocator = a, .client = &client, .root_url = "https://example.test/live/list.m3u8", .pool = pool };
    var reader = try media.hls_live.Reader(TestHttp).init(a, &transport, transport.root_url, .{ .transport_stream = codec.factory() });
    defer reader.deinit();
    try reader.reload();
    var first = (try reader.next()).?;
    defer first.deinit();
    var held = try first.reader.readPacket(0);
    defer held.deinit();
    client.manifest = TestHttp.changed;
    try reader.reload();
    var second = (try reader.next()).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), second.epoch);
}
test "native TS AVC HLS factory allocation failures reclaim both epochs and state" {
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, hlsAllocationRun, .{&pool});
}
